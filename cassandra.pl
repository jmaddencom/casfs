#!/usr/bin/perl -w

use Net::Cassandra;
use Data::Dumper;
use IO::String;
use Fuse;
use strict;
use JSON;
use POSIX qw(ESPIPE EINVAL ENOENT ENOTEMPTY ENOSYS EEXIST EPERM O_RDONLY O_RDWR O_APPEND O_CREAT);
use Time::HiRes qw(gettimeofday);

# Global hash to store column paths so we avoid ->new calls
my %CPS;
my $cassandra = Net::Cassandra->new( hostname => 'HOSTNAME_HERE' );
my $client = $cassandra->client;

# On startup, make sure the root directory is there
my @rootc = getdir("/");
if(!@rootc)
{
  # FIXME: specify mode, ownership
  mkdir("/");
}

my ($mountpoint) = "";
$mountpoint = shift(@ARGV) if @ARGV;
Fuse::main(
  mountpoint => $mountpoint, 
  threaded => 0,
  debug => 1,
  getattr => "main::getattr", 
  getdir  => "main::getdir",
  mkdir   => "main::mkdir",
  rmdir   => "main::rmdir",
  open    => "main::open",
  mknod	  => "main::mknod",
  read    => "main::read",
  utime   => "main::utime",
  flush   => "main::flush",
  symlink => "main::symlink",
  unlink  => "main::unlink",
  write   => "main::write",
  chown   => "main::chown",
  chmod   => "main::chmod",
  truncate => "main::truncate",
  statfs  => "main::statfs",
);

sub statfs
{	
  # FIXME: Make these values configurable or something.  How we determine
  # size and blocks free is pretty arbitrary though.
  return 255,1000000,500000,1000000,500000,4096;
}

sub truncate
{
  my($file, $offset) = @_;
  
  print "**** truncate($file, $offset) called\n";

  # The easy case: zero out the file size and delete its blocks  
  # FIXME: Check for permissions
  if($offset == 0)
  {
    # Get the size so we know how many blocks to delete; delete blocks
    my $hash = get($file, "size");
    my $size = $hash->{'column'}->{'value'};
    my $blocks = int($size / 4096);
    $blocks++ if $size % 4096;
    for my $b(0 .. $blocks)
    {
      del("BLOCKS:/$file/$b");
    }

    add($file, "size", 0);
    add($file, "mtime", time);
    return 0;
  }
  else
  {
    # FIXME: Not yet implemented
    return -ESPIPE();
  }
  return 0;
}

sub chmod
{
  my ($file, $mode) = @_;
  print STDERR "**** chmod($file, $mode) called\n";
  
  add($file, "mode", $mode);
  return 0;
}

sub chown
{
  my($file, $uid, $gid) = @_;
  print STDERR "*** chown($file, $uid, $gid) called\n";
  
  add($file, "uid", $uid) if $uid >= 0;
  add($file, "gid", $gid) if $gid >= 0;
  return 0;
}

sub unlink
{
  my($file) = @_;
  
  # FIXME: We delete the file when really we should be dealing with decrementing link counts
  # FIXME: Do exists and perm checks first

  # Use truncate to remove its blocks, then delete it.
  &truncate($file, 0);
  del($file);

  # Look up basedir, re-write its children
  my $basedir = getbasedir($file);
  my $hash = get($basedir, "content");
  my $json = $hash->{'column'}->{'value'};
  my $arr = from_json($json);
  my @children = @{$arr};
  my @newchildren;
  foreach(@children)
  {
    if($_ ne $file)
    {
      push @newchildren, $_;
    }
  }
  $json = to_json(\@newchildren);
  add($basedir, "content", $json);
  
  return 0;
}

sub symlink
{
  my($file, $link) = @_;
  
  # New file type here: 'l' where content is the location of the destination file 
  # FIXME: Document this
  # FIXME: Set all fields properly (mode, uid/gid, size?, times)
  add($link, "size", "4096");
  add($link, "type", "l");
  add($link, "mode", "16877");  # bad
  add($link, "content", $file);
}

# FIXME: This should cause us to do more periodic flushing rather than behaving like O_SYNC.
# For now, we should be able to safely update mtime here.
sub flush
{
  my($file) = @_;
  add($file, "mtime", time);
  return 0;  
}

# Update access and modification times (Known as setattr in fuse)
sub utime
{
  my($file, $atime, $mtime) = @_;
  print STDERR "**** utime($file, $atime, $mtime) called\n";
  
  add($file, "atime", $atime);
  add($file, "mtime", $mtime);
  
  # FIXME: Return proper errno
  return 0;
}

sub write
{
  my($file, $buffer, $offset) = @_;
  
  print STDERR "**** write called: $file, (buffer), $offset\n";
my $st_t = Time::HiRes::time;        
  # FIXME: Make sure file exists

  my $hash = get($file, "size");
  my $size = $hash->{'column'}->{'value'};

  my $oldsize = $size;
  if($offset > $size)
  {
    print STDERR "offset requested is $offset but the file is only $size bytes\n";
    return -EINVAL();
  }
  elsif($offset < $size)
  {
    print STDERR "$offset < $size so we're overwriting\n";
    $size = $size - $offset + length($buffer);
  }
  else
  {
    print STDERR "offset == size, we're appending ", length($buffer), " bytes\n";
    $size = $size + length($buffer);
  }

  # From $offset, we determine which block we start writing into.  
  # $inner_offset is the position inside the first block where we start 
  # writing.  
  # FIXME: Special case: If offset is < 4096 and not 0, can we do something
  # tricky?  If we don't, small writes cause the filesystem to hang.
  my $block = int($offset / 4096);
  my $inner_offset = $offset % 4096;

  # Determine the length of the first read.  Typically 4096, but if we have
  # an $inner_offset or if length is less than 4096, it'll change.
  my $length = length($buffer);
  my $current_length = $length;
  $current_length = 4096 if $length > 4096;
  
  # Open the buffer as a file.  As we read from it, we'll write data to blocks
  my $fhr =  IO::String->new($buffer);

  # Read from $buffer into $data as long as there is data left to read. 
  # $position is our pointer in the buffer.
  my $position = 0;
  while($position < $length)
  {
    # Temporary buffer used throughout
    my $data;
    
    if($inner_offset)
    {
      # Special case: partial block overwrite so we have to retrieve the old 
      # block first.
      my $blockdata = &read($file, "4096", $offset);
      my $fhw = IO::String->new($blockdata);
      
      # Seek to the offset location
      $fhw->seek($inner_offset, 0);
      
      # Adjust the length we're reading from $buffer downward to compensate 
      # for the $inner_offset.
      $current_length -= $inner_offset;
      
      # Read from the buffer, write to $oldblock
      read($fhr, $data, $current_length);
      $fhw->print($data);
      close $fhw;
      
      # Store the updated block
      add("BLOCKS:/$file/$block", "content", $blockdata);
      
      # Update $position within the buffer to account for the data just written
      $position += $current_length;
    }
    else
    {
      # No offset, but if the amount of data left to write is less than 4096, 
      # we're still writing a partial block so we have to read the existing
      # block first.
      if($length - $position < 4096)
      {
        $current_length = $length - $position;
        my $blockdata = &read($file, "4096", $offset);
        my $fhw = IO::String->new($blockdata);

        # Read from the buffer, write to $oldblock
        read($fhr, $data, $current_length);
        $fhw->print($data);
        close $fhw;

        # Store the updated block
        add("BLOCKS:/$file/$block", "content", $blockdata);
      }
      else
      {
        # Standard 4k read from $buffer
        $current_length = 4096;
        read($fhr, $data, $current_length);

        # Store the block
        add("BLOCKS:/$file/$block", "content", $data);
      }
      
      # Update position
      $position += $current_length;
    }
    
    # If we loop, we'll be writing to the next block, so increment that now.
    $block++;
  }
  
  # Update the file's size if needed.
  add($file, "size", $size) if $size != $oldsize;
print STDERR "***** write took ", Time::HiRes::time - $st_t, "\n";

  return length($buffer);
}

sub read
{
my $wst_t = Time::HiRes::time;
  my($file, $length, $offset) = @_;

  # FIXME: Make sure file exists
  print STDERR "**** read called: $file, $length, $offset\n";


  # Fail if the offset is past the end of the file
my $st_t = Time::HiRes::time;
  my $hash = get($file, "size");
  my $size = $hash->{'column'}->{'value'};
  $size = 0 if !$size;
  return -EINVAL() if $offset > $size;
print STDERR "***** size/einval check took: ", Time::HiRes::time - $st_t, "\n";

  # Return 0 (int, not string) if offset is equal to size to send EOF
  return 0 if $offset == $size;
  
  # We determine the first block by offset/4096 then retrieve it.  If the 
  # above calculation has a modulus, we have to scan and read within the block.  
  # If length > 4096, we have to retrieve multiple blocks.  
  my $block = int($offset / 4096);

  # Data we're going to return
  my $data;

  # Modulus is the offset into the first block at which we start our read  
  my $inner_offset = $offset % 4096;

  # Establish block count, the number of blocks we're going to multiget.  ++ it
  # if there's a modulus, meaning it spans a partial block in front.
  my $block_count = $length / 4096;
  $block_count++ if $length % 4096;
  my $block_stop = $block + $block_count;

  # Build the list of blocks we're going to get
  my @keys;

  for my $b($block .. $block_stop-1)
  {
    push @keys, "BLOCKS:/$file/$b";
  }
$st_t = Time::HiRes::time;
  my $obj = multiget(\@keys, "content");
print STDERR "***** multiget took: ", Time::HiRes::time - $st_t, "\n";
  my $buffer = "";

  for my $b($block .. $block_stop-1)
  {
    $buffer .= $obj->{"BLOCKS:/$file/$b"}->{'column'}->{'value'};
  }

  my $fh = IO::String->new($buffer);
  $fh->seek($inner_offset, 0);
  read($fh, $data, $length);
  close $fh;

print STDERR "***** whole process took: ", Time::HiRes::time - $wst_t, "\n";
  return $data;
}

sub mknod
{
  my($file, $modes, $dev) = @_;
  
  print STDERR "**** mknod($file, $modes, $dev) called\n";
  
  # Create the file
  # FIXME: Make sure the file doesn't yet exist
  # FIXME: Check permissions to make sure we can add the file
  # FIXME: add ownership, permissions to the file
  add($file, "type", "f");
  add($file, "mode", $modes);
  add($file, "size", "0");
  add($file, "ctime", time);

  # Link it to its parent
  my $basedir = getbasedir($file);
  my $hash = get($basedir, "content");
  my $json = $hash->{'column'}->{'value'};
  my $arr = from_json($json);
  my @children = @{$arr};
  push @children, $file;
  $json = to_json(\@children);
  add($basedir, "content", $json);
  return 0;
}

sub getbasedir
{
  my($file) = @_;

  # determine basedir
  $file =~ /(.*\/).+$/;
  my $basedir = $1;  
  $basedir =~ s/\/$//g if length($basedir) > 1;  
  return $basedir;
}

sub open
{
  my($file, $flags) = @_;
  
  print STDERR "**** Open called: $file, $flags\n";
  
  # FIXME: perms need to be checked here
  
  
  return 0;
}

sub rmdir
{
  my($dir) = @_;
  
  print STDERR "**** rmdir($dir) called\n";
  
  # Return error if the directory is not empty
  my $hash = get($dir, "content");
  my $json = $hash->{'column'}->{'value'};
  my $arr = from_json($json);
  my $isnotempty = 0;
  foreach(@${arr})
  {
    next if $_ eq "." || $_ eq "..";
    $isnotempty = 1;
  }
  return -ENOTEMPTY() if $isnotempty;
  
  # determine basedir
  $dir =~ /(.*\/).+$/;
  my $basedir = $1;  
  $basedir =~ s/\/$//g if length($basedir) > 1;
  
  # Look up basedir, re-write its children
  $hash = get($basedir, "content");
  $json = $hash->{'column'}->{'value'};
  $arr = from_json($json);
  my @children = @{$arr};
  my @newchildren;
  foreach(@children)
  {
    if($_ ne $dir)
    {
      push @newchildren, $_;
    }
  }
  $json = to_json(\@newchildren);
print STDERR "Sending $json as children for basedir $basedir\n";
  add($basedir, "content", $json);
  
  # Remove the directory
  del($dir);
  
  # FIXME: Return errno
  
}

sub getattr
{
  my($file) = @_;
  
  print STDERR "**** getattr($file) called\n";
  
  my $hash = get($file, "size");
  if($hash->{'result'} eq "FAIL")
  {
    return -ENOENT();
  }
  
  # Do a get_slice for perf
  my $obj = get_slice($file, ('size', 'mode', 'atime', 'mtime', 'ctime', 'uid', 'gid'));
  my %hash;
  foreach(@{$obj})
  {
    my $key = $_->column->name;
    my $val = $_->column->value;
    $hash{$key} = $val;
  }
  $hash{'size'} = 4096 if !$hash{'size'};
  $hash{'mode'} = 16877 if !$hash{'mode'};
  $hash{'mtime'} = 1 if !$hash{'mtime'};
  $hash{'atime'} = 1 if !$hash{'atime'};
  $hash{'ctime'} = 1 if !$hash{'ctime'};
  
  my ($dev, $ino, $rdev, $nlink, $blksize) = (0,0,0,2,4096);
  my $blocks = $hash{'size'} / $blksize;
  
  return ($dev,$ino,$hash{'mode'},$nlink,$hash{'uid'},$hash{'gid'},$rdev,$hash{'size'},$hash{'atime'},$hash{'mtime'},$hash{'ctime'},$blksize,$blocks);
}

sub mkdir
{
  my($dir, $modes) = @_;
  
  print STDERR "**** mkdir($dir, $modes) called\n";
  
  # Create the directory.  Specify type d, empty content since we're just
  # creating it.
  # FIXME: Do something with the modes
  # FIXME: populate the rest of the entry
  # FIXME: Make sure it doesn't already exist?  Looks like FUSE already
  # does this for us.
  
  
  # HACK: mkdir() from FUSE sends us the integer representation of the directory
  # (always seems to be 493) and I haven't figured out, after 16 or so hours 
  # of messing with it, how to get that thing into the proper octal, which is actually
  # 16877.  So we hard-code it.  Ugh.
  
  $modes = 16877;

  add($dir, "type", "d");
  add($dir, "size", "4096");
  add($dir, "mode", $modes);
  add($dir, "atime", time);
  add($dir, "mtime", time);
  add($dir, "ctime", time);
  my $dirs = to_json([ '.', '..' ]);
  add($dir, "content", "$dirs");
  
  # Link this directory to its parent by finding it, re-writing it.
  my $basedir = getbasedir($dir);
print STDERR "Looking up $basedir to mod its contents\n";
  my $hash = get($basedir, "content");
  my $json = $hash->{'column'}->{'value'};
  my $arr = from_json($json);
  my @children = @{$arr};
  push @children, $dir;
  $json = to_json(\@children);
  add($basedir, "content", $json);
  
# This part succeeds but maybe the $dir row isn't created?
  
  # FIXME: proper return codes
  return 0;
}

# Return the contents of the requested directory
sub getdir
{
  my($parentdir) = @_;

  print STDERR "**** getdir($parentdir) called\n";
  
  # Get this entry
  # FIXME: Do a get first to ensure it's a directory
  my $hashref = get($parentdir, "content");
  
  # Return error if the get failed
  if($hashref->{'result'} eq "FAIL")
  {
    return -ENOENT();
  }
  # dir children are stored in content in a json string.
  my $json = $hashref->{'column'}->{'value'};
  my $children = from_json($json);
  my @children = @{$children};
  
  # Strip the basename off of the filenames
  foreach(@children)
  {
    $_ =~ s/.*\/(.+)$/$1/;
  }
  push @children, "0";

  return @children;
}

sub del
{
  my($key) = @_;
  
  print STDERR "****** deleting key $key\n";
  
  my $timestamp = timestamp();
  my $cpath = Net::Cassandra::Backend::ColumnPath->new(
        { column_family => 'Standard1'}
    );

  eval {
    $client->remove('Keyspace1',$key,$cpath,$timestamp, 
      Net::Cassandra::Backend::ConsistencyLevel::QUORUM
    );
  };
  
  if($@)
  {
    print STDERR "****** del() of key:$key returned $@\n";
  }
}

sub timestamp
{
  my ($sec, $usec) = gettimeofday; 
  $sec = $sec*1000; # sec -> ms
  $usec = int($usec / 1000); # usec -> ms
  my $time = $sec + $usec;
  return $time;
}

sub add
{
  my($key, $column, $content) = @_;
  
  my $timestamp = timestamp();
  
  my $cp = $CPS{$column};
  if(!$cp)
  {
    $cp =  Net::Cassandra::Backend::ColumnPath->new(
      { column_family => 'Standard1', column => $column }
    );
    $CPS{$column} = $cp;
  }
  
  eval {
    $client->insert(
      'Keyspace1',
      $key,
      $cp,
      $content,
      $timestamp,
      Net::Cassandra::Backend::ConsistencyLevel::ONE
    );
  };
  if($@)
  {
    print STDERR "****** insert() of key:$key|column:$column|content:$content returned $@\n";
  }
  else
  {
    get($key, $column);
  }
  # FIXME: Better debugging...
}

sub get 
{	
  my ($key, $column) = @_;
  my $obj;
  eval {
    $obj = $client->get('Keyspace1',
      $key,
      Net::Cassandra::Backend::ColumnPath->new(
        { column_family => 'Standard1', column => $column}
      ),
     Net::Cassandra::Backend::ConsistencyLevel::ONE
    );
  };
  if($@)
  {
    print STDERR "****** get() of key:$key|column:$column returned $@\n";
    $obj->{'result'} = "FAIL";
  }
  else
  {
    $obj->{'result'} = "OK";
  }
  return $obj;
}

sub multiget
{
  my($keys, $column) = @_;
  my $obj;
  eval {  
    $obj = $client->multiget('Keyspace1',
      $keys,
      Net::Cassandra::Backend::ColumnPath->new(
        { column_family => 'Standard1', column => $column }
      ),
      Net::Cassandra::Backend::ConsistencyLevel::ONE
    );
  };
  if($@)
  {
    print STDERR "****** $@\n";
  }
  return $obj;
}

sub get_slice
{
  my($key, @columns) = @_;
  my $obj;
  eval {  
    $obj = $client->get_slice('Keyspace1',
      $key,
      Net::Cassandra::Backend::ColumnParent->new(
        { column_family => 'Standard1' } 
      ),
      Net::Cassandra::Backend::SlicePredicate->new(
        { column_names => \@columns }
      ), 
      Net::Cassandra::Backend::ConsistencyLevel::QUORUM
    );
  };
  if($@)
  {
    print STDERR "****** $@\n";
  }

  return $obj;
}

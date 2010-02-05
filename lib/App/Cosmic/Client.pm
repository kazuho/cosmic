package App::Cosmic::Client;

use strict;
use warnings;

use Errno ();
use IO::Handle;
use IPC::Open2;
use JSON qw(from_json to_json);

use App::Cosmic;
use base qw(App::Cosmic);

use constant DEV_LOCK_FILE => CLIENT_CONF_DIR . '/dev.lock';
use constant DEV_MAP_FILE  => CLIENT_CONF_DIR . '/dev.map';

__PACKAGE__->mk_accessors(qw(def def_file device_file));

# FIXME support mounting multiple volumes from single host:port, this is
# a limitation in ndb protocol

sub new {
    my $klass = shift;
    my $self = bless {}, $klass;
    
    # setup
    die "invalid args, see --help"
        unless @ARGV == 2;
    $self->def_file(shift @ARGV);
    $self->device_file(shift @ARGV);
    $self->def(from_json(do {
        open my $fh, '<', "@{[CLIENT_CONF_DIR]}/@{[$self->def_file]}"
            or die "no definition for:@{[$self->def_file]}:$!";
        join '', <$fh>;
    }));
    die "no `uuid' in definition"
        unless $self->def->{global_name};
    die "no `nodes' array in definition"
        unless $self->def->{nodes} && ref $self->def->{nodes} eq 'ARRAY';
    
    $self;
}

sub connect {
    __PACKAGE__->new->_connect;
}

sub _connect {
    my $self = shift;
    
    print "registering my IP address to disk servers...\n";
    $self->_change_client();
    
    print "mounting disks...\n";
    $self->_mount;
    
    print "setting up RAID array...\n";
    $self->_setup_raid;
}

sub disconnect {
    __PACKAGE__->new->_disconnect;
}

sub _disconnect {
    # TODO
}

sub _change_client {
    my ($self, $new_addr) = @_;
    $new_addr ||= '';
    
    # update all nodes to new_addr using 2pc
    my %nodes = map {
        $_ => {}, # pid: pid, out: output to remote, in: input from remote
    } @{$self->def->{nodes}};
    
    # spawn and check
    for my $node (sort keys %nodes) {
        $nodes{$node}->{pid} = open2(
            $nodes{$node}->{in},
            $nodes{$node}->{out},
            join(
                ' ',
                "ssh root\@$node exec cosmic srv-change-client",
                $self->def->{global_name},
                (defined $new_addr ? ($new_addr) : ()),
                '2>&1',
            ),
        ) or die "open3 failed:$!";
        my $line = $nodes{$node}->{in}->getline;
        chomp $line;
        unless ($line =~ /^ok phase 1$/) {
            die join(
                '',
                "$node rejected update:\n",
                $line,
                $nodes{$node}->{in}->getlines,
            );
        }
    }
    
    # request reset
    for my $node (sort keys %nodes) {
        $nodes{$node}->{out}->print("prepare\n");
        my $line = $nodes{$node}->{in}->getline;
        chomp $line;
        unless ($line =~ /^ok phase 2$/) {
            die join(
                '',
                "$node rejected update:\n",
                $line,
                $nodes{$node}->{in}->getlines,
            );
        }
    }
    
    # commit
    for my $node (sort keys %nodes) {
        $nodes{$node}->{out}->print("commit\n");
        $nodes{$node}->{out}->close;
    }
    
    # check response
    for my $node (sort keys %nodes) {
        my $lines = join '', $nodes{$node}->{in}->getlines;
        die "unexpected response from node:$node:\n$lines"
            if length $lines != 0;
        while (waitpid($nodes{$node}->{pid}, 0) == -1) {}
        die "child process exitted with $? while takling to node:$node"
            if $? != 0;
    }
}

sub _mount {
    my $self = shift;
    my $lock = lock_file(DEV_LOCK_FILE);
    
    # mount disk
    for my $node (sort @{$self->def->{nodes}}) {
        my $dev = sub {
            for my $size_file (glob '/sys/block/nbd*/size') {
                if (read_oneline($size_file) == 0) {
                    $size_file =~ m|^/sys/block/(nbd[0-9]+)/size$|
                        or die "unexpected filename:$size_file";
                    return "/dev/$1";
                }
            }
            die "failed to find an unconnected device file";
        }->();
        _system('nbd-client', $node, NBD_PORT, $dev) == 0
            or die "failed to connect nbd node to:$dev:$?";
        $self->_update_device_map($node, $dev);
    }
}

sub _unmount {
    my $self = shift;
    my $lock = lock_file(DEV_LOCK_FILE);
    
    # update device map, and then load it
    my $dev_map = $self->_update_device_map;
    
    # unmount disk
    for my $node (sort @{$self->def->{nodes}}) {
        if (my $dev = $dev_map->{$node}) {
            _system('nbd-client', '-d', $node) == 0
                or die "failed to disconnect nbd node:$?";
        }
    }
    
    # update device map
    $self->_update_device_map;
}

sub _update_device_map {
    my $self = shift;
    
    # read current map (if exists)
    my $dev_map = do {
        if (open my $fh, '<', DEV_MAP_FILE) {
            from_json(join '', <$fh>);
        } elsif ($! == Errno::ENOENT) {
            +{};
        } else {
            die "failed to open file:@{[DEV_MAP_FILE]}:$!:" . ($! + 0);
        }
    };
    # remove all non-connected devices from map
    for my $node (sort keys %$dev_map) {
        my $size_file = $dev_map->{$node};
        $size_file =~ s|^/dev/(.*)$|/sys/block/$1/size|
            or die "unexpected device file:$size_file";
        if (read_oneline($size_file) == 0) {
            delete $dev_map->{$node};
        }
    }
    # add new nodes
    while (@_) {
        my $node = shift;
        my $dev = shift;
        $dev_map->{$node} = $dev;
    }
    # save
    write_file(DEV_MAP_FILE, to_json($dev_map));
    
    $dev_map;
}

sub _setup_raid {
}

sub _system {
    my @cmd = @_;
    print join(' ', @cmd), "\n";
    system(@cmd);
}


1;

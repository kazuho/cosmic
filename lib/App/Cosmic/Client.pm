package App::Cosmic::Client;

use strict;
use warnings;

use Errno ();
use Getopt::Long;
use IO::Handle;
use IPC::Open2;
use JSON qw(from_json to_json);

use App::Cosmic;
use base qw(App::Cosmic);

use constant DEV_LOCK_FILE    => CLIENT_CONF_DIR . '/dev.lock';
use constant DEV_MAP_FILE     => CLIENT_CONF_DIR . '/dev.map';
use constant DISK_DIR         => CLIENT_CONF_DIR . '/disks';
use constant ISCSID_CONF_FILE => '/etc/iscsi/iscsid.conf';

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
        open my $fh, '<', "@{[DISK_DIR]}/@{[$self->def_file]}"
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
    my $opt_create;
    GetOptions(
        create => \$opt_create,
    ) or exit(1);
    __PACKAGE__->new->_connect(
        create => $opt_create,
    );
}

sub _connect {
    my ($self, %opts) = @_;
    
    print "registering my credentials to block device servers...\n";
    $self->_change_client();
    
    print "mounting disks...\n";
    my $node_devs = $self->_mount;
    
    print "starting RAID array...\n";
    $self->_start_raid(\%opts, $node_devs);
}

sub disconnect {
    __PACKAGE__->new->_disconnect;
}

sub _disconnect {
    my $self = shift;
    
    print "stopping RAID array...\n";
    $self->_stop_raid;
    
    print "unmounting disk...\n";
    $self->_unmount;
}

sub _change_client {
    my $self = shift;
    
    # read user,pass from iscsi.conf
    my @userpass = $self->_read_userpass;
    
    # update all nodes to new_addr using 2pc
    my %nodes = map {
        $_ => {
            # pid => pid,
            # out: output to remote,
            # in: input from remote
            output => '',
        },
    } @{$self->def->{nodes}};
    
    # spawn and check
    for my $node (sort keys %nodes) {
        $nodes{$node}->{pid} = open2(
            $nodes{$node}->{in},
            $nodes{$node}->{out},
            join(
                ' ',
                "ssh root\@$node exec cosmic-server change-credentials",
                $self->def->{global_name},
                @userpass,
                '2>&1',
            ),
        ) or die "open3 failed:$!";
    }
    
    # proceed, step by step
    while (1) {
        my $done = 0;
        for my $node (sort keys %nodes) {
            my $success;
            while (my $line = $nodes{$node}->{in}->getline) {
                $nodes{$node}->{output} .= $line;
                chomp $line;
                if ($line =~ /^cosmic-done$/) {
                    $done++;
                    $success = 1;
                    last;
                } elsif ($line =~ /^cosmic-ok /) {
                    $success = 1;
                    last;
                }
            }
            die "change-credentials failed on node $node:\n"
                . $nodes{$node}->{output}
                    unless $success;
        }
        die "huh?"
            if $done && $done != keys %nodes;
        last
            if $done;
        for my $node (sort keys %nodes) {
            $nodes{$node}->{out}->print("next\n");
        }
    }
}

sub _mount {
    my $self = shift;
    my $lock = lock_file(DEV_LOCK_FILE);
    
    my %r;
    
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
        print "  connecting $node to $dev\n";
        systeml('nbd-client', $node, NBD_PORT, $dev) == 0
            or die "failed to connect nbd node to:$dev:$?";
        $self->_update_device_map($node, $dev);
        $r{$node} = $dev;
    }
    die "no nodes" unless %r;
    
    \%r;
}

sub _unmount {
    my $self = shift;
    my $lock = lock_file(DEV_LOCK_FILE);
    
    # update device map, and then load it
    my $dev_map = $self->_update_device_map;
    
    # unmount disk
    for my $node (sort @{$self->def->{nodes}}) {
        if (my $dev = $dev_map->{$node}) {
            systeml('nbd-client', '-d', $dev) == 0
                or die "failed to disconnect nbd node:$?";
        }
    }
    
    # update device map
    sleep 1;
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

sub _start_raid {
    my ($self, $opts, $node_devs) = @_;
    
    # build command
    my @cmd = qw(mdadm);
    if ($opts->{create}) {
        push(
            @cmd,
            '--create',
            $self->device_file,
            '--level=1',
            '--bitmap=internal',
            '--homehost=nonexistent.example.com',
            '--raid-devices=' . scalar(keys %$node_devs),
        );
    } else {
        push(
            @cmd,
            '--assemble',
            $self->device_file,
        );
    }
    for my $node (sort keys %$node_devs) {
        push @cmd, $node_devs->{$node};
    }
    
    # execute command and read its output
    print join(' ', @cmd), "\n";
    open my $fh, '-|', join(' ', @cmd) . ' 2>&1',
        or die "failed to invoke mdadm:$!";
    my $last_line = '';
    while (my $line = <$fh>) {
        print $line;
        $last_line = $line;
    }
    close $fh;
    die "mdadm failed with exit code:$?"
        unless $? == 0;
    
    # try --re-add if assembly is in degraded mode
    if (! $opts->{create}) {
        if ($last_line =~ /has been started with (\d+) drives? \(out of (\d+)\)\.\s*$/
                && $1 != $2) {
            # read output of mdadm --detail device
            my @lines = do {
                open my $fh, '-|', "mdadm --detail @{[$self->device_file]}"
                    or die "failed to invoke mdadm";
                <$fh>;
            };
            die "mdadm --detail failed with exit code:$?"
                unless $? == 0;
            # build list of devices to re-add
            splice @lines, 0, @lines - scalar(keys %$node_devs);
            my %readd = map { $_ => 1 } values %$node_devs;
            for my $line (@lines) {
                if ($line =~ m{\s+active\s+sync\s+(/dev/nbd\d+)\s*$}) {
                    delete $readd{$1};
                }
            }
            # check, just to make sure
            die "failed to build list of inactive nodes"
                if scalar(keys %readd) == scalar(keys %$node_devs);
            # readd
            systeml(
                qw(mdadm --manage),
                $self->device_file,
                '--re-add',
                sort keys %readd,
            ) == 0
                or warn "mdadm --re-add failed with exit code:$?";
        }
    }
}

sub _stop_raid {
    my $self = shift;
    systeml(
        qw(mdadm --stop), $self->device_file,
    ) == 0
        or die "mdadm failed with exit code:$?";
}

sub _read_userpass {
    my $self = shift;
    my ($user, $pass);
     
    open my $fh, '<', ISCSID_CONF_FILE
        or die "failed to open file:@{[ISCSID_CONF_FILE]}:$!";
    while (my $line = <$fh>) {
        if ($line =~ /^\s*node\.session\.auth\.([^\.]+)\s*=\s*(.*?)\s*/) {
            if ($1 eq 'username') {
                $user = $2;
            } elsif ($1 eq 'password') {
                $pass = $2;
            }
        }
    }
    close $fh;
     
    die "node.session.auth.username not defined in @{[ISCSID_CONF_FILE]}"
        unless $user;
    die "node.session.auth.password not defined in @{[ISCSID_CONF_FILE]}"
        unless $pass;
     
    ($user, $pass);
}

1;

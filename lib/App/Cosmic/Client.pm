package App::Cosmic::Client;

use strict;
use warnings;

use Cwd qw(realpath);
use Errno ();
use Getopt::Long;
use File::Basename qw(dirname);
use IO::Handle;
use IPC::Open2;
use JSON qw(from_json to_json);

use App::Cosmic;
use base qw(App::Cosmic);

use constant DISK_DIR            => CLIENT_CONF_DIR . '/disks';
use constant ISCSID_CONF_FILE    => '/etc/iscsi/iscsid.conf';
use constant ISCSI_MOUNT_TIMEOUT => 60;

__PACKAGE__->mk_accessors(qw(def def_file device_file));

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

sub disconnect {
    __PACKAGE__->new->_disconnect;
}

sub _connect {
    my ($self, %opts) = @_;
    
    print "registering my credentials to block device servers...\n";
    $self->_change_client();
    
    print "mounting disks...\n";
    $self->_mount;
    
    print "starting RAID array...\n";
    $self->_start_raid(\%opts);
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

# mounts all nodes of a block device and returns hash of node => device_file
sub _mount {
    my $self = shift;
    
    for my $node (sort @{$self->def->{nodes}}) {
        systeml(
            qw(iscsiadm --mode=discovery --type=sendtargets),
            "--portal=$node",
        ) == 0
            or die "iscsiadm failed:$?";
        systeml(
            qw(iscsiadm --mode=node),
            "--portal=$node",
            '--target=' . to_iqn($node, $self->def->{global_name}),
            '--login',
        ) == 0
            or die "iscsiadm failed:$?";
        for (my $i = ISCSI_MOUNT_TIMEOUT; ; $i--) {
            last if
                readlink _to_device_file($node, $self->def->{global_name});
            die "failed to locate device file"
                if $i <= 0;
            sleep 1;
        }
    }
}

sub _unmount {
    my $self = shift;
    
    for my $node (sort @{$self->def->{nodes}}) {
        systeml(
            qw(iscsiadm --mode=node),
            '--target=' . to_iqn($node, $self->def->{global_name}),
            "--portal=$node",
            '--logout',
        ) == 0
            or warn "iscsiadm failed:$?";
    }
}

sub _start_raid {
    my ($self, $opts) = @_;
    
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
            '--raid-devices=' . scalar(@{$self->def->{nodes}}),
        );
    } else {
        push(
            @cmd,
            '--assemble',
            $self->device_file,
        );
    }
    for my $node (sort @{$self->def->{nodes}}) {
        push @cmd, _to_device_file($node, $self->def->{global_name});
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
            # build list of active devices
            # build list of devices to re-add
            splice @lines, 0, @lines - scalar @{$self->def->{nodes}};
            my %active;
            for my $line (@lines) {
                if ($line =~ m{\s+active\s+sync\s+(/dev/sd.)\s*$}) {
                    $active{$1} = 1;
                }
            }
            my @readd = map {
                my $path = _to_device_file($_, $self->def->{global_name});
                my $f = readlink $path
                    or die "readlink failed on:$path:$!";
                $f = realpath(dirname($path) . "/$f")
                    if $f !~ m|^/|;
                $active{$f} ? () : ($f)
            } @{$self->def->{nodes}};
            # check, just to make sure
            die "failed to build list of inactive nodes"
                unless @readd;
            # readd
            systeml(
                qw(mdadm --manage),
                $self->device_file,
                '--re-add',
                @readd,
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
        chomp $line;
        if ($line =~ /^\s*node\.session\.auth\.([a-z]+)\s*=\s*(.*?\s|.*$)/i) {
            my ($n, $v) = ($1, $2);
            if ($n =~ /^username$/i) {
                $user = $v;
            } elsif ($n =~ /^password$/i) {
                $pass = $v;
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

sub _to_device_file {
    my ($host, $ident) = @_;
    "/dev/disk/by-path/ip-$host:3260-iscsi-" . to_iqn($host, $ident) . "-lun-0";
}

1;

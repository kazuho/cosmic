package App::Cosmic::Client;

use strict;
use warnings;

use Cwd qw(realpath);
use Errno ();
use File::Basename qw(dirname);
use IO::Handle;
use IPC::Open2;
use JSON qw(from_json to_json);

use App::Cosmic;
use base qw(App::Cosmic);

use constant DISK_DIR            => CLIENT_CONF_DIR . '/disks';
use constant ISCSID_CONF_FILE    => '/etc/iscsi/iscsid.conf';
use constant ISCSI_MOUNT_TIMEOUT => 60;

__PACKAGE__->mk_accessors(qw(def global_name device));

sub new {
    my ($klass, $global_name, $device) = @_;
    bless {
        global_name => $global_name,
        device => $device,
    }, $klass;
}

sub create {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV >= 5;
    my ($global_name, $device, $size, @nodes) = @ARGV;
    validate_global_name($global_name);
    __PACKAGE__->new($global_name, $device)->_create($size, @nodes);
}

sub remove {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV == 1;
    my ($global_name) = @ARGV;
    validate_global_name($global_name);
    __PACKAGE__->new($global_name, undef)->_load->_remove();
}

sub connect {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV == 2;
    my ($global_name, $device) = @ARGV;
    validate_global_name($global_name);
    __PACKAGE__->new($global_name, $device)->_load->_connect;
}

sub disconnect {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV == 2;
    my ($global_name, $device) = @ARGV;
    validate_global_name($global_name);
    __PACKAGE__->new($global_name, $device)->_load->_disconnect;
}

sub _create {
    my ($self, $size, @nodes) = @_;
    
    # setup def
    $self->def({
        nodes => [ @nodes ],
    });
    # create block devices
    print "creating block device on servers...\n";
    $self->_sync_run(
        "create @{[$self->global_name]} $size",
    );
    # commit def to disk
    $self->_save;
    
    # connect
    $self->_connect(1);
}

sub _remove {
    my $self = shift;
    
    print "removing block device from servers...\n";
    $self->_sync_run(
        "remove @{[$self->global_name]}",
    );
    # unlink def from disk
    sync_unlink("@{[DISK_DIR]}/@{[$self->global_name]}")
        or die "failed to remove file:@{[DISK_DIR]}/@{[$self->global_name]}:$!";
}

sub _connect {
    my ($self, $do_create) = @_;
    
    print "registering my credentials to block device servers...\n";
    $self->_change_client;
    
    print "mounting disks...\n";
    $self->_mount;
    
    print "starting RAID array...\n";
    $self->_start_raid($do_create);
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
    my ($user, $pass) = $self->_read_userpass;
    
    $self->_sync_run(
        "change-credentials @{[$self->global_name]} $user $pass",
    );
}

sub _sync_run {
    my ($self, $cmd) = @_;
    
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
            "ssh root\@$node exec cosmic-server $cmd 2>&1",
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
            die "command failed on node $node:\n"
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
    
    for my $node (sort @{$self->def->{nodes}}) {
        systeml(
            qw(iscsiadm --mode=discovery --type=sendtargets),
            "--portal=$node",
        ) == 0
            or die "iscsiadm failed:$?";
        systeml(
            qw(iscsiadm --mode=node),
            "--portal=$node",
            '--target=' . to_iqn($node, $self->global_name),
            '--login',
        ) == 0
            or die "iscsiadm failed:$?";
        for (my $i = ISCSI_MOUNT_TIMEOUT; ; $i--) {
            last if
                readlink _to_device($node, $self->global_name);
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
            '--target=' . to_iqn($node, $self->global_name),
            "--portal=$node",
            '--logout',
        ) == 0
            or warn "iscsiadm failed:$?";
    }
}

sub _start_raid {
    my ($self, $do_create) = @_;
    
    # build command
    my @cmd = qw(mdadm);
    if ($do_create) {
        push(
            @cmd,
            '--create',
            $self->device,
            '--run',
            '--level=1',
            '--bitmap=internal',
            '--homehost=nonexistent.example.com',
            '--raid-devices=' . scalar(@{$self->def->{nodes}}),
        );
    } else {
        push(
            @cmd,
            '--assemble',
            $self->device,
        );
    }
    for my $node (sort @{$self->def->{nodes}}) {
        push @cmd, _to_device($node, $self->global_name);
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
    unless ($do_create) {
        if ($last_line =~ /has been started with (\d+) drives? \(out of (\d+)\)\.\s*$/
                && $1 != $2) {
            # read output of mdadm --detail device
            my @lines = do {
                open my $fh, '-|', "mdadm --detail @{[$self->device]}"
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
                my $path = _to_device($_, $self->global_name);
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
                $self->device,
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
        qw(mdadm --stop), $self->device,
    ) == 0
        or die "mdadm failed with exit code:$?";
}

sub _load {
    my $self = shift;
    
    $self->def(from_json(do {
        open my $fh, '<', "@{[DISK_DIR]}/@{[$self->global_name]}"
            or die "no definition for:@{[$self->global_name]}:$!";
        join '', <$fh>;
    }));
    die "no `nodes' array in definition"
        unless $self->def->{nodes} && ref $self->def->{nodes} eq 'ARRAY';
    
    $self;
}

sub _save {
    my $self = shift;
    
    write_file("@{[DISK_DIR]}/@{[$self->global_name]}", to_json($self->def));
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

sub _to_device {
    my ($host, $ident) = @_;
    "/dev/disk/by-path/ip-$host:3260-iscsi-" . to_iqn($host, $ident) . "-lun-0";
}

1;

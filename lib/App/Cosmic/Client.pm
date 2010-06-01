package App::Cosmic::Client;

use strict;
use warnings;

use Cwd qw(realpath);
use Errno ();
use File::Basename qw(dirname);
use Getopt::Long;
use IO::Handle;
use IPC::Open2;
use JSON qw(from_json to_json);

use App::Cosmic;
use base qw(App::Cosmic);

use constant DISK_DIR            => CLIENT_CONF_DIR . '/disks';
use constant ISCSID_CONF_FILE    => $ENV{COSMIC_ISCSID_CONF_FILE} || '/etc/iscsi/iscsid.conf';
use constant ISCSID_ITOR_FILE    => $ENV{COSMIC_ISCSID_ITOR_FILE} || '/etc/iscsi/initiatorname.iscsi';
use constant ISCSI_MOUNT_TIMEOUT => 60;

__PACKAGE__->mk_accessors(qw(def global_name device));

sub new {
    my ($klass, $global_name, $device) = @_;
    bless {
        global_name => $global_name,
        device      => $device,
    }, $klass;
}

sub create {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV >= 5;
    my ($global_name, $device, $size, @nodes) = @ARGV;
    validate_global_name($global_name);
    die "specified md array seems to be in use"
        if _get_devices_of_array($device);
    __PACKAGE__->new($global_name, $device)->_create($size, @nodes);
}

sub add {
    my $klass = shift;
    die 'invalid args, see --help'
        unless @ARGV == 4;
    my ($global_name, $device, $size, $node) = @ARGV;
    validate_global_name($global_name);
    __PACKAGE__->new($global_name, $device)->_load
        ->_assert_md_ownership->_add($size, $node);
}

sub remove {
    my $klass = shift;
    die 'invalid args, see --help'
        unless @ARGV == 3;
    my ($global_name, $device, $node) = @ARGV;
    __PACKAGE__->new($global_name, $device)->_load
        ->_assert_md_ownership->_remove($node);
}

sub destroy {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV == 1;
    my ($global_name) = @ARGV;
    validate_global_name($global_name);
    __PACKAGE__->new($global_name, undef)->_load->_destroy();
}

sub connect {
    my $klass = shift;
    my $do_create;
    if (@ARGV && $ARGV[0] eq '--initialize') {
        shift @ARGV;
        $do_create = 1;
    }
    die "invalid args, see --help"
        unless @ARGV == 2;
    my ($global_name, $device) = @ARGV;
    validate_global_name($global_name);
    die "specified md array seems to be in use"
        if _get_devices_of_array($device);
    __PACKAGE__->new($global_name, $device)->_load->_connect($do_create);
}

sub disconnect {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV == 2;
    my ($global_name, $device) = @ARGV;
    validate_global_name($global_name);
    __PACKAGE__->new($global_name, $device)->_load
        ->_assert_md_ownership->_disconnect;
}

sub status {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV == 2;
    my ($global_name, $device) = @ARGV;
    validate_global_name($global_name);
    __PACKAGE__->new($global_name, $device)->_load
        ->_assert_md_ownership->_status;
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
    $self->_connect(1)
        unless $self->device eq '-';
}

sub _add {
    my ($self, $size, $node) = @_;
    
    die "node $node is already part of the specfied array"
        if grep { $_ eq $node } @{$self->def->{nodes}};
    
    # create block device
    print "creating block device on server...\n";
    $self->_sync_run(
        "create @{[$self->global_name]} $size",
        undef,
        [ $node ],
    );
    # update def and commit def to disk
    push @{$self->def->{nodes}}, $node;
    $self->_save;
    
    # connect target
    $self->_change_client([ $node ]);
    $self->_mount([ $node ]);
    
    # adjust # of raid devices
    print "adding device to array...\n";
    systeml(
        qw(mdadm --grow), $self->device,
        "--raid-devices=" . (1 + scalar keys %{$self->_raid_status}),
    ) == 0
        or die "mdadm failed:$?";
    # add device
    systeml(
        qw(mdadm --manage), $self->device,
        '--add', _to_device(_parse_node($node)->{host}, $self->global_name),
    ) == 0
        or die "mdadm failed:$?";
}

sub _remove {
    my ($self, $node) = @_;
    
    die "node $node is not part of the specified array"
        unless grep { $_ eq $node } @{$self->def->{nodes}};
    
    my $devfile = _to_device(_parse_node($node)->{host}, $self->global_name, 1)
        or die "$node is not connected";
    
    # set flag on array
    systeml(
        qw(mdadm --manage), $self->device, '--fail', $devfile,
    ) == 0
        or die "mdadm failed:$?";
    # remove from array
    system(
        qw(mdadm --manage), $self->device, '--remove', $devfile,
    ) == 0
        or die "mdadm failed:$?";
    # update def and commit to disk
    $self->def->{nodes} = [ grep { $_ ne $node } @{$self->def->{nodes}} ];
    $self->_save;
    # disconnect
    $self->_unmount([ $node ]);
    # remove from server
    $self->_sync_run(
        "destroy @{[$self->global_name]}",
        undef,
        [ $node ],
    );
    # update # of raid devices
    $self->_adjust_num_devices;
}

sub _destroy {
    my $self = shift;
    
    print "removing block device from servers...\n";
    $self->_sync_run(
        "destroy @{[$self->global_name]}",
    );
    # unlink def from disk
    sync_unlink("@{[DISK_DIR]}/@{[$self->global_name]}")
        or die "failed to unlink file:@{[DISK_DIR]}/@{[$self->global_name]}:$!";
}

sub _connect {
    my ($self, $do_create) = @_;
    
    # connect target
    $self->_change_client;
    $self->_mount;
    
    print "starting RAID array...\n";
    $self->_start_raid($do_create);
}

sub _disconnect {
    my $self = shift;
    
    $self->_stop_raid;
    
    $self->_unmount;
}

sub _status {
    my $self = shift;
    
    my $status = $self->_raid_status();
    for my $node (sort @{$self->def->{nodes}}) {
        my $def = _parse_node($node);
        my $dev_status = 'not connected';
        my $devfile;
        eval {
            $devfile = _to_device($def->{host}, $self->global_name, 1);
        };
        if ($devfile) {
            $dev_status = $status->{$devfile} || 'connected but not assembled';
        } else {
            $devfile = '';
        }
        print "$node:$devfile:$dev_status\n";
    }
}

sub _change_client {
    my ($self, $nodes) = @_;
    $nodes ||= $self->def->{nodes};
    
    print "registering my credentials to block device servers...\n";
    
    # read user,pass from iscsi.conf
    my ($user, $pass) = $self->_read_userpass;
    
    $self->_sync_run(
        "change-credentials @{[$self->global_name]} $user $pass",
        "ITOR=@{[$self->_read_itor_iqn]}",
        undef,
        $nodes,
    );
}

sub _sync_run {
    my ($self, $cmd, $env, $target_nodes) = @_;
    
    $target_nodes ||= $self->def->{nodes};
    
    # update all nodes to new_addr using 2pc
    my %nodes = map {
        $_ => {
            # pid => pid,
            # out: output to remote,
            # in: input from remote
            output => '',
        },
    } @$target_nodes;
    
    # spawn and check
    for my $node (sort keys %nodes) {
        my $def = _parse_node($node);
        my $argv = "ssh $def->{user}\@$def->{host} exec $def->{cmd_prefix}";
        $argv .= " env $env"
            if $env;
        $argv .= " cosmic-server $cmd 2>&1";
        $nodes{$node}->{pid} = open2(
            $nodes{$node}->{in},
            $nodes{$node}->{out},
            $argv,
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
    my ($self, $nodes) = @_;
    $nodes ||= $self->def->{nodes};
    
    print "mounting disks...\n";
    
    for my $node (map {
        _parse_node($_)->{host}
    } sort @$nodes) {
        # mount iscsi target
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
        # wait for completion
        for (my $i = ISCSI_MOUNT_TIMEOUT; ; $i--) {
            last if -e _to_device($node, $self->global_name);
            die "failed to locate device file"
                if $i <= 0;
            sleep 1;
        }
    }
}

sub _unmount {
    my ($self, $nodes) = @_;
    
    print "unmounting disk...\n";
    
    $nodes ||= $self->def->{nodes};
    
    for my $node (map { _parse_node($_)->{host} } sort @$nodes) {
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
    for my $node (map {
        _parse_node($_)->{host}
    } sort @{$self->def->{nodes}}) {
        push @cmd, _to_device($node, $self->global_name, 1);
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
        if ($last_line =~ /has been started with (\d+) drives/
                && $1 != scalar(@{$self->def->{nodes}})) {
            # build list of active devices
            my $active = $self->_raid_status;
            for my $k (keys %$active) {
                delete $active->{$k}
                    unless $active->{$k} =~ /^active\s/;
            }
            # build list of devices to re-add
            my @readd = map {
                my $path = _to_device(
                    _parse_node($_)->{host},
                    $self->global_name,
                    1,
                );
                $active->{$path} ? () : ($path)
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
        $self->_adjust_num_devices
            if $last_line =~ /has been started with \d+ drives.*out of/;
    }
}

sub _stop_raid {
    my $self = shift;
    
    print "stopping RAID array...\n";
    
    systeml(
        qw(mdadm --stop), $self->device,
    ) == 0
        or die "mdadm failed with exit code:$?";
}

sub _adjust_num_devices {
    my $self = shift;
    print "updating number of devices in array...\n";
    systeml(
        qw(mdadm --grow), $self->device,
        "--raid-devices=" . scalar(@{$self->def->{nodes}}),
    ) == 0
        or warn "mdadm --grow failed with exit code:$?";
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

sub _assert_md_ownership {
    my $self = shift;
    
    # validate if a connected cosmic target occupies the specified md array
    my %cosmic_devices;
    for my $node (sort @{$self->def->{nodes}}) {
        my $devfile;
        eval {
            $devfile = _to_device(
                _parse_node($node)->{host},
                $self->global_name,
                1,
            );
        };
        $cosmic_devices{$devfile} = 1
            if $devfile;
    }
    my $md_devices = $self->_raid_status
        or die "specified md array seems to be inactive";
    for my $md_dev (sort keys %$md_devices) {
        die "the md array is not attached to the specified cosmic devices"
            unless $cosmic_devices{$md_dev};
    }
    
    $self;
}

sub _save {
    my $self = shift;
    
    write_file("@{[DISK_DIR]}/@{[$self->global_name]}", to_json($self->def));
}

sub _read_itor_iqn {
    my $self = shift;
    my $iqn;
    open my $fh, '<', ISCSID_ITOR_FILE
        or die "failed to open file:@{[ISCSID_ITOR_FILE]}:$!";
    while (my $line = <$fh>) {
        chomp $line;
        if ($line =~ /^\s*InitiatorName\s*=\s*(.*)\s*?$/i) {
            $iqn = $1;
            last;
        }
    }
    close $fh;
    
    die "InitiatorName not defined in @{[ISCSID_ITOR_FILE]}"
        unless $iqn;
    $iqn;
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

sub _raid_status {
    my $self = shift;
    my $ret = _get_devices_of_array($self->device)
        or die "mdadm --detail failed with exit code:$?";
    $ret;
}

sub _get_devices_of_array {
    my $md = shift;
    open my $fh, '-|', "mdadm --detail $md"
        or die "failed to spawn mdadm --detail $md:$!";
    my @lines = <$fh>;
    close $fh;
    return
        if $? != 0;
    _parse_raid_status(@lines);
}

sub _to_device {
    my ($host, $ident, $resolve_path) = @_;
    my $src = "/dev/disk/by-path/ip-$host:3260-iscsi-" . to_iqn($host, $ident) . "-lun-0";
    return $src unless $resolve_path;
    my $dest = readlink $src
        or die "readlink failed:$src:$!";
    $dest = realpath(dirname($src) . "/$dest")
        if $dest !~ m{^/};
    $dest;
}

sub _parse_node {
    my $ret = {
        host       => $_[0],
        user       => 'root',
        cmd_prefix => '',
    };
    if ($ret->{host} =~ m{/}) {
        ($ret->{host}, $ret->{cmd_prefix}) = ($`, $');
    }
    if ($ret->{host} =~ m{\@}) {
        ($ret->{user}, $ret->{host}) = ($`, $');
    }
    $ret;
}

sub _parse_raid_status {
    my @lines = @_;
    while (@lines) {
        my $l = shift @lines;
        last if $l =~ /^\s+Number\s+Major\s+Minor\s+RaidDevice\s+State\s*$/;
    }
    my %devices;
    while (@lines) {
        my $l = shift @lines;
        chomp $l;
        if ($l =~ m{^\s+\d+\s+\d+\s+\d+\s+(?:\d+|-)\s+(.*?)\s+(/dev/[^ ]+)\s*$}) {
            $devices{$2} = $1;
        }
    }
    die "failed to parse output of mdadm --detail"
        unless %devices;
    \%devices;
}

1;

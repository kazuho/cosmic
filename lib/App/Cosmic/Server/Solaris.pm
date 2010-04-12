package App::Cosmic::Server::Solaris;

use strict;
use warnings;

use File::Temp qw(tempfile);

use App::Cosmic;
use App::Cosmic::Server;

use base qw(App::Cosmic::Server);

use constant ITADM           => '/usr/sbin/itadm';
use constant SBDADM          => '/usr/sbin/sbdadm';
use constant STMFADM         => '/usr/sbin/stmfadm';
use constant ZFS             => '/usr/sbin/zfs';
use constant ZFS_BLOCK_SIZE  => $ENV{COSMIC_ZFS_BLOCK_SIZE} || '64K';
use constant ZFS_CREATE_ARGS => $ENV{COSMIC_ZFS_CREATE_ARGS} || undef;

sub _devices {
    my $self = shift;
    my %devices;
    my $iqn_prefix = to_iqn($self->iqn_host, '');
    open my $fh, '-|', "@{[ITADM]} list-target"
        or die "failed to invoke itadm:$!";
    while (my $l = <$fh>) {
        if ($l =~ /^$iqn_prefix(\S+)/) {
            $devices{$1} = "$iqn_prefix$1";
        }
    }
    \%devices;
}

sub _start {
    my $self = shift;
    
    # disable access to all devices
    for my $global_name (sort keys %{$self->_devices}) {
        $self->_disallow_current($global_name);
    }
}

sub _create_device {
    my ($self, $global_name, $size) = @_;
    
    # create volume
    systeml(
        ZFS, 'create',
        (ZFS_CREATE_ARGS ? split(/\s+/, ZFS_CREATE_ARGS) : ()),
        '-b', ZFS_BLOCK_SIZE, qw(-V), $size,
        $self->device_prefix . $global_name,
    ) == 0
        or die "zfs failed:$?";
    # create hostgroup for authorization
    systeml(
        STMFADM, 'create-hg', "cosmic/$global_name",
    ) == 0
        or die "stmfadm failed:$?";
    # create logical unit
    systeml(
        SBDADM, 'create-lu', $self->_device_path_from_global_name($global_name),
    ) == 0
        or die "sbdadm failed:$?";
    # add view
    systeml(
        STMFADM, qw(add-view --host-group), "cosmic/$global_name",
        $self->_guid_from_global_name($global_name),
    ) == 0
        or die "stmfadm failed:$?";
    # create target
    systeml(
        ITADM, qw(create-target -n), to_iqn($self->iqn_host, $global_name),
    ) == 0
        or die "itadm failed:$?";
    # make target offline
    $self->_offline_target($global_name);
}

sub _remove_device {
    my ($self, $global_name) = @_;
    
    # drop active connection and make it offline
    $self->_disallow_current($global_name);
    # delete target
    systeml(
        ITADM, qw(delete-target), to_iqn($self->iqn_host, $global_name),
    ) == 0
        or die "itadm failed:$?";
    # remove view
    systeml(
        STMFADM, qw(remove-view -a -l),
        $self->_guid_from_global_name($global_name),
    ) == 0
        or die "stmfadm failed:$?";
    # delete logical unit
    systeml(
        SBDADM, 'delete-lu',
        $self->_guid_from_global_name($global_name),
    ) == 0
        or die "sbdadm failed:$?";
    # delete hostgroup
    systeml(
        STMFADM, 'delete-hg', "cosmic/$global_name",
    ) == 0
        or die "stmfadm failed:$?";
    # delete volume
    systeml(
        ZFS, qw(destroy), $self->device_prefix . $global_name,
    ) == 0
        or die "zfs failed:$?";
}

sub _resize_device {
    my ($self, $global_name, $size) = @_;
    
    # resize volume
    systeml(
        ZFS, 'set', "volsize=$size", $self->device_prefix . $global_name,
    ) == 0
        or die "zfs failed:$?";
    # update lu
    systeml(
        SBDADM, qw(modify-lu -s), $size,
        $self->_guid_from_global_name($global_name),
    ) == 0
        or die "sbadm failed:$?";
}

sub _disallow_current {
    my ($self, $global_name) = @_;
    
    # disactivate
    $self->_offline_target($global_name);
    # disallow current initiator
    my $itor_file = $self->_itor_file_of($global_name);
    if (-e $itor_file) {
        systeml(
            STMFADM, qw(remove-hg-member -g), "cosmic/$global_name",
            read_oneline($itor_file),
        ) == 0
            or die "stmfadm failed:$?";
        sync_unlink($itor_file);
    }
}

sub _allow_one {
    my ($self, $global_name, $new_user, $new_pass) = @_;
    
    my $new_initiator = $ENV{ITOR}
        or die "could not receive initator name from client";
    
    # define initiator (may exist already, ignore error)
    systeml(
        ITADM, qw(create-initiator -u), $new_user, $new_initiator,
    );
    # set password
    my $passfile = do {
        my ($fh, $fn) = tempfile(UNLINK => 1);
        print $fh $new_pass;
        close $fh;
        $fn;
    };
    systeml(
        ITADM, qw(modify-initiator -S), $passfile,
        $new_initiator,
    ) == 0
        or die "itadm failed:$?";
    
    # store the new initiator name to file, and then update auth info
    write_file($self->_itor_file_of($global_name), $new_initiator);
    systeml(
        STMFADM, qw(add-hg-member -g), "cosmic/$global_name",
        $new_initiator,
    ) == 0
        or die "stmfadm failed:$?";
    
    # activate
    $self->_online_target($global_name);
}

sub _online_target {
    my ($self, $global_name) = @_;
    systeml(
        STMFADM, 'online-target', to_iqn($self->iqn_host, $global_name),
    ) == 0
        or die "stmfadm failed:$?";
}

sub _offline_target {
    my ($self, $global_name) = @_;
    systeml(
        STMFADM, 'offline-target', to_iqn($self->iqn_host, $global_name),
    ) == 0
        or die "stmfadm failed:$?";
    # something seems to be async here.  if we run "cosmic-server remove"
    # without this sleep, kernel panic occurs somewhere around zfs remove.
    sleep 1;
}

sub _guid_from_global_name {
    my ($self, $global_name) = @_;
    my $device_path = $self->_device_path_from_global_name($global_name);
    my $sbds = $self->_sbd_list;
    
    for my $guid (keys %$sbds) {
        return $guid
            if $sbds->{$guid}->{source} eq $device_path;
    }
    
    die "failed to obtain guid of $global_name using sbdadm";
}

sub _sbd_list {
    my $self = shift;
    my %sbds;
    
    open my $fh, '-|', "@{[SBDADM]} list-lu"
        or die "failed to invoke sbdadm:$!";
    # skip header
    while (my $l = <$fh>) {
        last if $l =~ /^--------------------------------\s/;
    }
    # read and compare like: GUID\s+DATA_SIZE\s+SOURCE
    while (my $l = <$fh>) {
        chomp $l;
        my ($guid, $sz, $src) = split /\s+/, $l;
        $sbds{$guid} = +{
            size   => $sz,
            source => $src,
        };
    }
    close $fh;
    
    \%sbds;
}

sub _device_path_from_global_name {
    my ($self, $global_name) = @_;
    "/dev/zvol/rdsk/" . $self->device_prefix . $global_name;
}

sub _itor_file_of {
    my ($self, $global_name) = @_;
    SERVER_CONF_DIR . "/$global_name.itor";
}

1;

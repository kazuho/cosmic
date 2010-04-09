package App::Cosmic::Server::Solaris;

use strict;
use warnings;

use App::Cosmic;
use App::Cosmic::Server;

use base qw(App::Cosmic::Server);

use constant ZFS             => '/usr/sbin/zfs';
use constant ZFS_BLOCK_SIZE  => $ENV{COSMIC_ZFS_BLOCK_SIZE} || '64K';
use constant ZFS_CREATE_ARGS => $ENV{COSMIC_ZFS_CREATE_ARGS} || undef;
use constant ISCSITADM       => '/usr/sbin/iscsitadm';
use constant DUMMY_IQN       => 'iqn.2010-02.com.example.nonexistent';

sub _devices {
    my $self = shift;
    my $targets = $self->_read_targets;
    
    +{
        map {
            $_ => $targets->{$_}->{'iSCSI Name'},
        } keys %$targets,
    };
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
    my $vol = $self->device_prefix . $global_name;
    
    # create volume
    systeml(
        ZFS, 'create',
        (ZFS_CREATE_ARGS ? split(/\s+/, ZFS_CREATE_ARGS) : ()),
        '-b', ZFS_BLOCK_SIZE, qw(-V), $size, $vol,
    ) == 0
        or die "zfs failed:$?";
    
    # set shareiscsi flag
    systeml(
        ZFS, qw(set shareiscsi=on), $vol,
    ) == 0
        or die "zfs failed:$?";
    
    # disable access from all initiators
    systeml(
        ISCSITADM, qw(modify target --acl), DUMMY_IQN, $vol
    ) == 0
        or die "iscsitadm failed:$?";
}

sub _remove_device {
    my ($self, $global_name) = @_;
    
    $self->_disallow_current($global_name);
    systeml(
        ZFS, qw(destroy), $self->device_prefix . $global_name,
    ) == 0
        or die "zfs failed:$?";
}

sub _disallow_current {
    my ($self, $global_name) = @_;
    
    # disable access, then unlink the information
    my $owner_file = $self->_owner_file_of($global_name);
    if (-e $owner_file) {
        my $cur = read_oneline($owner_file);
        systeml(
            ISCSITADM,
            qw(delete target --acl),
            $cur,
            $self->device_prefix . $global_name,
        ) == 0
            or die "iscsitadm failed:$?";
        sync_unlink($owner_file);
    }
}

sub _allow_one {
    my ($self, $global_name, $new_user, $new_pass) = @_;
    
    my $new_initiator = $ENV{ITOR}
        or die "could not receive initator name from client";
    
    # save, and then enable access
    write_file(
        $self->_owner_file_of($global_name),
        $new_initiator,
    );
    # define initiator (ignore errors since it might be already defined.  If
    # not defined after the call, following calls should fail)
    systeml(
        ISCSITADM, qw(create initiator --iqn), $new_initiator, $new_initiator
    );
    # set username
    systeml(
        ISCSITADM, qw(modify initiator --chap-name), $new_user, $new_initiator
    ) == 0
        or die "iscsitadm failed:$?";
    # set passphrase
    my $pid = fork;
    die "fork(2) failed:$!"
        unless defined $pid;
    if ($pid == 0) {
        # child process
        exec qw(iscsitadm-set-secret.py), $new_pass, $new_initiator;
        die "failed to exec iscsitadm-set-secret.py:$!";
    }
    while (waitpid($pid, 0) == -1) {}
    die "iscsitadm failed:$?"
        unless $? == 0;
    # update acl
    systeml(
        ISCSITADM,
        qw(modify target --acl),
        $new_initiator,
        $self->device_prefix . $global_name,
    ) == 0
        or die "iscsitadm failed:$?";
}

sub _read_targets {
    my $self = shift;
    my %targets;
    
    open my $fh, '-|', "@{[ISCSITADM]} list target"
        or die "failed to invoke iscsitadm:$!";
    my $target = undef;
    while (my $l = <$fh>) {
        chomp $l;
        if ($l =~ /^\S/) {
            if ($l =~ /^Target: /) {
                $target = $';
                if ($target =~ s/^@{[$self->device_prefix]}//) {
                    $targets{$target} = {};
                } else {
                    undef $target;
                }
            } else {
                undef $target;
            }
        } elsif ($target && $l =~ /^\s+([^:]+)\s*:\s*/) {
            $targets{$target}->{$1} = $';
        }
    }
    
    \%targets;
}

sub _owner_file_of {
    my ($self, $global_name) = @_;
    SERVER_CONF_DIR . "/$global_name.itor";
}

1;

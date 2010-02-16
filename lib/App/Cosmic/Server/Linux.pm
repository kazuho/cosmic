package App::Cosmic::Server::Linux;

use strict;
use warnings;

use Errno ();
use File::Basename qw(dirname basename);
use List::Util qw(max);
use App::Cosmic;
use App::Cosmic::Server;

use base qw(App::Cosmic::Server);

use constant DUMMY_USERNAME   => 'dummyuser';
use constant DISABLE_PASSWORD => 'neverconnect00';

sub _devices {
    my $self = shift;
    my %devices;
    
    for my $tid ($self->_read_iet_session) {
        $tid->{tid} && $tid->{name} =~ /^iqn\.[^:]+:(.*)$/
            or die "unexepected format in /proc/net/iet/session";
        $devices{$1} = $tid->{name};
        # TODO move device name under some prefix to avoid collision
    }
    \%devices;
}

sub _start {
    my $self = shift;
    
    # register devices
    for my $p (glob $self->device_prefix . '*') {
        my $global_name = substr $p, length $self->device_prefix;
        $self->_register_device(
            $global_name,
            $self->_get_credentials_of($global_name),
        );
    }
}

sub _create_device {
    my ($self, $global_name, $size) = @_;
    
    # just to be sure
    unless (unlink($self->_credentials_file_of($global_name))
                || $! == Errno::ENOENT) {
        die "failed to remove @{[$self->_credentials_file_of($global_name)]}:$!";
    }
    
    # create lv
    my $lvpath = $self->device_prefix . $global_name;
    systeml(
        qw(lvcreate),
        "--size=$size",
        '--name=' . basename($lvpath),
        dirname($lvpath),
    ) == 0
        or die "lvm failed:$?";
    
    # start
    $self->_register_device($global_name, DUMMY_USERNAME, DISABLE_PASSWORD);
}

sub _remove_device {
    my ($self, $global_name) = @_;
    
    # disable and drop all connections -> unregister device -> remove lv
    $self->_disallow_current($global_name);
    $self->_unregister_device($global_name);
    sleep 1; # seems necessary
    systeml(
        qw(lvremove -f),
        $self->device_prefix . $global_name,
    ) == 0
        or die "lvm failed:$?";
    
    # unlink cred file (might not exist, and this is not a must)
    unlink $self->_credentials_file_of($global_name);
}

sub _disallow_current {
    my ($self, $global_name) = @_;
    
    # reset password and stop server, so that there will be no connections
    $self->_reflect_credentials_of(
        $global_name,
        ($self->_get_credentials_of($global_name))[0],
        DISABLE_PASSWORD,
    );
    $self->_set_credentials_of($global_name);
    sleep 1; # just in case set credentials is async
    $self->_disconnect($global_name);
    sleep 1;
    $self->_disconnect($global_name);
}

sub _allow_one {
    my ($self, $global_name, $new_user, $new_pass) = @_;
    
    $self->_set_credentials_of($global_name, $new_user, $new_pass);
    $self->_reflect_credentials_of($global_name, $new_user, $new_pass);
}

sub _register_device {
    my ($self, $global_name, $user, $pass) = @_;
    
    # new_tid = max(tid) + 1
    my $tid = 1 + max 0, map { $_->{tid} } $self->_read_iet_session;
    
    systeml(
        qw(ietadm --op new --user),
        "--tid=$tid",
        "--params=IncomingUser=$user,Password=$pass",
    ) == 0
        or die "failed to update credentials using ietadm:$?";
    
    systeml(
        qw(ietadm --op new),
        "--tid=$tid",
        "--params=Name=" . to_iqn($self->iqn_host, $global_name),
    ) == 0
        or die "failed to create an iSCSI node using ietadm:$?";
    
    systeml(
        qw(ietadm --op new),
        "--tid=$tid",
        "--lun=0",
        "--params=Path=" . $self->device_prefix . $global_name,
    ) == 0
        or die "failed to setup LUN using ietadm:$?";
}

sub _unregister_device {
    my ($self, $global_name) = @_;
    
    my $tid = $self->_sessions_of($global_name);
    systeml(
        qw(ietadm --op delete),
        "--tid=$tid->{tid}",
    ) == 0
        or die "an error ocurred while unregistering a device using ietadm:$?";
}

sub _reflect_credentials_of {
    my ($self, $global_name, $user, $pass) = @_;
    my $tid = $self->_sessions_of($global_name);
    
    systeml(
        qw(ietadm --op new --user),
        "--tid=$tid->{tid}",
        "--params=IncomingUser=$user,Password=$pass",
    ) == 0
        or die "failed to update credentials using ietadm:$?";
}

sub _disconnect {
    my ($self, $global_name) = @_;
    my $tid = $self->_sessions_of($global_name);
    
    for my $sid (@{$tid->{sid}}) {
        for my $cid (@{$sid->{cid}}) {
            systeml(
                qw(ietadm --op delete),
                "--tid=$tid->{tid}",
                "--sid=$sid->{sid}",
                "--cid=$cid->{cid}",
            ) == 0
                or die "failed to kill connections using ietadm:$?";
        }
    }
}

sub _read_iet_session {
    my $self = shift;
    
    open my $fh, '<', '/proc/net/iet/session'
        or die "failed to open file:/proc/net/iet/session:$!";
    
    my @tids;
    while (my $line = <$fh>) {
        chomp $line;
        $line =~ s/^\s*(.*)\s*$/$1/;
        my %h = map { split /:/, $_, 2 } split /\s+/, $line;
        if ($line =~ /^tid:/) {
            $h{sid} = [];
            push @tids, \%h;
        } elsif ($line =~ /^sid:/) {
            die "failed to parse /proc/net/iet/session"
                unless @tids;
            $h{cid} = [];
            push @{$tids[-1]->{sid}}, \%h;
        } elsif ($line =~ /^cid:/) {
            die "failed to parse /proc/net/iet/session"
                unless @tids && @{$tids[-1]->{sid}};
            push @{$tids[-1]->{sid}->[-1]->{cid}}, \%h;
        }
    }
    
    @tids;
}

sub _sessions_of {
    my ($self, $global_name) = @_;
    my @tids = grep {
        $_->{name} eq to_iqn($self->iqn_host, $global_name)
    } $self->_read_iet_session;
    die "no target found for name:$global_name"
        unless @tids;
    die "too many targets found for name:$global_name"
        if @tids > 1;
    $tids[0];
}

sub _get_credentials_of {
    my ($self, $global_name) = @_;
    my $line = read_oneline($self->_credentials_file_of($global_name), '');
    $line ? split(/ /, $line, 2) : (DUMMY_USERNAME, DISABLE_PASSWORD);
}

sub _set_credentials_of {
    my ($self, $global_name, @userpass) = @_;
    write_file(
        $self->_credentials_file_of($global_name),
        @userpass ? join(' ', @userpass) : '',
    );
}

sub _credentials_file_of {
    my ($self, $global_name) = @_;
    SERVER_TMP_DIR . "/$global_name.cred";
}

1;

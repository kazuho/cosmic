package App::Cosmic::Server::IET;

use strict;
use warnings;

use List::Util qw(max);
use App::Cosmic;
use App::Cosmic::Server;
use base qw(App::Cosmic::Server);

sub _start {
    my $self = shift;
    
    # register devices
    for my $global_name ($self->_devices) {
        $self->_register_device(
            $global_name,
            $self->_get_credentials_of($global_name),
        );
    }
}

sub _stop {
    my $self = shift;
    
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
    
    $self->_kill_connections_of($global_name);
    
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

sub _kill_connections_of {
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

1;

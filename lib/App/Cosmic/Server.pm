package App::Cosmic::Server;

use strict;
use warnings;

use File::Basename qw(basename);
use IO::Handle;
use IO::Socket::INET;
use JSON qw(from_json);
use Socket qw(SOMAXCONN);

use App::Cosmic;
use base qw(App::Cosmic);

use constant GLOBAL_LOCK_FILE => SERVER_CONF_DIR . '/cosmic.lock';

__PACKAGE__->mk_accessors(qw(device_prefix));

sub new {
    my $klass = shift;
    my $self = bless {}, $klass;
    
    # read configuration
    my $json = from_json(do {
        my $fn = SERVER_CONF_DIR . '/cosmic.conf';
        open my $fh, '<', $fn
            or die "failed to open file:$fn:$!";
        join '', <$fh>;
    });
    for my $n (qw(device_prefix)) {
        die "`$n' not defined in cosmic.conf"
            unless $json->{$n};
        $self->{$n} = $json->{$n};
    }
    
    $self;
}

sub start {
    __PACKAGE__->new->_start;
}

sub _start {
    my $self = shift;
    
    my $listen_sock = IO::Socket::INET->new(
        LocalPort => NBD_PORT,
        Listen    => SOMAXCONN,
        Proto     => 'tcp',
        ReuseAddr => 1,
    ) or die "failed to listen on port @{[NBD_PORT]}:$!";
    
    # accept loop
    while (my $s = $listen_sock->accept) {
        unless (my $pid = fork) {
            die "fork(2) failed:$!"
                unless defined $pid;
            # child process
            $self->_handle_conn($s);
        }
    }
}

sub _handle_conn {
    my ($self, $s) = @_;
    
    my ($global_name, $device_lock);
    
    { # setup with global lock
        my $global_lock = lock_file(GLOBAL_LOCK_FILE);
        
        # check host
        $global_name = $self->_peerhost_to_global_name($s->peerhost)
            or die "connection refused, no devices allowed access from address:@{[$s->peerhost]}";
        
        # kill old server
        $self->_kill_server($global_name);
        
        # setup: lock the device, write pid
        $device_lock = lock_file($self->_device_lock_file_of($global_name));
        my $pid_file = $self->_pid_file_of($global_name);
        open my $fh, '>', $pid_file
            or die "failed to open file:$pid_file:$!";
        print $fh $$;
        $fh->sync;
        close $fh;
        sync_dir(SERVER_CONF_DIR);
    }
    
    print "connecting @{[$s->peerhost]} to $global_name...\n";
    
    # start
    open STDIN, '<&', $s
        or die "failed to dup socket to STDIN:$!";
    close $s;
    exec qw(nbd-server 0), $self->device_prefix . $global_name;
    die "failed to exec nbd-server:$!";
}

sub change_client {
    __PACKAGE__->new->_change_client;
}

sub _change_client {
    my $self = shift;
    
    # obtain new address
    die "invalid args, see --help"
        unless @ARGV == 1 || @ARGV == 2;
    my $global_name = shift @ARGV;
    my $new_addr = @ARGV ? shift(@ARGV) : do {
        ($ENV{SSH_CLIENT} || '') =~ m/^(.*?)\s/
            or die "failed to obtain client address from SSH_CLIENT";
        $1;
    };
    
    # lock
    my $global_lock = lock_file(GLOBAL_LOCK_FILE);
    
    # check existence of the device
    die "device $global_name does not exist:$!"
        unless -e $self->device_prefix . $global_name;
    
    # ready, wait for client
    print "ok phase 1\n";
    STDOUT->flush;
    
    # kill current
    $self->_kill_server($global_name);
    
    # prepare (reset ipof file)
    my $input = <STDIN>;
    unless ($input =~ /^prepare\n$/) {
        warn "client did not request prepare, aborting...\n";
        return;
    }
    write_file($self->_ipof_file_of($global_name), '');
    
    # commit (write new ipof file)
    $input = <STDIN>;
    unless ($input =~ /^commit\n$/) {
        warn "client did not request commit, aborting...\n";
        return;
    }
    write_file($self->_ipof_file_of($global_name), $new_addr);
}

sub _kill_server {
    my ($self, $global_name) = @_;
    
    # precond: should hold lock on LOCK_FILE
    
    my $pid_file = $self->_pid_file_of($global_name);
    if (-e $pid_file) {
        if (my $device_lock = lock_file(
            $self->_device_lock_file_of($global_name),
            1,
        )) {
            # server's running
            kill 'KILL', read_oneline($pid_file);
            lock_file("$pid_file.lock");
        }
        unlink $pid_file;
    }
}

sub _peerhost_to_global_name {
    my ($self, $peerhost) = @_;
    my $global_name;
    
    for my $fn (glob "@{[SERVER_CONF_DIR]}/*.ipof") {
        my $allowed = read_oneline($fn);
        warn "allowed:$allowed, peer:$peerhost";
        if ($allowed eq $peerhost) {
            die "$peerhost seems allowed to more than one devices"
                if defined $global_name;
            $global_name = basename $fn;
            $global_name =~ s/\.ipof$//;
        }
    }
    
    $global_name;
}

sub _ipof_file_of {
    my ($self, $global_name) = @_;
    SERVER_CONF_DIR . "/$global_name.ipof";
}

sub _pid_file_of {
    my ($self, $global_name) = @_;
    SERVER_CONF_DIR . "/$global_name.pid";
}

sub _device_lock_file_of {
    my ($self, $global_name) = @_;
    SERVER_CONF_DIR . "/$global_name.lock";
}

1;

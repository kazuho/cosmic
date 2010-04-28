package App::Cosmic::Server;

use strict;
use warnings;

use Errno ();
use Exporter qw(import);
use IO::Handle;
use IO::Socket::INET;
use JSON qw(from_json);
use List::Util qw(first);
use Socket qw(SOMAXCONN);

use App::Cosmic;
use base qw(App::Cosmic);

our @EXPORT = qw(SERVER_LOCK_FILE SERVER_TMP_DIR);

use constant SERVER_CONF_FILE => SERVER_CONF_DIR . '/cosmic.conf';
use constant SERVER_TMP_DIR   => '/tmp/cosmic-server';
use constant SERVER_LOCK_FILE => SERVER_TMP_DIR . 'lockfile';

__PACKAGE__->mk_accessors(qw(device_prefix iqn_host force));

sub instantiate {
    my $klass = __PACKAGE__ . '::' . ucfirst $^O;
    local $@ = undef;
    eval "require $klass";
    die $@ if $@;
    $klass->new;
}

sub new {
    my $klass = shift;
    my $self = bless {}, $klass;
    
    # create temporary dir
    unless (mkdir(SERVER_TMP_DIR) || $! == Errno::EEXIST) {
        die "failed to create temporary dir:@{[SERVER_TMP_DIR]}:$!";
    }
    chown 0, 0, SERVER_TMP_DIR
        or die "chown root:root @{[SERVER_TMP_DIR]} failed:$!";
    chmod 0755, SERVER_TMP_DIR
        or die "chmod 755 @{[SERVER_TMP_DIR]} failed:$!";
    
    # read configuration
    my $json = from_json(do {
        open my $fh, '<', SERVER_CONF_FILE
            or die "failed to open file:@{[SERVER_CONF_FILE]}:$!";
        join '', <$fh>;
    });
    $self->_apply_config($json);
    
    $self;
}

sub start {
    my $self = shift;
    $self->_start;
}

sub list {
    my $self = shift;
    my $devices = $self->_devices;
    
    print "$_: $devices->{$_}\n"
        for sort keys %$devices;
}

sub create {
    my $self = shift;
    die "invalid args, see --help"
        unless @ARGV == 2;
    validate_global_name($ARGV[0]);
    $self->_create(@ARGV);
}

sub destroy {
    my $self = shift;
    if ($ARGV[0] eq '--force') {
        $self->force(1);
        shift @ARGV;
    }
    die "invalid args, see --help"
        unless @ARGV == 1;
    validate_global_name($ARGV[0]);
    $self->_destroy(@ARGV);
}

sub resize {
    my $self = shift;
    die "invalid args, see --help"
        unless @ARGV == 2;
    validate_global_name($ARGV[0]);
    $self->_resize(@ARGV);
}

sub change_credentials {
    my $self = shift;
    die "invalid args, see --help"
        unless @ARGV == 3;
    validate_global_name($ARGV[0]);
    $self->_change_credentials(@ARGV);
}

sub _apply_config {
    my ($self, $json) = @_;
    
    for my $n (qw(device_prefix iqn_host)) {
        die "`$n' not defined in @{[SERVER_CONF_FILE]}"
            unless $json->{$n};
        $self->{$n} = $json->{$n};
    }
}

sub _create {
    my ($self, $global_name, $size) = @_;
    
    # lock
    my $global_lock = lock_file(SERVER_LOCK_FILE);
    
    # check existence of the device
    die "device $global_name already exists"
        if $self->_device_exists($global_name);
    
    $self->_print_and_wait("cosmic-ok phase 1\n")
        or return;
    
    # doit
    $self->_create_device($global_name, $size);
    
    print "cosmic-done\n";
    STDOUT->flush;
}

sub _destroy {
    my ($self, $global_name) = @_;
    
    # lock
    my $global_lock = lock_file(SERVER_LOCK_FILE);
    
    # check existence of the device
    die "device $global_name does not exist"
        unless $self->_device_exists($global_name) || $self->force;
    
    $self->_print_and_wait("cosmic-ok phase 1\n")
        or return;
    
    # doit
    $self->_destroy_device($global_name);
    
    print "cosmic-done\n";
    STDOUT->flush;
}

sub _resize {
    my ($self, $global_name, $size) = @_;
    
    # lock
    my $global_lock = lock_file(SERVER_LOCK_FILE);
    
    # check existence of the device
    die "device $global_name does not exist"
        unless $self->_device_exists($global_name);
    
    $self->_print_and_wait("cosmic-ok phase 1\n")
        or return;
    
    # doit
    $self->_resize_device($global_name, $size);
    
    print "cosmic-done\n";
    STDOUT->flush;
}

sub _change_credentials {
    my ($self, $global_name, $new_user, $new_pass) = @_;
    
    # lock
    my $global_lock = lock_file(SERVER_LOCK_FILE);
    
    # check existence of the device
    die "device $global_name does not exist:$!"
        unless $self->_device_exists($global_name);
    
    $self->_print_and_wait("cosmic-ok phase 1\n")
        or return;
    
    # disallow current client, and kill its connection
    $self->_disallow_current($global_name);
    
    $self->_print_and_wait("cosmic-ok phase 2\n")
        or return;
    
    # allow access from new client
    $self->_allow_one($global_name, $new_user, $new_pass);
    
    print "cosmic-done\n";
    STDOUT->flush;
}

sub _device_exists {
    my ($self, $global_name) = @_;
    
    ! ! first { $_ eq $global_name } keys %{$self->_devices};
}

sub _print_and_wait {
    my ($self, $send_msg) = @_;
    
    print $send_msg;
    STDOUT->flush;
    
    my $input = <STDIN>;
    return 1
        if $input =~ /^next\n$/;
    
    chomp $input;
    warn "aborting by client request:$input";
    return;
}

1;

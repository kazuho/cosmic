package App::Cosmic::Server;

use strict;
use warnings;

use Errno ();
use Exporter qw(import);
use IO::Handle;
use IO::Socket::INET;
use JSON qw(from_json);
use Socket qw(SOMAXCONN);

use App::Cosmic;
use base qw(App::Cosmic);

our @EXPORT = qw(SERVER_LOCK_FILE SERVER_TMP_DIR);

use constant SERVER_CONF_FILE => SERVER_CONF_DIR . '/cosmic.conf';
use constant SERVER_TMP_DIR   => '/tmp/cosmic-server';
use constant SERVER_LOCK_FILE => SERVER_TMP_DIR . 'lockfile';

__PACKAGE__->mk_accessors(qw(device_prefix iqn_host));

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
    for my $n (qw(device_prefix iqn_host)) {
        die "`$n' not defined in @{[SERVER_CONF_FILE]}"
            unless $json->{$n};
        $self->{$n} = $json->{$n};
    }
    
    $self;
}

sub start {
    my $klass = shift;
    $klass->new->_start;
}

sub create {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV == 2;
    validate_global_name($ARGV[0]);
    $klass->new->_create(@ARGV);
}

sub remove {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV == 1;
    validate_global_name($ARGV[0]);
    $klass->new->_remove(@ARGV);
}

sub change_credentials {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV == 3;
    validate_global_name($ARGV[0]);
    $klass->new->_change_credentials(@ARGV);
}

sub disconnect {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV == 1;
    validate_global_name($ARGV[0]);
    $klass->new->_disconnect(@ARGV);
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

sub _remove {
    my ($self, $global_name) = @_;
    
    # lock
    my $global_lock = lock_file(SERVER_LOCK_FILE);
    
    # check existence of the device
    die "device $global_name does not exist"
        unless $self->_device_exists($global_name);
    
    $self->_print_and_wait("cosmic-ok phase 1\n")
        or return;
    
    # doit
    $self->_remove_device($global_name);
    
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

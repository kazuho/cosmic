package App::Cosmic::Server;

use strict;
use warnings;

use Errno ();
use Exporter qw(import);
use File::Basename qw(dirname basename);
use IO::Handle;
use IO::Socket::INET;
use JSON qw(from_json);
use Socket qw(SOMAXCONN);

use App::Cosmic;
use base qw(App::Cosmic);

our @EXPORT = qw(GLOBAL_LOCK_FILE);

use constant SERVER_CONF_FILE => SERVER_CONF_DIR . '/cosmic.conf';
use constant SERVER_TMP_DIR   => '/tmp/cosmic-server';
use constant CRED_LOCK_FILE   => SERVER_TMP_DIR . '/cred.lock';
use constant DUMMY_USERNAME   => 'dummyuser';
use constant DISABLE_PASSWORD => 'neverconnect00';

__PACKAGE__->mk_accessors(qw(device_prefix iqn_host));

sub new {
    my $klass = shift;
    my $self = bless {}, $klass;
    
    # create temporary dir
    unless (mkdir SERVER_TMP_DIR || $! == Errno::EEXIST) {
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

sub _create {
    my ($self, $global_name, $size) = @_;
    
    # lock
    my $global_lock = lock_file(CRED_LOCK_FILE);
    
    # check existence of the device
    die "device $global_name already exists"
        if -e $self->device_prefix . $global_name;
    
    # create lv
    $self->_print_and_wait("cosmic-ok phase 1\n")
        or return;
    my $lvpath = $self->device_prefix . $global_name;
    my $vgpath = dirname $lvpath;
    my $lvname = basename $lvpath;
    systeml(
        qw(lvcreate),
        "--size=$size",
        "--name=$lvname",
        $vgpath,
    ) == 0
        or die "lvm failed:$?";
    # start
    $self->_register_device($global_name, DUMMY_USERNAME, DISABLE_PASSWORD);
    
    print "cosmic-done\n";
    STDOUT->flush;
}

sub _change_credentials {
    my ($self, $global_name, $new_user, $new_pass) = @_;
    
    # lock
    my $global_lock = lock_file(CRED_LOCK_FILE);
    
    # check existence of the device
    die "device $global_name does not exist:$!"
        unless -e $self->device_prefix . $global_name;
    
    # reset password and stop server, so that there will be no connections
    $self->_print_and_wait("cosmic-ok phase 1\n")
        or return;
    $self->_reflect_credentials_of(
        $global_name,
        ($self->_get_credentials_of($global_name))[0],
        DISABLE_PASSWORD,
    );
    $self->_set_credentials_of($global_name);
    $self->_disconnect($global_name);
    
    # update passphrase
    $self->_print_and_wait("cosmic-ok phase 2\n")
        or return;
    $self->_set_credentials_of($global_name, $new_user, $new_pass);
    $self->_reflect_credentials_of($global_name, $new_user, $new_pass);
    
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

sub _devices {
    my $self = shift;
    
    map {
        substr $_, length $self->device_prefix;
    } glob $self->device_prefix . '*';
}

sub _credentials_file_of {
    my ($self, $global_name) = @_;
    SERVER_TMP_DIR . "/$global_name.cred";
}

1;

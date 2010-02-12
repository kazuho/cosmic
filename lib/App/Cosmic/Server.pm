package App::Cosmic::Server;

use strict;
use warnings;

use Exporter qw(import);
use File::Basename qw(basename);
use IO::Handle;
use IO::Socket::INET;
use JSON qw(from_json);
use Socket qw(SOMAXCONN);

use App::Cosmic;
use base qw(App::Cosmic);

our @EXPORT = qw(GLOBAL_LOCK_FILE);

use constant CRED_LOCK_FILE => SERVER_CONF_DIR . '/cred.lock';
use constant DUMMY_USERNAME   => 'dummyuser';
use constant DISABLE_PASSWORD => 'neverconnect00';

__PACKAGE__->mk_accessors(qw(device_prefix iqn_host));

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
    for my $n (qw(device_prefix iqn_host)) {
        die "`$n' not defined in cosmic.conf"
            unless $json->{$n};
        $self->{$n} = $json->{$n};
    }
    
    $self;
}

sub start {
    my $klass = shift;
    $klass->new->_start;
}

sub change_credentials {
    my $klass = shift;
    die "invalid args, see --help"
        unless @ARGV == 3;
    $klass->new->_change_credentials(@ARGV);
}

sub disconnect {
    my $klass = shift;
    $klass->new->_disconnect;
}

sub _change_credentials {
    my ($self, $global_name, $new_user, $new_pass) = @_;
    
    # FIXME validate $global_name
    
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
    write_file($self->_credentials_file_of($global_name), '');
    $self->_kill_connections_of($global_name);
    
    # update passphrase
    $self->_print_and_wait("cosmic-ok phase 2\n")
        or return;
    write_file(
        $self->_credentials_file_of($global_name),
        "$new_user $new_pass",
    );
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

sub _disconnect {
    my $self = shift;
    
    die "invalid args, see --help"
        unless @ARGV == 1;
    my $global_name = shift @ARGV;
    $self->_kill_connections_of($global_name);
}

sub _get_credentials_of {
    my ($self, $global_name) = @_;
    my $line = read_oneline($self->_credentials_file_of($global_name));
    $line ? split(/ /, $line, 2) : (DUMMY_USERNAME, DISABLE_PASSWORD);
}

sub _credentials_file_of {
    my ($self, $global_name) = @_;
    SERVER_CONF_DIR . "/$global_name.cred";
}

1;

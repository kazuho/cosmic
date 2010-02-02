package App::Cosmic::Server;

use strict;
use warnings;

use IO::Handle;

use App::Cosmic;
use base qw(App::Cosmic);

use constant LOCK_FILE => SERVER_CONF_DIR . '/lock';

sub new {
    my $klass = shift;
    bless {}, $klass;
}

sub change_client {
    __PACKAGE__->_change_client;
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
    my $lockfh = lock_file(LOCK_FILE);
    
    # prepare new file
    my $newfn = SERVER_CONF_DIR . "ipof.tmp";
    {
        open my $fh, '>', $newfn
            or die "failed to create file:$newfn:$!";
        print $fh $new_addr;
        $fh->sync
            or die "failed to fsync file:$newfn:$!";
    }
    
    # kill current
    _kill_server($global_name);
    
    # ready, wait for client
    print "cosmic-change-client-is-ready\n";
    STDOUT->flush;
    
    my $input = <STDIN>;
    unless ($input =~ /^commit\n$/) {
        unlink $newfn;
        warn "client did not request commit, aborting...\n";
        return;
    }
    
    # rename
    rename $newfn, _ipof_file_of($global_name)
        or die "failed to rename(2) to:@{[_ipof_file_of($global_name)]}:$!";
    
    # sync dir
    sync_dir(SERVER_CONF_DIR);
}

sub _kill_server {
    my ($self, $global_name) = @_;
    
    # precond: should hold lock on LOCK_FILE
    
    if (my $pid_file = _pid_file_of($global_name)) {
        if (my $pid_lock = lock_file("$pid_file.lock", 1)) {
            # server's running
            my $pid = do {
                open my $fh, '<', $pid_file
                    or die "failed to open file:$pid_file:$!";
                <$fh>;
            };
            chomp $pid;
            kill 'KILL', $pid;
            lock_file("$pid_file.lock");
        }
        unlink $pid_file;
    }
}

1;

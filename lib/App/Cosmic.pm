package App::Cosmic;

use strict;
use warnings;

use Errno ();
use Exporter qw(import);
use Fcntl qw(:flock);
use File::Basename qw(dirname);
use IO::Handle;
use POSIX qw(:fcntl_h);

our @EXPORT = (
    qw(CLIENT_CONF_DIR SERVER_CONF_DIR NBD_PORT mk_accessors read_oneline),
    qw(write_file sync_dir lock_file systeml to_iqn),
);

use constant CLIENT_CONF_DIR => '/etc/cosmic/client';
use constant SERVER_CONF_DIR => '/etc/cosmic/server';
use constant NBD_PORT => 12349;

sub mk_accessors {
    my $pkg = shift;
    for my $n (@_) {
        no strict 'refs';
        *{"$pkg\::$n"} = sub {
            my $self = shift;
            $self->{$n} = shift if @_;
            $self->{$n};
        };
    }
}

sub read_oneline {
    my $fn = shift;
    open my $fh, '<', $fn
        or die "failed to open file:$fn:$!";
    my $line = <$fh>;
    $line = ''
        unless defined $line;
    chomp $line;
    close $fh;
    $line;
}

sub write_file {
    my ($fn, $data) = @_;
    
    # write temporary file and fsync
    my $tmpfn = dirname($fn) . ".tmp.$$";
    open my $fh, '>', $tmpfn
        or die "failed to open file:$tmpfn:$!";
    print $fh $data;
    $fh->sync
        or die "fsync(2) failed:$!";
    close $fh;
    # rename
    rename $tmpfn, $fn
        or die "falied to rename file $tmpfn to $fn:$!";
    # sync directory
    sync_dir(dirname $fn);
}

sub sync_dir {
    # http://d.hatena.ne.jp/kazuhooku/20100202/1265106190
    my $dir = shift;
    sysopen my $d, $dir, O_RDONLY
        or die "failed to open directory:$dir:$!";
    open my $d2, '>&', fileno($d)
        or die "dup(2) failed:$!";
    $d2->sync
        or die "fsync(2) failed:$!";
}

sub lock_file {
    my ($fn, $nonblock) = @_;
    sysopen my $fh, $fn, O_RDWR | O_CREAT
        or die "failed to open lock file:$fn:$!";
    flock $fh, LOCK_EX
        and return $fh;
    if ($! == Errno::EWOULDBLOCK) {
        return;
    }
    die "flock(LOCK_EX) failed on file:$fn:$!";
}

sub systeml {
    my @cmd = @_;
    print join(' ', @cmd), "\n";
    system(@cmd);
}

sub to_iqn {
    my ($host, $ident) = @_;
    "iqn.2010-02.arpa.in-addr.$host:$ident";
}

1;

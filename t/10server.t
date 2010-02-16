use strict;
use warnings;

use IPC::Open2;
use Test::More;

BEGIN {
    plan skip_all => '$TEST_SERVER not defined'
        unless $ENV{TEST_SERVER};
    plan skip_all => 'test not applicable for this platform'
        unless $^O eq 'linux' || $^O eq 'solaris';
};

use App::Cosmic;

my $server;

my %DEFAULT_HOOKS = (
    pre_start => sub {},
);
my %hooks = (
    linux => {
        %DEFAULT_HOOKS,
        ISCSITARGET   => '/etc/init.d/iscsitarget',
        init          => sub {
            require App::Cosmic::Server::IET;
            App::Cosmic::Server::IET->new;
        },
        pre_start     => sub {
            my $self = shift;
            systeml($self->{ISCSITARGET}, 'stop') == 0
                or die "iscsitarget failed:$?";
            systeml($self->{ISCSITARGET}, 'start') == 0
                or die "iscsitarget failed:$?";
        },
        volume_exists => sub {
            my ($self, $global_name, $not_exists) = @_;
            my $f = $not_exists ? sub { ! $_[0] } : sub { $_[0] };
            my $s = $not_exists ? ' not' : '';
            $server->_device_exists($global_name);
            ok $f->($server->_device_exists($global_name)), "volume$s exists";
            ok $f->(
                0 != grep {
                    $_->{name} eq to_iqn($server->iqn_host, $global_name)
                } $server->_read_iet_session,
            ), "volume$s registered as iscsi target";
        },
    },
);

sub run_hook {
    my $n = shift;
    my $f = $hooks{$^O}->{$n}
        or die "hook '$n' not defined for $^O";
    $f->($hooks{$^O}, @_);
}

sub run_phased {
    my ($cmd, $lock_cnt) = @_;
    my $major_cmd = (split / /, $cmd, 2)[0];
    my $pid = open2(
        my $infh,
        my $outfh,
        "cosmic-server $cmd 2>&1",
    ) or die "failed to invoke cosmic-server $major_cmd:$?";
    $outfh->print("next\n" x $lock_cnt);
    my $indata = join '', <$infh>;
    print $indata;
    like $indata, qr/\ncosmic-done\n$/m, "$major_cmd returns success";
    close $infh, close $outfh;
    while (waitpid($pid, 0) == -1) {}
    is $?, 0, 'major_cmd exits normally';
}

# init
$| = 1;
$server = run_hook('init');

# start
run_hook('pre_start');
systeml(qw(cosmic-server start)) == 0
    or die "cosmic-server start failed:$?";

# create new volume
run_phased('create test9999 100M', 1);
run_hook('volume_exists', 'test9999');

# change credentials
run_phased('change-credentials test9999 aaa bb', 2);
# TODO check if actually changed

# disconnect
systeml(qw(cosmic-server disconnect test9999)) == 0
    or die "cosmic-server disconnect failed:$?";
# TODO check if actually disconnected

# remove disk
run_phased('remove test9999', 1);
run_hook('volume_exists', 'test9999', 1);

done_testing;

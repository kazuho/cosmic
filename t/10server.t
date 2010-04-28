use strict;
use warnings;

use Cwd qw(abs_path);
use IPC::Open2;
use Test::More;

BEGIN {
    plan skip_all => '$TEST_SERVER not defined'
        unless $ENV{TEST_SERVER};
    plan skip_all => 'test not applicable for this platform'
        unless $^O eq 'linux' || $^O eq 'solaris';
};

use App::Cosmic;
use App::Cosmic::Server;

$| = 1;
$ENV{PATH} = "@{[abs_path('blib/script')]}:$ENV{PATH}";

my $server = App::Cosmic::Server->instantiate;

my %DEFAULT_HOOKS = (
    pre_start     => sub {},
    volume_exists => sub {
        my ($self, $global_name, $not_exists) = @_;
        my $f = $not_exists ? sub { ! $_[0] } : sub { $_[0] };
        my $s = $not_exists ? ' not' : '';
        ok $f->($server->_device_exists($global_name)), "volume$s exists";
        ok $f->($self->{is_exported}->($self, $global_name)), "volume is$s exported";
    },
);
my %hooks = (
    linux => {
        %DEFAULT_HOOKS,
        ISCSITARGET     => '/etc/init.d/iscsitarget',
        pre_start       => sub {
            my $self = shift;
            systeml($self->{ISCSITARGET}, 'stop') == 0
                or die "iscsitarget failed:$?";
            systeml($self->{ISCSITARGET}, 'start') == 0
                or die "iscsitarget failed:$?";
        },
        is_exported     => sub {
            my ($self, $global_name) = @_;
            0 != grep {
                $_->{name} eq to_iqn($server->iqn_host, $global_name)
            } $server->_read_iet_session;
        },
        get_volume_size => sub {
            die "NYI";
        },
    },
    solaris => {
        %DEFAULT_HOOKS,
        is_exported => sub {
            my ($self, $global_name) = @_;
            $server->_device_exists($global_name);
        },
        get_volume_size => sub {
            my ($self, $global_name) = @_;
            $server->_sbd_list->{
                $server->_guid_from_global_name($global_name)
            }->{size};
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
        "$^X -Iblib/lib blib/script/cosmic-server $cmd 2>&1",
    ) or die "failed to invoke cosmic-server $major_cmd:$?";
    $outfh->print("next\n" x $lock_cnt);
    my $indata = join '', <$infh>;
    print $indata;
    like $indata, qr/\ncosmic-done\n$/m, "$major_cmd returns success";
    close $infh, close $outfh;
    while (waitpid($pid, 0) == -1) {}
    is $?, 0, 'major_cmd exits normally';
}

# start
run_hook('pre_start');
systeml(qw(cosmic-server start)) == 0
    or die "cosmic-server start failed:$?";

# create new volume
run_phased('create test9999 100M', 1);
run_hook('volume_exists', 'test9999');

# change credentials
$ENV{ITOR} = "iqn.2010-02.com.example.nonexistent";
run_phased('change-credentials test9999 aaa ABCDEFGHabcd', 2);
# TODO check if actually changed

# resize
if ($^O eq 'solaris') {
    run_phased('resize test9999 200M', 1);
    is(
        run_hook('get_volume_size', 'test9999'),
        200 * 1024 * 1024,
        'check volume size',
    );
}

# remove disk
run_phased('destroy test9999', 1);
run_hook('volume_exists', 'test9999', 1);

done_testing;

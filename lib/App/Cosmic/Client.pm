package App::Cosmic::Client;

use strict;
use warnings;

use IO::Handle;
use IPC::Open2;
use JSON qw(from_json);

use App::Cosmic;
use base qw(App::Cosmic);

__PACKAGE__->mk_accessors(qw(def def_file device_file));

sub new {
    my $klass = shift;
    my $self = bless {}, $klass;
    
    # setup
    die "invalid args, see --help"
        unless @ARGV == 2;
    $self->def_file(shift @ARGV);
    $self->device_file(shift @ARGV);
    $self->device_file(CLIENT_CONF_DIR . '/' . $self->device_file)
        unless $self->device_file =~ m|/|;
    $self->def(from_json(do {
        open my $fh, '<', $self->def_file
            or die "failed to open definition file:@{[$self->def_file]}:$!";
        join '', <$fh>;
    }));
    die "no `uuid' in definition"
        unless $self->def->{global_name};
    die "no `nodes' array in definition"
        unless $self->def->{nodes} && ref $self->def->{nodes} eq 'ARRAY';
    
    $self;
}

sub connect {
    __PACKAGE__->new->_connect;
}

sub _connect {
    my $self = shift;
    
    print "registering my IP address to disk servers...\n";
    $self->_change_client();
    
    print "mounting disks...\n";
    $self->_mount;
    
    print "setting up RAID array...\n";
    $self->_setup_raid;
}

sub _change_client {
    my ($self, $new_addr) = @_;
    
    # update all nodes to new_addr using 2pc
    my %nodes = map {
        $_ => {}, # pid: pid, out: output to remote, in: input from remote
    } @{$self->def->{nodes}};
    
    # spawn and check
    for my $node (sort keys %nodes) {
        $nodes{$node}->{pid} = open3(
            $nodes{$node}->{out},
            $nodes{$node}->{in},
            'ssh',
            "root\@$node",
            qw(exec cosmic change-client),
            $self->def->{global_name},
            (defined $new_addr ? ($new_addr) : ()),
            '2>&1',
        ) or die "open3 failed:$!";
        my $line = $nodes{$node}->{in}->getline;
        unless ($line =~ /^cosmic-change-client-is-ready/) {
            die join(
                '',
                "$node rejected update:\n",
                $line, $nodes{$node}->{in}->getlines,
            );
        }
    }
    
    # commit
    for my $node (sort keys %nodes) {
        $nodes{$node}->{out}->print("commit\n");
        $nodes{$node}->{out}->close;
    }
    
    # check response
    for my $node (sort keys %nodes) {
        my $lines = $nodes{$node}->{in}->getlines;
        die "unexpected response from node:$node:\n$lines"
            if length $lines != 0;
        while (waitpid($nodes{$node}->{pid}, 0) >= 0) {}
        die "child process exitted with $? while takling to node:$node"
            if $? != 0;
    }
}

sub _mount {
    my $self = shift;
    
    # mount disk
    for my $node (sort @{$self->def->{nodes}}) {
        my $dev = $self->_device_file_of($node);
        _system('nbd-client', $node, NBD_PORT, $dev) == 0
            or die "failed to connect nbd node to:$dev:$?";
    }
}

sub _setup_raid {
}

sub _device_file_of {
    my ($self, $node) = @_;
    # FIXME support mounting multiple volumes from single host:port, this is
    # a limitation in ndb protocol
    return "/dev/cosmic-$node";
}

sub _system {
    my @cmd = @_;
    print join(' ', @cmd), "\n";
    system(@cmd);
}

1;

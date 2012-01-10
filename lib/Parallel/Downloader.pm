use strictures;

package Parallel::Downloader;

# VERSION

# ABSTRACT: simple downloading of multiple files at once

# COPYRIGHT

=head1 SYNOPSIS

    use HTTP::Request::Common qw( GET POST );
    use Parallel::Downloader 'async_download';

    # simple example
    my @requests = map GET( "http://google.com" ), ( 1..15 );
    my @responses = async_download( requests => \@requests );

    # complex example
    my @complex_reqs = ( ( map POST( "http://google.com", [ type_id => $_ ] ), ( 1..60 ) ),
                       ( map POST( "http://yahoo.com", [ type_id => $_ ] ), ( 1..60 ) ) );

    my $downloader = Parallel::Downloader->new(
        requests => \@complex_reqs,
        workers => 50,
        conns_per_host => 12,
        aehttp_args => {
            timeout => 30,
            on_prepare => sub {
                print "download started ($AnyEvent::HTTP::ACTIVE / $AnyEvent::HTTP::MAX_PER_HOST)\n"
            }
        },
        debug => 1,
        logger => sub {
            my ( $downloader, $message ) = @_;
            print "downloader sez [$message->{type}]: $message->{msg}\n";
        },
    );
    my @complex_responses = $downloader->run;

=cut

use Moo;
use MooX::Types::MooseLike qw( Bool Int HashRef CodeRef ArrayRef );

sub {
    has requests => ( is => 'ro', isa => ArrayRef, required => 1, coerce => \&_interleave_by_host );
    has workers        => ( is => 'ro', isa => Int,     default => sub { 10 } );
    has conns_per_host => ( is => 'ro', isa => Int,     default => sub { 4 }, trigger => \&_init_workers_per_host );
    has aehttp_args    => ( is => 'ro', isa => HashRef, default => sub { {} } );
    has debug          => ( is => 'ro', isa => Bool,    default => sub { 0 } );
    has logger         => ( is => 'ro', isa => CodeRef, default => sub { \&_default_log } );

    has _responses => ( is => 'ro', isa => ArrayRef, default => sub { [] } );
    has _cv => ( is => 'ro', isa => sub { $_[0]->isa( 'AnyEvent::CondVar' ) }, default => sub { AnyEvent->condvar } );
  }
  ->();

use AnyEvent::HTTP;
use Sub::Exporter::Simple 'async_download';

=head1 FUNCTIONS

=head2 async_download

Can be requested to be exported, will instantiate a Parallel::Downloader
object with the given parameters, run it and return the results.

=cut

sub async_download {
    return __PACKAGE__->new( @_ )->run;
}

sub _init_workers_per_host {
    my ( undef, $limit ) = @_;
    $AnyEvent::HTTP::MAX_PER_HOST = $limit;
    return;
}

sub _interleave_by_host {
    my ( $requests ) = @_;

    my %hosts;
    for ( @{$requests} ) {
        my $host_name = $_->uri->host;
        my $host = $hosts{$host_name} ||= [];
        push @{$host}, $_;
    }

    my @interleaved_list;
    while ( keys %hosts ) {
        push @interleaved_list, shift @{$_} for values %hosts;
        for ( keys %hosts ) {
            next if @{ $hosts{$_} };
            delete $hosts{$_};
        }
    }

    return \@interleaved_list;
}

=head1 METHODS

=head2 run

Runs the downloads for the given parameters and returns an array of array
references, each containing the decoded contents, the headers and the
HTTP::Request object.

=cut

sub run {
    my ( $self ) = @_;

    for ( 1 .. $self->_sanitize_worker_max ) {
        $self->_cv->begin;
        $self->_log( msg => "$_ started", type => "WorkerStart", worker_id => $_ );
        $self->_add_request( $_ );
    }

    $self->_cv->recv;

    return @{ $self->_responses };
}

sub _add_request {
    my ( $self, $worker_id ) = @_;

    my $req = shift @{ $self->requests };
    return $self->_end_worker( $worker_id ) if !$req;

    my $post_download_sub = $self->_make_post_download_sub( $worker_id, $req );

    http_request(
        $req->method,
        $req->uri->as_string,
        body    => $req->content,
        headers => $req->{_headers},
        %{ $self->aehttp_args },
        $post_download_sub
    );

    my $host_name = $req->uri->host;
    $self->_log(
        msg       => "$worker_id accepted new request for $host_name",
        type      => "WorkerRequestAdd",
        worker_id => $worker_id,
        req       => $req
    );

    return;
}

sub _make_post_download_sub {
    my ( $self, $worker_id, $req ) = @_;

    my $post_download_sub = sub {
        push @{ $self->_responses }, [ @_, $req ];

        my $host_name = $req->uri->host;
        $self->_log(
            msg       => "$worker_id completed a request for $host_name",
            type      => "WorkerRequestEnd",
            worker_id => $worker_id,
            req       => $req
        );

        $self->_add_request( $worker_id );
        return;
    };

    return $post_download_sub;
}

sub _end_worker {
    my ( $self, $worker_id ) = @_;
    $self->_log( msg => "$worker_id ended", type => "WorkerEnd", worker_id => $worker_id );
    $self->_cv->end;
    return;
}

sub _sanitize_worker_max {
    my ( $self ) = @_;

    die "max should be 0 or more" if $self->workers < 0;

    my $request_count = @{ $self->requests };

    return $request_count if !$self->workers;                    # 0 = as many parallel as possible
    return $request_count if $self->workers > $request_count;    # not more than the request count

    return $self->workers;
}

sub _log {
    my ( $self, %msg ) = @_;
    return if !$self->debug;
    $self->logger->( $self, \%msg );
    return;
}

sub _default_log {
    my ( $self, $msg ) = @_;
    print "$msg->{msg}\n";
    return;
}

1;

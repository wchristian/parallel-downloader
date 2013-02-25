use strictures;

package Parallel::Downloader;

# VERSION

# ABSTRACT: simply download multiple files at once

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

=head1 DESCRIPTION

This is not a library to build a parallel downloader on top of. It is a
downloading client build on top of AnyEvent::HTTP.

Its goal is not to be better, faster, or smaller than anything else. Its goal is
to provide the user with a single function they can call with a bunch of HTTP
requests and which gives them the responses for them with as little fuss as
possible and most importantly, without downloading them in sequence.

It handles the busywork of grouping requests by hosts and limiting the amount of
simultaneous requests per host, separate from capping the amount of overall
connections. This allows the user to maximize their own connection without
abusing remote hosts.

Of course, there are facilities to customize the exact limits employed and to
add logging and such; but C<async_download> is the premier piece of API and
should be enough for most uses.

=cut

use Moo;
use MooX::Types::MooseLike::Base qw( Bool Int HashRef CodeRef ArrayRef );

sub {
    has requests       => ( is => 'ro', isa => ArrayRef, required => 1 );
    has workers        => ( is => 'ro', isa => Int,      default  => sub { 10 } );
    has conns_per_host => ( is => 'ro', isa => Int,      default  => sub { 4 } );
    has aehttp_args    => ( is => 'ro', isa => HashRef,  default  => sub { {} } );
    has debug          => ( is => 'ro', isa => Bool,     default  => sub { 0 } );
    has logger         => ( is => 'ro', isa => CodeRef,  default  => sub { \&_default_log } );
    has build_response => ( is => 'ro', isa => CodeRef,  default  => sub { \&_default_build_response } );
    has sorted         => ( is => 'ro', isa => Bool,     default  => sub { 1 } );

    has _responses => ( is => 'ro', isa => ArrayRef, default => sub { [] } );
    has _cv => ( is => 'ro', isa => sub { $_[0]->isa( 'AnyEvent::CondVar' ) }, default => sub { AnyEvent->condvar } );
  }
  ->();

use AnyEvent::HTTP;
use Sub::Exporter::Simple 'async_download';

=head1 FUNCTIONS

=head2 async_download

Can be requested to be exported, will instantiate a Parallel::Downloader object
with the given parameters, run it and return the results. Its parameters are as
follows:

=head3 requests (required)

Reference to an array of HTTP::Request objects, all of which will be downloaded.

=head3 aehttp_args

A reference to a hash containing arguments that will be passed to
AnyEvent::HTTP::http_request.

Default is an empty hashref.

=head3 conns_per_host

Sets the number of connections allowed per host by changing the corresponding
AnyEvent::HTTP package variable.

Default is '4'.

=head3 debug

A boolean that determines whether logging operations are a NOP or actually run.
Set to any true value to activate the logging.

Default is '0'.

=head3 logger

A reference to a sub that will receive a hash containing logging information.
Whether that sub then prints them to screen or into a database or other targets
is up to the user.

Default is a sub that prints to the screen.

=head3 workers

The amount of workers to be used for downloading. Useful for controlling the
global amount of connections your machine will try to establish.

Default is '10'.

=head3 build_response

A reference to a sub that will be called on completion of a request to build the
response variable that will be returned for this request. It receives as
parameters the body of the response, a hash ref of the response headers and the
original request.

Default is a sub that returns the parameters wrapped in an array reference.

=head3 sorted

A boolean that determines whether the returned responses are sorted in the same
order as the input requests. Can be useful to disable if build_response was
overridden to not return an array or not return the request as the third element
of the response array.

Default is '1'.

=cut

sub async_download {
    return __PACKAGE__->new( @_ )->run;
}

sub _requests_interleaved_by_host {
    my ( $self, $requests ) = @_;

    my %hosts;
    for ( @{ $self->requests } ) {
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

    local $AnyEvent::HTTP::MAX_PER_HOST = $self->conns_per_host;

    my $consumable_list = $self->_requests_interleaved_by_host;

    for ( 1 .. $self->_sanitize_worker_max ) {
        $self->_cv->begin;
        $self->_log( msg => "$_ started", type => "WorkerStart", worker_id => $_ );
        $self->_add_request( $_, $consumable_list );
    }

    $self->_cv->recv;

    return @{ $self->_responses } if !$self->sorted;

    my %unsorted = map { 0 + $_->[2] => $_ } @{ $self->_responses };
    my @sorted = map { $unsorted{ 0 + $_ } } @{ $self->requests };

    return @sorted;
}

sub _add_request {
    my ( $self, $worker_id, $requests ) = @_;

    my $req = shift @{$requests};
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
        push @{ $self->_responses }, $self->build_response->( @_, $req );

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

sub _default_build_response {
    my ( $body, $hdr, $req ) = @_;
    return [ $body, $hdr, $req ];
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

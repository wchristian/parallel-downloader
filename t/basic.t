use strictures;

package basic_test;

use Test::InDistDir;
use Test::More;

use Parallel::Downloader 'async_download';
use HTTP::Request::Common qw( GET );

run();
done_testing;
exit;

sub run {
    my @results =
      async_download( requests => [ map GET( $_ ), qw( http://google.de http://google.com http://website.broke  ) ] );

    is( $results[$_][1]{Status}, 200 ) for ( 0, 1 );
    is( $results[2][0], undef );

    return;
}

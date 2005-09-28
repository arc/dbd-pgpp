# Test fetching

use Test::More;
use DBI;
use strict;
$|=1;

my @len = (1, 10, 50, 100, 1000, 1467, 1468, 2000, 3000, 4000, 4466,
           4467, 5000, 10000, 20000, 30000, 40000);

# 1487 goes into an infinite loop; apparently, so does 1487 + (N * 1500) for
# integer N
my %bad_len = map { $_ => 1 } 1487, 2987, 4487;
push @len, sort keys %bad_len;

if (defined $ENV{DBI_DSN}) {
    plan tests => 1 + 3 * @len;
}
else {
    plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file.';
}

my $db = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
                       {RaiseError => 0, PrintError => 0, AutoCommit => 1});

ok(defined $db, "Connect to database for testing result fetches");


Length: for (@len) {
  SKIP: {
        skip "Not testing for infinite-loop bug with length $_", 3
            if $bad_len{$_};

        my $value = $db->selectrow_array(qq[SELECT Repeat('a', $_)]);
        ok(defined $value, "Long result row returned ($_)");
        is(length $value, $_, "Long result row has correct length ($_)");
        is($value, 'a' x $_, "Long result row of $_ bytes has correct value");
    }
}

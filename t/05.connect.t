if (!exists($ENV{PG_TEST_DB}) || !exists($ENV{PG_TEST_USER})) {
	print "1..0 # Skipped: Please set an environment variable require for a test. Refer to README.\n";
	exit 0;
}


use DBI;
use strict;

print "1..1\n";
my $n = 1;

my $dbh = DBI->connect("dbi:PgPP:dbname=$ENV{PG_TEST_DB};host=$ENV{PG_TEST_HOST}", $ENV{PG_TEST_USER}, $ENV{PG_TEST_PASS});
$dbh->disconnect;
print "ok $n\n";

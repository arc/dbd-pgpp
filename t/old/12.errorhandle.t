if (!exists($ENV{PG_TEST_DB}) || !exists($ENV{PG_TEST_USER})) {
	print "1..0 # Skipped: Please set an environment variable require for a test. Refer to README.\n";
	exit 0;
}

use DBI;
use strict;

print "1..3\n";
my $n = 1;

my $pgsql;
eval {
	$pgsql = DBI->connect(
		"dbi:PgPP:dbname=$ENV{PG_TEST_DB};host=$ENV{PG_TEST_HOST}",
		$ENV{PG_TEST_USER}, $ENV{PG_TEST_PASS}, {
			RaiseError => 1, PrintError => 0
	}) or die $DBI::errstr;
};
print 'not ' if $@;
print "ok $n\n"; $n++;


eval {
	my $sth = $pgsql->prepare(q{SELECT * FROM unknowntable});
	$sth->execute();
};
print "not " unless defined $pgsql && $pgsql->errstr && $@;
print "ok $n\n"; $n++;

eval {
	$pgsql->disconnect;
};
print 'not ' if $@;
print "ok $n\n";

1;

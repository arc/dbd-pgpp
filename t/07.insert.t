if (!exists($ENV{PG_TEST_DB}) || !exists($ENV{PG_TEST_USER})) {
	print "1..0 # Skipped: Please set an environment variable require for a test. Refer to README\n";
	exit 0;
}

use DBI;
use strict;

print "1..4\n";
my $n = 1;

my $pgsql = DBI->connect(
	"dbi:PgPP:dbname=$ENV{PG_TEST_DB};host=$ENV{PG_TEST_HOST}",
	$ENV{PG_TEST_USER}, $ENV{PG_TEST_PASS}, {
		RaiseError => 1,
});
print "ok $n\n"; $n++;

eval {
	$pgsql->do(q{
		INSERT INTO test (id, name, value) VALUES (1, 'foo', 'horse')
	});
	$pgsql->do(q{
		INSERT INTO test (id, name, value) VALUES (2, 'bar', 'chicken')
	});
	$pgsql->do(q{
		INSERT INTO test (id, name, value) VALUES (3, 'baz', 'pig')
	});
};
print "not " if $@;
print "ok $n\n"; $n++;

my $rows = 0;
eval {
	my $sth = $pgsql->prepare(q{SELECT COUNT(id) FROM test});
	$sth->execute;
	while (my $record = $sth->fetch()) {
		$rows = $record->[0];
	}
};
print "not " if $@ || $rows != 3;
print "ok $n\n"; $n++;

$pgsql->disconnect;
print "ok $n\n";

1;

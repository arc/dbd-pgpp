if (!exists($ENV{PG_TEST_DB}) || !exists($ENV{PG_TEST_USER})) {
	print "1..0 # Skipped: Please set an environment variable require for a test. Refer to README.\n";
	exit 0;
}


use DBI;
use strict;

print "1..4\n";
my $n = 1;


my $pgsql;
eval {
	$pgsql = DBI->connect(
		"dbi:PgPP:dbname=$ENV{PG_TEST_DB};host=$ENV{PG_TEST_HOST}",
		$ENV{PG_TEST_USER}, $ENV{PG_TEST_PASS}, {
			RaiseError => 1, PrintError => 0
	});
};
print 'not ' if $@;
print "ok $n\n"; $n++;

eval {
	$pgsql->do(q{DROP TABLE test});
};
print "not " if $@;
print "ok $n\n"; $n++;

my $rows = 0;
eval {
	my $sth = $pgsql->prepare(q{
		SELECT id, name FROM test WHERE id = 1
	});
	$sth->execute;
	while (my $record = $sth->fetch()) {
		++$rows;
	}
};
print "not " if !defined($@) || $rows > 0;
print "ok $n\n"; $n++;

eval {
	$pgsql->disconnect;
};
print 'not ' if $@;
print "ok $n\n";

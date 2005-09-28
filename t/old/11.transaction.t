if (!exists($ENV{PG_TEST_DB}) || !exists($ENV{PG_TEST_USER})) {
	print "1..0 # Skipped: Please set an environment variable require for a test. Refer to README.\n";
	exit 0;
}


use DBI;
use strict;

print "1..6\n";

my $pgsql;
eval {
	$pgsql = DBI->connect(
		"dbi:PgPP:dbname=$ENV{PG_TEST_DB};host=$ENV{PG_TEST_HOST}",
		$ENV{PG_TEST_USER}, $ENV{PG_TEST_PASS}, {
			RaiseError => 1, AutoCommit => 0,
	});
};
print 'not ' if $@;
print "ok 1\n";

my $rows = 0;
eval {
	my $sth = $pgsql->prepare(q{
		SELECT id, name FROM test
	});
	$sth->execute;
	while (my $record = $sth->fetch()) {
		++$rows;
	}
};
print "ok 2\n";


eval {
	my $row = $pgsql->do(q{INSERT INTO test (id, name, value) VALUES (4, 'hoge', 'mufufu')
	});
	die 'no match' if $row != 1;
};
print "not " if $@;
print "ok 3\n";


my $rows2 = 0;
eval {
	$pgsql->rollback();
	my $sth = $pgsql->prepare(q{SELECT id, name FROM test});
	$sth->execute;
	while (my $record = $sth->fetch()) {
		++$rows2;
	}
};
print "not " if $@ || $rows != $rows2;
print "ok 4\n";

my $rows3 = 0;
eval {
	$pgsql->do(q{INSERT INTO test (id, name, value) VALUES (5, 'hoge', 'mufufu')});
	$pgsql->commit;
	my $sth = $pgsql->prepare(q{SELECT id, name FROM test});
	$sth->execute;
	while (my $record = $sth->fetch()) {
		++$rows3;
	}
};
print "not " if $@ || ($rows2 + 1) != $rows3;
print "ok 5\n";


eval {
	$pgsql->disconnect;
};
print 'not ' if $@;
print "ok 6\n";
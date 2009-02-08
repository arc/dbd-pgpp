# Test fetching

use Test::More;
use DBI;
use strict;

if (defined $ENV{DBI_DSN}) {
    plan tests => 3;
}
else {
    plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file.';
}

my $db = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
                       {RaiseError => 1, PrintError => 0, AutoCommit => 1});

$db->do(q[
    CREATE TEMPORARY TABLE t (id serial primary key, s text not null)
]);

insert($db, 'foo');
is(id($db), 1, "First last_insert_id works");

insert($db, 'bar');
is(id($db), 2, "Second last_insert_id works");

is(id($db), 2, "Repeated last_insert_id works");

sub insert {
    my ($db, $s) = @_;
    $db->do('INSERT INTO t (s) VALUES (?)', undef, $s);
}

sub id {
    my ($db) = @_;
    return $db->last_insert_id(undef, '', 't', undef);
}

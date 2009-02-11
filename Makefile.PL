use ExtUtils::MakeMaker;

use 5.006;

WriteMakefile(
    NAME	 => 'DBD::PgPP',
    VERSION_FROM => 'lib/DBD/PgPP.pm',
    AUTHOR       => 'OYAMA Hiroyuki <oyama@module.jp>',
    test         => { TESTS => 't/*/*.t' },
    PREREQ_PM	 => {
        'DBI'        => 1.59,   # earliest version tested recently
        'Test::More' => 0,
    },
);

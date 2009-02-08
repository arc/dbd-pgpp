use ExtUtils::MakeMaker;

WriteMakefile(
	'NAME'		=> 'DBD::PgPP',
	'VERSION_FROM'	=> 'lib/DBD/PgPP.pm', # finds $VERSION
	'PREREQ_PM'		=> {
		'DBI' => 0.0,
                'Test::More' => 0,
	},
	($] >= 5.005
		? (AUTHOR     => 'OYAMA Hiroyuki <oyama@module.jp>')
		: ()
	),
);

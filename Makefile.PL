use ExtUtils::MakeMaker;

WriteMakefile(
	'NAME'		=> 'DBD::PgPP',
	'VERSION_FROM'	=> 'PgPP.pm', # finds $VERSION
	'PREREQ_PM'		=> {
		'Net::PostgreSQL' => 0.04,
	},
	($] >= 5.005
		? (AUTHOR     => 'OYAMA Hiroyuki <oyama@crayfish.co.jp>')
		: ()
	),
);

package DBD::PgPP;
use strict;

use DBI;
use Carp;
use vars qw($VERSION $err $errstr $state $drh);

$VERSION = '0.01';
$err = 0;
$errstr = '';
$state = undef;
$drh = undef;

sub driver
{
	return $drh if $drh;

	my $class = shift;
	my $attr  = shift;
	$class .= '::dr';

	$drh = DBI::_new_drh($class, {
		Name        => 'PgPP',
		Version     => $VERSION,
		Err         => \$DBD::PgPP::err,
		Errstr      => \$DBD::PgPP::errstr,
		State       => \$DBD::PgPP::state,
		Attribution => 'DBD::PgPP by Hiroyuki OYAMA',
	}, {});
}


sub _parse_dsn
{
	my $class = shift;
	my ($dsn, $args) = @_;
	my($hash, $var, $val);
	return if ! defined $dsn;

	while (length $dsn) {
		if ($dsn =~ /([^:;]*)[:;](.*)/) {
			$val = $1;
			$dsn = $2;
		}
		else {
			$val = $dsn;
			$dsn = '';
		}
		if ($val =~ /([^=]*)=(.*)/) {
			$var = $1;
			$val = $2;
			if ($var eq 'hostname' || $var eq 'host') {
				$hash->{'host'} = $val;
			}
			elsif ($var eq 'db' || $var eq 'dbname') {
				$hash->{'database'} = $val;
			}
			else {
				$hash->{$var} = $val;
			}
		}
		else {
			for $var (@$args) {
				if (!defined($hash->{$var})) {
					$hash->{$var} = $val;
					last;
				}
			}
		}
	}
	return $hash;
}


sub _parse_dsn_host
{
	my($class, $dsn) = @_;
	my $hash = $class->_parse_dsn($dsn, ['host', 'port']);
	($hash->{'host'}, $hash->{'port'});
}



package DBD::PgPP::dr;

$DBD::PgPP::dr::imp_data_size = 0;

use Net::PostgreSQL;
use strict;


sub connect
{
	my $drh = shift;
	my ($dsn, $user, $password, $attrhash) = @_;

	my $data_source_info = DBD::PgPP->_parse_dsn(
		$dsn, ['database', 'host', 'port'],
	);
	$user     ||= '';
	$password ||= '';


	my $dbh = DBI::_new_dbh($drh, {
		Name         => $dsn,
		USER         => $user,
		CURRENT_USRE => $user,
	}, {});
	eval {
		my $pgsql = Net::PostgreSQL->new(
			hostname => $data_source_info->{host},
			port     => $data_source_info->{port},
			database => $data_source_info->{database},
			user     => $user,
			password => $password,
		);
		$dbh->STORE(pgpp_connection => $pgsql);
#		$dbh->STORE(thread_id => $mysql->{server_thread_id});

		if (! $attrhash->{AutoCommit}) {
			my $pgsth = $pgsql->prepare('BEGIN');
			$pgsth->execute();
		}
	};
	if ($@) {
		return $dbh->DBI::set_err(1, $@);
	}
	return $dbh;
}


sub data_sources
{
	return ("dbi:PgPP:");
}


sub disconnect_all {}



package DBD::PgPP::db;

$DBD::PgPP::db::imp_data_size = 0;
use strict;


sub prepare
{
	my $dbh = shift;
	my ($statement, @attribs) = @_;

	my $sth = DBI::_new_sth($dbh, {
		Statement => $statement,
	});
	$sth->STORE(pgpp_handle => $dbh->FETCH('pgpp_connection'));
	$sth->STORE(pgpp_params => []);
	$sth->STORE(NUM_OF_PARAMS => ($statement =~ tr/?//));
	$sth;
}


sub commit
{
	my $dbh = shift;
	my $pgsql = $dbh->FETCH('pgpp_connection');
	eval {
		my $pgsth = $pgsql->prepare('COMMIT');
		$pgsth->execute();
	};
	if ($@) {
		$dbh->DBI::set_err(
			1, $@ #$pgsql->get_error_message
		);
		return undef;
	}
	return 1;
}


sub rollback
{
	my $dbh = shift;
	my $pgsql = $dbh->FETCH('pgpp_connection');
	eval {
		my $pgsth = $pgsql->prepare('ROLLBACK');
		$pgsth->execute();
	};
	if ($@) {
		$dbh->DBI::set_err(
			1, $@ #$pgsql->get_error_message
		);
		return undef;
	}
	return 1;
}



sub disconnect
{
	return 1;
}


sub FETCH
{
	my $dbh = shift;
	my $key = shift;

	return $dbh->{$key} if $key =~ /^(?:pgpp_.*)$/;
	return $dbh->{AutoCommit} if $key =~ /^AutoCommit$/;

	return $dbh->SUPER::FETCH($key);
}


sub STORE
{
	my $dbh = shift;
	my ($key, $value) = @_;

	if ($key =~ /^(?:pgpp_.*|AutoCommit)$/) {
		$dbh->{$key} = $value;
		return 1;
	}
	return $dbh->SUPER::STORE($key, $value);
}


sub DESTROY
{
	my $dbh = shift;
	my $pgsql = $dbh->FETCH('pgpp_connection');
	$pgsql->close;
}


package DBD::PgPP::st;

$DBD::PgPP::st::imp_data_size = 0;
use strict;


sub bind_param
{
	my $sth = shift;
	my ($index, $value, $attr) = @_;
	my $type = (ref $attr) ? $attr->{TYPE} : $attr;
	if ($type) {
		my $dbh = $sth->{Database};
		$value = $dbh->quote($sth, $type);
	}
	my $params = $sth->FETCH('pgpp_param');
	$params->[$index - 1] = $value;
}


sub execute
{
	my $sth = shift;
	my @bind_values = @_;
	my $params = (@bind_values) ?
		\@bind_values : $sth->FETCH('pgpp_params');
	my $num_param = $sth->FETCH('NUM_OF_PARAMS');
	if (@$params != $num_param) {
		# ...
	}
	my $statement = $sth->{Statement};
	for (my $i = 0; $i < $num_param; $i++) {
		my $dbh = $sth->{Database};
		my $quoted_param = $dbh->quote($params->[$i]);
		$statement =~ s/\?/$quoted_param/e;
	}
	my $pgsql = $sth->FETCH('pgpp_handle');
	my $result;
	eval {
		$sth->{pgpp_record_iterator} = undef;
		my $pgsql_sth = $pgsql->prepare($statement);
		$pgsql_sth->execute();
		$sth->{pgpp_record_iterator} = $pgsql_sth;
		my $dbh = $sth->{Database};

		if (defined $pgsql->{affected_rows}) {
			$sth->{pgpp_rows} = $pgsql->{affected_rows};
			$result = $pgsql->{affected_rows};
		}
		else {
			$sth->{pgpp_rows} = 0;
			$result = $pgsql->{affected_rows};
		}
		if ($pgsql->{row_description}) {
			$sth->STORE(NUM_OF_FIELDS => scalar @{$pgsql->{row_description}});
			$sth->STORE(NAME => [ map {$_->{name}} @{$pgsql->{row_description}} ]);
		}
#		$pgsql->get_affected_rows_length;
	};
	if ($pgsql->has_error) {
		$sth->DBI::set_err(1, $pgsql->get_error_message);
		return undef;
	}

	return $pgsql->has_error
		? undef : $result
			? $result : '0E0';
}


sub fetch
{
	my $sth = shift;

	my $iterator = $sth->FETCH('pgpp_record_iterator');
	my $row = $iterator->fetch();
	return undef unless $row;

	if ($sth->FETCH('ChopBlanks')) {
		map {s/\s+$//} @$row;
	}
	return $sth->_set_fbav($row);
}
*fetchrow_arrayref = \&fetch;


sub rows
{
	my $sth = shift;
	$sth->{pgpp_rows};
}


sub FETCH
{
	my $dbh = shift;
	my $key = shift;

#	return $dbh->{AutoCommit} if $key eq 'AutoCommit';
	return $dbh->{NAME} if $key eq 'NAME';
	return $dbh->{$key} if $key =~ /^pgpp_/;
	return $dbh->SUPER::FETCH($key);
}


sub STORE
{
	my $dbh = shift;
	my ($key, $value) = @_;

	if ($key eq 'NAME') {
		$dbh->{NAME} = $value;
		return 1;
	}
	elsif ($key =~ /^pgpp_/) {
		$dbh->{$key} = $value;
		return 1;
	}
	return $dbh->SUPER::STORE($key, $value);
}


sub DESTROY
{
	my $dbh = shift;

}


1;
__END__

=head1 NAME

DBD::PgPP - Pure Perl PostgreSQL database driver for the DBI module

=head1 SYNOPSIS

  use DBI;

  my $dbh = DBI->connect('dbi:PgPP:dbname=$dbname', '', ''');

  # See the DBI module documentation for full details

=head1 DESCRIPTION

DBD::PgPP is a Pure Perl module which works with the DBI module to provide access to PostgreSQL database.

=head1 MODULE DOCUMENTATION

This documentation describes driver specific behavior and restrictions. It is not supposed to be used as the only refference of the user. In any case consult the DBI documentation first !

=head1 THE DBI CLASS

=head2 DBI Class Methods

=over 4

=item B<connect>

To connecto to a database with a minimum of parameters, use the following syntax:
  $dbh = DBI->connect('dbi:PgPP:dbname=$dbname', '', '');

This connects to the database $dbname at localhost without any user authentication. This is sufficient for the defaults of PostgreSQL.

The following connect statement shows all possible parameters:

  $dbh = DBI->connect('dbi:PgPP:dbname=$dbname;host=$host;port=$port', $username, $password);

If a host is specified, the postmaster on this host needs to be started with the C<-i> option (TCP/IP socket).


For authentication with username and password appropriate entries have to be made in pg_hba.conf. Please refer to the L<pg_hba.conf> and the L<pg_passwd> for the different types of authentication.

=back

=head1 SEE ALSO

L<DBI>, L<Net::PostgreSQL>

=head1 AUTHOR

Hiroyuki OYAMA E<lt>oyama@crayfish.co.jpE<gt>

=head1 COPYRIGHT AND LICENCE

Copyright (C) 2002 Hiroyuki OYAMA. Japan. All rights reserved.

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut

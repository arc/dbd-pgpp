
=head1 NAME

DBD::PgPP - Pure Perl PostgreSQL driver for the DBI

=head1 SYNOPSIS

  use DBI;

  my $dbh = DBI->connect('dbi:PgPP:dbname=$dbname', '', ''');

  # See the DBI module documentation for full details

=cut

package DBD::PgPP;
use strict;

use DBI;
use Carp;
use vars qw($VERSION $err $errstr $state $drh);

$VERSION = '0.04';
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
		my $pgsql = DBD::PgPP::Protocol->new(
			hostname => $data_source_info->{host},
			port     => $data_source_info->{port},
			database => $data_source_info->{database},
			user     => $user,
			password => $password,
			debug    => $data_source_info->{debug},
		);
		$dbh->STORE(pgpp_connection => $pgsql);
#		$dbh->STORE(thread_id => $mysql->{server_thread_id});

		if (! $attrhash->{AutoCommit}) {
			my $pgsth = $pgsql->prepare('BEGIN');
			$pgsth->execute();
		}
	};
	if ($@) {
		$dbh->DBI::set_err(1, $@);
		return undef;
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
	$pgsql->close if defined $pgsql;
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
	if ($@) {
		$sth->DBI::set_err(1, $@);
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
	return defined $sth->{pgpp_rows}
		? $sth->{pgpp_rows}
		: 0;
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


package DBD::PgPP::Protocol;

use 5.004;
use IO::Socket;
use Carp;
use vars qw($VERSION $DEBUG);
use strict;
$VERSION = '0.04';

use constant DEFAULT_UNIX_SOCKET => '/tmp';
use constant DEFAULT_PORT_NUMBER => 5432;
use constant DEFAULT_TIMEOUT     => 60;
use constant BUFFER_LENGTH       => 1500;

use constant AUTH_OK                 => 0;
use constant AUTH_KERBEROS_V4        => 1;
use constant AUTH_KERBEROS_V5        => 2;
use constant AUTH_CLEARTEXT_PASSWORD => 3;
use constant AUTH_CRYPT_PASSWORD     => 4;
use constant AUTH_MD5_PASSWORD       => 5;
use constant AUTH_SCM_CREDENTIAL     => 6;


sub new {
	my $class = shift;
	my %args = @_;

	my $self = bless {
		hostname    => $args{hostname},
		path        => $args{path}     || DEFAULT_UNIX_SOCKET,
		port        => $args{port}     || DEFAULT_PORT_NUMBER,
		database    => $args{database} || $ENV{USER} || '',
		user        => $args{user}     || $ENV{USER} || '',
		password    => $args{password} || '',
		args        => $args{args}     || '',
		tty         => $args{tty}      || '',
		timeout     => $args{timeout}  || DEFAULT_TIMEOUT,
		'socket'    => undef,
		backend_pid => '',
		secret_key  => '',
		selected_record => undef,
		error_message => '',
		affected_rows => undef,
		last_oid      => undef,
	}, $class;
	$DEBUG = 1 if $args{debug};
	$self->_initialize();
	$self;
}


sub close {
	my $self = shift;
	my $socket = $self->{'socket'};
	return unless $socket;
	return unless fileno $socket;

	my $terminate_packet = 'X'. "\0";
	_dump_packet($terminate_packet);
	$socket->send($terminate_packet, 0);
	$socket->close();
}


sub DESTROY {
	my $self = shift;
	$self->close if $self;
}


sub _initialize {
	my $self = shift;
	$self->_connect();
	$self->_do_startup();
}


sub _connect {
	my $self = shift;

	my $pgsql;
	if ($self->{hostname}) {
		$pgsql = IO::Socket::INET->new(
			PeerAddr => $self->{hostname},
			PeerPort => $self->{port},
			Proto    => 'tcp',
			Timeout  => $self->{timeout},
		) or croak "Couldn't connect to $self->{hostname}:$self->{port}/tcp: $@";
	} else {
		$self->{path} =~ s{/$}{};
		my $path = sprintf '%s/.s.PGSQL.%d',
			$self->{path}, $self->{port};
		$pgsql = IO::Socket::UNIX->new(
			Type => SOCK_STREAM,
			Peer => $path,
		) or croak "Couldn't connect to $self->{path}/.s.PGSQL.$self->{port}: $@";	
	}
	$pgsql->autoflush(1);
	$self->{'socket'} = $pgsql;
}


sub get_handle {
	my $self = shift;
	$self->{'socket'};
}


sub _do_startup {
	my $self = shift;
	my $server = $self->{'socket'};

	# create message body
	my $packet = pack('nna64a32a64a64a64',
		2,                 # Protocol major version - Int16bit
		0,                 # Protocol minor version - Int16bit
		$self->{database}, # Database naem          - LimString64
		$self->{user},     # User name              - LimString32
		$self->{args},     # Command line args      - LimString64
		'',                # Unused                 - LimString64
		$self->{tty}       # Debugging msg tty      - LimString64
	);
	# add packet length
	$packet = pack('N', length($packet) + 4). $packet;
	_dump_packet($packet);
	$server->send($packet, 0);

	$self->_do_authentication();
}


sub _dump_packet {
	return unless $DBD::PgPP::Protocol::DEBUG;
	my $packet = shift;

	printf "%s()\n", (caller 1)[3];
	while ($packet =~ m/(.{1,16})/g) {
		my $chunk = $1;
		print join ' ', map {sprintf '%02X', ord $_} split //, $chunk;
		print '   ' x (16 - length $chunk);
		print '  ';
		print join '', map {
			sprintf '%s', (/[\w\d\*\,\?\%\=\'\;\(\)\.-]/) ? $_ : '.'
		} split //, $chunk;
		print "\n";
	}
}


sub get_stream {
	my $self = shift;
	return $self->{stream} if defined $self->{stream};
	$self->{stream} = DBD::PgPP::PacketStream->new($self->{'socket'});
	return $self->{stream};
}


sub _do_authentication {
	my $self = shift;
	my $stream = $self->get_stream();
	while (1) {
		my $packet = $stream->each();
		printf "Recieve %s\n", ref($packet) if $DEBUG;
		last if $packet->is_end_of_response;
		croak $packet->get_message() if $packet->is_error;
		$packet->compute($self);
	}
}


sub prepare {
	my $self = shift;
	my $sql = shift;

	$self->{error_message} = '';
	return DBD::PgPP::ProtocolStatement->new($self, $sql);
}


sub has_error {
	my $self = shift;
	return 1 if $self->{error_message};
}


sub get_error_message {
	my $self = shift;
	return $self->{error_message};
}



package DBD::PgPP::ProtocolStatement;
use strict;
use Carp;

sub new {
	my $class = shift;
	my $pgsql = shift;
	my $statement = shift;
	bless {
		postgres  => $pgsql,
		statement => $statement,
		stream    => undef,
		finish    => undef,
	}, $class;
}


sub execute {
	my $self = shift;
	my $pgsql = $self->{postgres};
	my $handle = $pgsql->get_handle();

	my $query_packet = 'Q'. $self->{statement}. "\0";
	DBD::PgPP::Protocol::_dump_packet($query_packet);
	$handle->send($query_packet, 0);
	$self->{finisy}        = undef;
	$self->{affected_rows} = 0;
	$self->{last_oid}      = undef;

	my $stream = $pgsql->get_stream();
	my $packet = $stream->each();
	printf "Recieve %s\n", ref($packet) if $DBD::PgPP::Protocol::DEBUG;
	if ($packet->is_error()) {
		$self->_to_end_of_response($stream);
		die $packet->get_message();
	}
	elsif ($packet->is_end_of_response()) {
		$self->{finish} = 1;
		return;
	}
	if ($packet->is_empty) {
		$self->{finish} = 1;
		$self->_to_end_of_response($stream);
		return;
	}
	if ($packet->is_cursor_response) {
		$packet->compute($pgsql);
		my $row_info = $stream->each();
		if ($row_info->is_error()) {
			$self->_to_end_of_response($stream);
			croak $packet->get_message();
		}
		$row_info->compute($pgsql);
		$self->{stream} = DBD::PgPP::ReadOnlyPacketStream->new($handle);
		$self->{stream}->set_buffer($stream->get_buffer);
		while (1) {
			my $tmp_packet = $self->{stream}->each();
			printf "-Recieve %s\n", ref($tmp_packet) if $DBD::PgPP::Protocol::DEBUG;
			if ($tmp_packet->is_error()) {
				$self->_to_end_of_response($stream);
				croak $packet->get_message();
			}
			$tmp_packet->compute($pgsql);
			last if $tmp_packet->is_end_of_response;
		}
		$self->{stream}->rewind();
		$stream->set_buffer('');
		return;
	}
	else {
		$packet->compute($pgsql);
		$self->{finish} = 1;
		while (1) {
			my $end = $stream->each();
			printf "-Recieve %s\n", ref($end) if $DBD::PgPP::Protocol::DEBUG;
			if ($end->is_error()) {
				$self->_to_end_of_response($stream);
				croak $end->get_message();
			}
			last if $end->is_end_of_response();
		}
		return;
	}
}


sub _to_end_of_response {
	my $self = shift;
	my $stream = shift;

	while (1) {
		my $packet = $stream->each();
		$packet->compute($self);
		last if $packet->is_end_of_response();
	}
}


sub fetch
{
	my $self = shift;
	my $pgsql = $self->{postgres};
	my $stream = $self->{stream};

	return undef if $self->{finish};

	while (1) {
		my $packet = $stream->each();
		printf "%s\n", ref $packet if $DBD::PgPP::Protocol::DEBUG;
		warn $packet->get_message() if $packet->is_error;
		return undef if $packet->is_end_of_response;
		$packet->compute($pgsql);
		my $result =  $packet->get_result();
		return $result if $result;
	}
}



package DBD::PgPP::PacketStream;

use Carp;
use strict;

# Message Identifies
use constant ASCII_ROW             => 'D';
use constant AUTHENTICATION        => 'R';
use constant BACKEND_KEY_DATA      => 'K';
use constant BINARY_ROW            => 'B';
use constant COMPLETED_RESPONSE    => 'C';
use constant COPY_IN_RESPONSE      => 'G';
use constant COPY_OUT_RESPONSE     => 'H';
use constant CURSOR_RESPONSE       => 'P';
use constant EMPTY_QUERY_RESPONSE  => 'I';
use constant ERROR_RESPONSE        => 'E';
use constant FUNCTION_RESPONSE     => 'V';
use constant NOTICE_RESPONSE       => 'N';
use constant NOTIFICATION_RESPONSE => 'A';
use constant READY_FOR_QUERY       => 'Z';
use constant ROW_DESCRIPTION       => 'T';

# Authentication Message Specifies
use constant AUTHENTICATION_OK                 => 0;
use constant AUTHENTICATION_KERBEROS_V4        => 1;
use constant AUTHENTICATION_KERBEROS_V5        => 2;
use constant AUTHENTICATION_CLEARTEXT_PASSWORD => 3;
use constant AUTHENTICATION_CRYPT_PASSWORD     => 4;
use constant AUTHENTICATION_MD5_PASSWORD       => 5;
use constant AUTHENTICATION_SCM_CREDENTIAL     => 6;


sub new {
	my $class = shift;
	my $handle = shift;
	bless {
		handle   => $handle,
		buffer   => '',
	}, $class;
}


sub set_buffer {
	my $self = shift;
	$self->{buffer} = shift;
}


sub get_buffer {
	my $self = shift;
	$self->{buffer};
}


sub each {
	my $self = shift;
	my $type = $self->_get_byte();

	if ($type eq ASCII_ROW) {
		return $self->_each_ascii_row();
	}
	elsif ($type eq AUTHENTICATION) {
		return $self->_each_authentication();
	}
	elsif ($type eq BACKEND_KEY_DATA) {
		return $self->_each_backend_key_data();
	}
	elsif ($type eq BINARY_ROW) {
		return $self->_each_binary_row();
	}
	elsif ($type eq COMPLETED_RESPONSE) {
		return $self->_each_completed_response();
	}
	elsif ($type eq COPY_IN_RESPONSE) {
		return $self->_each_copy_in_response();
	}
	elsif ($type eq COPY_OUT_RESPONSE) {
		return $self->_each_copy_out_response();
	}
	elsif ($type eq CURSOR_RESPONSE) {
		return $self->_each_cursor_response();
	}
	elsif ($type eq EMPTY_QUERY_RESPONSE) {
		return $self->_each_empty_query_response();
	}
	elsif ($type eq ERROR_RESPONSE) {
		return $self->_each_error_response();
	}
	elsif ($type eq FUNCTION_RESPONSE) {
		return $self->_each_function_response();
	}
	elsif ($type eq NOTICE_RESPONSE) {
		return $self->_each_notice_response();
	}
	elsif ($type eq NOTIFICATION_RESPONSE) {
		return $self->_each_notification_response();
	}
	elsif ($type eq READY_FOR_QUERY) {
		return $self->_each_ready_for_query();
	}
	elsif ($type eq ROW_DESCRIPTION) {
		return $self->_each_row_description();
	}
	else {
		croak "Unknown message type: '$type'";
	}
}


sub _each_authentication {
	my $self = shift;

	my $code = $self->_get_int32();
	if ($code == AUTHENTICATION_OK) {
		return DBD::PgPP::AuthenticationOk->new();
	}
	elsif ($code == AUTHENTICATION_KERBEROS_V4) {
		return DBD::PgPP::AuthenticationKerberosV4->new();
	}
	elsif ($code == AUTHENTICATION_KERBEROS_V5) {
		return DBD::PgPP::AuthenticationKerberosV5->new();
	}
	elsif ($code == AUTHENTICATION_CLEARTEXT_PASSWORD) {
		return DBD::PgPP::AuthenticationCleartextPassword->new();
	}
	elsif ($code == AUTHENTICATION_CRYPT_PASSWORD) {
		my $salt = $self->_get_byte(2);
		return DBD::PgPP::AuthenticationCryptPassword->new($salt);
	}
	elsif ($code == AUTHENTICATION_MD5_PASSWORD) {
		my $salt = $self->_get_byte(4);
		return DBD::PgPP::AuthenticationMD5Password->new($salt);
	}
	elsif ($code == AUTHENTICATION_SCM_CREDENTIAL) {
		return DBD::PgPP::AuthenticationSCMCredential->new();
	}
	else {
		croak "Unknown authentication type: $code";
	}
}


sub _each_backend_key_data {
	my $self = shift;
	my $process_id = $self->_get_int32();
	my $secret_key = $self->_get_int32();
	return DBD::PgPP::BackendKeyData->new($process_id, $secret_key);
}


sub _each_error_response {
	my $self = shift;
	my $error_message = $self->_get_c_string();
	return DBD::PgPP::ErrorResponse->new($error_message);
}


sub _each_notice_response {
	my $self = shift;
	my $notice_message = $self->_get_c_string();
	return DBD::PgPP::NoticeResponse->new($notice_message);
}

sub _each_notification_response {
	my $self = shift;
	my $process_id = $self->_get_int32();
	my $condition = $self->_get_c_string();
	return DBD::PgPP::NotificationResponse->new($process_id, $condition);
}


sub _each_ready_for_query {
	my $self = shift;
	return DBD::PgPP::ReadyForQuery->new();
}


sub _each_cursor_response {
	my $self = shift;
	my $name = $self->_get_c_string();
	return DBD::PgPP::CursorResponse->new($name);
}


sub _each_row_description {
	my $self = shift;
	my $row_number = $self->_get_int16();
	my @description;
	for my $i (1..$row_number) {
		push @description, {
			name     => $self->_get_c_string(),
			type     => $self->_get_int32(),
		    size     => $self->_get_int16(),
		    modifier => $self->_get_int32(),
		};
	}
	return DBD::PgPP::RowDescription->new(\@description);
}


sub _each_ascii_row {
	my $self = shift;
	return DBD::PgPP::AsciiRow->new($self);
}


sub _each_completed_response {
	my $self = shift;
	my $tag = $self->_get_c_string();
	return DBD::PgPP::CompletedResponse->new($tag);
}


sub _each_empty_query_response {
	my $self = shift;
	my $unused = $self->_get_c_string();
	return DBD::PgPP::EmptyQueryResponse->new($unused);
}


sub _get_byte {
	my $self = shift;
	my $length = shift || 1;

	$self->_if_short_then_add_buffer($length);
	my $result = substr $self->{buffer}, 0, $length;
	$self->{buffer} = substr $self->{buffer}, $length;
	return $result;
}


sub _get_int32 {
	my $self = shift;
	$self->_if_short_then_add_buffer(4);
	my $result = unpack 'N', substr $self->{buffer}, 0, 4;
	$self->{buffer} = substr $self->{buffer}, 4;
	return $result;
}


sub _get_int16 {
	my $self = shift;
	$self->_if_short_then_add_buffer(2);
	my $result = unpack 'n', substr $self->{buffer}, 0, 2;
	$self->{buffer} = substr $self->{buffer}, 2;
	return $result;
}


sub _get_c_string {
	my $self = shift;

	my $length = 0;
	while (1) {
		$length = index $self->{buffer}, "\0";
		last if $length >= 0;
		$self->_if_short_then_add_buffer(1);
	}
	my $result = substr $self->{buffer}, 0, $length;
	$self->{buffer} = substr $self->{buffer}, $length + 1;
	return $result;
}


sub _if_short_then_add_buffer {
	my $self = shift;
	my $length = shift || 0;
	return if length($self->{buffer}) >= $length;

	my $handle = $self->{handle};
	my $packet = '';
	$handle->recv($packet, 1500, 0);
	DBD::PgPP::Protocol::_dump_packet($packet);
	$self->{buffer} .= $packet;
	return length $packet;
}



package DBD::PgPP::ReadOnlyPacketStream;
use base 'DBD::PgPP::PacketStream';
use strict;
use Carp;

# Message Identifies
use constant ASCII_ROW             => 'D';
use constant AUTHENTICATION        => 'R';
use constant BACKEND_KEY_DATA      => 'K';
use constant BINARY_ROW            => 'B';
use constant COMPLETED_RESPONSE    => 'C';
use constant COPY_IN_RESPONSE      => 'G';
use constant COPY_OUT_RESPONSE     => 'H';
use constant CURSOR_RESPONSE       => 'P';
use constant EMPTY_QUERY_RESPONSE  => 'I';
use constant ERROR_RESPONSE        => 'E';
use constant FUNCTION_RESPONSE     => 'V';
use constant NOTICE_RESPONSE       => 'N';
use constant NOTIFICATION_RESPONSE => 'A';
use constant READY_FOR_QUERY       => 'Z';
use constant ROW_DESCRIPTION       => 'T';

# Authentication Message Specifies
use constant AUTHENTICATION_OK                 => 0;
use constant AUTHENTICATION_KERBEROS_V4        => 1;
use constant AUTHENTICATION_KERBEROS_V5        => 2;
use constant AUTHENTICATION_CLEARTEXT_PASSWORD => 3;
use constant AUTHENTICATION_CRYPT_PASSWORD     => 4;
use constant AUTHENTICATION_MD5_PASSWORD       => 5;
use constant AUTHENTICATION_SCM_CREDENTIAL     => 6;

sub new {
	my $class = shift;
	my $handle = shift;
	bless {
		handle   => $handle,
		buffer   => '',
		position => 0,
	}, $class;
}


sub rewind {
	my $self = shift;
	$self->{position} = 0;
}


sub _get_byte {
	my $self = shift;
	my $length = shift || 1;

	$self->_if_short_then_add_buffer($length);
	my $result = substr $self->{buffer}, $self->{position}, $length;
	$self->{position} += $length;
	return $result;
}


sub _get_int32 {
	my $self = shift;
	$self->_if_short_then_add_buffer(4);
	my $result = unpack 'N', substr $self->{buffer}, $self->{position}, 4;
	$self->{position} += 4;
	return $result;
}


sub _get_int16 {
	my $self = shift;
	$self->_if_short_then_add_buffer(2);
	my $result = unpack 'n', substr $self->{buffer}, $self->{position}, 2;
	$self->{buffer} += 2;
	return $result;
}


sub _get_c_string {
	my $self = shift;
	my $length = 0;
	while (1) {
		$length = index($self->{buffer}, "\0", $self->{position}) - $self->{position};
		last if $length >= 0;
		$self->_if_short_then_add_buffer(1);
	}
	my $result = substr $self->{buffer}, $self->{position}, $length;
	$self->{position} += $length + 1;
	return $result;
}


sub _if_short_then_add_buffer {
	my $self = shift;
	my $length = shift || 0;

	return if (length($self->{buffer}) - $self->{position}) >= $length;

	my $handle = $self->{handle};
	my $packet = '';
	$handle->recv($packet, 1500, 0);
	DBD::PgPP::Protocol::_dump_packet($packet);
	$self->{buffer} .= $packet;
	return length $packet;
}



package DBD::PgPP::Response;
use strict;

sub new {
	my $class = shift;
	bless {
	}, $class; 
}


sub compute {
	my $self = shift;
	my $postgres = shift;
}


sub is_empty { undef }
sub is_error { undef }
sub is_end_of_response { undef }
sub get_result { undef }
sub is_cursor_response { undef }


package DBD::PgPP::AuthenticationOk;
use base 'DBD::PgPP::Response';



package DBD::PgPP::AuthenticationKerberosV4;
use base 'DBD::PgPP::Response'; 
use Carp;
use strict;

sub compute {
	croak "authentication type 'Kerberos V4' not supported.\n"
}



package DBD::PgPP::AuthenticationKerberosV5;
use base 'DBD::PgPP::Response'; 
use Carp;
use strict;

sub compute {
	croak "authentication type 'Kerberos V5' not supported.\n"
}


package DBD::PgPP::AuthenticationCleartextPassword;
use base 'DBD::PgPP::Response'; 

sub compute {
	my $self = shift;
	my $pgsql = shift;
	my $handle = $pgsql->get_handle;
	my $password = $pgsql->{password};

	my $packet = pack('N', length($password) + 4 + 1). $password. "\0";
	DBD::PgPP::Protocol::_dump_packet($packet);
	$handle->send($packet, 0);
}


package DBD::PgPP::AuthenticationCryptPassword;
use base 'DBD::PgPP::Response'; 
use Carp;

sub new {
	my $class = shift;
	my $self = $class->SUPER::new();
	$self->{salt} = shift;	
	$self;
}


sub get_salt {
	my $self = shift;
	$self->{salt};
}


sub compute {
	my $self = shift;
	my $pgsql = shift;
	my $handle = $pgsql->get_handle();
	my $password = $pgsql->{password} || '';

	$password = _encode_crypt($password, $self->{salt});
	my $packet = pack('N', length($password) + 4 + 1). $password. "\0";
	DBD::PgPP::Protocol::_dump_packet($packet);
	$handle->send($packet, 0);
}


sub _encode_crypt
{
	my $password = shift;
	my $salt = shift;

	my $crypted = '';
	eval {
		$crypted = crypt($password, $salt);
		die "is MD5 crypt()" if _is_md5_crypt($crypted, $salt);
	};
	if ($@) {
		croak "authentication type 'crypt' not supported on your platform. please use  'trust' or 'md5' or 'ident' authentication";
	}
	return $crypted;
}


sub _is_md5_crypt {
	my $crypted = shift;
	my $salt = shift;

	$crypted =~ /^\$1\$$salt\$/;
}



package DBD::PgPP::AuthenticationMD5Password;
use base 'DBD::PgPP::AuthenticationCryptPassword';
use Carp;

sub new {
	my $class = shift;
	my $self = $class->SUPER::new();
	$self->{salt} = shift;
	$self;
}


sub compute {
	my $self = shift;
	my $pgsql = shift;
	my $handle = $pgsql->get_handle();
	my $password = $pgsql->{password} || '';

	my $encoded_password = _encode_md5(
		$pgsql->{user},
		$password, $self->{salt}
	);
	my $packet = pack('N', length($encoded_password) + 4 + 1). $encoded_password. "\0";
	DBD::PgPP::Protocol::_dump_packet($packet);
	$handle->send($packet, 0);
}


sub _encode_md5 {
	my $user = shift;
	my $password = shift;
	my $salt = shift;

	my $md5 = DBD::PgPP::EncodeMD5->create();
	$md5->add($password);
	$md5->add($user);
	my $tmp_digest = $md5->hexdigest;
	$md5->add($tmp_digest);
	$md5->add($salt);
	my $md5_digest = 'md5'. $md5->hexdigest;

	return $md5_digest;
}



package DBD::PgPP::AuthenticationSCMCredential;
use base 'DBD::PgPP::Response';
use Carp;

sub compute {
	croak "authentication type 'SCM Credential' not supported.\n"
}



package DBD::PgPP::BackendKeyData;
use base 'DBD::PgPP::Response';

sub new {
	my $class = shift;
	my $self = $class->SUPER::new();
	$self->{process_id} = shift;
	$self->{secret_key} = shift;
	$self;
}


sub get_process_id {
	my $self = shift;
	$self->{process_id};
}


sub get_secret_key {
	my $self = shift;
	$self->{secret_key};
}


sub compute {
	my $self = shift;
	my $postgres = shift;

	$postgres->{process_id} = $self->get_process_id;
	$postgres->{secret_key} = $self->get_secret_key;
}



package DBD::PgPP::ErrorResponse;
use base 'DBD::PgPP::Response';

sub new {
	my $class = shift;
	my $self = $class->SUPER::new();
	$self->{message} = shift;
	$self;
}


sub get_message {
	my $self = shift;
	$self->{message};
}


sub is_error { 1 }



package DBD::PgPP::NoticeResponse;
use base 'DBD::PgPP::ErrorResponse';

sub is_error { undef }



package DBD::PgPP::NotificationResponse;
use base 'DBD::PgPP::Response';

sub new {
	my $class = shift;
	my $self = $class->SUPER::new();
	$self->{process_id} = shift;
	$self->{condition} = shift;
	$self;
}


sub get_process_id {
	my $self = shift;
	$self->{process_id};
}


sub get_condition {
	my $self = shift;
	$self->{condition};
}



package DBD::PgPP::ReadyForQuery;
use base 'DBD::PgPP::Response';

sub is_end_of_response { 1 }



package DBD::PgPP::CursorResponse;
use base 'DBD::PgPP::Response';
use strict;

sub new {
	my $class = shift;
	my $self = $class->SUPER::new();
	$self->{name} = shift;
	$self;
}


sub get_name
{
	my $self = shift;
	$self->{name};
}


sub compute {
	my $self = shift;
	my $pgsql = shift;

	$pgsql->{cursor_name} = $self->get_name();
}


sub is_cursor_response { 1 }


package DBD::PgPP::RowDescription;
use base 'DBD::PgPP::Response';
use strict;

sub new {
	my $class = shift;
	my $self = $class->SUPER::new();
	$self->{row_description} = shift;
	$self;
}


sub compute
{
	my $self = shift;
	my $pgsql = shift;

	$pgsql->{row_description} = $self->{row_description};
}


sub is_cursor_response { 1 }



package DBD::PgPP::AsciiRow;
use base 'DBD::PgPP::Response';
use strict;

sub new {
	my $class = shift;
	my $self = $class->SUPER::new();
	$self->{stream} = shift;
	$self;
}


sub compute
{
	my $self = shift;
	my $pgsql = shift;
	my $stream = $self->{stream};

	my $fields_length = scalar @{$pgsql->{row_description}};

	my $bitmap_length = $self->_get_length_of_null_bitmap($fields_length);
	my $bitmap = unpack 'C*', $stream->_get_byte($bitmap_length);
	my @result;
	my $shift = 1;
	for my $i (1..$fields_length) {
		if ($self->_is_not_null($bitmap, $bitmap_length, $i)) {
			my $length = $stream->_get_int32();
			my $value = $stream->_get_byte($length - 4);
			push @result, $value;
			next;
		}
		push @result, undef;
		next;
	}
	$self->{result} = \@result;
}


sub _get_length_of_null_bitmap {
	my $self = shift;
	my $number = shift;
	use integer;
	my $length = $number / 8;
	++$length if $number % 8;
	return $length;
}


sub _is_not_null {
	my $self = shift;
	my $bitmap = shift || 0;
	my $length = shift || 0;
	my $index = shift || 0;

	($bitmap >> (($length * 8) - $index)) & 0x01;
}


sub get_result
{
	my $self = shift;
	$self->{result};
}


sub is_cursor_response { 1 }



package DBD::PgPP::CompletedResponse;
use base 'DBD::PgPP::Response';
use strict;
use Carp;

sub new
{
	my $class = shift;
	my $self = $class->SUPER::new();
	$self->{tag} = shift;
	$self;
}


sub get_tag {
	my $self = shift;
	$self->{tag};
}


sub compute
{
	my $self = shift;
	my $pgsql = shift;
	my $tag = $self->{tag};

	if ($tag =~ /^INSERT (\d+) (\d+)/) {
		$pgsql->{affected_oid}  = $1;
		$pgsql->{affected_rows} = $2;
	}
	elsif ($tag =~ /^DELETE (\d+)/) {
		$pgsql->{affected_rows} = $1;
	}
	elsif ($tag =~ /^UPDATE (\d+)/) {
		$pgsql->{affected_rows} = $1;
	}
}



package DBD::PgPP::EmptyQueryResponse;
use base 'DBD::PgPP::Response';
use strict;

sub is_empty { 1 }



package DBD::PgPP::EncodeMD5;

=pod

=begin wish

Please do not question closely about this source code ;-)

=end wish

=cut

use strict;
use vars qw($a $b $c $d);
my ($x, $n, $m, $l, $r, $z);


sub create {
	my $class = shift;
	my $md5;

	eval {
		require Digest::MD5;
		$md5 = Digest::MD5->new;
	};
	if ($@) {
		$md5 = $class->new();
	}
	return $md5;
}


sub new {
	my $class = shift;
	bless {
		source => '',
	}, $class;
}


sub add {
	my $self = shift;
	$self->{source} .= join '', @_;
}



sub hexdigest {
	my $self = shift;

	my @A = unpack(
		'N4C24',
		unpack 'u', 'H9T4C`>_-JXF8NMS^$#)4=@<,$18%"0X4!`L0%P8*#Q4``04``04#!P``'
	);
	my @K = map { int abs 2 ** 32 * sin $_ } 1..64;
	my ($p);


	my $position = 0;
	do {
		$_ = substr $self->{source}, $position, 64;
		$position += 64;
		$l += $r = length $_;
		$r++, $_ .= "\x80" if $r < 64 && !$p++;
		my @W = unpack 'V16', $_. "\0" x 7;
		$W[14] = $l * 8 if $r < 57;
		($a, $b, $c, $d) = @A;

		for (0..63) {
			#no warnings;
			local($^W) = 0;
			$a = _m($b + 
				_l($A[4 + 4 * ($_ >> 4) + $_ % 4],
					_m(&{(
						sub {
							$b & $c | $d & ~ $b;
						},
						sub {
							$b & $d | $c & ~ $d;
						},
						sub {
							$b ^ $c ^ $d;
						},
						sub {
							$c ^ ($b | ~ $d);
						}
						)[$z = $_ / 16]}
					+ $W[($A[20 + $z] + $A[24 + $z] * ($_ % 16)) % 16] + $K[$_] + $a)
				)
			);
			($a, $b, $c, $d) = ($d, $a, $b, $c)
		}

		my $i = $A[0];
		$A[0] = _m($A[0] + $a);
		$A[1] = _m($A[1] + $b);
		$A[2] = _m($A[2] + $c);
		$A[3] = _m($A[3] + $d);

	} while ($r > 56);

	($x, $n, $m, $l, $r, $z) = ();
	$self->{source} = '';

	return unpack 'H32', pack 'V4', @A;
}


sub _l {
	($x = pop @_) << ($n=pop) | 2 ** $n - 1 & $x >> 32 - $n;
}

sub _m {
	($x = pop @_) - ($m = 1 + ~ 0) * int($x / $m);
}


1;
__END__


=head1 DESCRIPTION

DBD::PgPP is a Pure Perl client interface for the PostgreSQL database. This module implements network protocol between server and client of PostgreSQL, thus you don't need external PostgreSQL client library like B<libpq> for this module to work. It means this module enables you to connect to PostgreSQL server from some operation systems which PostgreSQL is not ported. How nifty!


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

  $dbh = DBI->connect(
      "dbi:PgPP:dbname=$dbname",
      $username, $password
  );

  $dbh = DBI->connect(
      "dbi:PgPP:dbname=$dbname;host=$host;port=$port",
      $username, $password
  );

  $dbh = DBI->connect(
      "dbi:PgPP:dbname=$dbname;path=$path;port=$port",
      $username, $password
  );

      parameter | hard coded default
      ----------+-------------------
      dbname    | current userid
      host      | localhost
      port      | 5432
      path      | /tmp
      debug     | undef

If a host is specified, the postmaster on this host needs to be started with the C<-i> option (TCP/IP socket).


For authentication with username and password appropriate entries have to be made in pg_hba.conf. Please refer to the L<pg_hba.conf> and the L<pg_passwd> for the different types of authentication.

=back

=head1 SUPPORT OPERATING SYSTEM

This module has been tested on these OSes.

=over 4

=item * Mac OS 9

with MacPerl5.6.1r1 built for PowerPC

=item * Mac OS X

with perl v5.6.0 built for darwin
 
=item * Windows2000

with ActivePerl5.6.1 build631.

=item * FreeBSD 4.6

with perl v5.6.1 built for i386-freebsd

=item * FreeBSD 3.4

with perl v5.6.1 built for i386-freebsd

with perl v5.005_03 built for i386-freebsd

=item * Linux

with perl v5.005_03 built for ppc-linux

=item * Solaris 2.6 (SPARC)

with perl5.6.1 built for sun4-solaris.

with perl5.004_04 built for sun4-solaris.

Can use on Solaris2.6 with perl5.004_04, although I<make test> is failure.

=back


=head1 LIMITATION

=over 4

=item * Can't use 'crypt' authentication in a part of FreeBSD.

=item * Can't use the 'Kerberos v4/5' authentication.

=item * Can't use the SSL Connection.

=item * Can't use BLOB data.

=back


=head1 DEPENDENCIES

This module requires these other modules and libraries:

  L<DBI>, L<IO::Socket>


=head1 TODO

=over 4

=item * Add the original crypt (pure perl) method.

=back

=head1 SEE ALSO

L<DBI>, L<http://developer.postgresql.org/docs/postgres/protocol.html>

=head1 AUTHOR

Hiroyuki OYAMA E<lt>oyama@crayfish.co.jpE<gt>

=head1 COPYRIGHT AND LICENCE

Copyright (C) 2002 Hiroyuki OYAMA. Japan. All rights reserved.

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut

package DBD::PgPP;
use strict;

use DBI;
use Carp ();
use IO::Socket ();

=head1 NAME

DBD::PgPP - Pure Perl PostgreSQL driver for the DBI

=head1 SYNOPSIS

  use DBI;

  my $dbh = DBI->connect('dbi:PgPP:dbname=$dbname', '', '');

  # See the DBI module documentation for full details

=cut

our $VERSION = '0.05';
my $BUFFER_LEN = 1500;
my $DEBUG;

my %BYTEA_DEMANGLE = (
    '\\' => '\\',
    map { sprintf('%03o', $_) => chr $_ } 0 .. 255,
);

{
    my $drh;
    sub driver {
        my ($class, $attr) = @_;
        return $drh ||= DBI::_new_drh("$class\::dr", {
            Name        => 'PgPP',
            Version     => $VERSION,
            Err         => \(my $err    = 0),
            Errstr      => \(my $errstr = ''),
            State       => \(my $state  = undef),
            Attribution => 'DBD::PgPP by Hiroyuki OYAMA',
        }, {});
    }
}

sub _parse_dsn {
    my ($class, $dsn, $args) = @_;

    return if !defined $dsn;

    my ($hash, $var, $val);
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

sub _parse_dsn_host {
    my ($class, $dsn) = @_;
    my $hash = $class->_parse_dsn($dsn, ['host', 'port']);
    return @$hash{qw<host port>};
}


package DBD::PgPP::dr;

$DBD::PgPP::dr::imp_data_size = 0;

sub connect {
    my ($drh, $dsn, $user, $password, $attrhash) = @_;

    my $data_source_info
        = DBD::PgPP->_parse_dsn($dsn, ['database', 'host', 'port']);
    $user     ||= '';
    $password ||= '';

    my $dbh = DBI::_new_dbh($drh, { Name => $dsn, USER => $user }, {});
    eval {
        my $pgsql = DBD::PgPP::Protocol->new(
            hostname => $data_source_info->{host},
            port     => $data_source_info->{port},
            database => $data_source_info->{database},
            user     => $user,
            password => $password,
            debug    => $data_source_info->{debug},
            path     => $data_source_info->{path},
        );
        $dbh->STORE(pgpp_connection => $pgsql);
    };
    if ($@) {
        $dbh->DBI::set_err(1, $@);
        return undef;
    }
    return $dbh;
}

sub data_sources { 'dbi:PgPP:' }

sub disconnect_all {}


package DBD::PgPP::db;

$DBD::PgPP::db::imp_data_size = 0;

# We need to implement ->quote, because otherwise we get the default DBI
# one, which ignores backslashes.  The DBD::Pg implementation doubles all
# backslashes and apostrophes; this version backslash-protects all of them.
# XXX: What about null characters, or byte sequences that don't form valid
# characters in the relevant encoding?
# XXX: What about the mysterious additional '$data_type' argument?
sub quote {
    my ($dbh, $s) = @_;

    if (!defined $s) {
        # Yes, _every_ DBD that needs its own quote method has to check for
        # nulls separately.
        return 'NULL';
    }
    else {
        # It's best to always put quotes round it, even if it looks like a
        # simple integer.  Otherwise you can't compare the result of quoting
        # Perl numeric zero to a boolean column.  (You can't _reliably_
        # compare a Perl scalar to a boolean column anyway, because there
        # are six Postgres syntaxes for TRUE, and six for FALSE, and
        # everything else is an error -- but that's another story, and at
        # least if you quote '0' it looks false to Postgres.  Sigh.  I have
        # some plans for a pure-Perl DBD which understands the 7.4 protocol,
        # and can therefore fix up bools in _both_ directions.)
        $s =~ s/(?=[\\\'])/\\/g;
        return "'$s'";
    }
}

sub prepare {
    my ($dbh, $statement, @attribs) = @_;

    my $pgsql = $dbh->FETCH('pgpp_connection');
    my $parsed = $pgsql->parse_statement($statement);

    my $sth = DBI::_new_sth($dbh, { Statement => $statement });
    $sth->STORE(pgpp_parsed_stmt => $parsed);
    $sth->STORE(pgpp_handle => $pgsql);
    $sth->STORE(pgpp_params => []);
    $sth->STORE(NUM_OF_PARAMS => scalar grep { ref } @$parsed);
    $sth;
}

sub commit {
    my ($dbh) = @_;

    my $pgsql = $dbh->FETCH('pgpp_connection');
    eval {
        my $pgsth = $pgsql->prepare('COMMIT');
        $pgsth->execute;
    };
    if ($@) {
        $dbh->DBI::set_err(1, $@); # $pgsql->get_error_message ???
        return undef;
    }
    return 1;
}

sub rollback {
    my ($dbh) = @_;
    my $pgsql = $dbh->FETCH('pgpp_connection');
    eval {
        my $pgsth = $pgsql->prepare('ROLLBACK');
        $pgsth->execute;
    };
    if ($@) {
        $dbh->DBI::set_err(1, $@); # $pgsql->get_error_message ???
        return undef;
    }
    return 1;
}

sub disconnect {
    my ($dbh) = @_;

    if (my $conn = $dbh->FETCH('pgpp_connection')) {
        $conn->close;
        $dbh->STORE('pgpp_connection', undef);
    }

    return 1;
}

sub FETCH {
    my ($dbh, $key) = @_;

    return $dbh->{$key} if $key =~ /^pgpp_/;
    return $dbh->{AutoCommit} if $key eq 'AutoCommit';
    return $dbh->SUPER::FETCH($key);
}

sub STORE {
    my ($dbh, $key, $new) = @_;

    if ($key eq 'AutoCommit') {
        my $old = $dbh->{$key};
        my $never_set = !$dbh->{pgpp_ever_set_autocommit};

        # This logic is stolen from DBD::Pg
        if (!$old && $new && $never_set) {
            # Do nothing; fall through
        }
        elsif (!$old && $new) {
            # Turning it on: commit
            # XXX: Avoid this if no uncommitted changes.
            # XXX: Desirable?  See dbi-dev archives.
            # XXX: Handle errors.
            my $st = $dbh->{pgpp_connection}->prepare('COMMIT');
            $st->execute;
        }
        elsif ($old && !$new   ||  !$old && !$new && $never_set) {
            # Turning it off, or initializing it to off at
            # connection time: begin a new transaction
            # XXX: Handle errors.
            my $st = $dbh->{pgpp_connection}->prepare('BEGIN');
            $st->execute;
        }

        $dbh->{pgpp_ever_set_autocommit} = 1;
        $dbh->{$key} = $new;

        return 1;
    }

    if ($key =~ /^pgpp_/) {
        $dbh->{$key} = $new;
        return 1;
    }

    return $dbh->SUPER::STORE($key, $new);
}

sub DESTROY {
    my ($dbh) = @_;
    $dbh->disconnect;
}


package DBD::PgPP::st;

$DBD::PgPP::st::imp_data_size = 0;

sub bind_param {
    my ($sth, $index, $value, $attr) = @_;
    my $type = ref($attr) ? $attr->{TYPE} : $attr;
    my $dbh = $sth->{Database};
    my $params = $sth->FETCH('pgpp_params');
    $params->[$index - 1] = $dbh->quote($value, $type);
}

sub execute {
    my ($sth, @args) = @_;

    my $pgsql = $sth->FETCH('pgpp_handle');
    die "execute on disconnected database" if $pgsql->{closed};

    my $num_params = $sth->FETCH('NUM_OF_PARAMS');

    if (@args) {
        return $sth->set_err(1, "Wrong number of arguments")
            if @args != $num_params;
        my $dbh = $sth->{Database};
        $_ = $dbh->quote($_) for @args;
    }
    else {
        my $bind_params = $sth->FETCH('pgpp_params');
        return $sth->set_err(1, "Wrong number of bound parameters")
            if @$bind_params != $num_params;

        # They've already been quoted by ->bind_param
        @args = @$bind_params;
    }

    my $parsed_statement = $sth->FETCH('pgpp_parsed_stmt');
    my $statement = join '', map { ref() ? $args[$$_] : $_ } @$parsed_statement;

    my $result;
    eval {
        $sth->{pgpp_record_iterator} = undef;
        my $pgsql_sth = $pgsql->prepare($statement);
        $pgsql_sth->execute;
        $sth->{pgpp_record_iterator} = $pgsql_sth;
        my $dbh = $sth->{Database};

        if (defined $pgsql_sth->{affected_rows}) {
            $sth->{pgpp_rows} = $pgsql_sth->{affected_rows};
            $result = $pgsql_sth->{affected_rows};
        }
        else {
            $sth->{pgpp_rows} = 0;
            $result = $pgsql_sth->{affected_rows};
        }
        if (!$pgsql_sth->{row_description}) {
            $sth->STORE(NUM_OF_FIELDS => 0);
            $sth->STORE(NAME          => []);
        }
        else {
            $sth->STORE(NUM_OF_FIELDS => scalar @{$pgsql_sth->{row_description}});
            $sth->STORE(NAME => [ map {$_->{name}} @{$pgsql_sth->{row_description}} ]);
        }
    };
    if ($@) {
        $sth->DBI::set_err(1, $@);
        return undef;
    }

    return $pgsql->has_error ? undef
         : $result           ? $result
         :                     '0E0';
}

sub fetch {
    my ($sth) = @_;

    my $iterator = $sth->FETCH('pgpp_record_iterator');
    return undef if $iterator->{finished};

    if (my $row = $iterator->fetch) {
        if ($sth->FETCH('ChopBlanks')) {
            s/\s+\z// for @$row;
        }
        return $sth->_set_fbav($row);
    }

    $iterator->{finished} = 1;
    return undef;
}
*fetchrow_arrayref = \&fetch;

sub rows {
    my ($sth) = @_;
    return defined $sth->{pgpp_rows} ? $sth->{pgpp_rows} : 0;
}

sub FETCH {
    my ($dbh, $key) = @_;

    # return $dbh->{AutoCommit} if $key eq 'AutoCommit';
    return $dbh->{NAME} if $key eq 'NAME';
    return $dbh->{$key} if $key =~ /^pgpp_/;
    return $dbh->SUPER::FETCH($key);
}

sub STORE {
    my ($sth, $key, $value) = @_;

    if ($key eq 'NAME') {
        $sth->{NAME} = $value;
        return 1;
    }
    elsif ($key =~ /^pgpp_/) {
        $sth->{$key} = $value;
        return 1;
    }
    elsif ($key eq 'NUM_OF_FIELDS') {
        # Don't set this twice; DBI doesn't seem to like it.
        # XXX: why not?  Perhaps this conceals a PgPP bug.
        my $curr = $sth->FETCH($key);
        return 1 if $curr && $curr == $value;
    }
    return $sth->SUPER::STORE($key, $value);
}

sub DESTROY { return }


package DBD::PgPP::Protocol;

use constant DEFAULT_UNIX_SOCKET => '/tmp';
use constant DEFAULT_PORT_NUMBER => 5432;
use constant DEFAULT_TIMEOUT     => 60;

use constant AUTH_OK                 => 0;
use constant AUTH_KERBEROS_V4        => 1;
use constant AUTH_KERBEROS_V5        => 2;
use constant AUTH_CLEARTEXT_PASSWORD => 3;
use constant AUTH_CRYPT_PASSWORD     => 4;
use constant AUTH_MD5_PASSWORD       => 5;
use constant AUTH_SCM_CREDENTIAL     => 6;

sub new {
    my ($class, %args) = @_;

    my $self = bless {
        hostname        => $args{hostname},
        path            => $args{path}     || DEFAULT_UNIX_SOCKET,
        port            => $args{port}     || DEFAULT_PORT_NUMBER,
        database        => $args{database} || $ENV{USER} || '',
        user            => $args{user}     || $ENV{USER} || '',
        password        => $args{password} || '',
        args            => $args{args}     || '',
        tty             => $args{tty}      || '',
        timeout         => $args{timeout}  || DEFAULT_TIMEOUT,
        'socket'        => undef,
        backend_pid     => '',
        secret_key      => '',
        selected_record => undef,
        error_message   => '',
        last_oid        => undef,
    }, $class;
    $DEBUG = 1 if $args{debug};
    $self->_initialize;
    return $self;
}

sub close {
    my ($self) = @_;
    my $socket = $self->{'socket'} or return;
    return if !fileno $socket;

    my $terminate_packet = 'X' . pack 'N', 5;
    _dump_packet($terminate_packet);
    $socket->send($terminate_packet, 0);
    $socket->close;
    $self->{closed} = 1;
}

sub DESTROY {
    my ($self) = @_;
    $self->close if $self;
}

sub _initialize {
    my ($self) = @_;
    $self->_connect;
    $self->_do_startup;
}

sub _connect {
    my ($self) = @_;

    my $sock;
    if ($self->{hostname}) {
        $sock = IO::Socket::INET->new(
            PeerAddr => $self->{hostname},
            PeerPort => $self->{port},
            Proto    => 'tcp',
            Timeout  => $self->{timeout},
        ) or Carp::croak("Couldn't connect to $self->{hostname}:$self->{port}/tcp: $!");
    }
    else {
        (my $path = $self->{path}) =~ s{/*\z}{/.s.PGSQL.$self->{port}};
        $sock = IO::Socket::UNIX->new(
            Type => IO::Socket::SOCK_STREAM,
            Peer => $path,
        ) or Carp::croak("Couldn't connect to $path: $!");
    }
    $sock->autoflush(1);
    $self->{socket} = $sock;
}

sub get_handle { $_[0]{socket} }

sub _do_startup {
    my ($self) = @_;

    # create message body
    my $packet = pack 'n n a64 a32 a64 a64 a64', (
        2,                      # Protocol major version - Int16bit
        0,                      # Protocol minor version - Int16bit
        $self->{database},      # Database name          - LimString64
        $self->{user},          # User name              - LimString32
        $self->{args},          # Command line args      - LimString64
        '',                     # Unused                 - LimString64
        $self->{tty}            # Debugging msg tty      - LimString64
    );

    # add packet length
    $packet = pack('N', length($packet) + 4). $packet;
    _dump_packet($packet);

    $self->{socket}->send($packet, 0);
    $self->_do_authentication;
}

sub _dump_packet {
    return unless $DBD::PgPP::Protocol::DEBUG;

    my ($packet) = @_;

    printf "%s()\n", (caller 1)[3];
    while ($packet =~ m/(.{1,16})/g) {
        my $chunk = $1;
        print join ' ', map { sprintf '%02X', ord $_ } split //, $chunk;
        print '   ' x (16 - length $chunk);
        print '  ';
        print join '',
            map { sprintf '%s', (/[[:graph:] ]/) ? $_ : '.' } split //, $chunk;
        print "\n";
    }
}

sub get_stream {
    my ($self) = @_;
    $self->{stream} = DBD::PgPP::PacketStream->new($self->{'socket'})
        if !defined $self->{stream};
    return $self->{stream};
}

sub _do_authentication {
    my ($self) = @_;
    my $stream = $self->get_stream;
    while (1) {
        my $packet = $stream->each;
        printf "Receive %s\n", ref($packet) if $DEBUG;
        last if $packet->is_end_of_response;
        Carp::croak($packet->get_message) if $packet->is_error;
        $packet->compute($self);
    }
}

sub prepare {
    my ($self, $sql) = @_;

    $self->{error_message} = '';
    return DBD::PgPP::ProtocolStatement->new($self, $sql);
}

sub has_error {
    my ($self) = @_;
    return 1 if $self->{error_message};
}

sub get_error_message {
    my ($self) = @_;
    return $self->{error_message};
}

sub parse_statement {
    my ($invocant, $statement) = @_;

    my $param_num = 0;
    my $comment_depth = 0;
    my @tokens = ('');
  Parse: for ($statement) {
        # Observe the default action at the end
        if    (m{\G \z}xmsgc) { last Parse }
        elsif (m{\G( /\* .*? ) (?= /\* | \*/) }xmsgc) { $comment_depth++ }
        elsif ($comment_depth && m{\G( .*? ) (?= /\* | \*/)}xmsgc) { }
        elsif ($comment_depth && m{\G( \*/ )}xmsgc)   { $comment_depth-- }
        elsif (m{\G \?}xmsgc) {
            pop @tokens if $tokens[-1] eq '';
            push @tokens, \(my $tmp = $param_num++), '';
            redo Parse;
        }
        elsif (m{\G( -- [^\n]* )}xmsgc) { }
        elsif (m{\G( \' (?> [^\\\']* (?> \\. [^\\\']*)* ) \' )}xmsgc) { }
        elsif (m{\G( \" [^\"]* \" )}xmsgc) { }
        elsif (m{\G( \s+ | \w+ | ::? | \$[0-9]+ | [-/*\$]
                 | [^[:ascii:]]+ | [\0-\037\177]+)}xmsgc) { }
        elsif (m{\G( [+<>=~!\@\#%^&|`,;.()\[\]{}]+ )}xmsgc) { }
        elsif (m{\G( [\'\"\\] )}xmsgc) { } # unmatched: a bug in your query
        else {
            my $pos = pos;
            die "BUG: can't parse statement at $pos\n$statement\n";
        }

        $tokens[-1] .= $1;
        redo Parse;
    }

    pop @tokens if @tokens > 1 && $tokens[-1] eq '';

    return \@tokens;
}


package DBD::PgPP::ProtocolStatement;

sub new {
    my ($class, $pgsql, $statement) = @_;
    bless {
        postgres  => $pgsql,
        statement => $statement,
        rows      => [],
    }, $class;
}

sub execute {
    my ($self) = @_;

    my $pgsql = $self->{postgres};
    my $handle = $pgsql->get_handle;

    my $query_packet = "Q$self->{statement}\0";
    DBD::PgPP::Protocol::_dump_packet($query_packet);
    $handle->send($query_packet, 0);
    $self->{affected_rows} = 0;
    $self->{last_oid}      = undef;
    $self->{rows}          = [];

    my $stream = $pgsql->get_stream;
    my $packet = $stream->each;
    printf "Receive %s\n", ref($packet) if $DBD::PgPP::Protocol::DEBUG;
    if ($packet->is_error) {
        $self->_to_end_of_response($stream);
        die $packet->get_message;
    }
    elsif ($packet->is_end_of_response) {
        return;
    }
    elsif ($packet->is_empty) {
        $self->_to_end_of_response($stream);
        return;
    }
    while ($packet->is_notice_response) {
        # discard it for now
        $packet = $stream->each;
    }
    if ($packet->is_cursor_response) {
        $packet->compute($pgsql);
        my $row_info = $stream->each; # fetch RowDescription
        if ($row_info->is_error) {
            $self->_to_end_of_response($stream);
            Carp::croak($row_info->get_message);
        }
        $row_info->compute($self);
        while (1) {
            my $row_packet = $stream->each;
            printf "-Receive %s\n", ref $row_packet
                if $DBD::PgPP::Protocol::DEBUG;
            if ($row_packet->is_error) {
                $self->_to_end_of_response($stream);
                Carp::croak($row_packet->get_message);
            }
            $row_packet->compute($self);
            push @{ $self->{rows} }, $row_packet->get_result;
            last if $row_packet->is_end_of_response;
        }
        return;
    }
    else {                      # CompletedResponse
        $packet->compute($self);
        while (1) {
            my $end = $stream->each;
            printf "-Receive %s\n", ref($end) if $DBD::PgPP::Protocol::DEBUG;
            if ($end->is_error) {
                $self->_to_end_of_response($stream);
                Carp::croak($end->get_message);
            }
            last if $end->is_end_of_response;
        }
        return;
    }
}

sub _to_end_of_response {
    my ($self, $stream) = @_;

    while (1) {
        my $packet = $stream->each;
        $packet->compute($self);
        last if $packet->is_end_of_response;
    }
}

sub fetch {
    my ($self) = @_;
    return shift @{ $self->{rows} }; # shift returns undef if empty
}


package DBD::PgPP::PacketStream;

# Message Identifiers
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

# Authentication Message specifiers
use constant AUTHENTICATION_OK                 => 0;
use constant AUTHENTICATION_KERBEROS_V4        => 1;
use constant AUTHENTICATION_KERBEROS_V5        => 2;
use constant AUTHENTICATION_CLEARTEXT_PASSWORD => 3;
use constant AUTHENTICATION_CRYPT_PASSWORD     => 4;
use constant AUTHENTICATION_MD5_PASSWORD       => 5;
use constant AUTHENTICATION_SCM_CREDENTIAL     => 6;

sub new {
    my ($class, $handle) = @_;
    bless {
        handle => $handle,
        buffer => '',
    }, $class;
}

sub set_buffer {
    my ($self, $buffer) = @_;
    $self->{buffer} = $buffer;
}

sub get_buffer { $_[0]{buffer} }

sub each {
    my ($self) = @_;
    my $type = $self->_get_byte;
    # XXX: This would perhaps be better as a dispatch table
    return $type eq ASCII_ROW             ? $self->_each_ascii_row
         : $type eq AUTHENTICATION        ? $self->_each_authentication
         : $type eq BACKEND_KEY_DATA      ? $self->_each_backend_key_data
         : $type eq BINARY_ROW            ? $self->_each_binary_row
         : $type eq COMPLETED_RESPONSE    ? $self->_each_completed_response
         : $type eq COPY_IN_RESPONSE      ? $self->_each_copy_in_response
         : $type eq COPY_OUT_RESPONSE     ? $self->_each_copy_out_response
         : $type eq CURSOR_RESPONSE       ? $self->_each_cursor_response
         : $type eq EMPTY_QUERY_RESPONSE  ? $self->_each_empty_query_response
         : $type eq ERROR_RESPONSE        ? $self->_each_error_response
         : $type eq FUNCTION_RESPONSE     ? $self->_each_function_response
         : $type eq NOTICE_RESPONSE       ? $self->_each_notice_response
         : $type eq NOTIFICATION_RESPONSE ? $self->_each_notification_response
         : $type eq READY_FOR_QUERY       ? $self->_each_ready_for_query
         : $type eq ROW_DESCRIPTION       ? $self->_each_row_description
         :         Carp::croak("Unknown message type: '$type'");
}

sub _each_authentication {
    my ($self) = @_;

    my $code = $self->_get_int32;
    if ($code == AUTHENTICATION_OK) {
        return DBD::PgPP::AuthenticationOk->new;
    }
    elsif ($code == AUTHENTICATION_KERBEROS_V4) {
        return DBD::PgPP::AuthenticationKerberosV4->new;
    }
    elsif ($code == AUTHENTICATION_KERBEROS_V5) {
        return DBD::PgPP::AuthenticationKerberosV5->new;
    }
    elsif ($code == AUTHENTICATION_CLEARTEXT_PASSWORD) {
        return DBD::PgPP::AuthenticationCleartextPassword->new;
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
        return DBD::PgPP::AuthenticationSCMCredential->new;
    }
    else {
        Carp::croak("Unknown authentication type: $code");
    }
}

sub _each_backend_key_data {
    my ($self) = @_;
    my $process_id = $self->_get_int32;
    my $secret_key = $self->_get_int32;
    return DBD::PgPP::BackendKeyData->new($process_id, $secret_key);
}

sub _each_error_response {
    my ($self) = @_;
    my $error_message = $self->_get_c_string;
    return DBD::PgPP::ErrorResponse->new($error_message);
}

sub _each_notice_response {
    my ($self) = @_;
    my $notice_message = $self->_get_c_string;
    return DBD::PgPP::NoticeResponse->new($notice_message);
}

sub _each_notification_response {
    my ($self) = @_;
    my $process_id = $self->_get_int32;
    my $condition = $self->_get_c_string;
    return DBD::PgPP::NotificationResponse->new($process_id, $condition);
}

sub _each_ready_for_query {
    my ($self) = @_;
    return DBD::PgPP::ReadyForQuery->new;
}

sub _each_cursor_response {
    my ($self) = @_;
    my $name = $self->_get_c_string;
    return DBD::PgPP::CursorResponse->new($name);
}

sub _each_row_description {
    my ($self) = @_;
    my $row_number = $self->_get_int16;
    my @description;
    for my $i (1 .. $row_number) {
        push @description, {
            name     => $self->_get_c_string,
            type     => $self->_get_int32,
            size     => $self->_get_int16,
            modifier => $self->_get_int32,
        };
    }
    return DBD::PgPP::RowDescription->new(\@description);
}

sub _each_ascii_row {
    my ($self) = @_;
    return DBD::PgPP::AsciiRow->new($self);
}

sub _each_completed_response {
    my ($self) = @_;
    my $tag = $self->_get_c_string;
    return DBD::PgPP::CompletedResponse->new($tag);
}

sub _each_empty_query_response {
    my ($self) = @_;
    my $unused = $self->_get_c_string;
    return DBD::PgPP::EmptyQueryResponse->new($unused);
}

sub _get_byte {
    my ($self, $length) = @_;
    $length = 1 if !defined $length;

    $self->_if_short_then_add_buffer($length);
    return substr $self->{buffer}, 0, $length, '';
}

sub _get_int32 {
    my ($self) = @_;
    $self->_if_short_then_add_buffer(4);
    return unpack 'N', substr $self->{buffer}, 0, 4, '';
}

sub _get_int16 {
    my ($self) = @_;
    $self->_if_short_then_add_buffer(2);
    return unpack 'n', substr $self->{buffer}, 0, 2, '';
}

sub _get_c_string {
    my ($self) = @_;

    my $null_pos;
    while (1) {
        $null_pos = index $self->{buffer}, "\0";
        last if $null_pos >= 0;
        $self->_if_short_then_add_buffer(1 + length $self->{buffer});
    }
    my $result = substr $self->{buffer}, 0, $null_pos, '';
    substr $self->{buffer}, 0, 1, ''; # remove trailing \0
    return $result;
}

# This method means "I'm about to read *this* many bytes from the buffer, so
# make sure there are enough bytes available".  That is, on exit, you are
# guaranteed that $length bytes are available.
sub _if_short_then_add_buffer {
    my ($self, $length) = @_;
    $length ||= 0;

    my $handle = $self->{handle};
    while (length($self->{buffer}) < $length) {
        my $packet = '';
        $handle->recv($packet, $BUFFER_LEN, 0);
        DBD::PgPP::Protocol::_dump_packet($packet);
        $self->{buffer} .= $packet;
    }
}


package DBD::PgPP::Response;

sub new {
    my ($class) = @_;
    bless {}, $class;
}

sub compute            { return }
sub is_empty           { undef }
sub is_error           { undef }
sub is_end_of_response { undef }
sub get_result         { undef }
sub is_cursor_response { undef }
sub is_notice_response { undef }


package DBD::PgPP::AuthenticationOk;
use base qw<DBD::PgPP::Response>;


package DBD::PgPP::AuthenticationKerberosV4;
use base qw<DBD::PgPP::Response>;

sub compute { Carp::croak("authentication type 'Kerberos V4' not supported.\n") }


package DBD::PgPP::AuthenticationKerberosV5;
use base qw<DBD::PgPP::Response>;

sub compute { Carp::croak("authentication type 'Kerberos V5' not supported.\n") }


package DBD::PgPP::AuthenticationCleartextPassword;
use base qw<DBD::PgPP::Response>;

sub compute {
    my ($self, $pgsql) = @_;
    my $handle = $pgsql->get_handle;
    my $password = $pgsql->{password};

    my $packet = pack('N', length($password) + 4 + 1). $password. "\0";
    DBD::PgPP::Protocol::_dump_packet($packet);
    $handle->send($packet, 0);
}


package DBD::PgPP::AuthenticationCryptPassword;
use base qw<DBD::PgPP::Response>;

sub new {
    my ($class, $salt) = @_;
    my $self = $class->SUPER::new;
    $self->{salt} = $salt;
    $self;
}

sub get_salt { $_[0]{salt} }

sub compute {
    my ($self, $pgsql) = @_;
    my $handle = $pgsql->get_handle;
    my $password = $pgsql->{password} || '';

    $password = _encode_crypt($password, $self->{salt});
    my $packet = pack('N', length($password) + 4 + 1). $password. "\0";
    DBD::PgPP::Protocol::_dump_packet($packet);
    $handle->send($packet, 0);
}

sub _encode_crypt {
    my ($password, $salt) = @_;

    my $crypted = '';
    eval {
        $crypted = crypt($password, $salt);
        die "is MD5 crypt()" if _is_md5_crypt($crypted, $salt);
    };
    Carp::croak("authentication type 'crypt' not supported on your platform. please use  'trust' or 'md5' or 'ident' authentication")
          if $@;
    return $crypted;
}

sub _is_md5_crypt {
    my ($crypted, $salt) = @_;
    $crypted =~ /^\$1\$\Q$salt\E\$/;
}


package DBD::PgPP::AuthenticationMD5Password;
use base qw<DBD::PgPP::AuthenticationCryptPassword>;

sub new {
    my ($class, $salt) = @_;
    my $self = $class->SUPER::new;
    $self->{salt} = $salt;
    return $self;
}

sub compute {
    my ($self, $pgsql) = @_;
    my $handle = $pgsql->get_handle;
    my $password = $pgsql->{password} || '';

    my $md5ed_password = _encode_md5($pgsql->{user}, $password, $self->{salt});
    my $packet = pack('N', 1 + 4 + length $md5ed_password). "$md5ed_password\0";
    DBD::PgPP::Protocol::_dump_packet($packet);
    $handle->send($packet, 0);
}

sub _encode_md5 {
    my ($user, $password, $salt) = @_;

    my $md5 = DBD::PgPP::EncodeMD5->create;
    $md5->add($password);
    $md5->add($user);

    my $tmp_digest = $md5->hexdigest;
    $md5->add($tmp_digest);
    $md5->add($salt);

    return 'md5' . $md5->hexdigest;
}


package DBD::PgPP::AuthenticationSCMCredential;
use base qw<DBD::PgPP::Response>;

sub compute { Carp::croak("authentication type 'SCM Credential' not supported.\n") }


package DBD::PgPP::BackendKeyData;
use base qw<DBD::PgPP::Response>;

sub new {
    my ($class, $process_id, $secret_key) = @_;
    my $self = $class->SUPER::new;
    $self->{process_id} = $process_id;
    $self->{secret_key} = $secret_key;
    return $self;
}

sub get_process_id { $_[0]{process_id} }
sub get_secret_key { $_[0]{secret_key} }

sub compute {
    my ($self, $postgres) = @_;;

    $postgres->{process_id} = $self->get_process_id;
    $postgres->{secret_key} = $self->get_secret_key;
}


package DBD::PgPP::ErrorResponse;
use base qw<DBD::PgPP::Response>;

sub new {
    my ($class, $message) = @_;
    my $self = $class->SUPER::new;
    $self->{message} = $message;
    return $self;
}

sub get_message { $_[0]{message} }
sub is_error    { 1 }


package DBD::PgPP::NoticeResponse;
use base qw<DBD::PgPP::ErrorResponse>;

sub is_error           { undef }
sub is_notice_response { 1 }


package DBD::PgPP::NotificationResponse;
use base qw<DBD::PgPP::Response>;

sub new {
    my ($class, $process_id, $condition) = @_;
    my $self = $class->SUPER::new;
    $self->{process_id} = $process_id;
    $self->{condition} = $condition;
    return $self;
}

sub get_process_id { $_[0]{process_id} }
sub get_condition  { $_[0]{condition} }


package DBD::PgPP::ReadyForQuery;
use base qw<DBD::PgPP::Response>;

sub is_end_of_response { 1 }


package DBD::PgPP::CursorResponse;
use base qw<DBD::PgPP::Response>;

sub new {
    my ($class, $name) = @_;
    my $self = $class->SUPER::new;
    $self->{name} = $name;
    return $self;
}

sub get_name           { $_[0]{name} }
sub is_cursor_response { 1 }

sub compute {
    my ($self, $pgsql) = @_;
    $pgsql->{cursor_name} = $self->get_name;
}


package DBD::PgPP::RowDescription;
use base qw<DBD::PgPP::Response>;

sub new {
    my ($class, $row_description) = @_;
    my $self = $class->SUPER::new;
    $self->{row_description} = $row_description;
    return $self;
}

sub compute {
    my ($self, $pgsql_sth) = @_;
    $pgsql_sth->{row_description} = $self->{row_description};
}


package DBD::PgPP::AsciiRow;
use base qw<DBD::PgPP::Response>;

sub new {
    my ($class, $stream) = @_;
    my $self = $class->SUPER::new;
    $self->{stream} = $stream;
    return $self;
}

sub compute {
    my ($self, $pgsql_sth) = @_;

    my $stream = $self->{stream};
    my $fields_length = @{ $pgsql_sth->{row_description} };
    my $bitmap_length = $self->_get_length_of_null_bitmap($fields_length);
    my $non_null = unpack 'B*', $stream->_get_byte($bitmap_length);

    my @result;
    for my $i (0 .. $fields_length - 1) {
        my $value;
        if (substr $non_null, $i, 1) {
            my $length = $stream->_get_int32;
            $value = $stream->_get_byte($length - 4);
            my $type_oid = $pgsql_sth->{row_description}[$i]{type};
            if ($type_oid == 16) { # bool
                $value = ($value eq 'f') ? 0 : 1;
            }
            elsif ($type_oid == 17) { # bytea
                $value =~ s{\\(\\|[0-7]{3})}{$BYTEA_DEMANGLE{$1}}g;
            }
        }
        push @result, $value;
    }

    $self->{result} = \@result;
}

sub _get_length_of_null_bitmap {
    my ($self, $number) = @_;
    use integer;
    my $length = $number / 8;
    ++$length if $number % 8;
    return $length;
}

sub get_result         { $_[0]{result} }
sub is_cursor_response { 1 }


package DBD::PgPP::CompletedResponse;
use base qw<DBD::PgPP::Response>;

sub new {
    my ($class, $tag) = @_;
    my $self = $class->SUPER::new;
    $self->{tag} = $tag;
    return $self;
}

sub get_tag { $_[0]{tag} }

sub compute {
    my ($self, $pgsql_sth) = @_;
    my $tag = $self->{tag};

    if ($tag =~ /^INSERT (\d+) (\d+)/) {
        $pgsql_sth->{affected_oid}  = $1;
        $pgsql_sth->{affected_rows} = $2;
    }
    elsif ($tag =~ /^DELETE (\d+)/) {
        $pgsql_sth->{affected_rows} = $1;
    }
    elsif ($tag =~ /^UPDATE (\d+)/) {
        $pgsql_sth->{affected_rows} = $1;
    }
}


package DBD::PgPP::EmptyQueryResponse;
use base qw<DBD::PgPP::Response>;

sub is_empty { 1 }


package DBD::PgPP::EncodeMD5;

=pod

=begin wish

Please do not question closely about this source code ;-)

=end wish

=cut

use vars qw<$a $b $c $d>;

{
    my ($x, $n, $m, $l, $r, $z);

    sub create {
        my ($class) = @_;
        $class = 'Digest::MD5' if eval { require Digest::MD5; 1 };
        return $class->new;
    }

    sub new {
        my ($class) = @_;
        bless { source => '' }, $class;
    }

    sub add {
        my ($self, @data) = @_;
        $self->{source} .= join '', @data;
    }

    sub hexdigest {
        my ($self) = @_;

        my @A = unpack 'N4C24',
            unpack 'u', 'H9T4C`>_-JXF8NMS^$#)4=@<,$18%"0X4!`L0%P8*#Q4``04``04#!P``';
        my @K = map { int abs 2 ** 32 * sin $_ } 1 .. 64;
        my $p;
        my $position = 0;
        do {
            $_ = substr $self->{source}, $position, 64;
            $position += 64;
            $l += $r = length $_;
            $r++, $_ .= "\x80" if $r < 64 && !$p++;
            my @W = unpack 'V16', $_. "\0" x 7;
            $W[14] = $l * 8 if $r < 57;
            ($a, $b, $c, $d) = @A;

            for (0 .. 63) {
                no warnings;
                $a = _m($b + _l(
                    $A[4 + 4 * ($_ >> 4) + $_ % 4],
                    _m(&{(
                        sub { $b & $c | $d & ~ $b },
                        sub { $b & $d | $c & ~ $d },
                        sub { $b ^ $c ^ $d },
                        sub { $c ^ ($b | ~ $d) }
                    )[$z = $_ / 16]}
                           + $W[($A[20 + $z] + $A[24 + $z] * ($_ % 16)) % 16] + $K[$_] + $a)
                ));
                ($a, $b, $c, $d) = ($d, $a, $b, $c)
            }

            my $i = $A[0];
            $A[0] = _m($A[0] + $a);
            $A[1] = _m($A[1] + $b);
            $A[2] = _m($A[2] + $c);
            $A[3] = _m($A[3] + $d);

        } while $r > 56;

        ($x, $n, $m, $l, $r, $z) = ();
        $self->{source} = '';

        return unpack 'H32', pack 'V4', @A;
    }

    sub _l { ($x = pop @_) << ($n = pop) | 2 ** $n - 1 & $x >> 32 - $n }
    sub _m { ($x = pop @_) - ($m = 1 + ~ 0) * int($x / $m) }
}


1;
__END__

=head1 DESCRIPTION

DBD::PgPP is a pure-Perl client interface for the PostgreSQL database.  This
module implements the network protocol that allows a client to communicate
with a PostgreSQL server, so you don't need an external PostgreSQL client
library like B<libpq> for it to work.  That means this module enables you to
connect to PostgreSQL server from platforms where there's no PostgreSQL
port, or where installing PostgreSQL is prohibitively hard.

=head1 MODULE DOCUMENTATION

This documentation describes driver specific behavior and restrictions; it
does not attempt to describe everything you might need to use DBD::PgPP.  In
particular, users are advised to be familiar with the DBI documentation.

=head1 THE DBI CLASS

=head2 DBI Class Methods

=over 4

=item B<connect>

At a minimum, you need to use code like this to connect to the database:

  $dbh = DBI->connect('dbi:PgPP:dbname=$dbname', '', '');

This connects to the database $dbname on localhost without any user
authentication.  This may well be sufficient for some PostgreSQL
installations.

The following connect statement shows all possible parameters:

  $dbh = DBI->connect("dbi:PgPP:dbname=$dbname", $username, $password);

  $dbh = DBI->connect("dbi:PgPP:dbname=$dbname;host=$host;port=$port",
                      $username, $password);

  $dbh = DBI->connect("dbi:PgPP:dbname=$dbname;path=$path;port=$port",
                      $username, $password);

      parameter | hard coded default
      ----------+-------------------
      dbname    | current userid
      host      | localhost
      port      | 5432
      path      | /tmp
      debug     | undef

If a host is specified, the postmaster on this host needs to be started with
the C<-i> option (TCP/IP socket).

For authentication with username and password appropriate entries have to be
made in pg_hba.conf.  Please refer to the PostgreSQL documentation for
pg_hba.conf and pg_passwd for the various types of authentication.

=back

=head1 LIMITATIONS

=over 4

=item *

Can't use 'crypt' authentication in a part of FreeBSD.

=item *

Can't use the 'Kerberos v4/5' authentication.

=item *

Can't use the SSL Connection.

=item *

Can't use BLOB data.

=back

=head1 DEPENDENCIES

This module requires these other modules and libraries:

L<DBI>, L<IO::Socket>

=head1 TODO

=over 4

=item *

Add the original crypt (pure perl) method.

=back

=head1 SEE ALSO

L<DBI>, L<DBD::Pg>,
L<http://developer.postgresql.org/docs/postgres/protocol.html>

=head1 AUTHOR

Hiroyuki OYAMA E<lt>oyama@module.jpE<gt>

=head1 COPYRIGHT AND LICENCE

Copyright (C) 2004 Hiroyuki OYAMA.  All rights reserved.

This library is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut

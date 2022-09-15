#! /usr/bin/perl

use strict;
use warnings;
use utf8;
use Data::Dumper;
use DBI;


my %db_conf = (
	db_host	=>	"78.24.223.117",
	db_login	=>	'gpbt1',
	db_password	=>	'gpbt20220915',
	db_name	=>	"gpbtest"
);

my ($sql_message, $sql_log) = "";
my ($sql_message_rows_count, $sql_log_rows_count) = 0;


# использовал mysql, с указанными логином/паролем база gpbtest доступна с любых ip-адресов на выборку, вставку и удаление записей
my $dbh = DBI->connect("DBI:mysql:dbname=$db_conf{db_name}:hostname=$db_conf{db_host}",$db_conf{db_login},$db_conf{db_password}) or die ("db not connect");



######################################################################################
# задача тестовая, потому при каждом запуске скрипт прежде всего очищает таблицы 
# чтобы в случае многократного запуска не создались копии данных в таблице log 
# и не случилось ошибки при добавлении в таблицу message из-за дубликации primary key 
my $sql = "DELETE FROM message WHERE created";
my $sth = $dbh->do($sql) or print "ERROR IN SQL\n $sql \n";
$sql = "DELETE FROM log WHERE created";
$sth = $dbh->do($sql) or print "ERROR IN SQL\n $sql \n";
######################################################################################



# добавление данных занимает бОльшую часть времени работы скрипта, поэтому добавлять будем по 1000 строк за раз
# тут готовятся будущие sql-запросы для 2 таблиц
&make_sql_message();
&make_sql_log();


# объявляем основной хеш в который будем парсить строки лога
my %row_info = ();


# файл лога out должен лежать в папке рядом с этим скриптом парсера
open(MAILLOG,"< out") or die ("no find log file");


while (my $row = <MAILLOG>) {
	
	# чистим хеш для разбора каждой строки
	%row_info = (
		'created' => '',
		'int_id' => '',
		'str' => '',
		'id' => '',
		'flag' => '',
		'address' => ''
	);
	
	&row_parse($row);  # парсим взятую из лога строку


	# добавляем в sql-запросы полученные данные в зависимости от того обрабатываем реальную строку о прибытии письма или нет
	if ( $row_info{flag} eq '<=' and $row_info{id} ne '' ) {
		$sql_message .= "('$row_info{created}', '$row_info{id}', '$row_info{int_id}', '$row_info{str}'),";
		$sql_message_rows_count++;
		# если sql содержит уже 1000 строк на добавление, то выполняем его и формируем основу нового
		if ( $sql_message_rows_count >= 1000 ) {
			&do_sql($sql_message);
			&make_sql_message();
			#die;
		}

	} else {
		$sql_log .= "('$row_info{created}', '$row_info{int_id}', '$row_info{str}', '$row_info{address}'),";
		$sql_log_rows_count++;
		# если sql содержит уже 1000 строк на добавление, то выполняем его и формируем основу нового
		if ( $sql_log_rows_count >= 1000 ) {
			&do_sql($sql_log);
			&make_sql_log();
		}
			
		
	}
	
	
}

# выполняем sql-запросы с оставшимися данными на добавление (не достигшими 1000 строк)
if ( $sql_message_rows_count > 0 ) {
	&do_sql($sql_message);
}
if ( $sql_log_rows_count > 0 ) {
	&do_sql($sql_log);
}


close MAILLOG;
$dbh->disconnect;

# the END








#################### SUBs


sub do_sql {  #выполнение сформированных sql-запросов
	my $sql = shift;
	chop($sql);
	$sth = $dbh->do($sql) or print "ERROR IN SQL\n $sql \n";
}

sub make_sql_message { # создание основы sql-запроса таблицы message на старте скрипта и после каждого выполнения запроса с пакетом в 1000 строк
	$sql_message = "INSERT INTO message
	(created, id, int_id, str)
	VALUES ";
	$sql_message_rows_count = 0;
}


sub make_sql_log { # создание основы sql-запроса таблицы log на старте скрипта и после каждого выполнения запроса с пакетом в 1000 строк
	$sql_log = "INSERT INTO log
	(created, int_id, str, address)
	VALUES ";
	$sql_log_rows_count = 0;
}

sub row_parse { # разбор полученной из лога строки
	my $row = shift;
	
	chop $row; #убираем "перевод строки" из конца строки

	# парсим данные, которые не зависят от содержимого строки, это дата/время, внутренний id и сама строка без даты/времени в начале
	$row_info{created} = (split /\ /, $row)[0]." ".(split /\ /, $row)[1];
	$row_info{int_id} = (split /\ /, $row)[2];
	$row_info{str} = substr($row, length($row_info{created})+1);
	$row_info{str} =~ s/'/\\'/g; # чтобы далее при вставке в таблицы не случилось ошибки, экранируем в строке символ одинарной кавычки, если он в строке есть

	# получаем из строки то, что, возможно, является флагом
	# но еще не факт что это флаг т.к. не все строки его содержат
	$row_info{flag} = (split /\ /, $row)[3];

	# если имеем флаг получения письма, то берем из строки id
	# так как не все строки лога с флагом получения письма имеют id и являются строками о прибытии сообщения, то id или получим или нет
	# только строка лога с соответствующим флагом и имеющая id считается валидной записью о прибытии сообщения
	# это допусловие про id выходит за рамки задачи, но по условиям id в таблице message является prinmary key поэтому без него не обойтись
	if ( $row_info{flag} eq '<=' ) {
		$row_info{id} = (split /\ id=/, $row_info{str})[1] or $row_info{id} = '';
	} elsif ( length($row_info{flag}) == 2 ) { #если имеем всетаки флаг (длина 2) но не получения письма, то берем адрес
		$row_info{address} = (split /\ /, $row_info{str})[2];
	}
		#если флага в строке вообще небыло, то нет ни id ни адреса и далее в log сохранится лишь сама строка с int_id

}

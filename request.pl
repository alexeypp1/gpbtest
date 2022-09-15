#!/usr/bin/perl

use strict;
use warnings;
use CGI qw(:param);
use DBI;
use utf8;


my %db_conf = (
	db_host		=>	"localhost",
	db_login	=>	'gpbt2',
	db_password	=>	'gpbt20220915',
	db_name		=>	"gpbtest"
);


my $address = param("address");

print qq{
<form name="test" action="http://34369.ru/cgi-bin/request.pl" method="post">
	enter email address&nbsp;&nbsp;&nbsp;
	<input type="text" id="address" name="address" value="$address">
	<button type="submit">Submit</button>
</form>
<br><br>
};


############################


if ( $address ) {
	my $dbh = DBI->connect("DBI:mysql:dbname=$db_conf{db_name}:hostname=$db_conf{db_host}",$db_conf{db_login},$db_conf{db_password}) or die ("db not connect");

	my $sql = "SELECT q1.created, q1.str FROM (
		(
			SELECT created, int_id, str, '0' as address FROM message
			WHERE int_id IN (
				SELECT int_id FROM log
				WHERE address = '$address'
				GROUP BY int_id
				ORDER BY int_id
			)
			LIMIT 101
		)
		UNION ALL
		(
			SELECT created, int_id, str, address FROM log
			WHERE int_id IN (
				SELECT int_id FROM log
				WHERE address = '$address'
				GROUP BY int_id
				ORDER BY int_id
			)
			LIMIT 101
		)
		ORDER BY int_id, created, address DESC
		LIMIT 101
		
	)q1
	ORDER BY q1.int_id, q1.created
	";

	my $sth = $dbh->prepare($sql) or die "ERROR1";
	$sth->execute() or print "ERROR2";;

	my $i = 1;
	while ( my @row = $sth->fetchrow_array ) {
		print qq{$row[0]&nbsp&nbsp&nbsp$row[1]<br>};
		$i++;
		if ( $i > 100 ) {
			print qq{<br>...rows more 100...<br><br>};
			last;
		}
	}
	$sth->finish;
	$dbh->disconnect;
}


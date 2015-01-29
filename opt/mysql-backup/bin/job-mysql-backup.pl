#!/usr/bin/perl
#job-mysql-backup
#
#Description: Script to backup using RSYNC and MySQLDump
#
#Author:
#        Gabriel Prestes (helkmut@gmail.com)
#
#04-12-2012 : Created
#04-13-2012 : Modified
#06-01-2012 : Modified
#06-02-2012 : Modified
#06-06-2012 : Modified
#06-08-2012 : Modified
#07-19-2012 : Modified

use strict;
use Getopt::Long;
use POSIX;
use File::Basename;
use DBI;
use warnings;

#--------------------------------------------------
# Setting environment
#--------------------------------------------------
$ENV{"USER"}="root";
$ENV{"HOME"}="/root";

#--------------------------------------------------
# Global variables
#--------------------------------------------------
our $name = basename($0);
our $version = "0.3";
our $opt_path = "/opt/mysql-backup";
our $log_date = `/bin/date -I`;
chomp($log_date);
our $temp_log = "$opt_path/logs/job-mysql-backup-$log_date.log";
our $opt_props = "/opt/mysql-backup/conf/backup.props";
our ($opt_help, $opt_verbose, $opt_version);

#--------------------------------------------------
# Prop variables
#--------------------------------------------------
our $opt_datasource;
our $opt_datahost;
our $opt_datassh;
our $opt_datadest;
our $opt_datatar;
our $opt_datatar2;
our $opt_datatar3;
our $opt_datadump;
our $opt_datausb;
our $opt_datauser;
our $opt_datapasswd;
our $opt_backups;
our $opt_thrusb;
our $opt_mailaddr;

sub main {

        # --- Get Options --- #
        getoption();

        # --- Init function vars ---#
        my $counter = 0;
        my $flagcontrol=0;
        my $testspace=0;
        my @prop_split=();
        my @props_array=();
        my @nfs=();
        my @ls=();
        my $cmd;

        # --- Verbose ---#
        logger("|INIT AGENT - job-mysql-backup|");

        # --- Get props program --- #
        open (PROPS, "$opt_props") or error();
        @props_array = <PROPS>;
        close(PROPS);

        foreach(@props_array){

                chomp($_);
                @prop_split = split(/=/,$_);
                if($counter == 0){$opt_datasource = $prop_split[1];}
                if($counter == 1){$opt_datadest = $prop_split[1];}
                if($counter == 2){$opt_datatar = $prop_split[1];}
                if($counter == 3){$opt_datadump = $prop_split[1];}
                if($counter == 4){$opt_datausb = $prop_split[1];}
                if($counter == 5){$opt_datauser = $prop_split[1];}
                if($counter == 6){$opt_datapasswd = $prop_split[1];}
                if($counter == 7){$opt_datahost = $prop_split[1];}
                if($counter == 8){$opt_datassh = $prop_split[1];}
                if($counter == 9){$opt_backups = $prop_split[1];}
                if($counter == 10){$opt_thrusb = $prop_split[1];}
                if($counter == 11){$opt_mailaddr = $prop_split[1];}
                if($counter == 12){$opt_datatar2 = $prop_split[1];}
                if($counter == 13){$opt_datatar3 = $prop_split[1];}
                $counter++;

        }

        $counter=0;

        # --- Rotate logs more than 15 days --- #
        logger("|LOGs - Search for more than 15 days old|");
        $cmd=`\$\(which find\) $opt_path/logs/* -name "*" -mtime +15 -exec \$\(which rm\) -rf {} \\; > /dev/null 2>&1`;

        # --- Check if NFS is active --- #
        @nfs=`\$\(which ls\) $opt_datasource`;

        if($#nfs > 0){

                logger("|NFS Test - OK($opt_datasource)|");

        } else {

                logger("|ERROR - NFS Disabled($opt_datasource)|");
                mail("|ERROR - NFS Disabled($opt_datasource)|");
                exit_program();

        }

        # --- Stop MySQL --- #
        logger("|MySQL - Stop Instance|");
        $cmd=`\$\(which ssh\) -p $opt_datassh $opt_datahost /etc/init.d/mysqld stop`;
        $flagcontrol=+$?;

        # --- Copy datadir --- #
        if($flagcontrol==0){

                logger("|MySQL local - Stop MySQL|");
                $cmd=`/etc/init.d/mysqld stop`;

#DISABLED ->    logger("|RSYNC - Remove old datadir|");
#DISABLED ->    $cmd=`\$\(which rm\) -rf $opt_datadest/* >> $temp_log 2>&1`;

                logger("|RSYNC - Replicate MySQL Instance|");
                $cmd=`\$\(which rsync\) -agov --delete-after $opt_datasource/ $opt_datadest/ >> $temp_log 2>&1`;
                $flagcontrol=+$?;

        } else {

                logger("|ERROR - MySQL - Stop fail|");
                mail("|ERROR - MySQL - Stop fail|");
                exit_program();

        }

        if($flagcontrol!=0){

                logger("|RSYNC - Retring Replicate MySQL Instance|");
                sleep(10);
                $flagcontrol=0;
                $cmd=`\$\(which rsync\) -agov --delete-after $opt_datasource/ $opt_datadest/ >> $temp_log 2>&1`;
                $flagcontrol=+$?;

        }

        if($flagcontrol!=0){

                # --- Start MySQL --- #
                logger("|MySQL - Start Instance|");
                $cmd=`\$\(which ssh\) -p $opt_datassh $opt_datahost /etc/init.d/mysqld start`;
                logger("|ERROR - RSYNC - Replicate MySQL failed|");
                mail("|ERROR - RSYNC - Replicate MySQL failed|");
                exit_program();

        }

        # --- Start MySQL --- #
        logger("|MySQL - Start Instance|");
        $cmd=`\$\(which ssh\) -p $opt_datassh $opt_datahost /etc/init.d/mysqld start`;
        $flagcontrol=+$?;

        if($flagcontrol==0){mail("|OK - Copy of MySQL Master stored in backup. Processing the rest of the backup job|");}

        # --- Check and write pid --- #
        if(check_pid() == 0){

                logger("|PID - Another job in execution($opt_path/var/job-mysql-backup.pid) - Backup Agent is running for more than 24 hours|");
                mail("|PID - Another job in execution($opt_path/var/job-mysql-backup.pid) - Backup Agent is running for more than 24 hours|");
                exit;

        } else {

                write_pid();

        }

        # --- Dump tables and compress datadir --- #
        logger("|TAR - Compress datadir and send to $opt_datatar|");
        @ls=`\$\(which ls\) -tr $opt_datatar`;

        foreach(@ls){

                chomp($_);

                if(($counter==0) and ($#ls>$opt_backups)){

                        logger("|REMOVED datadir - $_|");
                        $cmd=`\$\(which rm \) -rf $opt_datatar/$_ >> $temp_log 2>&1`;
                        $cmd=`\$\(which rm \) -rf $opt_datatar2/$_ >> $temp_log 2>&1`;
                        $cmd=`\$\(which rm \) -rf $opt_datatar3/$_ >> $temp_log 2>&1`;

                }

                elsif($#ls>$opt_backups){

                        logger("|REMOVED datadir - $_|");
                        $cmd=`\$\(which rm \) -rf $opt_datatar/$_ >> $temp_log 2>&1`;
                        $cmd=`\$\(which rm \) -rf $opt_datatar2/$_ >> $temp_log 2>&1`;
                        $cmd=`\$\(which rm \) -rf $opt_datatar3/$_ >> $temp_log 2>&1`;

                }

                else {

                        logger("|PRESERVED datadir - $_|");

                }

                $counter++;

        }

        $counter=0;

        # --- TAR CREATE --- #
        $cmd=`\$\(which tar\) cfpz $opt_datatar/mysql-datadir-$log_date.tar.gz $opt_datadest`;
        $flagcontrol=+$?;

        # --- TAR TEST --- #
        if($flagcontrol==0){

                # --- White in Server HDs --- #
	        $cmd=`\$\(which cp\) -rf $opt_datatar/mysql-datadir-$log_date.tar.gz $opt_datatar2`;
	        $flagcontrol=+$?;
	        $cmd=`\$\(which cp\) -rf $opt_datatar/mysql-datadir-$log_date.tar.gz $opt_datatar3`;
	        $flagcontrol=+$?;

	        if($flagcontrol==0){mail("|OK - Copy of compressed datadir stored in $opt_datatar $opt_datatar2 and $opt_datatar3. Processing the rest of the backup job|");}


        }

        # --- Write in HD USB --- #
#DISABLED ->    if($flagcontrol==0){
#DISABLED ->
#DISABLED ->        $testspace=hdwrite();
#DISABLED ->
#DISABLED ->        logger("|CP - Copy $opt_datatar/mysql-datadir-$log_date.tar.gz to $opt_datausb|");
#DISABLED ->
#DISABLED ->        if($testspace==1){
#DISABLED ->
#DISABLED ->               $cmd=`\$\(which cp\) -rf $opt_datatar/mysql-datadir-$log_date.tar.gz $opt_datausb`;
#DISABLED ->
#DISABLED ->        } else {
#DISABLED ->
#DISABLED ->            @ls=`\$\(which ls\) -tr $opt_datausb`;
#DISABLED ->
#DISABLED ->            foreach(@ls){
#DISABLED ->
#DISABLED ->                    chomp($_);
#DISABLED ->
#DISABLED ->                    if(($_ =~ "RECYCLE") or ($_ =~ "Information")){next;}
#DISABLED ->
#DISABLED ->                    if(($counter==0) and ($#ls>$opt_backups)){
#DISABLED ->
#DISABLED ->                            logger("|REMOVED datadir in $opt_datausb - $_|");
#DISABLED ->                            $cmd=`\$\(which rm \) -rf $opt_datausb/$_ >> $temp_log 2>&1`;
#DISABLED ->
#DISABLED ->                    }
#DISABLED ->
#DISABLED ->                    elsif($#ls>$opt_backups){
#DISABLED ->
#DISABLED ->                            logger("|REMOVED datadir in $opt_datausb - $_|");
#DISABLED ->                            $cmd=`\$\(which rm \) -rf $opt_datausb/$_ >> $temp_log 2>&1`;
#DISABLED ->
#DISABLED ->                    }
#DISABLED ->
#DISABLED ->                    else {
#DISABLED ->
#DISABLED ->                            logger("|PRESERVED datadir - $_|");
#DISABLED ->
#DISABLED ->                    }
#DISABLED ->
#DISABLED ->                    $counter++;
#DISABLED ->
#DISABLED ->             }
#DISABLED ->
#DISABLED ->             $cmd=`\$\(which cp\) -rf $opt_datatar/mysql-datadir-$log_date.tar.gz $opt_datausb`;
#DISABLED ->
#DISABLED ->        }
#DISABLED ->
#DISABLED ->  } else {
#DISABLED ->
#DISABLED ->    logger("|ERROR - TAR - Compress failed");
#DISABLED ->
#DISABLED ->  }

        logger("|MySQL local - Start MySQL|");
        $cmd=`/etc/init.d/mysqld start`;
        $flagcontrol=+$?;

        if($flagcontrol==0){

		logger("|OK - MySQL local - Started|");
#DISABLED ->    # Dump tables in /dados
#DISABLED ->    $flagcontrol=+dumptables();

        } else{

                logger("|ERROR - MySQL local - Start fail|");

        }

        # --- Finish agent and thresholds --- #
        if($flagcontrol>0){

                logger("|CRITICAL - BACKUP FINISH WITH ERRORS|");
                mail("|CRITICAL - BACKUP FINISH WITH ERRORS|");
                exit_program();

        }

        if($flagcontrol==0){

                logger("|OK - BACKUP FINISH WITHOUT ERRORS|");
                mail("|OK - BACKUP FINISH WITHOUT ERRORS|");
                exit_program();

        }

        error();

}

#--------------------------------------------------------------------------------------

sub getoption {
     Getopt::Long::Configure('bundling');
     GetOptions(
            'V|version'                 => \$opt_version,
            'h|help'                    => \$opt_help,
            'v|verbose=i'               => \$opt_verbose,
        );

     if($opt_help){

             printHelp();
             exit_program();

     }

     if($opt_version){

             print "$name - '$version'\n";
             exit_program();

     }

}

#--------------------------------------------------------------------------------------

sub logger {

        return (0) if (not defined $opt_verbose);

        my $msg = shift (@_);
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
        $wday++;
        $yday++;
        $mon++;
        $year+=1900;
        $isdst++;

        if ($opt_verbose == 0){

                         print "$msg\n";

        } else {

           open(LOG, ">>$temp_log") or error();
           printf LOG ("%02i/%02i/%i - %02i:%02i:%02i => %s\n",$mday,$mon,$year,$hour,$min,$sec,$msg);
           close(LOG);

        }

}

#--------------------------------------------------------------------------------------

sub printHelp {

                my $help = <<'HELP';


                Thanks for use Job MySQL Backup.

                API required:

                strict;
                Getopt::Long;
                POSIX;
                File::Basename;
                DBI;
                warnings;

                Agent binary           : /opt/mysql-backup/bin/job-mysql-backup.pl &
                Configuration Agent in : /opt/mysql-backup/conf/backup.props
                Support                : helkmut@gmail.com

                Salmo 91:

                "Direi do Senhor: Não temerás os terrores da noite, nem a seta que voe de dia, nem peste que anda na escuridão, nem mortandade que assole ao meio-dia."





HELP

                system("clear");
                print $help;

}

#--------------------------------------------------------------------------------------

sub error {

        print "|ERROR - Unexpected return - contact LM² Consulting|\n";
        exit_program();

}

#--------------------------------------------------------------------------------------

sub mail {

        my $msg = shift (@_);
        my $cmd;

        $cmd=`\$\(which echo\) "$msg - for more details check the log agent or contact LM2 Consulting" | \$\(which mail\) -s "JOB - MySQL Backup report - $log_date" $opt_mailaddr`;

        return;

}

#--------------------------------------------------------------------------------------

sub write_pid {

        my $cmd;

        $cmd=`\$\(which touch\) $opt_path/var/job-mysql-backup.pid`;

        return 1;

}

#--------------------------------------------------------------------------------------

sub check_pid {

        if(-e "$opt_path/var/job-mysql-backup.pid"){

                return 0;

        } else {

                return 1;

        }

}

#--------------------------------------------------------------------------------------

sub exit_program {

        my $cmd;

        $cmd=`\$\(which rm\) -rf $opt_path/var/job-mysql-backup.pid`;

        exit;

}

#--------------------------------------------------------------------------------------

sub hdwrite {

        my @df=();
        my $opt_resultdf=0;
        my $returnfunction=0;

        @df=`\$\(which df\) -h`;

        foreach(@df){

                chomp($_);

                if(($_ =~ $opt_datausb) and ($_ =~ m/^.+ (.+)% .+$/)){

                        $opt_resultdf=$1;
                        logger("|CHECK SPACE IN HD USB: $opt_resultdf\% used|");

                        if($opt_resultdf < $opt_thrusb){

                                $returnfunction=1;

                        }

                }

        }

        return($returnfunction);

}

#--------------------------------------------------------------------------------------

sub dumptables {

        my @auxarray=();
        my $dbh;
        my $sth;
        my $results;
        my $cmd;
        my $query="SELECT table_schema,table_name FROM information_schema.tables WHERE table_type LIKE \'BASE TABLE\' AND table_schema NOT IN\(\'zsijnet2\',\'zsij2\',\'zrevisor2\',\'zpdfjornais2\',\'sijnet2\',\'sij_backup\'\)";
        my $flagcontrol=0;
        my $counter=0;

        $dbh = DBI->connect('DBI:mysql:mysql', $opt_datauser, $opt_datapasswd ) or do{ logger("Could not connect to database: $DBI::errstr"); exit_program();} ;


        $sth = $dbh->prepare($query);
        $sth->execute();

        while (my $ref = $sth->fetchrow_hashref()) {

                @auxarray=`\$\(which ps\) -ef | \$\(which grep\) mysqldump`;
                $counter=$#auxarray;

                if($counter<3){

                        logger("|DBD - Dump table : DATABASE = $ref->{'table_schema'}, TABLE_NAME = $ref->{'table_name'}");
                        $cmd=`\$\(which nohup\) \$\(which mysqldump\) --single-transaction -u $opt_datauser $ref->{'table_schema'} $ref->{'table_name'} -p$opt_datapasswd > $opt_datadump/mysql-backup-$ref->{'table_schema'}_$ref->{'table_name'}.dmp &`;
                        $flagcontrol=+$?;


                } else{

                        logger("|DBD - Dump table : DATABASE = $ref->{'table_schema'}, TABLE_NAME = $ref->{'table_name'}");
                        $cmd=`\$\(which mysqldump\) --single-transaction -u $opt_datauser $ref->{'table_schema'} $ref->{'table_name'} -p$opt_datapasswd > $opt_datadump/mysql-backup-$ref->{'table_schema'}_$ref->{'table_name'}.dmp`;
                        $flagcontrol=+$?;

                }

        }

        $sth->finish();
        $dbh->disconnect();

        return($flagcontrol);

}

#--------------------------------------------------------------------------------------

&main
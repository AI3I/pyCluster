# -*- perl -*-
# The system variables - those indicated will need to be changed to suit your
# circumstances (and callsign)
#
# Copyright (c) 1998-2007 - Dirk Koopman G1TLH
#
#

package main;

# this really does need to change for your system!!!!			   
# use CAPITAL LETTERS
$mycall = "AI3I-15";

# your name
$myname = "John Lewis";

# Your 'normal' callsign (in CAPITAL LETTERS) 
$myalias = "AI3I";

# Your latitude (+)ve = North (-)ve = South in degrees and decimal degrees
$mylatitude = +40.751638;

# Your Longtitude (+)ve = East, (-)ve = West in degrees and decimal degrees
$mylongitude = -79.551404;

# Your locator (USE CAPITAL LETTERS)
$mylocator = "FN00FS";

# Your QTH (roughly)
$myqth = "Western Pennsylvania";

# Your e-mail address
$myemail = "dxcluster\@ai3i.net";

# Your BBS addr
$mybbsaddr = "AI3I\@AI3I.#WPA.PA.US.NOAM";

# the default language (the key used must match the one in the Messages file)
$lang = 'en';

# the country codes that my node is located in
# 
# for example 'qw(EA EA8 EA9 EA0)' for Spain and all its islands.
# if you leave this blank then it will use the country code for
# your $mycall. This will suit 98% of sysops (including GB7 BTW).
#

@my_cc = qw();

# the tcp address of the cluster this can be an address of an ethernet port
# but this is more secure. For normal use this will be fine. 
$clusteraddr = "localhost";

# the port number of the cluster (just leave this, unless it REALLY matters to you)
$clusterport = 27754;

# your favorite way to say 'Yes'
$yes = 'Yes';

# your favorite way to say 'No'
$no = 'No';

# the interval between unsolicited prompts if not traffic
$user_interval = 11*60;

# data files live in 
$data = "$root/data";

# system files live in
$system = "$root/sys";

# command files live in
$cmd = "$root/cmd";

# local command files live in (and overide $cmd)
$localcmd = "$root/local_cmd";

# where the user data lives
$userfn = "$data/users";

# the "message of the day" file
$motd = "$data/motd";

# are we debugging ?
@debug = qw(chan state msg cron connect);

# are we doing xml?
$do_xml = 0;

# the SQL database DBI dsn
#$dsn = "dbi:SQLite:dbname=$root/data/dxspider.db";
#$dbuser = "";
#$dbpass = "";

1;

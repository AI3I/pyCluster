#!/usr/bin/perl
#
# This module impliments the outgoing PCxx generation routines
#
# These are all the namespace of DXProt and are separated for "clarity"
#
# Copyright (c) 1998 Dirk Koopman G1TLH
#
#
#

package DXProt;

@ISA = qw(DXChannel);

use DXUtil;
use DXM;
use DXDebug;

use strict;

use vars qw($sentencelth $pc19_version $pc9x_nodupe_first_slot);

$sentencelth = 180;
$pc9x_nodupe_first_slot = 1;

#
# All the PCxx generation routines
#

# create a talk string ($from, $to, $via, $text)
sub pc10
{
	my ($from, $to, $via, $text, $origin) = @_;
	my ($user1, $user2);
	if ($via && $via ne $to && $via ne '*') {
		$user1 = $via;
		$user2 = $to;
	} else {
		$user2 = ' ';
		$user1 = $to;
	}
	$origin ||= $main::mycall;
	$text = unpad($text);
	$text = ' ' unless $text && length $text > 0;
	$text =~ s/\^/%5E/g;
	return "PC10^$from^$user1^$text^*^$user2^$origin^~";
}

# create a dx message (call, freq, dxcall, text) see also pc61
sub pc11
{
	my ($mycall, $freq, $dxcall, $text) = @_;
	my $hops = get_hops(11);
	my $t = time;
	$text = ' ' if !$text;
	$text =~ s/\^/%5E/g;
	return sprintf "PC11^%.1f^$dxcall^%s^%s^$text^$mycall^$main::mycall^$hops^~", $freq, cldate($t), ztime($t);
}

# create a dx message (call, freq, dxcall, text, $ipaddr) see also pc11
sub pc61
{
	my ($mycall, $freq, $dxcall, $text, $ipaddr) = @_;
	my $hops = get_hops(61) || get_hops(11);
	my $t = time;
	$text = ' ' if !$text;
	$text =~ s/\^/%5E/g;
	return sprintf "PC61^%.1f^$dxcall^%s^%s^$text^$mycall^$main::mycall^$ipaddr^$hops^~", $freq, cldate($t), ztime($t);
}

# create an announce message
sub pc12
{
	my ($call, $text, $tonode, $sysop, $wx, $origin) = @_;
	my $hops = get_hops(12);
	$text ||= ' ';
	$text =~ s/\^/%5E/g;
	$tonode ||= '*';
	$sysop ||= ' ';
	$wx ||= '0';
	$origin ||= $main::mycall;
	return "PC12^$call^$tonode^$text^$sysop^$origin^$wx^$hops^~";
}

#
# add one or more users (I am expecting references that have 'call',
# 'conf' & 'here' method)
#
# this will create a list of PC16 with up pc16_max_users in each
# called $self->pc16(..)
#
sub pc16
{
	my $node = shift;
	my $ncall = $node->call;
	my @out;

	my $s = "";
	for (@_) {
		next unless $_;
		my $ref = $_;
		my $str = sprintf "^%s %s %d", $ref->call, $ref->conf ? '*' : '-', $ref->here;
		if (length($s) + length($str) > $sentencelth) {
			push @out, "PC16^$ncall" . $s . sprintf "^%s^", get_hops(16);
			$s = "";
		}
		$s .= $str;
	}
	push @out, "PC16^$ncall" . $s . sprintf "^%s^", get_hops(16);
	return @out;
}

# remove a local user
sub pc17
{
	my @out;
	while (@_) {
		my $node = shift;
		my $ref = shift;
		my $hops = get_hops(17);
		my $ncall = $node->call;
		my $ucall = $ref->call;
		push @out, "PC17^$ucall^$ncall^$hops^";
	}
	return @out;
}

# Request init string
sub pc18
{
	my $flags = shift;
	return "PC18^DXSpider Version: $main::version Build: $main::subversion.$main::build Git: $main::gitversion$flags^$DXProt::myprot_version^";
}

#
# add one or more nodes
#
sub pc19
{
	my @out;
	my @in;

	my $s = "";
	for (@_) {
		next unless $_;
		my $ref = $_;
		my $call = $ref->call;
		my $here = $ref->here;
		my $conf = $ref->conf;
		my $version = $ref->version;
		$version = $pc19_version unless $version =~ /^\d\d\d\d$/;

		my $str = "^$here^$call^$conf^$version";
		if (length($s) + length($str) > $sentencelth) {
			push @out, "PC19" . $s . sprintf "^%s^", get_hops(19);
			$s = "";
		}
		$s .= $str;
	}
	push @out, "PC19" . $s . sprintf "^%s^", get_hops(19);
	return @out;
}

# end of Rinit phase
sub pc20
{
	return 'PC20^';
}

# delete a node
sub pc21
{
	my @out;
	while (@_) {
		my $node = shift;
		my $hops = get_hops(21);
		my $call = $node->call;
		push @out, "PC21^$call^Gone^$hops^";
	}
	return @out;
}

# end of init phase
sub pc22
{
	return 'PC22^';
}

# here status
sub pc24
{
	my $self = shift;
	my $call = $self->call;
	my $flag = $self->here ? '1' : '0';
	my $hops = shift || get_hops(24);

	return "PC24^$call^$flag^$hops^";
}


# create a merged dx message (freq, dxcall, t, text, spotter, orig-node)
sub pc26
{
	my ($freq, $dxcall, $t, $text, $spotter, $orignode) = @_;
	$text = ' ' unless $text;
	$orignode = $main::mycall unless $orignode;
	return sprintf "PC26^%.1f^$dxcall^%s^%s^$text^$spotter^$orignode^ ^~", $freq, cldate($t), ztime($t);
}

# create a merged WWV spot (logger, t, sfi, a, k, forecast, orig-node)
sub pc27
{
	my ($logger, $t, $sfi, $a, $k, $forecast, $orignode) = @_;
	return sprintf "PC27^%s^%-2.2s^$sfi^$a^$k^$forecast^$logger^$orignode^ ^~", cldate($t), ztime($t);
}

# message start (fromnode, tonode, to, from, t, private, subject, origin)
sub pc28
{
	my ($tonode, $fromnode, $to, $from, $t, $private, $subject, $origin, $rr) = @_;
	my $date = cldate($t);
	my $time = ztime($t);
	$private = $private ? '1' : '0';
	$rr = $rr ? '1' : '0';
	$subject ||= ' ';
	return "PC28^$tonode^$fromnode^$to^$from^$date^$time^$private^$subject^ ^5^$rr^ ^$origin^~";
}

# message text (from and to node same way round as pc29)
sub pc29
{
	my ($fromnode, $tonode, $stream, $text) = @_;
	$text = ' ' unless defined $text && length $text > 0;
	$text =~ s/\^/%5E/og;			# remove ^
	return "PC29^$fromnode^$tonode^$stream^$text^~";
}

# subject acknowledge (will have to and from node reversed to pc28)
sub pc30
{
	my ($fromnode, $tonode, $stream) = @_;
	return "PC30^$fromnode^$tonode^$stream^";
}

# acknowledge this tranche of lines (to and from nodes reversed to pc29 and pc28
sub pc31
{
	my ($fromnode, $tonode, $stream) = @_;
	return "PC31^$fromnode^$tonode^$stream^";
}

#  end of message from the sending end (pc28 node order)
sub pc32
{
	my ($fromnode, $tonode, $stream) = @_;
	return "PC32^$fromnode^$tonode^$stream^";
}

# acknowledge end of message from receiving end (opposite pc28 node order)
sub pc33
{
	my ($fromnode, $tonode, $stream) = @_;
	return "PC33^$fromnode^$tonode^$stream^";
}

# remote cmd send
sub pc34
{
	my($fromnode, $tonode, $msg) = @_;
	return "PC34^$tonode^$fromnode^$msg^~";
}

# remote cmd reply
sub pc35
{
	my($fromnode, $tonode, $msg) = @_;
	return "PC35^$tonode^$fromnode^$msg^~";
}

# send all the DX clusters I reckon are connected
sub pc38
{
	return join '^', "PC38", map {$_->call} Route::Node::get_all();
}

# tell the local node to discconnect
sub pc39
{
	my ($call, $reason) = @_;
	my $hops = get_hops(39);
	$reason = "Gone." if !$reason;
	return "PC39^$call^$reason^$hops^";
}

# cue up bulletin or file for transfer
sub pc40
{
	my ($to, $from, $fn, $bull) = @_;
	$bull = $bull ? '1' : '0';
	return "PC40^$to^$from^$fn^$bull^5^";
}

# user info
sub pc41
{
	my $call = shift;
	$call = shift if ref $call;

	my $sort = shift || '0';
	my $info = shift || ' ';
	my $hops = shift || get_hops(41);
	return "PC41^$call^$sort^$info^$hops^~";
}

# abort message
sub pc42
{
	my ($fromnode, $tonode, $stream) = @_;
	return "PC42^$fromnode^$tonode^$stream^";
}

# remote db request
sub pc44
{
	my ($fromnode, $tonode, $stream, $db, $req, $call) = @_;
	$db = uc $db;
	return "PC44^$tonode^$fromnode^$stream^$db^$req^$call^";
}

# remote db data
sub pc45
{
	my ($fromnode, $tonode, $stream, $data) = @_;
	return "PC45^$tonode^$fromnode^$stream^$data^";
}

# remote db data complete
sub pc46
{
	my ($fromnode, $tonode, $stream) = @_;
	return "PC46^$tonode^$fromnode^$stream^";
}

# bull delete
sub pc49
{
	my ($from, $subject) = @_;
	my $hops = get_hops(49);
	return "PC49^$from^$subject^$hops^~";
}

# periodic update of users, plus keep link alive device (always H99)
sub pc50
{
	my $self = shift;
	my $call = $self->call;
	my $n = shift || '0';
	my $hops = shift || 'H99';
	return "PC50^$call^$n^$hops^";
}

# generate pings
sub pc51
{
	my ($to, $from, $val) = @_;
	return "PC51^$to^$from^$val^";
}

# clx remote cmd send
sub pc84
{
	my($fromnode, $tonode, $call, $msg) = @_;
	return "PC84^$tonode^$fromnode^$call^$msg^~";
}

# clx remote cmd reply
sub pc85
{
	my($fromnode, $tonode, $call, $msg) = @_;
	return "PC85^$tonode^$fromnode^$call^$msg^~";
}

# spider route broadcasts
#


sub _gen_pc92
{
	my $sort = shift;
	my $ext = shift;
	my $s = "PC92^$main::mycall^" . gen_pc9x_t() . "^$sort";
	if ($pc9x_nodupe_first_slot && ($sort eq 'A' || $sort eq 'D') && $_[0]->call eq $main::mycall) {
		shift;
		$s .= '^';
	}
	for (@_) {
		$s .= '^' . _encode_pc92_call($_, $ext);
		$ext = 0 unless $sort eq 'A';				# only the first slot has an ext.
	}
	return $s . '^H99^';
}

sub gen_pc92_with_time
{
	my $call = shift;
	my $sort = shift;
	my $t = shift;
	my $ext = 1;
	my $s = "PC92^$call^$t^$sort";
	if ($pc9x_nodupe_first_slot && ($sort eq 'A' || $sort eq 'D') && $_[0]->call eq $main::mycall) {
		shift;
		$s .= '^';
	}
	for (@_) {
		$s .= "^" . _encode_pc92_call($_, $ext);
	}
	return $s . '^H99^';
}

# add a local one
sub pc92a
{
	return _gen_pc92('A', 2, @_);
}

# delete a local one
sub pc92d
{
	return _gen_pc92('D', 0, @_);
}

# send a config
sub pc92c
{
	return _gen_pc92('C', 1, @_);
}

# send a keep alive
sub pc92k
{
	my $nref = shift;
	my $s = "PC92^$main::mycall^" . gen_pc9x_t() . "^K";
	$s .= "^" . _encode_pc92_call($nref, 1) . ":$main::me->{build}";
	$s .= "^" . scalar $nref->nodes;
	$s .= "^" . scalar $nref->users;
	return $s . '^H99^';
}

# send a 'find' message
sub pc92f
{
	my $target = shift;
	my $from = shift;
	return "PC92^$main::mycall^" . gen_pc9x_t() . "^F^$from^$target^H99^"
}

# send a 'reply' message
sub pc92r
{
	my $to = shift;
	my $target = shift;
	my $flag = shift;
	my $ms = shift;
	return "PC92^$main::mycall^" . gen_pc9x_t() . "^R^$to^$target^$flag^$ms^H99^"
}

sub pc93
{
	my $to = shift;				# *, callsign, chat group name, sysop
	my $from = shift;			# from user callsign
	my $via = shift || '*';			# *, node call
	my $line = shift;			# the text
	my $origin = shift;			# this will be present on proxying from PC10

	$line = unpad($line);
	$line =~ s/\^/\\5E/g;		# remove any ^ characters
	my $s = "PC93^$main::mycall^" . gen_pc9x_t() . "^$to^$from^$via^$line";
	$s .= "^$origin" if $origin;
	$s .= "^H99^";
	return $s;
}

1;
__END__




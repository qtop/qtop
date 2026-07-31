#!/bin/bash
##
## Copyright (C) 2008, 2009, 2010, ETH Zuerich / CSCS
## Copyright (C) 2012, University of Luxembourg / LCSB
##
## This module is free software; you can redistribute it and/or modify it
## under the terms of GNU general public license (GPL) version 2 or later.
## See the LICENSE file for details or refer to http://www.gnu.org
##
## Homepage: http://cern.ch/fotis/QTOP
##
## Authors: Fotis Georgatos <fotis@cern.ch>
## $Id: qtop 3053 2012-09-14 13:42:46Z fotis $
## $Revision: 3053 $
## $Date: 2012-09-14 15:42:46 +0200 (Fri, 14 Sep 2012) $
##
## IMPORTANT NOTE: THIS CODE IS WORK OF PROTOTYPING NATURE, THE REGULAR "AS-IS" CONDITIONS APPLY

## ### ### ### ### ### ### ### ### WARNING ### ### ### ### ### ### ### ### ### ### 
## qtop is currently being rewritten in Python; see https://github.com/sfranky/qtop

## TODO items / wishlist
## * Rewrite the whole thing in python; that will solve all these problems in a generic way:
## ** allow for more complex visualization schemes, including per rack view and other geometries
## ** support Rocks cluster format (relates to visualization item really)
## ** support WNs w. complex names (relates to visualization item really)
## ** support visualization of single user's jobs
## ** add support for arc m/w (ie. more complex pool account mappings) and other front-ends
## ** fix bug with SPACING=' ', whereby no WN header lines are reported aligned

## People have proposed to consider also ideas from:
## http://sourceforge.net/projects/pbswebmon
## http://www-rcf.usc.edu/~garrick/perl-PBS/

### CONTROL VARIABLES, YOU CAN USE ANY OF THESE INSIDE ~/.qtop/qtop.conf OR /etc/qtop.conf FILE
# DEBUG=yes			# enforce debugging output
# WNREMAP=yes			# enforce WN name remapping
# DOWNLOADQTOPCOLORMAP=yes	# enforce automatic creation of ~/.qtop and downloading of colormap file
## Better to choose only one of the following options
COLORAUTO=yes			# enforce color mode automation depending on input tty (DEFAULT)
# NOCOLOR			# enforce non-color mode in any case
# FORCECOLOR			# enforce color mode despite not having interactive input tty
## More environment variables that can be overriden using a qtop.conf file
# PBSPATH="/usr/bin"					# Some systems may prefer PBSPATH="/usr/local/bin"
# PBSNODES_A="$PBSPATH/pbsnodes -a"
# QSTAT_Q="$PBSPATH/qstat -q"
# QSTAT="$PBSPATH/qstat"
# COLORMAPFILE="~/.qtop/qtop.colormap"			# Make sure this file exists and has correct contents if you want color
# TMPDIR="/tmp"						# This is directly passed down to a mktemp call
# PBS							# Notify of PBS (instead of torque) pbsnodes output format
# SUSPEND_SECTION_SUMMARY
# SUSPEND_SECTION_NODES
# SUSPEND_SECTION_ACCOUNTS
# SOURCEDIR="MyDirectoryWithQstatNpbsnodesOutput"	# Put the results of the previous 3 commands in such a dir, for offline testing
# SBDII="bdii"
# JOBSUFFIX_OVERRIDE="jobsuffix.subdomain.example.com"	# Use this if autodetection fails
# LRMS_OVERRIDE="mylrms"				# Use this if autodetection fails
# GETUSERS_COMMAND="/opt/edg/sbin/edg-mkgridpool --list"
# WN_COLON="10"						# Put vertical bar every X nodes, experimental feature
# REPLACE_CHARS="0 1 2 3 4 5 6 7 8 9 A B C D E F G H I J K L M N O P Q R S T U V W X Y Z a b c d e f g h i j k l m n o p q r s t u v w x y z" # Replace sequence
# BS="*"
# SPACING=" "						# Experimental option to provide spaces within the output matrix; helps readability in small clusters
# DDEBUG=yes # put a hash in front of this line to enforce developer's breakpoints/output

### Use getopt to change the default parameters
##
## as much as possible we follow conventions seen in:
## * http://www.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap12.html#tag_12_01
## * http://www.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap12.html#tag_12_02
##
cat <<EOF >/tmp/qtop.help.$$

QTOP: PBS report tool. Kindly try also: watch -d $0 . All bugs added by fotis@cern.ch

Usage: (simple vs extended options)

$0 [-a] [-c OFF|ON|AUTO] [-f COLORMAPFILE ] [-h] [-o WN_COLON] [-p PBSPATH] [-s SOURCEDIR] [-x SUMMARY|NODES|ACCOUNTS]

$0 [-a] [-b BDII] [-c OFF|ON|AUTO] [-d] [-f COLORMAPFILE ] [-h] [-i PBS] [-j JOBSUFFIX_OVERRIDE] [-l LRMS_OVERRIDE] [-s SOURCEDIR] [-o WN_COLON] \\
		[-n PBSNODES_A] [-p PBSPATH] [-q QSTAT] [-r REPLACE_CHARS] [-s SOURCEDIR] [-u GETUSERS_COMMAND] [-x SUMMARY|NODES|ACCOUNTS] [-w]

-a	Try WN name remapping	This is used in situations where node names are not a pure arithmetic sequence (eg. rocks clusters)
-b 	Local BDII hostname	Useful on cluster with grid m/w, to compare values among LRMS and Information System
-c 	OFF|ON|AUTO colormode	Select use of ANSI-colored output. AUTO is the default and should work in most cases (sensitive upon input tty)
-d 	Debug information 	Use twice for extra output; mainly intended for usage by developer/debugging purposes
-f	Set COLORMAPFILE	Describe location of file with the definitions of color output, used for generating ANSI sequences
-i	Type of LRMS		One of TORQUE|PBS|OAR|SGE|GE|LL|LSF|SLURM|CONDOR (future-proof, currently only the first two are implemented)
-j	Set JOBSUFFIX_OVERRIDE	Use this if autodetection fails. eg. "jobsuffix.subdomain.example.com"
-l	Set LRMS_OVERRIDE	Use this if autodetection fails. eg. "mylrms"
-n	Set PBSNODES_A command	eg. '\$PBSPATH/pbsnodes -a'
-q	Set QSTAT command	eg. '\$PBSPATH/qstat'
-p	Set PBSPATH		Default is "/usr/bin". Some systems may have the respective pbs commands in PBSPATH='/usr/local/bin'
-s	Set SOURCEDIR		This is useful for offline testing of qtop. Directory should contain the stdout of pbsnodes -a , qstat -q , qstat
-o	Set vertical separator	Put vertical bar every WN_COLON nodes. Experimental option, output may be garbled in certain cases
-r 	Character sequence	Define a sequence of replacement symbols for the individual user accounts. Experimental option
-t	Set alternative TMPDIR. qtop creates there a subdirectory and multiple temporary files underneath
-u	Cmd to get DN mappings	Useful on grid CEs. By default, you should let 'getent passwd' to output the account mappings
-x	Exclude section		Exclude in output one or more of SUMMARY|NODES|ACCOUNTS sections
-w	Autotune, for first run	Create a ~/.qtop directory and wget a default colormap file (will overwrite the existing one, so use with care)

EOF

while getopts "adhb:c:f:i:j:l:n:o:p:q:r:s:t:u:x:w" options; do
  case $options in
    i ) exec $0'4'`echo $OPTARG|tr 'A-Z' 'a-z'` `echo $*|sed "s/-i $OPTARG//g"`;; # implement the escape mechanism for other LRMSs
    a ) WNREMAP=yes;;
    b ) SBDII=$OPTARG;;
    c ) COLORSWITCH=$OPTARG;;
    d ) [ $DEBUG ] && DDEBUG=yes
	DEBUG=yes;;
    f ) COLORMAPFILE=$OPTARG;;
    j ) JOBSUFFIX_OVERRIDE=$OPTARG;;
    l ) LRMS_OVERRIDE=$OPTARG;;
    n ) PBSNODES_A=$OPTARG;;
    o ) WN_COLON=$OPTARG;;
    p ) PBSPATH=$OPTARG;;
    q ) QSTAT=$OPTARG
        QSTAT_Q=$OPTARG -q;;
    r ) REPLACE_CHARS=$OPTARG;;
    s ) SOURCEDIR=$OPTARG;;
    t ) TMPDIR=$OPTARG;;
    u ) GETUSERS_COMMAND=$OPTARG;;
    x ) export SUSPEND_SECTION_$OPTARG=yes;;
    w ) DOWNLOADQTOPCOLORMAP=yes;;
    h ) cat /tmp/qtop.help.$$
        exit 1;;
    * ) cat /tmp/qtop.help.$$
        exit 1;;
  esac
done
rm /tmp/qtop.help.$$

### BEGIN tuning environment variables

LANG=C  # Not setting this can have consequences for sed, sort and join operations and some more (eg. date or sed or tr)
LC_COLLATE= # This one fixes a nasty annoying bug with sed/sort/join, which has been tyranning some users

## Pick any default parameters from /etc/qtop.conf and ~/.qtop/qtop.conf
[ -f /etc/qtop.conf ] && source /etc/qtop.conf
[ -f ~/.qtop/qtop.conf ] && source ~/.qtop/qtop.conf

## Calculate dependent variables and other defaults
TMPDIR=${TMPDIR:-"/tmp"}
WORKDIR=`mktemp -p /tmp -d qtop.XXXXXXXXXX || exit 1`
## This is useful for testing purposes with precalculated input files
[ "$SOURCEDIR" ] && QSTAT_Q="cat $SOURCEDIR/qstat?q*" && QSTAT="cat $SOURCEDIR/qstat.???" && PBSNODES_A="cat $SOURCEDIR/pbsnodes*"
PBSPATH=${PBSPATH:-"/usr/bin"}
QSTAT_Q=${QSTAT_Q:-"$PBSPATH/qstat -q"}
QSTAT=${QSTAT:-"$PBSPATH/qstat"}
PBSNODES_A=${PBSNODES_A:-"$PBSPATH/pbsnodes -a"}
SBDII=${SBDII:-$SITE_GIIS_URL}
# WN_COLON=${WN_COLON:-20} # experimental visualization feature
REPLACE_CHARS=${REPLACE_CHARS:-`(seq 48 57;seq 65 90;seq 97 250)|awk '{printf "%c ",$1}'`} # default string: 0-9, A-Z, a-z..\0xff
BS=${BS:-"*"}                   # Blocked Queue Symbol
## you may use the two next lines for debugging purposes
[ $DEBUG ] && env >$WORKDIR/qtop.env
[ $DEBUG ] && set >$WORKDIR/qtop.set

[ -z "$SGE_CELL" ] || (echo Bailing out... Not yet ready for Sun Grid Engine clusters |(cat;false)) || exit 1

## NOTE: Ideally, the following should be an atomic operation, alas it is not yet possible within shell code
$QSTAT        >$WORKDIR/qstat.out     || (echo Bailing out... qstat failed            |(cat;false)) || exit 1
$QSTAT_Q      >$WORKDIR/qstat.q       || (echo Bailing out... qstat -q failed         |(cat;false)) || exit 1
$PBSNODES_A   >$WORKDIR/pbsnodes.raw  || (echo Bailing out... pbsnodes -a failed      |(cat;false)) || exit 1

## Certain PBS instances provide a pbsnodes output that has to be truncated in this way
sed 's/[[:digit:]]\/, //g' -i $WORKDIR/pbsnodes.raw

## Now, run the magic heuristics module
[ $DEBUG ] && echo -n '=== DEBUG:'
NODELIST=$WORKDIR/pbsnodes.nodelist

## Detect if under PBSPro
grep pbs_version $WORKDIR/pbsnodes.raw|grep -q PBSPro && PBS=yes
[ $PBS ] && sed -i 's/^     pcpus =/     np =/g' $WORKDIR/pbsnodes.raw # difference in the way of reporting number of cores per node

CORES_PER_WN=`grep '^     np = ' $WORKDIR/pbsnodes.raw|sed 's/     np = //g'|sort -rnu|xargs`
CORES_PER_WN=`echo $CORES_PER_WN|cut -d' ' -f1` # In effect, pick the maximum value for reporting
[ $DEBUG ] && echo -n ' CORES_PER_WN='$CORES_PER_WN

JOBSUFFIX=${JOBSUFFIX_OVERRIDE:-"`cat $WORKDIR/pbsnodes.raw|grep jobs.= |tr ',' '\n'|cut -d. -f3-|sed 's/\/[[:digit:]]*$//g;s/^[[:digit:]]*\///g'|uniq`"}
[ $DEBUG ] && echo -n ' JOBSUFFIX='$JOBSUFFIX
WNDOMAIN=`cat $WORKDIR/pbsnodes.raw|grep -v '^ '|grep -v ^$|tee $NODELIST|sed 's/^[-[:alnum:]]*//g'|uniq|cut -c2-`
[ $DEBUG ] && echo -n ' WNDOMAIN='$WNDOMAIN
echo $WNDOMAIN|xargs|grep ' ' && echo '--- Aborting heuristics... Bailing out because WNDOMAIN is not unique ---' && exit 1

WNPREFIX=`cat $NODELIST|sed "s/\.$WNDOMAIN$//g"|sed 's/[[:digit:]]*$//g'|sort -u|xargs`
WNPREFIXTMP=`cat $NODELIST|sed "s/^$WNPREFIX//g"|cut -c1|sort -u`
[ `echo $WNPREFIXTMP|wc -w` = 1 ] && WNPREFIX=$WNPREFIX$WNPREFIXTMP
## If WNPREFIX is not unique go for a remapping spin
echo $WNPREFIX $WNREMAP|xargs|grep -q ' ' && \
	(echo -ne '\n=== WARN: --- Remapping WN names and retrying heuristics... good luck with this ---' 
	k=101 # fictitious first WN number
	for i in `cat $WORKDIR/pbsnodes.raw|grep -v '^ '|grep -v '^$'`;do
		echo "s/^$i$/wn$k/g;"|sort;k=`echo $k+1|bc`;done > $WORKDIR/pbsnodes.sed
	sed -i -f $WORKDIR/pbsnodes.sed $WORKDIR/pbsnodes.raw )

## Refresh values, just in case after possible remapping
WNDOMAIN=`cat $WORKDIR/pbsnodes.raw|grep -v '^ '|grep -v ^$|tee $NODELIST|sed 's/^[-[:alnum:]]*//g'|uniq|cut -c2-`
echo $WNDOMAIN|xargs|grep ' ' && echo '--- Aborting heuristics... Bailing out because WNDOMAIN is not unique ---' && exit 1
WNPREFIX=`cat $NODELIST|sed "s/\.$WNDOMAIN$//g"|sed 's/[[:digit:]]*$//g'|sort -u|xargs`
WNPREFIXTMP=`cat $NODELIST|sed "s/^$WNPREFIX//g"|cut -c1|sort -u`
[ `echo $WNPREFIXTMP|wc -w` = 1 ] && WNPREFIX=$WNPREFIX$WNPREFIXTMP
[ $DEBUG ] && echo -n ' WNPREFIX='$WNPREFIX || echo

## Now do the WNPREFIX check
echo $WNPREFIX|xargs|grep ' ' && echo '--- Aborting heuristics... Bailing out because WNPREFIX is not unique ---' && exit 1

WN_LIST=`cat $NODELIST|sed "s/\.$WNDOMAIN$//g"|sed "s/^$WNPREFIX//g"|sort -n|xargs`
#echo -n ' WN_LIST='$WN_LIST

WN_FIRST=`echo $WN_LIST|cut -d' ' -f1`
[ $DEBUG ] && echo -n ' WN_FIRST='$WN_FIRST

WN_LAST=`echo $WN_LIST|sed 's/.* //g'`
[ $DEBUG ] && echo -n ' WN_LAST='$WN_LAST

[ $WN_LAST ] || (echo; echo --- Aborting heuristics... Bailing out... Not possible to detect WN_LAST |(cat;false)) || exit 1

LRMS=${LRMS_OVERRIDE:-"`grep jobs.= $WORKDIR/pbsnodes.raw|tr ',' '\n'|cut -d. -f2|sed 's/\/[[:digit:]]*//g'|xargs -n1|uniq`"}
[ $DEBUG ] && echo -n ' LRMS='$LRMS
[ $DEBUG ] && echo ' ==='
[ $DEBUG ] && [ "$SOURCEDIR" ] && echo '=== DEBUG: SOURCEDIR='$SOURCEDIR' ==='

GETUSERS_COMMAND=${GETUSERS_COMMAND:-'/opt/edg/sbin/edg-mkgridpool --list'}
#GETUSERS_COMMAND_PARSER="grep -v '^  date'|sed 's/.\[0m//;s/.\[1m//;s/^  subject =.//g;s/:$//g'|xargs -l2"

## refresh with new WNPREFIX
cat $WORKDIR/pbsnodes.raw |sed "s/^$WNPREFIX/wn/g" >$WORKDIR/pbsnodes

## Show some color intelligence; can be forced with "COLOR=yes qtop" or "echo|qtop"; requires a colormap file
case $COLORSWITCH in
    OFF  ) NOCOLOR=yes ;;
    ON   ) FORCECOLOR=yes ;;
    AUTO ) COLORAUTO=yes ;;
esac

## here is a warning to fix your ~/.qtop/qtop.colormap file; you will need this to group scheduled jobs visually, by color
[ ! -f ~/.qtop/qtop.colormap ]	&& DOWNLOADQTOPCOLORMAP=YES
[ "$DOWNLOADQTOPCOLORMAP" ]	&& echo "WARN: It appears this is your first run of qtop... fix COLORMAPFILE or just fetch qtop.colormap with" \
				&& echo "WARN: wget http://cern.ch/fotis/qtop.colormap -nv -O ~/.qtop/qtop.colormap" \
				&& mkdir -p ~/.qtop

[ -f ~/.qtop/qtop.colormap ] && COLORMAPFILE=${COLORMAPFILE:-~/.qtop/qtop.colormap} # safety check
[ -f /etc/qtop.colormap ]    && COLORMAPFILE=${COLORMAPFILE:-/etc/qtop.colormap} # safety check
## Now do the testing and decide for going with color or without
[ -f $COLORMAPFILE ] && COLOR=yes
## Some magic for terminal detection; actually checks input tty
[ "$COLORAUTO" ] && [ -x /usr/bin/tty ] && NOCOLOR=$NOCOLOR`tty -s ||echo yes`
[ "$NOCOLOR" ] && COLOR=''
## FORCECOLOR available for user override, set this for enforcing color
[ "$FORCECOLOR" ] && COLOR=yes && NOCOLOR=''				


### END tuning environment variables




echo -n "QTOP: PBS report tool. Please try: watch -d $0 . All bugs added by fotis@cern.ch. Cross fingers now"

### BEGIN preprocessing section


## The following is needed for getting the user-DN mapping; work here is incomplete, due to m/w diversity
[ "$USER" = root ]	&& DOGETUSERS=yes
[ "$DOGETUSERS"  ]	&& $GETUSERS_COMMAND \
			| grep -v '^  date'|sed 's/.\[0m//;s/.\[1m//;s/^  subject =.//g;s/:$//g' |xargs -l2 |sort > $WORKDIR/pool_mappings \
			|| cat $WORKDIR/qstat.out |sed '1,3d'|awk '{print $3}'|sort -u			> $WORKDIR/pool_mappings

echo -n '.' # This is just to get a feeling of script progress
[ $DDEBUG ] && echo -e '\n=== DEBUG: sleep 3 # point1' && sleep 3 # developer's soft breakpoint

## the following annoying hack is the only shell way to get by, due to the fact that "join" requires string-sorted inputs.
LAST_CPUID=`echo $CORES_PER_WN-1|bc` # yes, this can be done with eval, too
seq 0 $LAST_CPUID|sort > $WORKDIR/CPUs2

cat $WORKDIR/qstat.out |sed '1,2d'|grep -v '^\-\-'|awk '{print $3}'|sort|uniq -c|sort -rn >$WORKDIR/qtopusers
echo $REPLACE_CHARS |xargs -n1|paste $WORKDIR/qtopusers - |cut -c9- |sort |tee $WORKDIR/qtopusers.processed \
        |sed "`grep -v ^# $COLORMAPFILE|awk '{printf "s/^"$1"/"$3"/g;\n"}'|sort -r|tee $WORKDIR/qtop.colormap.processed`" \
	|grep -v ^$|awk '{print "s/^"$2"/"$1$2"%0m/g;"}' |sort -r > $WORKDIR/qtopusers.colormap

cat $WORKDIR/qstat.out|sed '1,2d'|awk '{print $1" "$3}'|sed -r "s/\.$QSTAT_LRMS / /g"|sed -r "s/\.$LRMS / /g"|sort >$WORKDIR/qstat.processed
cat $WORKDIR/pbsnodes|egrep '(^wn|jobs.=)' \
	|sed -r "s/\.$LRMS\././g;s/\.$WNDOMAIN$//g;s/\.$JOBSUFFIX//g;s/$LRMS\//\//g" \
	|sed 's/\= job-sharing/= S/g;s/\= down,/= D/g;s/\= job-exclusive,/= J/g;s/\= offline,/= O/g' >$WORKDIR/pbsnodes.processed

echo -n '.' # This is just to get a feeling of script progress
[ $DDEBUG ] && echo -e '\n=== DEBUG: sleep 3 # point2' && sleep 3 # developer's soft breakpoint

## Prepare WN vectorfiles. The awk parsing in between is needed to reverse a PBS vs torque effect
## In PBS, core ID is job *suffix*, while in Torque it is *prefix*
for i in `seq -w $WN_FIRST $WN_LAST`;do
CORES=`cat $WORKDIR/pbsnodes|egrep "(^wn|np =)"|grep -A1 ^wn$i|xargs|cut -d= -f2` # This is needed to find max core id
echo $CORES|sed 's/[0-9 ]//g'|grep -v ^$ >/dev/null && echo && echo '--- Aborting... Bailing out because something went wrong in Cores calculation...' && exit 1
[ "$CORES" ] || CORES=099			# Fictional maximum number of cores, $CORES should not be empty
CORES=`(echo $CORES'+1')|xargs|bc`	# Add 1, to get ready for the sed expression
cat $WORKDIR/pbsnodes.processed \
	|grep -A1 ^wn$i|grep -v ^wn|sed 's/.*jobs.=//g'|tr , \  |sed 's/\// /g'|xargs -n2|awk '{print $2" "$1}' \
	|([ "$PBS" ] && sort -k2 || sort ) \
	|([ "$PBS" ] && join -1 2 - $WORKDIR/qstat.processed || join - $WORKDIR/qstat.processed)|sort -k3 \
	|join -1 3 - $WORKDIR/qtopusers.processed|sort -k3 \
	|join -1 3 -a 2 - $WORKDIR/CPUs2|sort -nk1|cut -d' ' -f4 \
	|sed 's/^$/_/g' \
	|sed $CORES',$s/_/#/g' \
	|([ "$NOCOLOR" ] && cat || sed -f $WORKDIR/qtopusers.colormap )>$WORKDIR/pbsnodes.wn$i;done

## so, files $WORKDIR/pbsnodes.wn$i will contain the vectors that correspond to WN status

### END preprocessing section


### BEGIN presentation section

echo -n '.' # This is just to get a feeling of script progress
[ $DDEBUG ] && echo -e '\n=== DEBUG: sleep 3 # point3' && sleep 3 # developer's soft breakpoint
echo

[ $SUSPEND_SECTION_SUMMARY ] || (
echo
(echo "===> Job accounting summary <==="
echo "(Rev$Revision: 3053 $)"
[ $NOCOLOR ] || echo '%40m%1;37m'
date
[ $NOCOLOR ] || echo '%0m'
echo "WORKDIR=$WORKDIR"
)|sed 's/%/\\033\[/g' |xargs -0 echo -e|xargs
## 1st line of Usage Totals was inspired by pbstop
echo -en "Usage Totals:\t"
(
## Nodes
[ $NOCOLOR ] || echo '%40m%1;37m'
echo `cat $WORKDIR/pbsnodes |grep 'state ='|egrep -vc '(down|offline)'`/`cat $WORKDIR/pbsnodes |grep -c 'state ='`
[ $NOCOLOR ] || echo '%0m'
echo "Nodes |\t"
## Cores
[ $NOCOLOR ] || echo '%40m%1;37m'
echo `cat $WORKDIR/pbsnodes.wn*|grep -vc _`/ # Shortest way to write this, more elegant too
echo `cat $WORKDIR/pbsnodes |grep "np ="|sed 's/.*=//g'|xargs|tr ' ' '+'|bc`
[ $NOCOLOR ] || echo '%0m'
echo "Cores |\t"
## Jobs
[ $NOCOLOR ] || echo '%40m%1;37m'
echo `cat $WORKDIR/qstat.q|tail -1|cut -c48-|sed 's/^ *//g;s/  */+/g'`
[ $NOCOLOR ] || echo '%0m'
echo "jobs (R+Q) reported by qstat -q\n"
) |sed 's/%/\\033\[/g' |xargs -0 echo -e|xargs

## Queue status
echo -en 'Queues: |'
[ $NOCOLOR ] && ((cat $WORKDIR/qstat.q|grep '  \-\- '|sort|awk '{ if ($9$10 != "ER") printf "'$BS'"; printf $1": "$6"+"$7"|\n"}'; echo "$BS implies blocked")|xargs)
[ $NOCOLOR ] || ((cat $WORKDIR/qstat.q|grep '  \-\- '|sort|awk '{ if ($9$10 != "ER") printf "'$BS'"; else printf $1" ";printf $1": "$6"+"$7" %0m|\n"}' \
	|sed -f $WORKDIR/qtop.colormap.processed;echo "$BS implies blocked")|sed 's/%/\\033\[/g'|xargs -0 echo -e|xargs|sed 's/| /|/g')

## The following reports CE statistics from the site bdii. Note that information reported should be about the *current* CE
## {Total,Free,Logical,Physical}CPUs:"
[ "$SBDII" ] && ADMINTEAM=`ldapsearch -LLL -x -h $SBDII -p 2170 -b 'o=grid' '(ObjectClass=GlueSite)' GlueSiteEmailContact|grep ^GlueSiteEmailContact|sed 's/.*://g'|uniq`
[ "$SBDII" ] && echo -en "Site BDII of <$ADMINTEAM> reports CPUs: |" && ldapsearch -x -LLL -h $SBDII -p 2170 -b 'o=grid'|grep CPUs \
	|sed 's/^GlueCEState//g;s/^GlueSubCluster//g;s/GlueCEInfo//g;s/CPUs//g'|sort -ur|tr '\n' '|'|xargs|sed 's/ical: /:/g'|sed 's/ //g'
)

[ $SUSPEND_SECTION_NODES ] || (
echo
echo "===> Worker Nodes occupancy <=== (you can read in COLUMNS the node IDs; nodes in free state are denoted with - )"
(
## Node ID
echo `seq -w $WN_FIRST $WN_LAST|cut -c1|xargs` = {_Worker_} |sed "s/ /$SPACING/g"
[ $WN_LAST -ge 10 ] && \
echo `seq -w $WN_FIRST $WN_LAST|cut -c2|xargs` = {__Node__} |sed "s/ /$SPACING/g"
[ $WN_LAST -ge 100 ] && \
echo `seq -w $WN_FIRST $WN_LAST|cut -c3|xargs` = {___ID___} |sed "s/ /$SPACING/g"
## Node status
seq -w $WN_FIRST $WN_LAST|sed "s/^/wn/g" >$WORKDIR/pbsnodes.listWN
echo `egrep '(^wn|state.=)' $WORKDIR/pbsnodes|xargs -l2|sed "s/\.$WNDOMAIN//g"|sort \
	|join -a 2 - $WORKDIR/pbsnodes.listWN |awk '{print $4}'|cut -c1|sed 's/^$/?/g' \
	|xargs|tr f -|sed "s/$/ /g"|sed "s/ /%/g"`=Node state |sed "s/%/$SPACING/g"
) | (
	[ 0"$WN_COLON" -ge 4 ] && sed 's/'`head -c $WN_COLON /dev/zero|tr '\0' '.'`'/&|/g;s/^/|/g;s/|*$/|/g'
	[ 0"$WN_COLON" -ge 4 ] || cat ) # These last two lines are just for placing a colon character in output

## Prepare colon files in the pbsnodes.wn* format
[ $WN_COLON ] && seq $CORES_PER_WN |xargs -n1 -i echo '|' | tee $WORKDIR/pbsnodes.wn000 >/dev/null # > $WORKDIR/pbsnodes.wn9999
[ $WN_COLON ] && seq -w 0 $WN_COLON $WN_LAST|sed '1d'|xargs --replace -n1 cp $WORKDIR/pbsnodes.wn000 $WORKDIR/pbsnodes.wn{}.

## ola ta lefta - matrix output kernel
seq 0 $LAST_CPUID|xargs -n1 echo =Core |paste -d' ' $WORKDIR/pbsnodes.wn* - | sed "s/ /$SPACING/g" \
	|sed 's/%/\\033\[/g' |echo -e `sed 's/[\x7f-\xff]/?/g'`|xargs -n1 echo -e # a method to interpret very long ANSI sequences.

## Print last digit of number of cores... inserted for debugging purposes
[ $DEBUG ] && (cat $WORKDIR/pbsnodes|egrep "(^wn|np =)"|xargs -l2 |sort \
	|cut -d= -f2|sed 's/^ //g;s/^[[:digit:]]$/0&/g'|cut -c1|xargs|sed "s/ /$SPACING/g"; echo '#Cores')|xargs
[ $DEBUG ] && (cat $WORKDIR/pbsnodes|egrep "(^wn|np =)"|xargs -l2 |sort \
	|cut -d= -f2|sed 's/^ //g;s/^[[:digit:]]$/0&/g'|cut -c2|xargs|sed "s/ /$SPACING/g"; echo '#Cores')|xargs

echo # empty line after matrix
)

[ $DDEBUG ] && echo -e '\n=== DEBUG: sleep 3 # point4' && sleep 3 # developer's soft breakpoint


[ $SUSPEND_SECTION_ACCOUNTS ] || (
echo "===> User accounts and pool mappings <=== ('all' includes those in C and W states, as reported by qstat)"
echo -e 'id|  R + Q  / all |  unix account  | Userinfo / Grid certificate DN (info provided is according to current user privileges)'
## No comments in terms of maintainability here, the following oneliner and formatter is a tricky piece of code but at least it works quite fine:
## (an alternative for the awk-ward construct is "substr($0,length($1 $2 $3 $4) +5)")
cat $WORKDIR/qstat.out |grep ' R '|awk '{printf $3"\n"}'|sort|uniq -c|awk '{printf $2" "$1"\n"}' > $WORKDIR/qstat.r
cat $WORKDIR/qstat.out |grep ' Q '|awk '{printf $3"\n"}'|sort|uniq -c|awk '{printf $2" "$1"\n"}' > $WORKDIR/qstat.q
cat $WORKDIR/qtopusers |sort -k2 \
	|join -1 2 - $WORKDIR/qtopusers.processed \
	|join -a 1 -e 0 - $WORKDIR/qstat.r|xargs -n1 --replace echo {} 0|cut -d' ' -f-4 \
	|join -a 1 -e 0 - $WORKDIR/qstat.q|xargs -n1 --replace echo {} 0|cut -d' ' -f-5 \
	|join -a 1 - $WORKDIR/pool_mappings|sort -rnk2 \
	|awk '{printf "%1s |%3s +%3s /%4s |%15s |",$3,$4,$5,$2,$1; $1=""; $2=""; $3=""; $4=""; $5="";printf substr($0, 4)"\n"}' \
	|([ "$NOCOLOR" ] \
		&& cat \
		|| sed "`sed 's/%0m//g' $WORKDIR/qtopusers.colormap |xargs`"|sed 's/$/%0m/g' |sed 's/%/\\033\[/g' |xargs -0 -l1 echo -e )
		## if in trouble due to line legth try this one |xargs -s 100000 -0 -l1 echo -e
) | tr '^' ' ' # This is added to allow an escape mechanism for situations where spaces are part of user GECOS field

### END presentation section

rm $WORKDIR/*
rmdir $WORKDIR
## EOF

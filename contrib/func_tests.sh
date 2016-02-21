#!/usr/bin/zsh
# The following script runs three known test cases for qtop, one for each of PBS, OAR, SGE
# No output after "Testing <scheduler>" means test passed. 
# The test actually runs qtop over known scheduler output files, and then diffs them against the expected output. 
# While diffing, one qtop output line is omitted 
# (the one containing word "WORKDIR"), as it contains an everchanging timestamp.
# WARNING: this will only run in zsh, not bash

export QTOPDIR="/home/sfranky/PycharmProjects/qtop"
alias -g NF='*(.om[1])' # newest file in a directory

alias sger='$QTOPDIR/qtop.py -s $QTOPDIR/contrib -FadvvO -b sge'
alias dsger="diff <(grep -v 'WORKDIR' NF) <(grep -v 'WORKDIR' $QTOPDIR/contrib/sger_dvv_out.ref)"
alias oar1='$QTOPDIR/qtop.py -s $QTOPDIR/contrib -FardvvvO -b oar'
alias doar1="diff <(grep -v 'WORKDIR' NF) <(grep -v 'WORKDIR' $QTOPDIR/contrib/oar1_dvv_out.ref)"
alias 4tab='$QTOPDIR/qtop.py -s $QTOPDIR/contrib -raFO -b pbs'
alias d4tab="diff <(grep -v 'WORKDIR' NF) <(grep -v 'WORKDIR' $QTOPDIR/contrib/pbs_dvv_out.ref)"


cd /tmp/  # qtop output is saved there
echo "Testing sge..."
sger
sleep 1
dsger

echo "Testing oar..."
oar1
sleep 1
doar1

echo "Testing pbs..."
4tab
sleep 1
d4tab


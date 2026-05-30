#! /bin/sh -
# The following script runs three known test cases for qtop, one for each of PBS, OAR, SGE
# No output after "Testing <scheduler>" means test passed. 
# The test actually runs qtop over known scheduler output files, and then diffs them against the expected output. 
# While diffing, one qtop output line is omitted 
# (the one containing word "WORKDIR\|Please try it with watch\|Log file created in"), as it contains an everchanging timestamp.

cd ..

echo "(No news is good news!)"
echo "Testing sge..."
grep -v 'WORKDIR\|Please try it with watch\|Log file created in' contrib/sger_dvv_out.ref > /tmp/qtop_testfile
./qtop.py -s contrib -c ON -Fadvv -b sge \
    | grep -v 'WORKDIR\|Please try it with watch\|Log file created in' | diff - /tmp/qtop_testfile

echo "Testing oar..."
grep -v 'WORKDIR\|Please try it with watch\|Log file created in' contrib/oar1_dvv_out.ref > /tmp/qtop_testfile
./qtop.py -c ON -s contrib -FAardvvv -b oar \
    | grep -v 'WORKDIR\|Please try it with watch\|Log file created in' | diff - /tmp/qtop_testfile

echo "Testing pbs..."
grep -v 'WORKDIR\|Please try it with watch\|Log file created in' contrib/pbs_dvv_out.ref > /tmp/qtop_testfile
./qtop.py -c ON -s contrib -raF -b pbs \
    | grep -v 'WORKDIR\|Please try it with watch\|Log file created in' | diff - /tmp/qtop_testfile

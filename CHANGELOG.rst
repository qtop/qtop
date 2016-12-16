0.9.20161216
------------
-  Feature enhancement: key H: highlight users and queues by string or regex #272
-  Feature enhancement: add -rr option: filter out unused core lines #270, #271
-  Feature enhancement: harmonize output filenames for window/full view and json exports #269
-  Bugfix: -r Option to remove empty core lines now works as advertised #267
-  Bugfix: fix for more arcane pbs node names in pbsnodes #266
-  Bugfix: fix for PBS plugin fails when jobs use non-consecutive CPUs #265
-  Bugfix: fix for adding -G flag under demo resulted in a crash, when `getent` was missing #262

0.9.20161207
------------

-  Feature enhancement: online help available with ``?`` ; #257
-  Feature enhancement: unhardwire ``__version__``, make it visible across code; #259 
-  Feature enhancement: Remove ``WORKDIR`` from banner, it was no longer serving a purpose; #243
-  Bugfix around missing ``core_job_map``, when not remapping; #253
-  Bugfix unhardwire ``/tmp``, make the choice of intermediate directory liberal; #254, #255
-  Bugfix under linux watch mode, fix watch issues of various types; #206, #248, #256
-  Bugfix around jobid consistency of input files in OAR - handle gracefully and report in debug log; #258

0.9.20161130
------------

-  Summary in the 1st section now shows ``Total:, Up:, Free:`` nodes
-  Added a nodes column per user in the 3rd section where user info is presented
-  Added -R feature to replay last frames; recording is automatic!
-  Added setup.py, ``pip install --user qtop`` should work
-  Added ``--version``, in case anyone wondered about it
-  Bail out if python version is <2.5
-  Supplied ``qtop`` launcher script, for better system integration
-  Create timestamped intermediate files, to improve ability to look back
-  Bugfix to avoid /tmp getting crowded with temporary files
-  Bugfix for grepping and pipelining qtop's output (1st attempt to treat #206)
-  Bugfixes on PBS (jobid arrays, node states etc)
-  Bugfixes on SGE (job states added, all jobs now visible, userid is correct)
-  Bugfixes on OAR (nodes per user are also visible)
-  Converted documentation files to .rst format

0.8.9
-----

-  watch mode (-w)

   - ((s)) node sorting with custom presets/user-inserted RegEx sorts
   - ((f)) node filtering
   - ((t)) matrix transposition
   - ((F)) toggle full nodename display/numbering
   - ((m)) toggle coloring code (user-id based/queue name-based)
-  queue info can now be colored, three different queues with the same initials can be colored distinctly
-  a good first amount of documentation
-  small bugfixes

0.8.8
-----

-  created a shiny new demo out of a demo mini-grid scheduler (!)

0.8.7
-----

-  added support for queuename display for OAR and PBS (SGE already
   there!)
-  overspill feature (aka oversubscribe/overcommitment) for SGE systems
-  strict checking cmdline var (will compare reported nr. of jobs
   against displayed/found jobs)
-  id column is now more readable (put in brackets)
-  fixed long-standing bug that would report incorrectly the queues of
   jobs in a node, in some cases
-  restored deprecated document file deletion
-  huge refactoring of the codebase

0.8.6
-----

-  Support for python 2.6 for early RHEL6/Centos6/ScientificLinux6
   distros
-  Added tarball creation for better user reporting
-  Added Viewport class
-  Several bugfixes

0.8.5
-----

-  initial support for data anonymisation
-  minor bugfixes

0.8.4
-----

-  Watch replacement with simple pager included (with full color
   functionality, compatible with older RHEL6 systems)
-  GECOS field completed by a less “intruding” command
-  Numerous enhancements and bug fixes

0.8.3
-----

-  The worker node occupancy table can now be viewed horizontally
   (transposed)
-  Custom conf files createable by users
-  Filter/select nodes by name/regex
-  Numerous enhancements and bug fixes

0.8.2
-----

-  Ability to select which of the three qtop parts to display
-  GECOS field implemented
-  States can be assigned more than one lines
-  New node line displays the queue the job belongs to
-  (trivial) auto-detection of batch-system
-  Numerous enhancements and bug fixes

0.8.1
-----

-  Overwrote PyYAML dependency with custom YAML parser

0.8.0
-----

-  Wrapping together support for PBS, OAR, SGE

0.7.3
-----

-  Support for OAR

0.7.2
-----

-  Introduce support for SGE

0.7.1
-----

-  Finalise support for PBS

0.7
---

Enhancements: - Input files are now using YAML for dumping and loading

0.6.7
-----

Enhancements: - created yaml files now have the pid appended to the
filename - pbs-related functions (which create the respective yaml
files) have moved to a dedicated module - took out
state\_dict[‘highest\_core\_busy’], seemed useless (and unused)

Bugfixes: - a separate read\_qstatq\_yaml function added, for
consistency (removed from qstatq2yaml) - change qstatq\_list from list
of tuples to list of dictionaries - offline\_down\_nodes was moved from
pbs.pbsnodes2yaml to read\_pbsnodes\_yaml

0.6.6
-----

Bugfixes: - got rid of all global variables (experimental)

0.6.5
-----

Enhancements: - PBS now supported

0.6.4
-----

Bugfixes: - lines that don’t contain *any* actual core are now not
printed in the matrices.

0.6.3
-----

Enhancements: - optional stopping of vertical separators (every ‘n’
position for x times) - additional vertical separator in the beginning

0.6.2
-----

Bugfixes: - WN matrix width bug ironed out.

0.6.1
-----

Enhancements: - Custom-cut matrices (horizontally, too!), -o switch

0.5.2
-----

Enhancements: - Custom-cut matrices (vertically, not horizontally),
width set by user.

0.5.1
-----

Enhancements: - If more than 20% of the WNs are empty, perform a blind
remap. - Code Cleanup

0.5.0
-----

Bugfixes: - Major rewrite of matrices calculation fixed

New features: - true blind remapping !!

Enhancements: - exotic cases of very high numbering schemes now handled
- more qstat entries successfully parsed - case of many unix accounts
(>62) now handled

0.4.1
-----

Bugfixes: - now understands additional probable names for pbsnodes,qstat
and qstat-q data files

0.4.0
-----

Bugfixes: - corrected colorless switch to have ON/OFF option (default
ON) - qstat\_q didn’t recognize some faulty cpu time entries - now
descriptions are in white, as before.

Enhancements: - Queues in the job accounting summary section are now
coloured

0.3.0
-----

Enhancements: - command-line arguments (mostly empty for now)! -
non-numbered WNs can now be displayed instead of numbered WN IDs

New features: - implement colorless switch (-c)

Bugfixes: - fixed issue with single named WN - better regex pattern and
algorithm for catching complicated numbered WN domain names

0.2.9
-----

New features: - handles cases of non-numbered WNs (e.g. fruit names) -
parses more complex domain names (with more than one dash)

Bugfixes: - correction in WN ID numbers display (tens were problematic
for larger numbers)

0.2.8
-----

Bugfixes: - colour implementation for all of the tables

0.2.7
-----

Bugfixes: - Exiting when there are two jobs on the same core reported on
pbsnodes (remapping functionality to be added) - Number of WNs >1000 is
now handled

0.2.6
-----

Bugfixes: - fixed some names not being detected (%,= chars missing from
regex)

Enhancements: - changed name to qtop, introduced configuration file
qtop.conf and colormap file qtop.colormap

0.2.5
-----

New features: - Working Cores added in Usage Totals - map now splits
into two if terminal width is smaller than the Worker Node number

0.2.4
-----

Enhancements: - implemented some stuff from PEP8 - un-hardwired the file
paths - refactored code around cpu\_core\_dict functionality
(responsible for drawing the map)

0.2.3
-----

Bugfixes: - corrected regex search pattern in make\_qstat to recognize
usernames like spec101u1 (number followed by number followed by letter)
now handles non-uniform setups - R + Q / all: all did not display
everything (E status)

0.2.2
-----

Enhancements: - masking/clipping functionality (when nodes start from
e.g. wn101, empty columns 1-100 are ommited)

0.2.1
-----

Enhancements: - Hashes displaying when the node has less cores than the
max declared by a WN (its np variable)

0.2.0
-----

Bugfixes: - unix accounts are now correctly ordered

0.1.9
-----

Bugfixes: - All CPU lines displaying correctly

0.1.8
-----

Enhancements: - unix account id assignment to CPU0, 1 implemented

0.1.7
-----

Enhancements: - ReadQstatQ function (write in yaml format using Pyyaml)
- output up to Node state!

0.1.6
-----

Bugfixes: - ReadPbsNodes function (write in yaml format using Pyyaml)

0.1.5
-----

Bugfixes: - implemented saving to 3 separate files, QSTAT\_ORIG\_FILE,
QSTATQ\_ORIG\_FILE, PBSNODES\_ORIG\_FILE

0.1.4
-----

Bugfixes: - some “wiremelting” concerning the save directory

0.1.3
-----

Bugfixes: - fixed tabs-to-spaces. Formatting should be correct now.

| Enhancements:
| - Now each state is saved in a separate file in a results folder

0.1.2
-----

Enhancements: - script reads qtop-input.out files from each job and
displays status for each job

0.1.1
-----

Enhancements: - changed implementation in get\_state()

0.1.0
-----

Enhancements: - just read a pbsnodes-a output file and gather the
results in a single line

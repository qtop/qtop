## 0.8.7
  - overspill feature (aka oversubscribe/overcommitment) for SGE systems
  - strict checking cmdline var (will compare reported nr. of jobs against displayed/found jobs)
  - added support for queuename display for OAR and PBS (SGE already there!)
  - id column is now more readable (put in brackets)
  - fixed long-standing bug that would report incorrectly the queues of jobs in a node, in some cases
  - restored deprecated document file deletion
  - huge refactoring of the codebase

## 0.8.6
  - Support for python 2.6 for early RHEL6 distros
  - Added tarball creation for better user reporting
  - Added Viewport class
  - Several bugfixes

## 0.8.4
  - Watch replacement with simple pager included (with full color functionality, compatible with older RHEL6 systems)
  - GECOS field completed by a less "intruding" command
  - Numerous enhancements and bug fixes

## 0.8.3
  - The worker node occupancy table can now be viewed horizontally (transposed)
  - Custom conf files createable by users
  - Filter/select nodes by name/regex
  - Numerous enhancements and bug fixes

## 0.8.2
  - Ability to select which of the three qtop parts to display
  - GECOS field implemented
  - States can be assigned more than one lines
  - New node line displays the queue the job belongs to
  - (trivial) auto-detection of batch-system
  - Numerous enhancements and bug fixes

## 0.8.1
  - Overwrote PyYAML dependency with custom YAML parser

## 0.8.0
  - Wrapping together support for PBS, OAR, SGE

## 0.7.3
  - Support for OAR

## 0.7.2
  - Introduce support for SGE

## 0.7.1
  - Finalise support for PBS

## 0.7

Enhancements:
  - Input files are now using YAML for dumping and loading

## 0.6.7

Enhancements:
  - created yaml files now have the pid appended to the filename
  - pbs-related functions (which create the respective yaml files) have moved to a dedicated module 
  - took out state_dict['highest_core_busy'], seemed useless (and unused)

Bugfixes: 
  - a separate read_qstatq_yaml function added, for consistency (removed from qstatq2yaml)
  - change qstatq_list from list of tuples to list of dictionaries
  - offline_down_nodes was moved from pbs.pbsnodes2yaml to read_pbsnodes_yaml

## 0.6.6

Bugfixes: 
  - got rid of all global variables (experimental)

## 0.6.5

Enhancements: 
  - PBS now supported

## 0.6.4

Bugfixes: 
  - lines that don't contain *any* actual core are now not printed in the matrices.

## 0.6.3

Enhancements: 
  - optional stopping of vertical separators (every 'n' position for x times)
  - additional vertical separator in the beginning

## 0.6.2

Bugfixes: 
  - WN matrix width bug ironed out.

## 0.6.1

Enhancements: 
  - Custom-cut matrices (horizontally, too!), -o switch

## 0.5.2

Enhancements: 
  - Custom-cut matrices (vertically, not horizontally), width set by user.

## 0.5.1

Enhancements: 
  - If more than 20% of the WNs are empty, perform a blind remap.
  - Code Cleanup

## 0.5.0

Bugfixes: 
  - Major rewrite of matrices calculation fixed

New features:
  - true blind remapping !!

Enhancements:
  - exotic cases of very high numbering schemes now handled
  - more qstat entries successfully parsed
  - case of many unix accounts (>62) now handled

## 0.4.1

Bugfixes: 
  - now understands additional probable names for pbsnodes,qstat and qstat-q data files

## 0.4.0

Bugfixes: 
  - corrected colorless switch to have ON/OFF option (default ON)
  - qstat_q didn't recognize some faulty cpu time entries
  - now descriptions are in white, as before.

Enhancements:
  - Queues in the job accounting summary section are now coloured

## 0.3.0

Enhancements:
  - command-line arguments (mostly empty for now)!
  - non-numbered WNs can now be displayed instead of numbered WN IDs

New features:
  - implement colorless switch (-c)
  
Bugfixes: 
  - fixed issue with single named WN
  - better regex pattern and algorithm for catching complicated numbered WN domain names

## 0.2.9

New features:
  - handles cases of non-numbered WNs (e.g. fruit names)
  - parses more complex domain names (with more than one dash)
  
Bugfixes: 
  - correction in WN ID numbers display (tens were problematic for larger numbers)

## 0.2.8

Bugfixes: 
  - colour implementation for all of the tables

## 0.2.7

Bugfixes: 
  - Exiting when there are two jobs on the same core reported on pbsnodes (remapping functionality to be added)
  - Number of WNs >1000 is now handled

## 0.2.6

Bugfixes: 
  - fixed some names not being detected (%,= chars missing from regex)

Enhancements:
  - changed name to qtop, introduced configuration file qtop.conf and colormap file qtop.colormap

## 0.2.5

New features:
  - Working Cores added in Usage Totals
  - map now splits into two if terminal width is smaller than the Worker Node number

## 0.2.4

Enhancements: 
  - implemented some stuff from PEP8
  - un-hardwired the file paths
  - refactored code around cpu_core_dict functionality (responsible for drawing the map)

## 0.2.3

Bugfixes: 
  - corrected regex search pattern in make_qstat to recognize usernames like spec101u1 (number followed by number followed by letter) now handles non-uniform setups 
  - R + Q / all: all did not display everything (E status)

## 0.2.2

Enhancements: 
  - masking/clipping functionality (when nodes start from e.g. wn101, empty columns 1-100 are ommited)

## 0.2.1

Enhancements: 
  - Hashes displaying when the node has less cores than the max declared by a WN (its np variable)

## 0.2.0

Bugfixes: 
  - unix accounts are now correctly ordered

## 0.1.9

Bugfixes: 
  - All CPU lines displaying correctly

## 0.1.8

Enhancements:
  - unix account id assignment to CPU0, 1 implemented

## 0.1.7

Enhancements:
  - ReadQstatQ function (write in yaml format using Pyyaml)
  - output up to Node state!

## 0.1.6

Bugfixes: 
  - ReadPbsNodes function (write in yaml format using Pyyaml)

## 0.1.5

Bugfixes: 
  - implemented saving to 3 separate files, QSTAT_ORIG_FILE, QSTATQ_ORIG_FILE, PBSNODES_ORIG_FILE

## 0.1.4

Bugfixes: 
  - some "wiremelting" concerning the save directory

## 0.1.3

Bugfixes: 
  - fixed tabs-to-spaces. Formatting should be correct now.

Enhancements:  
  - Now each state is saved in a separate file in a results folder

## 0.1.2

Enhancements:
  - script reads qtop-input.out files from each job and displays status for each job

## 0.1.1

Enhancements:
  - changed implementation in get_state()

## 0.1.0

Enhancements:
  - just read a pbsnodes-a output file and gather the results in a single line

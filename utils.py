import logging
import sys
from optparse import OptionParser
import fileutils
from constants import QTOP_LOGFILE


def init_logging(options):
    if not options.verbose:
        log_level = logging.WARN
    elif options.verbose == 1:
        log_level = logging.INFO
    elif options.verbose >= 2:
        log_level = logging.DEBUG

    fileutils.mkdir_p(QTOP_LOGFILE.rsplit('/', 1)[0])  # logfile path

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    if options.verbose >= 3:
        # this prepends date/time
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', "%Y-%m-%d %H:%M:%S")
    else:
        formatter = logging.Formatter('%(levelname)s - %(message)s')

    fh = logging.FileHandler(QTOP_LOGFILE)
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    fh = logging.StreamHandler()
    fh.setLevel(logging.ERROR)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.disabled = False  # TODO: maybe make this a cmdline switch? -D ?

    logging.info("\n" + "=" * 50 + "STARTING NEW LOG ENTRY..." + "=" * 50 + "\n\n")

    logging.debug('Verbosity level = %s' % options.verbose)
    logging.debug("input, output isatty: %s\t%s" % (sys.stdin.isatty(), sys.stdout.isatty()))


def parse_qtop_cmdline_args():
    parser = OptionParser()  # for more details see http://docs.python.org/library/optparse.html

    parser.add_option("-1", "--disablesection1", action="store_true", dest="sect_1_off", default=False,
                      help="Disable first section of qtop, i.e. Job Accounting Summary")
    parser.add_option("-2", "--disablesection2", action="store_true", dest="sect_2_off", default=False,
                      help="Disable second section of qtop, i.e. Worker Node Occupancy")
    parser.add_option("-3", "--disablesection3", action="store_true", dest="sect_3_off", default=False,
                      help="Disable third section of qtop, i.e. User Accounts and Pool Mappings")
    parser.add_option("-a", "--blindremapping", action="store_true", dest="BLINDREMAP", default=False,
                      help="This may be used in situations where node names are not a pure arithmetic seq "
                           "(e.g. rocks clusters)")
    # TODO . Must also anonymise input files, or at least exclude them from the tarball.
    # parser.add_option("-A", "--anonymize", action="store_true", dest="ANONYMIZE", default=False,
    #                   help="Masks unix account names and workernode names for security reasons (sending bug reports etc.)")
    parser.add_option("-b", "--batchSystem", action="store", dest="BATCH_SYSTEM", default=None)
    parser.add_option("-c", "--COLOR", action="store", dest="COLOR", default="AUTO", choices=['ON', 'OFF', 'AUTO'],
                      help="Enable/Disable color in qtop output. AUTO detects tty (for watch -d)")
    parser.add_option("-C", "--classic", action="store_true", dest="CLASSIC", default=False,
                      help="tries to mimic legacy qtop display as much as possible")
    parser.add_option("-d", "--debug", action="store_true", dest="DEBUG", default=False,
                      help="print debugging messages in stdout, not just in the log file.")
    parser.add_option("-F", "--ForceNames", action="store_true", dest="FORCE_NAMES", default=False,
                      help="force names to show up instead of numbered WNs even for very small numbers of WNs")
    parser.add_option("-f", "--setCUSTOMCONFFILE", action="store", type="string", dest="CONFFILE")
    parser.add_option("-g", "--get_gecos_via_getent_passwd", action="store_true", dest="GET_GECOS", default=False,
                      help="get user details by issuing getent passwd for all users mentioned in qtop input files.")
    parser.add_option("-m", "--noMasking", action="store_true", dest="NOMASKING", default=False,
                      help="Don't mask early empty WNs (default: if the first 30 WNs are unused, counting starts from 31).")
    parser.add_option("-o", "--option", action="append", dest="OPTION", type="string", default=[],
                      help="Override respective option in QTOPCONF_YAML file")
    parser.add_option("-O", "--onlysavetofile", action="store_true", dest="ONLYSAVETOFILE", default=False,
                      help="Do not print results to stdout")
    parser.add_option("-r", "--removeemptycorelines", dest="REM_EMPTY_CORELINES", action="store_true", default=False,
                      help="If a whole row consists of empty core lines, remove the row")
    parser.add_option("-s", "--SetSourceDir", dest="SOURCEDIR",
                      help="Set the source directory where the batch scheduler output files reside")
    parser.add_option("-S", "--StrictCheck", dest="STRICTCHECK", action="store_true",
                      help="Do a check on the quality of the scheduler output by comparing "
                           "the reported total running jobs against the actual ones found/displayed in qtop")
    parser.add_option("-T", "--Transpose", dest="TRANSPOSE", action="store_true", default=False,
                      help="Rotate matrices' positioning by 90 degrees")
    parser.add_option("-B", "--web", dest="WEB", action="store_true", default=False,
                      help="Enable web interface in 8080")
    parser.add_option("-v", "--verbose", dest="verbose", action="count",
                      help="Increase verbosity (specify multiple times for more)")
    parser.add_option("-w", "--watch", dest="WATCH", action="callback", callback=_watch_callback,
                      help="Mimic shell's watch behaviour. Use with optional argument, e.g. '-w 10' to refresh every 10 sec"
                           "instead of the default which is 2 sec.")
    parser.add_option("-L", "--sample", action="count", dest="SAMPLE", default=False,
                      help="Create a sample file. A single L creates a tarball with the log, scheduler output files, "
                           "qtop output. Two L's additionaly include the qtop_conf yaml file, and qtop source.")
    # parser.add_option("-f", "--setCOLORMAPFILE", action="store", type="string", dest="COLORFILE")  # TODO

    (options, args) = parser.parse_args()
    return options, args


def _watch_callback(option, opt_str, value, parser):
    assert value is None
    value = []

    def floatable(str):
        try:
            float(str)
            return True
        except ValueError:
            return False

    for arg in parser.rargs:
        # stop on --foo like options
        if arg[:2] == "--" and len(arg) > 2:
            break
        # stop on -a, but not on -3 or -3.0
        if arg[:1] == "-" and len(arg) > 1 and not floatable(arg):
            break
        value.append(arg)
    if not value:  # zero arguments!
        value.append(0)
    else:
        del parser.rargs[:len(value)]
    setattr(parser.values, option.dest, value)
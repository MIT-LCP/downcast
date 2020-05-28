#
# downcast - tools for unpacking patient data from DWC
#
# Copyright (c) 2018 Laboratory for Computational Physiology
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys
import os
import resource
from argparse import ArgumentParser, ArgumentTypeError
from datetime import timedelta

from .server import DWCDB
from .timestamp import T
from .extractor import (Extractor, WaveSampleQueue, NumericValueQueue,
                        EnumerationValueQueue, AlertQueue,
                        PatientMappingQueue, PatientBasicInfoQueue,
                        PatientDateAttributeQueue,
                        PatientStringAttributeQueue, BedTagQueue)

from .output.archive import Archive
from .output.numerics import NumericValueHandler
from .output.waveforms import WaveSampleHandler
from .output.enums import EnumerationValueHandler
from .output.alerts import AlertHandler
from .output.mapping import PatientMappingHandler
from .output.patients import PatientHandler

def main(args = None):
    (_, n) = resource.getrlimit(resource.RLIMIT_NOFILE)
    if n != resource.RLIM_INFINITY and n < 4096:
        sys.exit('RLIMIT_NOFILE too low (%d)' % (n,))
    resource.setrlimit(resource.RLIMIT_NOFILE, (n, n))

    opts = _parse_cmdline(args)
    _main_loop(opts)

def _parse_timestamp(arg):
    try:
        return T(arg)
    except Exception:
        raise ArgumentTypeError(
            "%r is not in the format 'YYYY-MM-DD HH:MM:SS.SSS +ZZ:ZZ'" % arg)

def _parse_cmdline(args):
    p = ArgumentParser(
        description = 'Extract and convert DWC patient data.',
        fromfile_prefix_chars = '@')

    g = p.add_argument_group('input selection')
    g.add_argument('--server', metavar = 'NAME',
                   help = 'name of DWC database server')
    g.add_argument('--password-file', metavar = 'FILE',
                   default = 'server.conf',
                   help = 'file containing login credentials')

    g = p.add_argument_group('output database location')
    g.add_argument('--output-dir', metavar = 'DIR',
                   help = 'directory to store output database')
    g.add_argument('--state-dir', metavar = 'DIR',
                   help = 'directory to store state files')

    g = p.add_argument_group('conversion modes')
    g.add_argument('--init', action = 'store_true',
                   help = 'initialize a new output database')
    g.add_argument('--batch', action = 'store_true',
                   help = 'process available data and exit')
    g.add_argument('--live', action = 'store_true',
                   help = 'collect data continuously')
    g.add_argument('--start', metavar = 'TIME', type = _parse_timestamp,
                   help = 'begin collecting data at the given time')
    g.add_argument('--end', metavar = 'TIME', type = _parse_timestamp,
                   help = 'collect data up to the given time')
    g.add_argument('--terminate', action = 'store_true',
                   help = 'handle final data after permanent shutdown')

    opts = p.parse_args(args)
    progname = sys.argv[0]

    if opts.output_dir is None:
        sys.exit(('%s: no --output-dir specified' % progname)
                 + '\n' + p.format_usage())
    if opts.server is None:
        sys.exit(('%s: no --server specified' % progname)
                 + '\n' + p.format_usage())

    if (opts.init + opts.batch + opts.live) != 1:
        sys.exit(('%s: must specify exactly one of --init, --batch, or --live'
                  % progname) + '\n' + p.format_usage())

    if opts.start is not None and not opts.init:
        sys.exit(('%s: --start can only be used with --init' % progname)
                 + '\n' + p.format_usage())
    if opts.end is not None and not opts.batch:
        sys.exit(('%s: --end can only be used with --batch' % progname)
                 + '\n' + p.format_usage())

    if opts.state_dir is None:
        opts.state_dir = opts.output_dir

    if opts.init:
        if os.path.exists(opts.state_dir):
            sys.exit("%s: directory %s already exists"
                     % (progname, opts.state_dir))
        if os.path.exists(opts.output_dir):
            sys.exit("%s: directory %s already exists"
                     % (progname, opts.state_dir))
    else:
        if not os.path.isdir(opts.state_dir):
            sys.exit("%s: directory %s does not exist"
                     % (progname, opts.state_dir))
        if not os.path.isdir(opts.output_dir):
            sys.exit("%s: directory %s does not exist"
                     % (progname, opts.state_dir))
    return opts

def _init_extractor(opts):
    DWCDB.load_config(opts.password_file)

    db = DWCDB(opts.server)
    ex = Extractor(db, opts.state_dir, fatal_exceptions = True,
                   deterministic_output = True, debug = True)

    pmq = PatientMappingQueue('mapping',
                              start_time = opts.start,
                              end_time = opts.end)
    ex.add_queue(pmq)

    ex.add_queue(PatientBasicInfoQueue(
        'patients',
        start_time = opts.start, end_time = opts.end))
    ex.add_queue(PatientStringAttributeQueue(
        'strings',
        start_time = opts.start, end_time = opts.end))
    ex.add_queue(PatientDateAttributeQueue(
        'dates',
        start_time = opts.start, end_time = opts.end))
    # ex.add_queue(BedTagQueue(
    #     'beds',
    #     start_time = opts.start, end_time = opts.end))

    ex.add_queue(WaveSampleQueue(
        'waves',
        start_time = opts.start, end_time = opts.end))
    ex.add_queue(NumericValueQueue(
        'numerics',
        start_time = opts.start, end_time = opts.end))
    ex.add_queue(EnumerationValueQueue(
        'enums',
        start_time = opts.start, end_time = opts.end))
    ex.add_queue(AlertQueue(
        'alerts',
        start_time = opts.start, end_time = opts.end))
    return ex

def _init_archive(opts, extractor):
    a = Archive(opts.output_dir, deterministic_output = True)

    # Scan the output directory to find patients for whom we have not
    # seen any data for a long time, and finalize those records.  We
    # need to do this periodically since otherwise nothing would
    # finalize records at the end of a patient stay.
    synctime = extractor.fully_processed_timestamp()
    a.finalize_before(synctime)
    a.flush()

    extractor.add_handler(NumericValueHandler(a))
    extractor.add_handler(WaveSampleHandler(a))
    extractor.add_handler(EnumerationValueHandler(a))
    extractor.add_handler(AlertHandler(a))
    extractor.add_handler(PatientMappingHandler(a))

    # FIXME: Handling patient messages is disabled for now - it causes
    # archive to split records unnecessarily.
    #extractor.add_handler(PatientHandler(a))

    # Create or refresh state files, and fail if they're not writable
    extractor.flush()
    return a

def _main_loop(opts):
    if opts.init:
        # In --init mode, simply create the extractor and write the
        # initial queue state files.
        extractor = _init_extractor(opts)
        extractor.flush()
        return

    # Otherwise, feed data from the extractor into the archive until
    # we reach the desired end point.
    while True:
        # We periodically stop and re-create the extractor and
        # archive, so that records can be finalized at the end of a
        # stay.  (We can't simply invoke finalize_before on a live
        # Archive object because different patients are handled by
        # different subprocesses - each process only knows about the
        # patients that have been delegated to it.)
        extractor = _init_extractor(opts)
        _init_archive(opts, extractor)
        next_sync = (extractor.fully_processed_timestamp()
                     + timedelta(hours = 3))
        try:
            # Save state to disk after every 500 queries.
            n = 500
            while extractor.fully_processed_timestamp() < next_sync:
                if extractor.idle() and not opts.live:
                    if opts.terminate:
                        extractor.dispatcher.terminate()
                        extractor.flush()
                        a = Archive(opts.output_dir)
                        a.terminate()
                    return

                extractor.run()
                n -= 1
                if n <= 0:
                    extractor.flush()
                    n = 500
        finally:
            extractor.flush()
            extractor.dispatcher.shutdown()

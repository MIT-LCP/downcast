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
    opts = _parse_cmdline(args)
    extractor = _init_extractor(opts)
    archive = _init_archive(opts, extractor)
    _main_loop(opts, extractor, archive)

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
    extractor.add_handler(NumericValueHandler(a))
    extractor.add_handler(WaveSampleHandler(a))
    extractor.add_handler(EnumerationValueHandler(a))
    extractor.add_handler(AlertHandler(a))
    extractor.add_handler(PatientMappingHandler(a))
    extractor.add_handler(PatientHandler(a))

    # Create or refresh state files, and fail if they're not writable
    extractor.flush()
    return a

def _main_loop(opts, extractor, archive):
    progname = sys.argv[0]

    if opts.init:
        return

    while opts.live or not extractor.idle():
        extractor.run()

    if opts.terminate:
        extractor.dispatcher.terminate()

    extractor.flush()

# Copyright (c) 2009-2011, Andrew McNabb
# Copyright (c) 2003-2008, Brent N. Chun

import optparse
import os
import shlex
import sys
import textwrap
import version
import fcntl
import sys
import re

from psshlib import psshutil
from psshlib.manager import Manager, ScpManager
from psshlib.exceptions import FatalError
from psshlib.task import Task
from psshlib.hosts import ServerPool

_DEFAULT_PARALLELISM = 32
_DEFAULT_TIMEOUT     = 0 # "infinity" by default

def common_parser():
    """
    Create a basic OptionParser with arguments common to all pssh programs.
    """
    # The "resolve" conflict handler avoids errors from the hosts option
    # conflicting with the help option.
    parser = optparse.OptionParser(conflict_handler='resolve',
            version=version.VERSION)
    # Ensure that options appearing after the command are sent to ssh.
    parser.disable_interspersed_args()
    parser.epilog = "Example: pssh -h nodes.txt -l irb2 -o /tmp/foo uptime"

    filter_group = optparse.OptionGroup(parser, "Host Filters",
            "Winnow down a large pool of hosts")
    filter_group.add_option('--sample-size', type='int',
            help="choose SAMPLE_SIZE hosts at random and only run against the sample")
    filter_group.add_option('--host-regexp',
            help="only run against hosts that match HOST_REGEXP. If used with --sample-size, "
                 "this filter is run first.")

    output_group = optparse.OptionGroup(parser, "Output Options",
            "Customize how session output is handled")
    output_group.add_option('-s', '--summary', dest='summary', action='store_true',
            help='print a summary of successes and failures')
    output_group.add_option('-B', '--progress-bar', dest='progress_bar', action='store_true',
            help="instead of printing each task's status message, just print a progress bar")
    output_group.add_option('-v', '--verbose', dest='verbose', action='store_true',
            help='turn on warning and diagnostic messages (OPTIONAL)')
    output_group.add_option('-o', '--outdir', dest='outdir',
            help='output directory for stdout files (OPTIONAL)')
    output_group.add_option('-e', '--errdir', dest='errdir',
            help='output directory for stderr files (OPTIONAL)')

    connection_group = optparse.OptionGroup(parser, "Connection Options",
            "Customize the destination, authentication, pooling, timing, and "
            "configuration of connections")
    connection_group.add_option('-h', '--hosts', dest='host_files', action='append',
            metavar='HOST_FILE',
            help='hosts file (each line "[user@]host[:port]")')
    connection_group.add_option('-H', '--host', dest='host_strings', action='append',
            metavar='HOST_STRING',
            help='additional host entries ("[user@]host[:port]")')
    connection_group.add_option('-l', '--user', dest='user',
            help='username (OPTIONAL)')
    connection_group.add_option('-p', '--par', dest='par', type='int',
            help='max number of parallel threads (OPTIONAL)')
    connection_group.add_option('-t', '--timeout', dest='timeout', type='int',
            help='timeout (secs) (0 = no timeout) per host (OPTIONAL)')
    connection_group.add_option('-A', '--askpass', dest='askpass', action='store_true',
            help='Ask for a password (OPTIONAL)')
    connection_group.add_option('-O', '--option', dest='options', action='append',
            metavar='OPTION', help='SSH option (OPTIONAL)')
    connection_group.add_option('-x', '--extra-args', action='callback', type='string',
            metavar='ARGS', callback=shlex_append, dest='extra',
            help='Extra command-line arguments, with processing for '
            'spaces, quotes, and backslashes')
    connection_group.add_option('-X', '--extra-arg', dest='extra', action='append',
            metavar='ARG', help='Extra command-line argument')

    map(parser.add_option_group, [connection_group, output_group, filter_group])
    parser.group_map = { # used so subparsers can easily find option groups
        'filter_group': filter_group,
        'output_group': output_group,
        'connection_group': connection_group,
    }

    parser.add_option('-T', '--test', type='int', dest='test_cases',
            help="run against specified number of servers, then stop and ask if it's OK to continue")

    return parser


def common_defaults(**kwargs):
    defaults = dict(par=_DEFAULT_PARALLELISM, timeout=_DEFAULT_TIMEOUT)
    defaults.update(**kwargs)
    envvars = [('user', 'PSSH_USER'),
            ('par', 'PSSH_PAR'),
            ('outdir', 'PSSH_OUTDIR'),
            ('errdir', 'PSSH_ERRDIR'),
            ('timeout', 'PSSH_TIMEOUT'),
            ('verbose', 'PSSH_VERBOSE'),
            ('print_out', 'PSSH_PRINT'),
            ('askpass', 'PSSH_ASKPASS'),
            ('inline', 'PSSH_INLINE'),
            ('recursive', 'PSSH_RECURSIVE'),
            ('archive', 'PSSH_ARCHIVE'),
            ('compress', 'PSSH_COMPRESS'),
            ('localdir', 'PSSH_LOCALDIR'),
            ]
    for option, var, in envvars:
        value = os.getenv(var)
        if value:
            defaults[option] = value

    value = os.getenv('PSSH_OPTIONS')
    if value:
        defaults['options'] = [value]

    value = os.getenv('PSSH_HOSTS')
    if value:
        message1 = ('Warning: the PSSH_HOSTS environment variable is '
            'deprecated.  Please use the "-h" option instead, and consider '
            'creating aliases for convenience.  For example:')
        message2 = "    alias pssh_abc='pssh -h /path/to/hosts_abc'"
        sys.stderr.write(textwrap.fill(message1))
        sys.stderr.write('\n')
        sys.stderr.write(message2)
        sys.stderr.write('\n')
        defaults['host_files'] = [value]

    return defaults


def shlex_append(option, opt_str, value, parser):
    """An optparse callback similar to the append action.

    The given value is processed with shlex, and the resulting list is
    concatenated to the option's dest list.
    """
    lst = getattr(parser.values, option.dest)
    if lst is None:
        lst = []
        setattr(parser.values, option.dest, lst)
    lst.extend(shlex.split(value))

class CLI(object):
    def __init__(self, opts=None):
        if opts:
            self.opts = opts
            self.args = ""
        else:
            self.opts, self.args = self.parse_args()

    def run(self, hosts=[], args=None, opts=None):
        args = args or self.args
        opts = opts or self.opts
        hosts = hosts or ServerPool(opts)
    
        if args is None:
            raise Exception
        elif not hosts:
            raise Exception

        self.setup(opts)

        manager = self.setup_manager(hosts, args, opts)
 
        psshutil.run_manager(manager)

        exitcode = self.teardown_manager(manager)

        return exitcode

    def parse_args(self):
        raise NotImplementedError

    def setup(self, opts):
        pass

    def setup_manager(self, hosts, args, opts):
        raise NotImplementedError

    def teardown_manager(self, manager):
        raise NotImplementedError

def pssh_option_parser():
    parser = common_parser()
    parser.usage = "%prog [OPTIONS] command [...]"
    parser.epilog = "Example: pssh -h hosts.txt -l irb2 -o /tmp/foo uptime"

    output_group = parser.group_map['output_group']

    output_group.add_option('-i', '--inline', dest='inline', action='store_true',
            help='inline aggregated output for each server')
    output_group.add_option('-P', '--print', dest='print_out', action='store_true',
            help='print output as we get it')

    pssh_group = optparse.OptionGroup(parser, 'PSSH Options',
            "Options specific to PSSH")

    pssh_group.add_option('-I', '--send-input', dest='send_input',
            action='store_true',
            help='read from standard input and send as input to ssh')
    parser.add_option_group(pssh_group)
    parser.group_map['pssh_group'] = pssh_group

    return parser

class SecureShellCLI(CLI):

    def parse_args(self):
        parser = pssh_option_parser()
        defaults = common_defaults(timeout=_DEFAULT_TIMEOUT)
        parser.set_defaults(**defaults)
        opts, args = parser.parse_args()
    
        if len(args) == 0 and not opts.send_input:
            parser.error('Command not specified.')
    
        if not opts.host_files and not opts.host_strings:
            parser.error('Hosts not specified.')
    
        return opts, args

    def setup(self, opts):
        if opts.outdir and not os.path.exists(opts.outdir):
            os.makedirs(opts.outdir)
        if opts.errdir and not os.path.exists(opts.errdir):
            os.makedirs(opts.errdir)

    def setup_manager(self, hosts, args, opts):
        cmdline = " ".join(args)
        if opts.send_input:
            stdin = sys.stdin.read()
        else:
            stdin = None
        manager = Manager(opts)
        for host, port, user in hosts:
            cmd = ['ssh', host, '-o', 'NumberOfPasswordPrompts=1',
                    '-o', 'SendEnv=PSSH_NODENUM']
            if opts.options:
                for opt in opts.options:
                    cmd += ['-o', opt]
            if user:
                cmd += ['-l', user]
            if port:
                cmd += ['-p', port]
            if opts.extra:
                cmd.extend(opts.extra)
            if cmdline:
                cmd.append(cmdline)
            t = Task(host, port, user, cmd, opts, stdin)
            manager.add_task(t)
        
        return manager

    def teardown_manager(self, manager):
        statuses = [ i.exitstatus for i in manager.done ]
        if min(statuses) < 0:
            # At least one process was killed.
            return 3
        # The any builtin was introduced in Python 2.5 (so we can't use it yet):
        #elif any(x==255 for x in statuses):
        for status in statuses:
            if status == 255:
                return 4
        for status in statuses:
            if status != 0:
                return 5
        return 0

def pscp_option_parser():
    parser = common_parser()
    parser.usage = "%prog [OPTIONS] -h hosts.txt local remote"
    parser.epilog = ("Example: pscp -h hosts.txt -l irb2 foo.txt " +
            "/home/irb2/foo.txt")

    pscp_group = optparse.OptionGroup(parser, 'PSCP Options',
            "Options specific to PSCP")
    pscp_group.add_option('-r', '--recursive', dest='recursive',
            action='store_true', help='recusively copy directories (OPTIONAL)')
    parser.add_option_group(pscp_group)
    parser.group_map['pscp_group'] = pscp_group

    return parser

class SecureCopyCLI(CLI):
    def parse_args(self):
        parser = pscp_option_parser()
        defaults = common_defaults()
        parser.set_defaults(**defaults)
        opts, args = parser.parse_args()
    
        if len(args) < 1:
            parser.error('Paths not specified.')
    
        if len(args) < 2:
            parser.error('Remote path not specified.')
    
        if not opts.host_files and not opts.host_strings:
            parser.error('Hosts not specified.')
    
        return opts, args

    def setup(self, opts):
        if opts.outdir and not os.path.exists(opts.outdir):
            os.makedirs(opts.outdir)
        if opts.errdir and not os.path.exists(opts.errdir):
            os.makedirs(opts.errdir)
            pass

    def setup_manager(self, hosts, args, opts):
        localargs = args[0:-1]
        remote = args[-1]
        if not re.match("^/", remote):
            print("Remote path %s must be an absolute path" % remote)
            sys.exit(3)
        manager = ScpManager(opts)
        for host, port, user in hosts:
            cmd = ['scp', '-qC']
            if opts.options:
                for opt in opts.options:
                    cmd += ['-o', opt]
            if port:
                cmd += ['-P', port]
            if opts.recursive:
                cmd.append('-r')
            if opts.extra:
                cmd.extend(opts.extra)
            cmd.extend(localargs)
            if user:
                cmd.append('%s@%s:%s' % (user, host, remote))
            else:
                cmd.append('%s:%s' % (host, remote))
            t = Task(host, port, user, cmd, opts)
            manager.add_task(t)

        return manager

    def teardown_manager(self, manager):
        statuses = [ i.exitstatus for i in manager.done ]
        if min(statuses) < 0:
            # At least one process was killed.
            return 3
        for status in statuses:
            if status != 0:
                return 4
        return 0

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
import logging

from psshlib import psshutil
from psshlib.manager import Manager, ScpManager, SshManager
from psshlib.exceptions import FatalError
from psshlib.task import Task, SshTask
from psshlib.hosts import ServerPool

_DEFAULT_PARALLELISM = 32
_DEFAULT_TIMEOUT     = 0 # "infinity" by default

_RE_SCRIPT_SHEBANG = ('^(#!)'      # starts with shebang
                      '\s*'        # any whitespace
                      '([^\s]+)')  # runtime (any non-whitespace characters)

LOG = logging.getLogger(__name__)

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
    output_group.add_option('-d', '--debug', action='store_true',
            help='print PSSH debug output')
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
    output_group.add_option('-F', '--fork-hosts', metavar='UID',
            help='given a unique ID, UID, output all failed hosts to a file called UID.failed.lst '
                 'and all successful hosts to a file called UID.passed.lst')

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
        if self.opts.debug:
            self._enable_debug_logging()
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

    def _enable_debug_logging(self):
        logging.basicConfig()
        LOG.setLevel(logging.DEBUG)

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


    pssh_group = optparse.OptionGroup(parser, 'PSSH Options',
            "Options specific to PSSH")
    pssh_group.add_option('-I', '--send-input', dest='send_input',
            action='store_true',
            help='read from standard input and send as input to ssh')
    pssh_group.add_option('-i', '--inline', dest='inline', action='store_true',
            help='inline aggregated output for each server')
    pssh_group.add_option('-P', '--print', dest='print_out', action='store_true',
            help='print output as we get it')
    pssh_group.add_option('--sqlite-db', metavar='FILENAME',
            help='store all output data in sqlite3 database FILENAME')
    pssh_group.add_option('--pickle-file', metavar='FILENAME',
            help='pickle SSH task data and dump to FILENAME')
    pssh_group.add_option('--script',
            help='run SCRIPT on remote hosts')
    pssh_group.add_option('--sudo', action='store_true',
            help='when used with the --script option, will do two things differently: '
                 '1) transfer the script to /root instead of /tmp, 2) run the script '
                 'as root, not the login user')
    pssh_group.add_option('--args', dest='script_args', 
            help='companion option for --script. Passes SCRIPT_ARGS as arguments'
                 ' to the script run on the remote host.')
    pssh_group.add_option('--runtime', 
            help='specify the runtime to use when running the script from --script')
    pssh_group.add_option('--copy-to', default='/tmp',
            help='where to remotely copy scripts passed via --script (defaults to '
                 '/root if --sudo is passed, otherwise /tmp)')
       
    parser.add_option_group(pssh_group)
    parser.group_map['pssh_group'] = pssh_group

    return parser

class SecureShellCLI(CLI):

    def parse_args(self):
        parser = pssh_option_parser()
        defaults = common_defaults(timeout=_DEFAULT_TIMEOUT)
        parser.set_defaults(**defaults)
        opts, args = parser.parse_args()
    
        if len(args) == 0 and not opts.send_input and not opts.script:
            parser.error('Command not specified.')
    
        if not opts.host_files and not opts.host_strings:
            parser.error('Hosts not specified.')

        if opts.script and not os.path.isfile(opts.script):
            parser.error('No such file, "%s".' % opts.script)

        if opts.copy_to and not opts.copy_to.startswith('/'):
            parser.error('Remote script directory must be a path')
    
        return opts, args

    def setup(self, opts):
        if opts.outdir and not os.path.exists(opts.outdir):
            os.makedirs(opts.outdir)
        if opts.errdir and not os.path.exists(opts.errdir):
            os.makedirs(opts.errdir)
        if opts.script_args is None:
            opts.script_args = ""
        else:
            opts.script_args = repr(opts.script_args) # escape all arguments

    def _generate_script_name(self):
        return "pssh-%s" % psshutil.simple_uuid()

    def _parse_runtime(self, line):
        try:
            result = re.search(_RE_SCRIPT_SHEBANG, line).group(2)
        except (IndexError, AttributeError):
            result = None
        return result

    def _get_script_runtime(self):
        firstline = open(self.opts.script, 'r').readline()
        parsed_runtime = self._parse_runtime(firstline)
        # if runtime is specified at CL, use that, otherwise try to parse it
        return self.opts.runtime or self._parse_runtime(firstline)

    def _get_script_dir(self):
        if self.opts.sudo:
            default = '/root'
        else:
            default = '/tmp'
        return self.opts.copy_to or default

    def _generate_script_envelope(self):
        script_name = self._generate_script_name()
        script_dir = self._get_script_dir()
        script = "%s/%s" % (script_dir, script_name)
        runtime = self._get_script_runtime()
        if runtime:
            runner = "%s %s" % (runtime, script)
        else:
            # may not work if script dir is mounted noexec.. but it's user's fault for
            # not writing a shebang line!
            runner = script
        if self.opts.sudo:
            envelope = (
                "cat | sudo -i tee %(script)s 1>/dev/null; CATRET=$?; sudo -i chmod 700 %(script)s; "
                "sudo -i %(runner)s %(script_args)s; RET=$((CATRET+$?)); sudo -i rm -f %(script)s; exit $RET"
            )
        else:
            envelope = (
                "cat > %(script)s; CATRET=$?; chmod 700 %(script)s; %(runner)s %(script_args)s; RET=$((CATRET+$?));"
                "rm -f %(script)s; exit $RET" 
            )
        return envelope % {
          'script': script,
          'runner': runner,
          'script_args': self.opts.script_args,
        }

    def setup_manager(self, hosts, args, opts):
        if not opts.script:
            cmdline = " ".join(args)
            if opts.send_input:
                stdin = sys.stdin.read()
            else:
                stdin = None
        else:
            cmdline = self._generate_script_envelope()
            stdin = open(opts.script, 'r').read()
        manager = SshManager(opts)
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
            t = SshTask(host, port, user, cmd, cmdline, opts, stdin)
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

def pnuke_option_parser():
    parser = common_parser()
    parser.usage = "%prog [OPTIONS] -h hosts.txt pattern"
    parser.epilog = "Example: pnuke -h hosts.txt -l irb2 java"
    return parser

class NukeCLI(CLI):
    def parse_args(self):
        parser = pnuke_option_parser()
        defaults = common_defaults(timeout=_DEFAULT_TIMEOUT)
        parser.set_defaults(**defaults)
        opts, args = parser.parse_args()
    
        if len(args) < 1:
            parser.error('Pattern not specified.')
    
        if len(args) > 1:
            parser.error('Extra arguments given after the pattern.')
    
        if not opts.host_files and not opts.host_strings:
            parser.error('Hosts not specified.')
    
        return opts, args

    def setup(self, opts):
        if opts.outdir and not os.path.exists(opts.outdir):
            os.makedirs(opts.outdir)
        if opts.errdir and not os.path.exists(opts.errdir):
            os.makedirs(opts.errdir)

    def setup_manager(self, hosts, args, opts):
        pattern = args[0]
        manager = Manager(opts)
        for host, port, user in hosts:
            cmd = ['ssh', host, '-o', 'NumberOfPasswordPrompts=1']
            if opts.options:
                for opt in opts.options:
                    cmd += ['-o', opt]
            if user:
                cmd += ['-l', user]
            if port:
                cmd += ['-p', port]
            if opts.extra:
                cmd.extend(opts.extra)
            cmd.append('pkill -9 %s' % pattern)
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

def prsync_option_parser():
    parser = common_parser()
    parser.usage = "%prog [OPTIONS] -h hosts.txt local remote"
    parser.epilog = ("Example: prsync -r -h hosts.txt -l irb2 foo " +
          "/home/irb2/foo")
    prsync_group = optparse.OptionGroup(parser, 'PRSYNC Options',
            "Options specific to PRSYNC")

    prsync_group.add_option('-r', '--recursive', dest='recursive',
            action='store_true', help='recusively copy directories (OPTIONAL)')
    prsync_group.add_option('-a', '--archive', dest='archive', action='store_true',
            help='use rsync -a (archive mode) (OPTIONAL)')
    prsync_group.add_option('-z', '--compress', dest='compress', action='store_true',
            help='use rsync compression (OPTIONAL)')
    prsync_group.add_option('-S', '--ssh-args', metavar="ARGS", dest='ssh_args',
            action='store', help='extra arguments for ssh')
    parser.add_option_group(prsync_group)
    parser.group_map['prsync_group'] = prsync_group

    return parser

class RemoteSyncCLI(CLI):
    def parse_args(self):
        parser = prsync_option_parser()
        defaults = common_defaults()
        parser.set_defaults(**defaults)
        opts, args = parser.parse_args()
    
        if len(args) < 1:
            parser.error('Paths not specified.')
    
        if len(args) < 2:
            parser.error('Remote path not specified.')
    
        if len(args) > 2:
            parser.error('Extra arguments given after the remote path.')
    
        if not opts.host_files and not opts.host_strings:
            parser.error('Hosts not specified.')
    
        return opts, args

    def setup(self, opts):
        if opts.outdir and not os.path.exists(opts.outdir):
            os.makedirs(opts.outdir)
        if opts.errdir and not os.path.exists(opts.errdir):
            os.makedirs(opts.errdir)

    def setup_manager(self, hosts, args, opts):
        local = args[0]
        remote = args[1]
        if not re.match("^/", remote):
            print("Remote path %s must be an absolute path" % remote)
            sys.exit(3)

        manager = ScpManager(opts)
        for host, port, user in hosts:
            ssh = ['ssh']
            if opts.options:
                for opt in opts.options:
                    ssh += ['-o', opt]
            if port:
                ssh += ['-p', port]
            if opts.ssh_args:
                ssh += [opts.ssh_args]
    
            cmd = ['rsync', '-e', ' '.join(ssh)]
            if opts.verbose:
                cmd.append('-v')
            if opts.recursive:
                cmd.append('-r')
            if opts.archive:
                cmd.append('-a')
            if opts.compress:
                cmd.append('-z')
            if opts.extra:
                cmd.extend(opts.extra)
            cmd.append(local)
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

def pslurp_option_parser():
    parser = common_parser()
    parser.usage = "%prog [OPTIONS] -h hosts.txt remote local"
    parser.epilog = ("Example: pslurp -h hosts.txt -L /tmp/outdir -l irb2 " +
            "         /home/irb2/foo.txt foo.txt")
    pslurp_group = optparse.OptionGroup(parser, 'PSLURP Options',
            "Options specific to PSLURP")

    pslurp_group.add_option('-r', '--recursive', dest='recursive',
            action='store_true', help='recusively copy directories (OPTIONAL)')
    pslurp_group.add_option('-L', '--localdir', dest='localdir',
            help='output directory for remote file copies')
    
    parser.add_option_group(pslurp_group)
    parser.group_map['pslurp_group'] = pslurp_group

    return parser

class SecureReverseCopyCLI(CLI):
    def parse_args(self):
        parser = pslurp_option_parser()
        defaults = common_defaults()
        parser.set_defaults(**defaults)
        opts, args = parser.parse_args()
    
        if len(args) < 1:
            parser.error('Paths not specified.')
    
        if len(args) < 2:
            parser.error('Local path not specified.')
    
        if len(args) > 2:
            parser.error('Extra arguments given after the local path.')
    
        if not opts.host_files and not opts.host_strings:
            parser.error('Hosts not specified.')
    
        return opts, args

    def setup(self, opts):
        if opts.localdir and not os.path.exists(opts.localdir):
            os.makedirs(opts.localdir)
        if opts.outdir and not os.path.exists(opts.outdir):
            os.makedirs(opts.outdir)
        if opts.errdir and not os.path.exists(opts.errdir):
            os.makedirs(opts.errdir)

    def setup_manager(self, hosts, args, opts):
        remote = args[0]
        local = args[1]
        if not re.match("^/", remote):
            print("Remote path %s must be an absolute path" % remote)
            sys.exit(3)

        for host, port, user in hosts:
            if opts.localdir:
                dirname = "%s/%s" % (opts.localdir, host)
            else:
                dirname = host
            if not os.path.exists(dirname):
                os.mkdir(dirname)

        manager = ScpManager(opts)
        for host, port, user in hosts:
            if opts.localdir:
                localpath = "%s/%s/%s" % (opts.localdir, host, local)
            else:
                localpath = "%s/%s" % (host, local)
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
            if user:
                cmd.append('%s@%s:%s' % (user, host, remote))
            else:
                cmd.append('%s:%s' % (host, remote))
            cmd.append(localpath)
            t = Task(host, port, user, cmd, opts)
            manager.add_task(t)

        return manager

    def teardown_manager(self, manager):
        statuses = [ i.exitstatus for i in manager.done ]
        if min(statuses) < 0:
            # At least one process was killed.
            return 3
        for status in statuses:
            if status == 255:
                return 4 
        for status in statuses:
            if status != 0:
                return 5
        return 0

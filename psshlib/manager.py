# Copyright (c) 2009, Andrew McNabb

from errno import EINTR
from copy import deepcopy
import os
import select
import signal
import sys
import datetime
import cPickle

from psshlib.askpass_server import PasswordServer
from psshlib import psshutil
from psshlib.ui import ProgressBar, ask_yes_or_no, clear_line, print_task_report, print_summary
from psshlib.exceptions import FatalError
from psshlib.output import Writer, SshTaskDatabase

READ_SIZE = 1 << 16


class Manager(object):
    """Executes tasks concurrently.

    Tasks are added with add_task() and executed in parallel with run().
    Returns a list of the exit statuses of the processes.

    Arguments:
        limit: Maximum number of commands running at once.
        timeout: Maximum allowed execution time in seconds.
    """
    def __init__(self, opts):
        self.opts = opts

        self.limit = opts.par
        self.timeout = opts.timeout
        self.askpass = opts.askpass
        self.outdir = opts.outdir
        self.errdir = opts.errdir
        self.iomap = IOMap()

        self.taskcount = 0
        self.tasks = []
        self.running = []
        self.done = []

        self.succeeded = []
        self.ssh_failed = []
        self.cmd_failed = []
        self.killed = []

        self.askpass_socket = None

        self.progress_bar = opts.progress_bar
        self.test_cases = opts.test_cases

    def _setup_progress_bar(self):
        """ This should be called after ``self.tasks`` is populated
        """
        if self.progress_bar:
            self.progress_bar = ProgressBar(len(self.tasks))

    def _split_manager(self):
        # set up the test manager and add first n tasks
        new_opts = deepcopy(self.opts)
        new_opts.__dict__['test_cases'] = None # remove test_cases option, or there'll be a recursion error
        new_opts.__dict__['summary'] = None # don't print summary now, do it later
        test_man = self.__class__(new_opts)
        map(test_man.add_task, self.tasks[slice(0, self.test_cases)])
        psshutil.run_manager(test_man)
        test_man.tally_results()

        print
        while True:
            answer = ask_yes_or_no("Paused run. OK to continue").lower()
            if answer == 'y':
                break
            elif answer == 'n':
                sys.exit(0)
        print

        finish_man = self.__class__(new_opts)
        # add remaining tasks
        map(finish_man.add_task, self.tasks[slice(self.test_cases, None)])
        psshutil.run_manager(finish_man)

        return test_man, finish_man

    def run(self):
        """Processes tasks previously added with add_task."""
        self._setup_progress_bar()
        if self.test_cases and self.test_cases < len(self.tasks):
            man1, man2 = self._split_manager()
            self.done = man1.done + man2.done
        else:
            self._run()

        self.tally_results()

        if self.opts.summary:
            print_summary(self.succeeded, self.ssh_failed, self.killed, self.cmd_failed)

        if self.opts.fork_hosts:
            failed_file = open(self.opts.fork_hosts + '.failed.lst', 'w')
            passed_file = open(self.opts.fork_hosts + '.passed.lst', 'w')

            for i in self.ssh_failed + self.killed + self.cmd_failed:
                failed_file.write(i.host + '\n')

            for i in self.succeeded:
                passed_file.write(i.host + '\n')

        return [task.exitstatus for task in self.done]

    def _run(self):
        try:
            if self.outdir or self.errdir:
                writer = Writer(self.outdir, self.errdir)
                writer.start()
            else:
                writer = None

            self._acquire_password()
            self.set_sigchld_handler()

            try:
                self.update_tasks(writer)
                wait = None
                while self.running or self.tasks:
                    # Opt for efficiency over subsecond timeout accuracy.
                    if wait is None or wait < 1:
                        wait = 1
                    self.iomap.poll(wait)
                    self.update_tasks(writer)
                    wait = self.check_timeout()
            except KeyboardInterrupt:
                # This exception handler tries to clean things up and prints
                # out a nice status message for each interrupted host.
                self.interrupted()
                if self.opts.allow_keyboard_interrupts:
                    raise


        except KeyboardInterrupt:
            # This exception handler doesn't print out any fancy status
            # information--it just stops.
            if self.opts.allow_keyboard_interrupts:
                raise

        if writer:
            writer.signal_quit()
            writer.join()

    def _acquire_password(self):
        if self.askpass:
            pass_server = PasswordServer()
            pass_server.start(self.iomap, self.limit)
            self.askpass_socket = pass_server.address

    def tally_results(self):
        for task in self.done:
            if task.exitstatus < 0:
                self.killed.append(task)
            elif task.exitstatus == 255:
                self.ssh_failed.append(task)
            elif task.exitstatus != 0:
                self.cmd_failed.append(task)
            else:
                self.succeeded.append(task)

    def clear_sigchld_handler(self):
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)

    def set_sigchld_handler(self):
        # TODO: find out whether set_wakeup_fd still works if the default
        # signal handler is used (I'm pretty sure it doesn't work if the
        # signal is ignored).
        signal.signal(signal.SIGCHLD, self.handle_sigchld)
        # This should keep reads and writes from getting EINTR.
        if hasattr(signal, 'siginterrupt'):
            signal.siginterrupt(signal.SIGCHLD, False)

    def handle_sigchld(self, number, frame):
        """Apparently we need a sigchld handler to make set_wakeup_fd work."""
        # Write to the signal pipe (only for Python <2.5, where the
        # set_wakeup_fd method doesn't exist).
        if self.iomap.wakeup_writefd:
            os.write(self.iomap.wakeup_writefd, '\0')
        for task in self.running:
            if task.proc:
                task.proc.poll()
        # Apparently some UNIX systems automatically resent the SIGCHLD
        # handler to SIG_DFL.  Reset it just in case.
        self.set_sigchld_handler()

    def add_task(self, task):
        """Adds a Task to be processed with run()."""
        self.tasks.append(task)

    def update_tasks(self, writer):
        """Reaps tasks and starts as many new ones as allowed."""
        # Mask signals to work around a Python bug:
        #   http://bugs.python.org/issue1068268
        # Since sigprocmask isn't in the stdlib, clear the SIGCHLD handler.
        # Since signals are masked, reap_tasks needs to be called once for
        # each loop.
        keep_running = True
        while keep_running:
            self.clear_sigchld_handler()
            self._start_tasks_once(writer)
            self.set_sigchld_handler()
            keep_running = self.reap_tasks()

    def _start_tasks_once(self, writer):
        """Starts tasks once.

        Due to http://bugs.python.org/issue1068268, signals must be masked
        when this method is called.
        """
        while 0 < len(self.tasks) and len(self.running) < self.limit:
            task = self.tasks.pop(0)
            self.running.append(task)
            task.start(self.taskcount, self.iomap, writer, self.askpass_socket)
            self.taskcount += 1

    def reap_tasks(self):
        """Checks to see if any tasks have terminated.

        After cleaning up, returns the number of tasks that finished.
        """
        still_running = []
        finished_count = 0
        for task in self.running:
            if task.running():
                still_running.append(task)
            else:
                self.finished(task)
                finished_count += 1
        self.running = still_running
        return finished_count

    def check_timeout(self):
        """Kills timed-out processes and returns the lowest time left."""
        if self.timeout <= 0:
            return None

        min_timeleft = None
        for task in self.running:
            timeleft = self.timeout - task.elapsed()
            if timeleft <= 0:
                task.timedout()
                continue
            if min_timeleft is None or timeleft < min_timeleft:
                min_timeleft = timeleft

        if min_timeleft is None:
            return 0
        else:
            return max(0, min_timeleft)

    def interrupted(self):
        """Cleans up after a keyboard interrupt."""
        for task in self.running:
            task.interrupted()
            self.finished(task)

        for task in self.tasks:
            task.cancel()
            self.finished(task)

    def finished(self, task):
        """Marks a task as complete and reports its status to stdout."""
        self.done.append(task)
        task.sequence = len(self.done)
        if self.progress_bar:
            self.progress_bar.tick()
        else:
            print_task_report(task)

class ScpManager(Manager):
    def tally_results(self):
        for task in self.done:
            if task.exitstatus < 0:
                self.killed.append(task)
            elif task.exitstatus != 0:
                self.ssh_failed.append(task)
            else:
                self.succeeded.append(task)

class SshManager(Manager):
    def run(self):
        super(SshManager, self).run()

        if self.opts.sqlite_db:
            sys.stdout.write('Exporting to database "%s".\n' % self.opts.sqlite_db)
            db = SshTaskDatabase(self.opts.sqlite_db)
            map(db.capture_data, self.done)
            db.close()

        if self.opts.pickle_file:
            sys.stdout.write('Exporting to pickle file "%s".\n' % self.opts.pickle_file)
            fd = open(self.opts.pickle_file, 'a')
            cPickle.dump(self, fd, cPickle.HIGHEST_PROTOCOL)
            fd.close()

        sys.stdout.write('\n')

    def __reduce__(self): # for pickling task data
        return (list, tuple(), None, (i.get_data() for i in self.done))

class IOMap(object):
    """A manager for file descriptors and their associated handlers.

    The poll method dispatches events to the appropriate handlers.
    """
    def __init__(self):
        self.readmap = {}
        self.writemap = {}

        # Setup the wakeup file descriptor to avoid hanging on lost signals.
        wakeup_readfd, wakeup_writefd = os.pipe()
        self.register_read(wakeup_readfd, self.wakeup_handler)
        # TODO: remove test when we stop supporting Python <2.5
        if hasattr(signal, 'set_wakeup_fd'):
            signal.set_wakeup_fd(wakeup_writefd)
            self.wakeup_writefd = None
        else:
            self.wakeup_writefd = wakeup_writefd

    def register_read(self, fd, handler):
        """Registers an IO handler for a file descriptor for reading."""
        self.readmap[fd] = handler

    def register_write(self, fd, handler):
        """Registers an IO handler for a file descriptor for writing."""
        self.writemap[fd] = handler

    def unregister(self, fd):
        """Unregisters the given file descriptor."""
        if fd in self.readmap:
            del self.readmap[fd]
        if fd in self.writemap:
            del self.writemap[fd]

    def poll(self, timeout=None):
        """Performs a poll and dispatches the resulting events."""
        if not self.readmap and not self.writemap:
            return
        rlist = list(self.readmap)
        wlist = list(self.writemap)
        try:
            rlist, wlist, _ = select.select(rlist, wlist, [], timeout)
        except select.error:
            _, e, _ = sys.exc_info()
            errno = e.args[0]
            if errno == EINTR:
                return
            else:
                raise
        for fd in rlist:
            handler = self.readmap[fd]
            handler(fd, self)
        for fd in wlist:
            handler = self.writemap[fd]
            handler(fd, self)

    def wakeup_handler(self, fd, iomap):
        """Handles read events on the signal wakeup pipe.

        This ensures that SIGCHLD signals aren't lost.
        """
        try:
            os.read(fd, READ_SIZE)
        except (OSError, IOError):
            _, e, _ = sys.exc_info()
            errno, message = e.args
            if errno != EINTR:
                sys.stderr.write('Fatal error reading from wakeup pipe: %s\n'
                        % message)
                raise FatalError


from psshlib.color import r,y,c,g,m,B,has_colors
from psshlib.psshutil import get_timestamp
import termios, fcntl, struct, sys
import sys

def print_summary(succeeded, ssh_failed, killed, cmd_failed=[]): # cmd_failed is only for pssh
    total_succeeded = len(succeeded)
    total_failures = len(ssh_failed) + len(killed) + len(cmd_failed)
    total = total_failures + total_succeeded

    summary_data = (
        ('Total', total),
        ('Failed', r(str(total_failures))),
        ('Succeeded', g(str(total_succeeded)))
    )

    failure_data = (
        ('Connections Failed', len(ssh_failed)),
        ('Tasks Killed', len(killed)),
        ('Tasks Failed', len(cmd_failed))
    )
    print()
    print("Summary:")
    print("  " + format_summary(summary_data))
    print()
    print("Failure Breakdown:")
    print("  " + format_summary(failure_data))
    print()

def format_summary(data):
    nonzero_data = [ i for i in data if i[1] != 0 ]
    if nonzero_data:
        keys, values = zip( *nonzero_data ) # filter out zero-data entries
        keys = map(B, keys)
        values = map(B, map(str, values))
        return_data = " / ".join([ "[%s] %s" % (i[1], i[0]) for i in zip(keys, values) ])
    else:
        return_data = "(none)"
    return return_data

def print_task_report(task):
    sequence = task.sequence
    errors = ', '.join(task.failures)
    if has_colors(sys.stdout):
        sequence = c("[%s]" % B(sequence))
        success = g("[%s]" % B("SUCCESS"))
        failure = r("[%s]" % B("FAILURE"))
        stderr = r("Stderr: ")
        errors = r(B(errors))
    else:
        sequence = "[%s]" % sequence
        success = "[SUCCESS]"
        failure = "[FAILURE]"
        stderr = "Stderr: "
    if task.failures:
        status = failure
    else:
        status = success
    print(' '.join((sequence, get_timestamp(), status, task.pretty_host, errors)))

    # NOTE: The extra flushes are to ensure that the data is output in
    # the correct order with the C implementation of io.
    if task.outputbuffer:
        sys.stdout.flush()
        try:
            sys.stdout.buffer.write(task.outputbuffer)
            sys.stdout.flush()
        except AttributeError:
            sys.stdout.write(task.outputbuffer)
    if task.errorbuffer:
        sys.stdout.write(stderr)
        # Flush the TextIOWrapper before writing to the binary buffer.
        sys.stdout.flush()
        try:
            sys.stdout.buffer.write(task.errorbuffer)
        except AttributeError:
            sys.stdout.write(task.errorbuffer)


def get_window_size():
    s = struct.pack("HHHH", 0, 0, 0, 0)
    fd_stdout = sys.stdout.fileno()
    size = fcntl.ioctl(fd_stdout, termios.TIOCGWINSZ, s)
    return struct.unpack("HHHH", size)[0:2]

def get_window_width():
    return get_window_size()[1]

def get_window_height():
    return get_window_size()[0]

def clear_line():
    sys.stdout.write('\r' + get_window_width() * ' ' + '\r')

def ask_yes_or_no(question):
    return raw_input("%s? [y/N]: " % question)

class ProgressBar(object):
    def __init__(self, total, lcap='[', rcap=']', fill='#'):
        self.total = total
        self.current = 0
        self.lcap = lcap
        self.rcap = rcap
        self.fill = fill
    @property
    def length(self):
        """ The ``length`` property is dynamic so that if user resizes terminal, 
        progress bar is also resized
        """
        return get_window_width() - 50
    def _get_bar(self):
        num_ticks = self._get_num_ticks()
        num_blanks = self.length - 2 - num_ticks
        bar = self.lcap + (self.fill * num_ticks) + (' ' * num_blanks) + self.rcap
        bar = "[%s%%] %s [%s]" % (B(self._percent_to_s()), bar, B(self._get_fraction_done()))
        return "  " + bar
    def _get_fraction_done(self):
        return "%s/%s" % ( str(self.current), str(self.total) )
    def _get_num_ticks(self):
        return int(round(self._get_percent_done() * (self.length - 2)))
    def _get_percent_done(self):
        return float(self.current)/self.total
    def _percent_to_s(self):
        return "%.2f" % ( self._get_percent_done() * 100 )
    def tick(self, amount=1):
        if self.current == 0:
            print
            print "Progress:"

        remaining = self.total - self.current

        if amount <= remaining:
            self.current += amount
        else:
            self.current += remaining
        
        clear_line()
        sys.stdout.write('\r' + self._get_bar()) # now write the progress bar
        sys.stdout.flush()
        
        if self.current == self.total:
            print 

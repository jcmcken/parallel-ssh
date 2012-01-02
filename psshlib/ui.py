
from psshlib.color import r,y,c,g,m,B,has_colors
from psshlib.psshutil import get_timestamp
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
    print 
    print "Summary:"
    print "  " + format_summary(summary_data)
    print
    print "Failure Breakdown:"
    print "  " + format_summary(failure_data)
    print

def format_summary(data):
    keys, values = zip( *[ i for i in data if i[1] != 0 ] ) # filter out zero-data entries
    keys = map(B, keys)
    values = map(B, map(str, values))
    return " / ".join([ "[%s] %s" % (i[1], i[0]) for i in zip(keys, values) ])

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

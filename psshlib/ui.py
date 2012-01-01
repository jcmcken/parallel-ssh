
import termios, fcntl, struct, sys
from psshlib.color import r,y,c,g,m,B

def print_summary(ssh_failed, cmd_failed, killed, succeeded):
    total_succeeded = len(succeeded)
    total_failures = len(ssh_failed) + len(cmd_failed) + len(killed)
    total = total_failures + total_succeeded
    print 
    print "Summary:"
    print "  [%s] %s / [%s] %s / [%s] %s" % (
        B(str(total)), B("Total"),
        B(r(str(total_failures))), B("Failed"),
        B(g(str(total_succeeded))), B("Succeeded")
    )
    print
    print "Failure Breakdown:"
    print "  [%s] %s / [%s] %s / [%s] %s" % (
        B(str(len(ssh_failed))), B("SSH Failed"),
        B(str(len(killed))), B("Tasks Killed"),
        B(str(len(cmd_failed))), B("Tasks Failed")
    )
    print


    

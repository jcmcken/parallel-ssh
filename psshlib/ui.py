
from psshlib.color import r,y,c,g,m,B

def print_summary(succeeded, ssh_failed, killed, cmd_failed=None): # cmd_failed is only for pssh
    total_succeeded = len(succeeded)
    total_failures = len(ssh_failed) + len(killed)
    if cmd_failed is not None:
        total_failures += len(cmd_failed)
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
    print "  [%s] %s / [%s] %s" % (
        B(str(len(ssh_failed))), B("Connection Failed"),
        B(str(len(killed))), B("Tasks Killed"),
    ),
    if cmd_failed is not None:
        print "/ [%s] %s" % ( B(str(len(cmd_failed))), B("Tasks Failed") )
    else:
        print
    print


    

import sys
import psshutil

class ServerPool(list):
    def __init__(self, options):
        self.options = options
        try:
            hosts = psshutil.read_host_files(options.host_files, default_user=options.user)
        except IOError:
            _, e, _ = sys.exc_info()
            sys.stderr.write('Could not open hosts file: %s\n' % e.strerror)
            sys.exit(1)
        if options.host_strings:
            for s in options.host_strings:
                hosts.extend(psshutil.parse_host_string(s, default_user=options.user))
        self = hosts

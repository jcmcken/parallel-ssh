import sys
import psshutil
import random

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
        sample_size = options.sample_size
        if sample_size:
            if sample_size <= 0:
                sys.stderr.write('Sample size cannot be negative')
                sys.exit(1)
            elif sample_size > len(hosts):
                sys.stderr.write('Sample size larger than population')
                sys.exit(1)
            hosts = random.sample(hosts, sample_size)
        super(ServerPool, self).__init__(hosts)

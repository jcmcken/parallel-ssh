import sys
import psshutil
import random
import re

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
        
        regexp = options.host_regexp
        if regexp:
            compiled = re.compile(regexp)
            hosts = [ i for i in hosts if compiled.match(i[0]) ]
            
            if not hosts:
                sys.stderr.write('No hosts matched supplied regular expression\n')
                sys.exit(1)

        sample_size = options.sample_size
        if sample_size:
            if sample_size <= 0:
                sys.stderr.write('Sample size cannot be negative\n')
                sys.exit(1)
            elif sample_size > len(hosts):
                sys.stderr.write('Sample size larger than population\n')
                sys.exit(1)
            hosts = random.sample(hosts, sample_size)

        super(ServerPool, self).__init__(hosts)

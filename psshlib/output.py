import threading
import os
import datetime

try:
    import queue
except ImportError:
    import Queue as queue

try:
    import sqlite3 as sqlite
    expand_sql_tuple = False
except ImportError:
    import sqlite 
    expand_sql_tuple = True
import _sqlite

import psshutil

class Writer(threading.Thread):
    """Thread that writes to files by processing requests from a Queue.

    Until AIO becomes widely available, it is impossible to make a nonblocking
    write to an ordinary file.  The Writer thread processes all writing to
    ordinary files so that the main thread can work without blocking.
    """
    OPEN = object()
    EOF = object()
    ABORT = object()

    def __init__(self, outdir, errdir):
        threading.Thread.__init__(self)
        # A daemon thread automatically dies if the program is terminated.
        self.setDaemon(True)
        self.queue = queue.Queue()
        self.outdir = outdir
        self.errdir = errdir

        self.host_counts = {}
        self.files = {}

    def run(self):
        while True:
            filename, data = self.queue.get()
            if filename == self.ABORT:
                return

            if data == self.OPEN:
                self.files[filename] = open(filename, 'wb', buffering=1)
                psshutil.set_cloexec(self.files[filename])
            else:
                dest = self.files[filename]
                if data == self.EOF:
                    dest.close()
                else:
                    dest.write(data)

    def open_files(self, host):
        """Called from another thread to create files for stdout and stderr.

        Returns a pair of filenames (outfile, errfile).  These filenames are
        used as handles for future operations.  Either or both may be None if
        outdir or errdir or not set.
        """
        outfile = errfile = None
        if self.outdir or self.errdir:
            count = self.host_counts.get(host, 0)
            self.host_counts[host] = count + 1
            if count:
                filename = "%s.%s" % (host, count)
            else:
                filename = host
            if self.outdir:
                outfile = os.path.join(self.outdir, filename)
                self.queue.put((outfile, self.OPEN))
            if self.errdir:
                errfile = os.path.join(self.errdir, filename)
                self.queue.put((errfile, self.OPEN))
        return outfile, errfile

    def write(self, filename, data):
        """Called from another thread to enqueue a write."""
        self.queue.put((filename, data))

    def close(self, filename):
        """Called from another thread to close the given file."""
        self.queue.put((filename, self.EOF))

    def signal_quit(self):
        """Called from another thread to request the Writer to quit."""
        self.queue.put((self.ABORT, None))

class SshTaskDatabase(object):
    version = '0.1'

    def __init__(self, uri):
        self.uri = uri
        self.conn = sqlite.connect(self.uri)
        self.cursor = self.conn.cursor()
        self._initialize_db()

    def _create_tables(self):
        self.cursor.execute(
            "CREATE TABLE meta ("
                "id INTEGER PRIMARY KEY,"
                "key VARCHAR(15),"
                "value VARCHAR(15)"
            ")"        
        )

        self.cursor.execute(
            "CREATE TABLE tasks ("
                "id INTEGER PRIMARY KEY,"
                "started INTEGER," # use SQLite ``date`` function to convert UNIX epoch -> datetime
                "hostname VARCHAR(255)," # normally would make this a foreign key, but no FK in python 2.4 sqlite
                "command TEXT,"
                "stdout TEXT,"
                "stderr TEXT,"
                "exitcode INTEGER"
            ")"
        )
    @property
    def _initial_meta(self):
        return [
        #    pk    key        value
            (None, 'created', datetime.datetime.utcnow().isoformat()),
            (None, 'schema_version', self.version),
        ]

    def _populate_initial(self):
        map(lambda t: self.insert('meta', t), self._initial_meta)

    def _schema_ver_is_valid(self):
        try:
            self.cursor.execute("select value from meta where key = 'schema_version'")
            version = self.cursor.fetchone()[0]
        except _sqlite.DatabaseError: # if no meta table exists
            return False
        except TypeError: # if return value is None
            return False
            
        return True

    def _initialize_db(self):
        if self._schema_ver_is_valid():
            return
        self._create_tables()
        self._populate_initial()
        self.conn.commit()

    def insert(self, table, values):
        placeholder = '(' + ','.join(['%s'] * len(values)) + ')'
        sql_string = "insert into %s values %s" % ( table, placeholder )
        if expand_sql_tuple: # pysqlite v1
            self.cursor.execute(sql_string, *values)
        else:                # pysqlite v2+ or Python stdlib sqlite3 lib
            self.cursor.execute(sql_string, values)

    def capture_data(self, task):
        started = psshutil.convert_task_time(task.timestamp).isoformat()
        entries = (None, started, task.host, task.raw_cmd, task.outputbuffer, task.errorbuffer, task.exitstatus)
        self.insert('tasks', entries)
        self.conn.commit()

    def close(self):
        self.cursor.close()

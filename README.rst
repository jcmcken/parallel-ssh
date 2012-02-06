Parallel SSH
------------

The ``parallel-ssh`` module and scripts implement parallelized versions of common SSH tasks including:

* Arbitrary commands over SSH (the ``pssh`` utility)
* File copying to and from remote servers over SSH (the ``pscp`` and ``pslurp`` utilities, respectively)
* Syncing files over SSH (the ``prsync`` utility)
* Forcefully killing processes over SSH (the ``pnuke`` utility)

This project utilizes the SSH binaries and libraries already installed on your system, making it ideal for administrators who do not want to manage external dependencies or for servers which need to be bootstrapped to make use of utilities (e.g. Chef, Puppet, Salt) that follow the master-agent paradigm (i.e. require agents to be installed in order to manage servers).

About This Project
------------------

Original work on ``parallel-ssh`` is by Brent Chun and Andrew McNabb. 

This fork of ``parallel-ssh`` implements a number of improvements, including major refactoring of the libraries to make them more modular and easier to extend and the addition of numerous extra options for more easily managing, viewing, and saving the status of command runs.

To see a list of all changes, view the ``ChangeLog``.

Examples
--------

One of the most useful additions to the ``pssh`` utility in particular are the ``--script`` and ``--sudo`` options::

    $ pssh -h hostfile.txt --script restart_iptables.sh --sudo

These options let you run arbitrary scripts (as root, if needed) on remote hosts, without having to properly escape or make special considerations for the contents of those scripts. All that's required is that the script contain a shebang line referencing an interpreter that exists on the remote host.

All of the utilities received some new common options, some of which include the ``--host-regexp`` and ``--sample-size`` options. More information can be found by simply typing ``[utility] --help``. (All of the help output for the commands has been reorganized and made easier to read)

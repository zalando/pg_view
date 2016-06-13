pg_view: PostgreSQL Real-Time Activity View Utility
=======

.. image:: https://travis-ci.org/zalando/pg_view.svg?branch=master
    :target: https://travis-ci.org/zalando/pg_view
    :alt: Build Status
.. image:: https://img.shields.io/pypi/dw/pg-view.svg
      :target: https://pypi.python.org/pypi/pg-view
      :alt: PyPI Downloads
.. image:: https://img.shields.io/pypi/l/pg-view.svg
      :target: https://pypi.python.org/pypi/pg-view
      :alt: License


Intro
--------

**pg_view** is a powerful command-line tool that offers a detailed, real-time view of your PostgreSQL database and system metrics. It combines the indicators commonly displayed by sar or iostat with output from PostgreSQL’s process activity view, and presents global and per-process statistics in an easy-to-interpret way. 

pg_view shows these types of data:

- per-process statistics, combined with ``pg_stat_activity`` view output for the backend and autovacuum processes
- global system stats
- per-partition information
- memory stats

pg_view can be especially helpful when you’re monitoring system load, query locks and I/O utilization during lengthy data migrations. It’s also useful when you’re running servers 24x7 and aiming for zero downtime. Learn more about it at `tech.zalando.com <https://tech.zalando.com/blog/getting-a-quick-view-of-your-postgresql-stats/>`_.

Installation and Configuration
------------

To run pg_view, you’ll need:

- Linux 2.6
- Python >= 2.6
- psycopg2
- curses

By default, pg_view assumes that it can connect to a local PostgreSQL instance with the user postgres and no password. Some systems might require you to change your pg_hba.conf file or set the password in .pgpass. You can use (-c) to specify a different user name in the configuration file, although specifying the file will turn off autodetection of connection parameters and available databases.

How pg_view Works:
------------

pg_view queries system/process information files once per second. It also runs external programs — df, du, etc. — to obtain filesystem information. Please note that the latter function might add an extra load to your disk subsystem.

.. image:: https://raw.github.com/zalando/pg_view/master/images/pg_view_screenshot.png
   :alt: pg_view screenshot

Connection Arguments
--------------------

By default, pg_view tries to autodetect all PostgreSQL clusters running on the host it's running at. To achieve
this it performs the following steps:

* read /proc/ filesystem and detect pid files for the postmaster processes
* get the working directories from the symlink at /proc/pid/cwd
* get to the working directories and read PG_VERSION for PostgreSQL verions. If we can't, assume it's not a PostgreSQL directory and skip.
* try to get all sockets the process is listening to from /proc/net/unix, /proc/net/tcp and /proc/net/tcp6
* if that fails and version is 9.1 or above, read connection arguments from postmaster.pid
* check all arguments, picking the first one that allows us to establish a connection
* if we can't get either the port/host or port/socket_directory pair, bail out.

If the program is unable to detect connection arguments using the algorithm above it's possible to specify
those arguments manually using the configuration file supplied with -c option. This file should consist of
one or more sections, containing key = value pairs. Each section's title represents a database cluster name,
this name is only used to for display purposes (the actual name of the DB to connect to can be specified by the dbname parameter and is 'postgres' by default), and the key - value pairs should contain connection parameters. The valid keys are:

host
    hostname or ip address, or unix_socket_directory path of the database server

port
    the port the database server listsens on

user
    database role name

The special 'DEFAULT' contains the parameters that apply for every database cluster if the corresponding parameter
is missing from the database-specific section. For instance::

    [DEFAULT]
    port=5435

    [testdb]
    host=localhost

    [testdb2]
    host=/tmp/test

    [testdb3]
    host=192.168.1.0
    port=5433
    dbname=test

The application will try to connect to both testdb and testdb2 clusters using port 5435 (database postgres) upon reading this file, while testdb3 will be reached using port 5433 and database name 'test'.

Finally, if the auto-detection code works for you, it's possible to select only a single database by specifying
the database instance name (in most cases mathes the last component of $PGDATA) with ``-i`` command-line option. If there are more thana single instance with the same name - you can additionally specify the required PG version with ``-V``.

Usage
-----
see ``python pg_view --help``

Output:
The tool supports 3 output methods:
* ncurses (default)
* console (``-o console``)
* json (``-o json``).

Below is the description of some of the options:

* system
    * iowait
            the percent of the CPU resources waiting on I/O
    * ctxt
            the number of context switches in the system
    * run, block
            the number of running and waiting processes.
    * For other parameters, please, refer to man 5 proc and look for /proc/stat
* memory
    * dirty
            the total amount of memory waiting to be written on disk.
            The higher the value is, the more one has to wait during the flush.
    * as
            (CommittedAs) the total amount of memory required to store the workload
            in the worst case scenario.
    * limit
            maximum amount of memory that can be physically allocated. If ``as`` is higher
            than the ``limit`` - the processes will start getting out of memory errors,
            which will lead to PostgreSQL shutdown (but not to the data corruption.

      For the explanation of other parameters, please, refer to the
      `Linux kernel documentation <http://git.kernel.org/cgit/linux/kernel/git/torvalds/linux.git/tree/Documentation/filesystems/proc.txt>`_

* partitions
    * type
            either containing database data (data) or WAL (xlog)
    * fill
            the rate of adding new data to the corresponding directory (``/data`` or ``/pg_xlog``).
    * until_full
            the time until the current partition will run out of space if we only consider writes
            to the corresponding data directory (``/data`` or ``/pg_xlog``). This column is only shown
            during the warning (3h) or critical (1h) conditions. This column only considers momentary
            writes, so if a single process writes 100MB/s on a partition with remaining 100GB left for
            only 2 seconds, it will show a critial status during those 2 seconds.
    * total, left, read, write
            the amount of space total, free, read and write rate (MB/s) on a partition. Note that write rate is different from
            fill rate: it considers the whole partition, not only Postgres directories and shows data modifications, i.e deletion of files at the rate of 10MB/s will be shown as a positive write rate.
    * path_size
            size of the corresponding PostgreSQL directory.

* postgres processes
    * type
            either a system process (autovacuum launcher, logger, archiver, etc) or a process that
            executes queries (backend or autovacuum). By default, only user processes are shown (press
            's' to show all of them) in curses mode, and all in the console one.
    * s
            process state (``R`` - 'running', ``S`` - 'sleeping', ``D`` - 'uninterruptable sleep', see ``man ps``
            for more details).
    * utime, stime, guest
            consumption of CPU resources by process. Since PostgreSQL backends can't use more than one
            CPU, the percentage of a single CPU time is shown here.
    * read, write
            amount of data read or written from the partition (in MB/s).
    * age
            time from the process start
    * db
            the database the process runs on
    * query
            the query the process executes.


Hotkeys:

* f
    instantly freeze the output. Press ``f`` for the second time to resume.
* u
    toggle display of measurement units.
* a
    auto-hide some of the fields from the PostgreSQL output. Currently, if this option is turned to on, the following fields can
    be hidden to leave space for the remaining ones: ``type``, ``s``, ``utime``, ``stime``, ``guest``
* h
    show the help screen

Releasing
---------

    $ ./release.sh <NEW-VERSION>


License
-------

`Apache 2.0 <http://www.apache.org/licenses/LICENSE-2.0>`_

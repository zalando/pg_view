pg_view
=======

[![Build Status](https://travis-ci.org/zalando/pg_view.svg?branch=master)](https://travis-ci.org/zalando/pg_view)

[![PyPI Downloads](https://img.shields.io/pypi/dw/pg-view.svg)](https://pypi.python.org/pypi/pg-view)

[![Latest PyPI version](https://img.shields.io/pypi/v/pg-view.svg)](https://pypi.python.org/pypi/pg-view)

[![License](https://img.shields.io/pypi/l/pg-view.svg)](https://pypi.python.org/pypi/pg-view)

PostgreSQL Activity View Utility

Synopsis
---------

`pg_view` is a command-line tool to display the state to the PostgreSQL processes.
It shows the per-process statistics combined with `pg_stat_activity` output for the processes
that have the rows there, global system stats, per-partition information and the memory stats.
You can find a blog post about it at [tech.zalando.com](http://tech.zalando.com/getting-a-quick-view-of-your-postgresql-stats/).

Requirements
------------

Linux 2.6, python 2.6, psycopg2, curses

By default pg_view assumes it's able to connect to the local PostgreSQL instance with the user postgres and no password. On some systems it might be necessary to change your pg_hba.conf or set the password in .pgpass. A different user name can be specified in the configuration file, although specifying that file (with -c) turns off autodetection of connection parameters and available databases.

How it works:

The program queries system /process information files once every tick (by default the tick is 1s). It also
runs some external programs, like df or du to get filesystem information. The latter might put an extra
load on a disk subsystem.

Screenshot
-----------
![Screenshot](https://raw.github.com/zalando/pg_view/master/images/pg_view_screenshot.png "pg_view screenshot")

Connection arguments
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

* `host`:             hostname or ip address, or unix_socket_directory path of the database server
* `port`:             the port the database server listsens on
* `user`:             database role name

The special 'DEFAULT' contains the parameters that apply for every database cluster if the corresponding parameter
is missing from the database-specific section. For instance:

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
the database instance name (in most cases mathes the last component of $PGDATA) with `-i` command-line option. If there are more thana single instance with the same name - you can additionally specify the required PG version with `-V`.

Usage
-----
see `python pg_view --help`

Output:
The tool supports 3 output methods:
* ncurses (default)
* console (`-o console`)
* json (`-o json`).

Below is the description of some of the options:
* system:
	* `iowait`: the percent of the CPU resources waiting on I/O
	* `ctxt`: the number of context switches in the system
	* `run`, `block`: the number of running and waiting processes.
	* For other parameters, please, refer to man 5 proc and look for /proc/stat
* memory:
    * `dirty`:  the total amount of memory waiting to be written on disk. The higher
    	        the value is, the more one has to wait during the flush.
    * `as`:	(CommittedAs) the total amount of memory required to store the workload
    		in the worst case scenario
    * `limit`:	maximum amount of memory that can be physically allocated. If `as` is higher
    		than the `limit` - the processes will start getting out of memory errors,
    		which will lead to PostgreSQL shutdown (but not to the data corruption).
    For the explanation of other parameters, please, refer to the description of
    [`/proc/memstat`](http://git.kernel.org/cgit/linux/kernel/git/torvalds/linux.git/tree/Documentation/filesystems/proc.txt)

* partitions:
	* `type`: 		 either containing database data (data) or WAL (xlog)
	* `fill`: 		 the rate of adding new data to the corresponding directory (`/data` or `/pg_xlog`).
	* `until_full`:  the time until the current partition will run out of space if we only consider writes
				 to the corresponding data directory (`/data` or `/pg_xlog`). This column is only shown
				 during the warning (3h) or critical (1h) conditions. This column only considers momentary
				 writes, so if a single process writes 100MB/s on a partition with remaining 100GB left for
				 only 2 seconds, it will show a critial status during those 2 seconds.
	* `total`, `left`, `read`, `write`: the amount of space total, free, read and write rate (MB/s) on a partition. Note that write rate is different from fill rate: it considers the whole partition, not only Postgres directories and shows data modifications, i.e deletion of files at the rate of 10MB/s will be shown as a positive write rate.
	* `path_size`:	 size of the corresponding PostgreSQL directory.

* postgres processes:
	* `type`:		 either a system process (autovacuum launcher, logger, archiver, etc) or a process that
				 executes queries (backend or autovacuum). By default, only user processes are shown (press
				 's' to show all of them) in curses mode, and all in the console one.
	* `s`:			 process state (`R` - 'running', `S` - 'sleeping', `D` - 'uninterruptable sleep', see `man ps`
				 for more details).
	* `utime`,
	* `stime`,
	* `guest`:		 consumption of CPU resources by process. Since PostgreSQL backends can't use more than one
				 CPU, the percentage of a single CPU time is shown here.
	* `read`, `write`:  amount of data read or written from the partition (in MB/s).
	* `age`:		 time from the process start
	* `db`:			 the database the process runs on
	* `query`:		 the query the process executes.


Hotkeys:
* `f`: instantly freeze the output. Press `f` for the second time to resume.
* `u`: toggle display of measurement units.
* `a`: auto-hide some of the fields from the PostgreSQL output. Currently, if this option is turned to on, the following fields can be hidden to leave space for the remaining ones: `type`, `s`, `utime`, `stime`, `guest`
* `h`: show the help screen

License
-------

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

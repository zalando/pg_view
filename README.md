pg_view
=======

PostgreSQL Activity View Utility

Synopsis:
========

pg_view is a command-line tool to display the state of the PostgreSQL processes.
It shows the per-process statistics combined with pg_stat_activity output for the processes
that have the rows there, global system stats, per-partition information and the memory stats.

Requirements:
============

Linux 2.6, python 2.6, psycopg2, curses

The tool assumes an ability to connect to the PostgreSQL via the local socket path with the user postgres and no password (or the password set in .pgpass).

How it works:

The program queries system /process information files once every tick (by default the tick is 1s). It also
runs some external programs, like df or du to get filesystem information. The latter might put an extra
load on a disk subsystem.

Usage:
======
see python pg_view --help.

Output:
The tool supports 3 output methods:
* ncurses (default)
* console (-o console)
* json (-o json).

For the explanation of the output, please, look at the preview.jpeg file (showing the ncurses output).

Below is the description of some of the options:
system:
	iowait: the percent of the CPU resources waiting on I/O
	ctxt: the number of context switches in the system
	run, block: the number of running and waiting processes.
	For other parameters, please, refer to man 5 proc and look for /proc/stat
memory:
    dirty: the total amount of memory waiting to be written on disk. The higher
    	   the value is, the more one has to wait during the flush.
    as:	   (CommittedAs) the total amount of memory required to store the workload
    		in the worst case scenario
    limit:	maximum amount of memory that can be physically allocated. If as is higher
    		than the limit - the processes will start getting out of memory errors,
    		which will lead to PostgreSQL shutdown (but not to the data corruption).
    For the explanation of other parameters, please, refer to the description of
    /proc/memstat:
    http://git.kernel.org/cgit/linux/kernel/git/torvalds/linux.git/tree/Documentation/filesystems/proc.txt

partitions:
	type: 		 either containing database data (data) or WAL (xlog)
	fill: 		 the rate of adding new data to the corresponding directory (/data or /pg_xlog).
	until_full:  the time until the current partition will run out of space if we only consider writes
				 to the corresponding data directory (/data or /pg_xlog). This column is only shown
				 during the warning (3h) or critical (1h) conditions. This column only considers momentary
				 writes, so if a single process writes 100MB/s on a partition with remaining 100GB left for
				 only 2 seconds, it will show a critial status during those 2 seconds.
	total, left,
	read, write: the amount of space total, free, read and write rate (MB/s) on a partition. Note that write
				 rate is different from fill rate: it considers the whole partition, not only Postgres
				 directories and shows data modifications, i.e deletion of files at the rate of 10MB/s will
				 be shown as a positive write rate.
	path_size:	 size of the corresponding PostgreSQL directory.

postgres processes:
	type:		 either a system process (autovacuum launcher, logger, archiver, etc) or a process that
				 executes queries (backend or autovacuum). By default, only user processes are shown (press
				 's' to show all of them) in curses mode, and all in the console one.
	s:			 process state ('R' - 'running', 'S' - 'sleeping', 'D' - 'uninterruptable sleep', see man ps
				 for more details).
	utime,
	stime,
	guest:		 consumption of CPU resources by process. Since PostgreSQL backends can't use more than one
				 CPU, the percentage of a single CPU time is shown here.
	read/write:  amount of data read or written from the partition (in MB/s).
	age:		 time from the process start
	db:			 the database the process runs on
	query:		 the query the process executes.


Hotkeys: when in ncurses mode, press 'h' to see hotkeys.

SELECT_PGSTAT_VERSION_LESS_THAN_92 = """
    SELECT datname,
           procpid as pid,
           usename,
           client_addr,
           client_port,
           round(extract(epoch from (now() - xact_start))) as age,
           waiting,
           NULLIF(array_to_string(array_agg(DISTINCT other.pid ORDER BY other.pid), ','), '')
                as locked_by,
           CASE WHEN current_query = '<IDLE> in transaction' THEN
                    CASE WHEN xact_start != query_start THEN
                             'idle in transaction ' || CAST(
                                 abs(round(extract(epoch from (now() - query_start)))) AS text
                             )
                         ELSE 'idle in transaction'
                    END
                WHEN current_query = '<IDLE>' THEN 'idle'
                ELSE current_query
           END AS query
      FROM pg_stat_activity a
      LEFT JOIN pg_locks  this ON (this.pid = procpid and this.granted = 'f')
      LEFT JOIN pg_locks other ON this.locktype = other.locktype
                               AND this.database IS NOT DISTINCT FROM other.database
                               AND this.relation IS NOT DISTINCT FROM other.relation
                               AND this.page IS NOT DISTINCT FROM other.page
                               AND this.tuple IS NOT DISTINCT FROM other.tuple
                               AND this.virtualxid IS NOT DISTINCT FROM other.virtualxid
                               AND this.transactionid IS NOT DISTINCT FROM other.transactionid
                               AND this.classid IS NOT DISTINCT FROM other.classid
                               AND this.objid IS NOT DISTINCT FROM other.objid
                               AND this.objsubid IS NOT DISTINCT FROM other.objsubid
                               AND this.pid != other.pid
                               AND other.granted = 't'
      WHERE procpid != pg_backend_pid()
      GROUP BY 1,2,3,4,5,6,7,9
"""

SELECT_PGSTAT_VERSION_LESS_THAN_96 = """
    SELECT datname,
           a.pid as pid,
           usename,
           client_addr,
           client_port,
           round(extract(epoch from (now() - xact_start))) as age,
           waiting,
           NULLIF(array_to_string(array_agg(DISTINCT other.pid ORDER BY other.pid), ','), '')
                as locked_by,
           CASE WHEN state = 'idle in transaction' THEN
                    CASE WHEN xact_start != state_change THEN
                             'idle in transaction ' || CAST(
                                 abs(round(extract(epoch from (now() - state_change)))) AS text
                             )
                         ELSE 'idle in transaction'
                    END
                WHEN state = 'active' THEN query
                ELSE state
           END AS query
      FROM pg_stat_activity a
      LEFT JOIN pg_locks  this ON (this.pid = a.pid and this.granted = 'f')
      LEFT JOIN pg_locks other ON this.locktype = other.locktype
                               AND this.database IS NOT DISTINCT FROM other.database
                               AND this.relation IS NOT DISTINCT FROM other.relation
                               AND this.page IS NOT DISTINCT FROM other.page
                               AND this.tuple IS NOT DISTINCT FROM other.tuple
                               AND this.virtualxid IS NOT DISTINCT FROM other.virtualxid
                               AND this.transactionid IS NOT DISTINCT FROM other.transactionid
                               AND this.classid IS NOT DISTINCT FROM other.classid
                               AND this.objid IS NOT DISTINCT FROM other.objid
                               AND this.objsubid IS NOT DISTINCT FROM other.objsubid
                               AND this.pid != other.pid
                               AND other.granted = 't'
      WHERE a.pid != pg_backend_pid()
      GROUP BY 1,2,3,4,5,6,7,9
"""

SELECT_PGSTAT_NEVER_VERSION = """
    SELECT datname,
           a.pid as pid,
           usename,
           client_addr,
           client_port,
           round(extract(epoch from (now() - xact_start))) as age,
           wait_event_type IS NOT DISTINCT FROM 'Lock' AS waiting,
           NULLIF(array_to_string(ARRAY(SELECT unnest(pg_blocking_pids(a.pid)) ORDER BY 1), ','), '')
                as locked_by,
           CASE WHEN state = 'idle in transaction' THEN
                    CASE WHEN xact_start != state_change THEN
                             'idle in transaction ' || CAST(
                                 abs(round(extract(epoch from (now() - state_change)))) AS text
                             )
                         ELSE 'idle in transaction'
                    END
                WHEN state = 'active' THEN query
                ELSE state
           END AS query
      FROM pg_stat_activity a
      WHERE a.pid != pg_backend_pid() AND a.datname IS NOT NULL
      GROUP BY 1,2,3,4,5,6,7,9
"""

SELECT_PG_IS_IN_RECOVERY = "SELECT case WHEN pg_is_in_recovery() THEN 'standby' ELSE 'master' END AS role"
SHOW_MAX_CONNECTIONS = 'SHOW max_connections'

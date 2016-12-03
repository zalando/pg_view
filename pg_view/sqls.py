SELECT_PGSTAT_VERSION_LESS_THAN_92 = """
    SELECT datname,
           procpid as pid,
           usename,
           client_addr,
           client_port,
           round(extract(epoch from (now() - xact_start))) as age,
           waiting,
           string_agg(other.pid::TEXT, ',' ORDER BY other.pid) as locked_by,
           CASE
              WHEN current_query = '<IDLE>' THEN 'idle'
              WHEN current_query = '<IDLE> in transaction' THEN
                  CASE WHEN xact_start != query_start THEN
                      'idle in transaction'||' '||CAST(
                          abs(round(extract(epoch from (now() - query_start)))) AS text
                      )
                  ELSE
                      'idle in transaction'
                  END
             WHEN current_query = '<IDLE> in transaction (aborted)' THEN 'idle in transaction (aborted)'
            ELSE current_query
           END AS query
    FROM pg_stat_activity
    LEFT JOIN pg_locks  this ON (this.pid = procpid and this.granted = 'f')
      -- acquire the same type of lock that is granted
    LEFT JOIN pg_locks other ON ((this.locktype = other.locktype AND other.granted = 't')
                               AND ( ( this.locktype IN ('relation', 'extend')
                                      AND this.database = other.database
                                      AND this.relation = other.relation)
                                     OR (this.locktype ='page'
                                      AND this.database = other.database
                                      AND this.relation = other.relation
                                      AND this.page = other.page)
                                     OR (this.locktype ='tuple'
                                      AND this.database = other.database
                                      AND this.relation = other.relation
                                      AND this.page = other.page
                                      AND this.tuple = other.tuple)
                                     OR (this.locktype ='transactionid'
                                      AND this.transactionid = other.transactionid)
                                     OR (this.locktype = 'virtualxid'
                                      AND this.virtualxid = other.virtualxid)
                                     OR (this.locktype IN ('object', 'userlock', 'advisory')
                                      AND this.database = other.database
                                      AND this.classid = other.classid
                                      AND this.objid = other.objid
                                      AND this.objsubid = other.objsubid))
                                   )
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
           string_agg(other.pid::TEXT, ',' ORDER BY other.pid) as locked_by,
           CASE
              WHEN state = 'idle in transaction' THEN
                  CASE WHEN xact_start != state_change THEN
                      state||' '||CAST( abs(round(extract(epoch from (now() - state_change)))) AS text )
                  ELSE
                      state
                  END
              WHEN state = 'active' THEN query
              ELSE state
              END AS query
    FROM pg_stat_activity a
    LEFT JOIN pg_locks  this ON (this.pid = a.pid and this.granted = 'f')
      -- acquire the same type of lock that is granted
    LEFT JOIN pg_locks other ON ((this.locktype = other.locktype AND other.granted = 't')
                               AND ( ( this.locktype IN ('relation', 'extend')
                                      AND this.database = other.database
                                      AND this.relation = other.relation)
                                     OR (this.locktype ='page'
                                      AND this.database = other.database
                                      AND this.relation = other.relation
                                      AND this.page = other.page)
                                     OR (this.locktype ='tuple'
                                      AND this.database = other.database
                                      AND this.relation = other.relation
                                      AND this.page = other.page
                                      AND this.tuple = other.tuple)
                                     OR (this.locktype ='transactionid'
                                      AND this.transactionid = other.transactionid)
                                     OR (this.locktype = 'virtualxid'
                                      AND this.virtualxid = other.virtualxid)
                                     OR (this.locktype IN ('object', 'userlock', 'advisory')
                                      AND this.database = other.database
                                      AND this.classid = other.classid
                                      AND this.objid = other.objid
                                      AND this.objsubid = other.objsubid))
                                   )
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
           CASE WHEN wait_event IS NULL THEN false ELSE true END as waiting,
           string_agg(other.pid::TEXT, ',' ORDER BY other.pid) as locked_by,
           CASE
              WHEN state = 'idle in transaction' THEN
                  CASE WHEN xact_start != state_change THEN
                      state||' '||CAST( abs(round(extract(epoch from (now() - state_change)))) AS text )
                  ELSE
                      state
                  END
              WHEN state = 'active' THEN query
              ELSE state
              END AS query
    FROM pg_stat_activity a
    LEFT JOIN pg_locks  this ON (this.pid = a.pid and this.granted = 'f')
    -- acquire the same type of lock that is granted
    LEFT JOIN pg_locks other ON ((this.locktype = other.locktype AND other.granted = 't')
                               AND ( ( this.locktype IN ('relation', 'extend')
                                      AND this.database = other.database
                                      AND this.relation = other.relation)
                                     OR (this.locktype ='page'
                                      AND this.database = other.database
                                      AND this.relation = other.relation
                                      AND this.page = other.page)
                                     OR (this.locktype ='tuple'
                                      AND this.database = other.database
                                      AND this.relation = other.relation
                                      AND this.page = other.page
                                      AND this.tuple = other.tuple)
                                     OR (this.locktype ='transactionid'
                                      AND this.transactionid = other.transactionid)
                                     OR (this.locktype = 'virtualxid'
                                      AND this.virtualxid = other.virtualxid)
                                     OR (this.locktype IN ('object', 'userlock', 'advisory')
                                      AND this.database = other.database
                                      AND this.classid = other.classid
                                      AND this.objid = other.objid
                                      AND this.objsubid = other.objsubid))
                                   )
    WHERE a.pid != pg_backend_pid()
    GROUP BY 1,2,3,4,5,6,7,9
"""

SELECT_PG_IS_IN_RECOVERY = "SELECT case WHEN pg_is_in_recovery() THEN 'standby' ELSE 'master' END AS role"
SHOW_MAX_CONNECTIONS = 'SHOW max_connections'

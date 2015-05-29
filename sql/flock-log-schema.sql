-- DB for flock execution log which would be on separate database from flock
-- Task execution log
drop table if exists task_log;
create table task_log (
   log_time timestamp not null default current_timestamp,
   tid bigint not null,
   -- 'S' for start execution,  'C' for complete. 'X' for worker expired
   event_type char not null,
   -- eta in epoch time.
   eta int unsigned null,
   start_time int unsigned null,
   new_eta int unsigned null,
   wid int not null,
   -- execution error if any
   error varchar(1000) null,
   key log_time (log_time),
   key tid (tid)
);

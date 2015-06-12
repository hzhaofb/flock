-- flock MySql database schema

-- Execution environment such as java, python indicates worker's execution environment
-- for processing task. This is very small and static table.
drop table if exists environment;
create table environment (
    eid tinyint unsigned not null primary key auto_increment,
    name varchar(25) not null
);
insert into environment (name) values ('java'),('python'),('go');

-- Task function represent the piece of code that will be executed to process the task.
-- Each function takes task_key and optional params as input and output new eta to reschedule
-- the task. If new eta is null, task is deleted.
-- The side effects of the task function are more important for the application.
drop table if exists func;
create table func (
   fid smallint unsigned not null primary key auto_increment,
   eid tinyint unsigned not null,
   name varchar(100) not null,
   settings text null,
   modified timestamp default current_timestamp on update current_timestamp,
   unique key eid_class_method (eid, name)
);

-- Worker is naturally identified with ip address and process id running on the server.
-- wstatus is a json blob including cpu, memory, network, taskIds for reconciliation
drop table if exists worker;
create table worker (
  -- wid is signed as we are using the negative range for "mark and collect" algorithm
  -- max wid is more than 8 million
  wid mediumint not null primary key auto_increment,
  ip varchar(50) not null,
  pid int not null,
  eid tinyint unsigned not null,
  heartbeat timestamp default current_timestamp on update current_timestamp,
  admin_cmd varchar(50) null,
  wstatus text,
  unique key ip_pid (ip, pid)
);

-- schedule for tasks. Task id (tid) is also used to assign new task
-- ids
drop table if exists schedule;
create table schedule (
   tid bigint not null primary key,
   -- epoch time
   eta int unsigned not null,
   -- wid is signed as we are using the negative range for "mark and collect" algorithm
   wid mediumint not null default 0,
   eid tinyint unsigned not null,
   key schedule_wid_eid_eta (wid, eid, eta)
);

-- Task details.
drop table if exists task;
create table task (
   tid bigint not null primary key auto_increment,
   fid smallint unsigned not null,
   -- if task_key is less than 41, store key in short_key, otherwise store sha1 of task_key
   short_key varchar(40) not null,
   task_key varchar(4096) null,
   modified timestamp default current_timestamp on update current_timestamp,
   params text null,
   unique key type_key (fid, short_key)
);

-- In memory summary table to track flock servers active in the system
drop table if exists server;
create table server (
   sid int not null primary key auto_increment,
   ip varchar(50) not null,
   pid int not null,
   heartbeat timestamp default current_timestamp,
   unique key ip_pid (ip, pid)
) engine = memory;

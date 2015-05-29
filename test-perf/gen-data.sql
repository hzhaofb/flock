-- create a table with sequence used for generate task and schedule
drop table if exists seq;
create table seq (
 id int not null primary key auto_increment,
 modified timestamp default current_timestamp on update current_timestamp
);

DELIMITER $$
DROP PROCEDURE IF EXISTS genSeq$$
CREATE PROCEDURE genSeq(IN cnt int)
BEGIN
  DECLARE i int;
  SET i = 0;
  WHILE i < cnt DO
    insert into seq values
    (),(),(),(),(),(),(),(),(),(),
    (),(),(),(),(),(),(),(),(),(),
    (),(),(),(),(),(),(),(),(),(),
    (),(),(),(),(),(),(),(),(),(),
    (),(),(),(),(),(),(),(),(),(),
    (),(),(),(),(),(),(),(),(),(),
    (),(),(),(),(),(),(),(),(),(),
    (),(),(),(),(),(),(),(),(),(),
    (),(),(),(),(),(),(),(),(),(),
    (),(),(),(),(),(),(),(),(),();
    set i = i + 1;
  END WHILE;
END$$

DELIMITER $$
DROP PROCEDURE IF EXISTS popData$$
CREATE PROCEDURE popData(IN low int, IN high int)
BEGIN
  DECLARE utime int;
  DECLARE prev_tid int;
  set utime = unix_timestamp(now());
  WHILE low <= high DO
    select max(tid) into prev_tid from task;
    if prev_tid is null then
       set prev_tid = 0;
    end if;
    insert into task (fid, short_key)
      select id % 10, concat('user-', (low * 1000000 + id))
      from seq;
    set low = low + 1;
    insert into schedule (tid, eta, eid)
      select tid, (tid % 1000000) * (low % 2), utime + tid - prev_tid, 1
      from task
      where tid > prev_tid;
    -- report progress
    select low, now();
  END WHILE;
END$$
DELIMITER ;

-- populate func definition
insert into func (eid, name) values
  (1, 'testMethod1'),
  (1, 'testMethod2'),
  (1, 'testMethod3'),
  (1, 'testMethod4'),
  (1, 'testMethod5'),
  (1, 'testMethod6'),
  (1, 'testMethod7'),
  (1, 'testMethod8'),
  (1, 'testMethod9'),
  (1, 'testMethod10');

-- each count inserts 1 mil tasks, we want 2 billion
-- call popData(2000);

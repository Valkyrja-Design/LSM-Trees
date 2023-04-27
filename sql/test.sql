create extension lsm;
create table lsmtest(id int, val int);
create table normal(id int, val int);
create index lsm_index on lsmtest using lsm(id);
create index idx on normal(id);

insert into lsmtest values (2,20);
select lsm_start_merge('lsm_index');
select lsm_wait_for_merge('lsm_index');
insert into lsmtest values (3,30);
select lsm_start_merge('lsm_index');
select lsm_wait_for_merge('lsm_index');
insert into lsmtest values (4,40);
-- select lsm_start_merge('lsm_index');
-- select lsm_wait_for_merge('lsm_index');
-- insert into lsmtest values (5,50);
-- select lsm_start_merge('lsm_index');
-- select lsm_wait_for_merge('lsm_index');
-- select lsm_get_merge_count('lsm_index');

select * from lsmtest where id = 1;
-- select * from lsmtest order by id;
-- select * from lsmtest order by id desc;
-- analyze lsmtest;
-- explain (COSTS OFF, TIMING OFF, SUMMARY OFF) select * from lsmtest order by id;

-- insert into lsmtest values (generate_series(1,100000), 1);
-- insert into lsmtest values (generate_series(1000001,200000), 2);
-- insert into lsmtest values (generate_series(2000001,300000), 3);
-- insert into lsmtest values (generate_series(1,100000), 1);
-- insert into lsmtest values (generate_series(1000001,200000), 2);
-- insert into lsmtest values (generate_series(2000001,300000), 3);
-- select * from lsmtest where id = 1;
-- select * from lsmtest where id = 1000000;
-- select * from lsmtest where id = 2000000;
-- select * from lsmtest where id = 3000000;
-- analyze lsmtest;
-- explain (COSTS OFF, TIMING OFF, SUMMARY OFF) select * from lsmtest where id = 1;
-- select lsm_get_merge_count('lsm_index') > 5;

-- truncate table lsmtest;
-- insert into lsmtest values (generate_series(1,1000000), 1);
-- select * from lsmtest where id = 1;

-- reindex table lsmtest;
-- select * from lsmtest where id = 1;


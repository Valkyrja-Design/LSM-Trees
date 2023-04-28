create extension lsm;
create table lsm_time_test(id int, val int,username varchar);
create table normal_time_test(id int, val int,username varchar);
create index lsm_index2 on lsm_time_test using lsm(id);
create index idx2 on normal_time_test(id);
insert into lsm_time_test values (trunc(random()*100000000),generate_series(1,10000000),'hsdhvjdsvbjhbdsjvbbdsbvbdsjbvjbjdsbvbsjdhvjvdjvewdvjwevdjewfbfuigfuiwegfiuwgfiuwgefiugweifgwigfiwugfuiwgfiwgefuewgfiugewfivjvfdjewvfvjwevfjwvfjvwdbvjbjsdv');
insert into normal_time_test values (trunc(random()*100000000),generate_series(1,10000000),'hsdhvjdsvbjhbdsjvbbdsbvbdsjbvjbjdsbvbsjdhvjvdjvewdvjwevdjewfbfuigfuiwegfiuwgfiuwgefiugweifgwigfiwugfuiwgfiwgefuewgfiugewfivjvfdjewvfvjwevfjwvfjvwdbvjbjsdv');
delete from lsm_time_test;
delete from normal_time_test;
# LSM-Trees Extension to PostgreSQL 

See [this](http://big-elephants.com/2015-10/writing-postgres-extensions-part-i/) for how to write an extension.  
See [Index Access Method](https://www.postgresql.org/docs/current/indexam.html) for how to write your custom index.     
See [this](https://www.pgcon.org/events/pgcon_2020/sessions/session/18/slides/28/Introducing%20LSM-tree%20into%20PostgreSQL) for details on lsm.

# Using the extension 

First set ```shared_preload_libraries = 'lsm'``` in ```/etc/postgresql/15/main/postgresql.conf```
```
install postgres@15, llvm
make 
sudo make install
// start postgresql "sudo -u postgres psql -p 5433"
create extension lsm;
create index <index_name> on <table_name> using lsm(<column_name>);
```
# LSM-Trees Extension to PostgreSQL 

See [this](http://big-elephants.com/2015-10/writing-postgres-extensions-part-i/) for how to write an extension.  
See [Index Access Method](https://www.postgresql.org/docs/current/indexam.html) for how to write your custom index.

# Using the extension 

First set ```shared_preload_libraries = 'lsm'``` in ```/etc/postgresql/15/main/postgresql.conf```
```
make 
sudo make install
// start postgresql 
create extension lsm;
create index <index_name> on <table_name> using lsm(<column_name>);
```
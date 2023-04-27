# MODULE_big = lsm
# OBJS = lsm.o

EXTENSION = lsm        # the extension's name
DATA = lsm--1.0.sql  # script files to install
MODULES = lsm
REGRESS = test     # our test script file (without extension)

# postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

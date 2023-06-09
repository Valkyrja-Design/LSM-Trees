#include "postgres.h"
#include "access/attnum.h"
#include "utils/relcache.h"
#include "access/reloptions.h"
#include "access/nbtree.h"
#include "access/table.h"
#include "access/relation.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "nodes/makefuncs.h"
#include "catalog/dependency.h"
#include "catalog/pg_operator.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/storage.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "utils/rel.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "postmaster/bgworker.h"
#include "pgstat.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "postmaster/autovacuum.h"
#include "commands/vacuum.h"

#include "lsm.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* 
	declare functions as PostgreSQL user-defined function
*/
PG_FUNCTION_INFO_V1(lsm_handler);
PG_FUNCTION_INFO_V1(lsm_nbtree_wrapper);
PG_FUNCTION_INFO_V1(lsm_get_merge_count);
PG_FUNCTION_INFO_V1(lsm_start_merge);
PG_FUNCTION_INFO_V1(lsm_wait_for_merge);
PG_FUNCTION_INFO_V1(lsm_get_top_index_size);

extern void lsm_compactor_func(Datum arg);

extern void _PG_init(void);
extern void _PG_fini(void);

/*
	Hash table provided by Postgres
	https://github.com/postgres/postgres/blob/master/src/backend/utils/hash/dynahash.c
*/
static HTAB *lsm_map; // maps relation Oid to control structure (lsm_entry)
/*
	Lightweight Lock, provides shared and exclusive access modes
	https://github.com/postgres/postgres/blob/master/src/backend/storage/lmgr/lwlock.c
*/
static LWLock *lsm_map_lock; // lock for access to lsm_map

/*
	The lsm_released_locks list is used to manage the LWLocks used in the LSM extension. 
	Whenever a process acquires a lock, it adds it to this list. 
	When the process releases the lock, it removes it from the list.
*/
static List *lsm_released_locks;  
/*
	https://github.com/postgres/postgres/blob/master/src/include/nodes/pg_list.h
*/
static List *lsm_entries;
/*
	lsm_copying boolean is used to indicate whether the LSM index is being copied or not.
	Copying is performed during compaction when the LSM index is merged with another index. 
	The lsm_copying variable is used to ensure that the background compaction process does not interfere with other operations on the LSM index.
*/
static bool lsm_copying;   
// relation option kind for our index
static relopt_kind lsm_relopt_kind;

// configurations
static int lsm_max_indices;
static int lsm_top_index_size;

/* See : https://pgpedia.info/h/hooks.html */
// Previous hooks
static ProcessUtility_hook_type prev_process_utility_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
// Only for vers >= 15
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static ExecutorFinish_hook_type prev_executor_finish_hook = NULL;

// background compactor daemon termination flag
static volatile bool lsm_terminate;

// Our custom shared memory request function (hook) to reserve space for control data of all the indices
/* https://pgpedia.info/s/shmem_request_hook.html */
static void lsm_shmem_request(void)
{
	// Call previous request hook if not NULL
#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif

	// reserve memory totalling to size of lsm_max_indices lsm_entry's
	RequestAddinShmemSpace(hash_estimate_size(lsm_max_indices, sizeof(lsm_entry)));
	// request an LWLock for the lsm_map
	RequestNamedLWLockTranche("lsm", 1);
}

// Our custom shared memory startup function (hook) to initialize lsm_map (a hashtable) and acquire lsm_map_lock
static void lsm_shmem_startup(void)
{
	// stores hash table info needed by ShmemInitHash()
	HASHCTL info;

	// call previous hook if not NULL
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	memset(&info, 0, sizeof(info));
	// hash key size
	info.keysize = sizeof(Oid);
	// value size
	info.entrysize = sizeof(lsm_entry);
	// initialize hash table
	lsm_map = ShmemInitHash(
		"lsm hash",
		lsm_max_indices,
		lsm_max_indices,
		&info,
		HASH_ELEM | HASH_BLOBS);

	// initialize lock
	lsm_map_lock = &(GetNamedLWLockTranche("lsm"))->lock;
}

// Initialize lsm_entry control struct
static void lsm_init_entry(lsm_entry *entry, Relation index)
{
	SpinLockInit(&entry->spinlock);
	entry->active_index = 0;
	entry->compactor = NULL;
	entry->merge_in_progress = false;
	entry->start_merge = false;
	entry->n_merges = 0;
	entry->n_inserts = 0;
	entry->top_indices[0] = entry->top_indices[1] = InvalidOid;
	entry->access_count[0] = entry->access_count[1] = 0;
	/*
		rd_index is a tuple describing this index and we use it to fetch Oid of the relation it indexes
		Relation : https://github.com/postgres/postgres/blob/master/src/include/utils/rel.h
		Form_pg_index : https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_index.h
	*/
	entry->heap = index->rd_index->indrelid;
	entry->db_id = MyDatabaseId;
	entry->user_id = GetUserId();
	entry->top_index_size = index->rd_options ? ((lsm_options *)index->rd_options)->top_index_size : 0;
}

// returns size of B-tree in units of blocks
static BlockNumber lsm_get_index_size(Oid relid)
{
	// index_open returns a pointer to the index relation with given Oid
	Relation index = index_open(relid, AccessShareLock);
	BlockNumber size = RelationGetNumberOfBlocks(index);
	index_close(index, AccessShareLock);

	return size;
}

// fetch or create lsm_map entry for a given index
static lsm_entry *lsm_get_entry(Relation index)
{
	lsm_entry *entry;
	bool found = true;
	// Acquire lsm_map_lock
	LWLockAcquire(lsm_map_lock, LW_SHARED);
	// search for Oid of given index
	entry = (lsm_entry *)hash_search(lsm_map, &RelationGetRelid(index), HASH_FIND, &found);

	// didn't find entry so create one
	if (entry == NULL)
	{
		// Need exclusive lock to modify lsm_map
		LWLockRelease(lsm_map_lock);
		LWLockAcquire(lsm_map_lock, LW_EXCLUSIVE);
		entry = (lsm_entry *)hash_search(lsm_map, &RelationGetRelid(index), HASH_ENTER, &found);
	}

	// found still false even after creating so initialize the entry
	if (!found)
	{
		char *relname = RelationGetRelationName(index);
		lsm_init_entry(entry, index);
		// set Oids of top indices, named as {relname}_top{n}
		for (int i = 0; i < 2; i++)
		{
			// psprintf similar to sprintf but returns buffer with string
			char *top_idx_name = psprintf("%s_top%d", relname, i);
			// gets the relation Oid from its name and namespace
			entry->top_indices[i] = get_relname_relid(top_idx_name, RelationGetNamespace(index));
			// couldn't fetch
			if (entry->top_indices[i] == InvalidOid)
				elog(ERROR, "LSM: failed to find index %s", top_idx_name);
		}
		// active index is one with more entries from the two
		entry->active_index = lsm_get_index_size(entry->top_indices[0]) >= lsm_get_index_size(entry->top_indices[1]) ? 0 : 1;
	}
	LWLockRelease(lsm_map_lock);
	return entry;
}

// Truncate top index to 0 tuples and rebuild from scratch by scanning heap
static void lsm_truncate_index(Oid index_oid, Oid heap_oid)
{
	Relation index = index_open(index_oid, AccessExclusiveLock);
	Relation heap = table_open(heap_oid, AccessShareLock);
	IndexInfo *indexInfo = BuildDummyIndexInfo(index);
	RelationTruncate(index, 0);
	elog(LOG, "lsm: truncate index %s", RelationGetRelationName(index));
	/*
		call Access Method specific index build procedure
		void index_build(Relation heapRelation,
						Relation indexRelation,
						IndexInfo *indexInfo,
						bool isreindex,
						bool parallel)
		isreindex indicates we are recreating a previously-existing index
		https://github.com/postgres/postgres/blob/master/src/backend/catalog/index.c
	*/
	index_build(heap, index, indexInfo, true, false);
	index_close(index, AccessExclusiveLock);
	table_close(heap, AccessShareLock);
}

#define INSERT_FLAGS UNIQUE_CHECK_NO, false

// constructing lsm_options from reloptions
static bytea *lsm_get_options(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"fillfactor", RELOPT_TYPE_INT, offsetof(BTOptions, fillfactor)},
		{"vacuum_cleanup", RELOPT_TYPE_REAL, offsetof(BTOptions, vacuum_cleanup_index_scale_factor)},
		{"deduplicate_items", RELOPT_TYPE_BOOL, offsetof(BTOptions, deduplicate_items)},
		{"top_index_size", RELOPT_TYPE_INT, offsetof(lsm_options, top_index_size)},
		// {"unique", RELOPT_TYPE_BOOL, offsetof(lsm_options, unique)}
	};
	/*
		build_reloptions parses above and returns a struct with those parsed options
		https://github.com/postgres/postgres/blob/master/src/backend/access/common/reloptions.c#L1910
	*/
	return (bytea *)build_reloptions(reloptions, validate, lsm_relopt_kind, sizeof(lsm_options), tab, lengthof(tab));
}

// builds index tuple comparator context
static SortSupport lsm_build_sortkeys(Relation index)
{
	int key_size = IndexRelationGetNumberOfKeyAttributes(index);
	SortSupport sortKeys = (SortSupport)palloc0(key_size * sizeof(SortSupportData));
	/*
		Builds an insert scan key.
		A scan key is the internal representation of a WHERE clause of the form 'index_key operator constant',
		where the index key is one of the columns of the index and the operator is one of the members of the
		operator family associated with that index column.
	*/
	BTScanInsert insert_key = _bt_mkscankey(index, NULL); /* BTScanInsert: https://github.com/postgres/postgres/blob/master/src/include/access/nbtree.h */
	Oid save_am = index->rd_rel->relam;
	index->rd_rel->relam = BTREE_AM_OID;

	for (int i = 0; i < key_size; i++)
	{

		SortSupport sortKey = &sortKeys[i];			/* SortSupport: https://github.com/postgres/postgres/blob/master/src/include/utils/sortsupport.h */
		ScanKey scanKey = &insert_key->scankeys[i]; /* ScanKey: https://github.com/postgres/postgres/blob/master/src/include/access/skey.h */
		int16 strategy;

		// Need to fill these fields before calling PrepareSortSupportFromIndexRel()
		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = scanKey->sk_collation;
		sortKey->ssup_nulls_first = (scanKey->sk_flags & SK_BT_NULLS_FIRST) != 0;
		sortKey->ssup_attno = scanKey->sk_attno;
		// no abbreviation
		sortKey->abbreviate = false;
		Assert(sortKey->ssup_attno != 0);

		strategy = (scanKey->sk_flags & SK_BT_DESC) != 0 ? BTGreaterStrategyNumber : BTLessStrategyNumber;

		// this needs am to be B-tree hence we set it above
		PrepareSortSupportFromIndexRel(index, strategy, sortKey);
	}
	index->rd_rel->relam = save_am;
	return sortKeys;
}

// compare index tuples
static int lsm_compare_index_tuples(IndexScanDesc scan1, IndexScanDesc scan2, SortSupport sortKeys)
{
	/* IndexScanDesc: https://github.com/postgres/postgres/blob/master/src/include/access/relscan.h */

	int n_keys = IndexRelationGetNumberOfKeyAttributes(scan1->indexRelation);

	for (int i = 1; i <= n_keys; i++)
	{
		Datum datum[2];
		bool isNull[2];
		int result;

		// IndexTuple	xs_itup;		/* index tuple returned by AM */
		// struct TupleDescData *xs_itupdesc;	/* rowtype descriptor of xs_itup */

		datum[0] = index_getattr(scan1->xs_itup, i, scan1->xs_itupdesc, &isNull[0]);
		datum[1] = index_getattr(scan2->xs_itup, i, scan2->xs_itupdesc, &isNull[1]);
		// sortKey is basically comparator
		result = ApplySortComparator(datum[0], isNull[0], datum[1], isNull[1], &sortKeys[i - 1]);

		// If not equal return
		if (result)
			return result;
	}
	return ItemPointerCompare(&scan1->xs_heaptid, &scan2->xs_heaptid);
}

//create background worker process that will execute the function lsm_compactor_func() in a separate process
static void lsm_launch_worker(lsm_entry *entry)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	pid_t worker_pid;

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, sizeof(worker.bgw_name), "compactor_%d", entry->base);
	snprintf(worker.bgw_type, sizeof(worker.bgw_type), "compactor_%d", entry->base);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy(worker.bgw_function_name, "lsm_compactor_func");
	strcpy(worker.bgw_library_name, "lsm");
	worker.bgw_main_arg = PointerGetDatum(entry);
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		elog(ERROR, "LSM: failed to start compactor daemon!");
	}
	if (WaitForBackgroundWorkerStartup(handle, &worker_pid) != BGWH_STARTED)
	{
		elog(ERROR, "LSM: compactor daemon startup failed!");
	}
	entry->compactor = BackendPidGetProc(worker_pid);
	for (int i = 0; entry->compactor == NULL || i < 100; i++)
	{
		pg_usleep(10000); // wait for compactor to be registerd as proccess
		entry->compactor = BackendPidGetProc(worker_pid);
	}

	if (entry->compactor == NULL)
	{
		elog(ERROR, "LSM: compactor daemon %d stopped!", worker_pid);
	}
}

static void lsm_terminate_merge(int sig)
{
	lsm_terminate = true;
	SetLatch(MyLatch);
}

// merges src with dest
static void lsm_merge_indices(Oid dest, Oid src, Oid heap_oid)
{
	Relation src_index = index_open(src, AccessShareLock);
	Relation heap = table_open(heap_oid, AccessShareLock);
	Relation base = index_open(dest, RowExclusiveLock);
	IndexScanDesc scan;
	bool flag;
	Oid prev_am = src_index->rd_rel->relam;

	elog(LOG, "LSM: Merging top index %s with size %d blocks", RelationGetRelationName(src_index), RelationGetNumberOfBlocks(src_index));

	base->rd_rel->relam = BTREE_AM_OID;
	scan = index_beginscan(heap, src_index, SnapshotAny, 0, 0);
	scan->xs_want_itup = true;
	btrescan(scan, NULL, 0, 0, 0);
	for (flag = _bt_first(scan, ForwardScanDirection); flag; flag = _bt_next(scan, ForwardScanDirection))
	{
		IndexTuple itup = scan->xs_itup;
		if (BTreeTupleIsPosting(itup))
		{
			ItemPointerData prev_tid = itup->t_tid;
			unsigned short prev_info = itup->t_info;
			itup->t_info = (prev_info & ~(INDEX_SIZE_MASK | INDEX_ALT_TID_MASK)) + BTreeTupleGetPostingOffset(itup);
			itup->t_tid = scan->xs_heaptid;
			_bt_doinsert(base, itup, INSERT_FLAGS, heap);

			itup->t_tid = prev_tid;
			itup->t_info = prev_info;
		}
		else
		{
			_bt_doinsert(base, itup, INSERT_FLAGS, heap);
		}
	}
	index_endscan(scan);
	base->rd_rel->relam = prev_am;
	index_close(src_index, AccessShareLock);
	index_close(base, RowExclusiveLock);
	table_close(heap, AccessShareLock);
}

void lsm_compactor_func(Datum arg)
{
	lsm_entry *entry = (lsm_entry *)DatumGetPointer(arg);
	char *name;

	// signal handlers
	pqsignal(SIGINT, lsm_terminate_merge);
	pqsignal(SIGQUIT, lsm_terminate_merge);
	pqsignal(SIGTERM, lsm_terminate_merge);

	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(entry->db_id, entry->user_id, 0);

	name = psprintf("LSM merger for %d", entry->base);
	pgstat_report_appname(name);
	pfree(name);

	while (!lsm_terminate)
	{
		int merge_index = -1;
		int wl;
		pgstat_report_activity(STATE_IDLE, "waiting");
		wl = WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1L, PG_WAIT_EXTENSION);

		// quit if db system dies or lsm_terminate true
		if ((wl & WL_POSTMASTER_DEATH) || lsm_terminate)
			break;

		ResetLatch(MyLatch);

		// check merge flag
		SpinLockAcquire(&entry->spinlock);
		if (entry->start_merge)
		{
			merge_index = 1 - entry->active_index;
			entry->start_merge = false;
		}
		SpinLockRelease(&entry->spinlock);

		if (merge_index >= 0)
		{
			StartTransactionCommand();
			{
				// merge with base
				pgstat_report_activity(STATE_RUNNING, "merging indices");
				lsm_merge_indices(entry->base, entry->top_indices[merge_index], entry->heap); //1st arg is dest and second is src

				// truncate merge index
				pgstat_report_activity(STATE_RUNNING, "truncating");
				lsm_truncate_index(entry->top_indices[merge_index], entry->heap);
			}
			CommitTransactionCommand();

			SpinLockAcquire(&entry->spinlock);
			entry->merge_in_progress = false;
			SpinLockRelease(&entry->spinlock);
		}
	}
	entry->compactor = NULL;
}

/*
	Access Methods
	Called when you first creste index using "create index <ind_name> on <rel> using lsm(<attributes>);"
*/

static IndexBuildResult *
lsm_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	bool found;
	lsm_entry *entry;
	LWLockAcquire(lsm_map_lock, LW_EXCLUSIVE);
	elog(LOG, "LSM build %s", index->rd_rel->relname.data);
	// setting entry indicates to utility hook that lsm index was created
	entry = hash_search(lsm_map, &RelationGetRelid(index), HASH_ENTER, &found);
	if (!found)
		lsm_init_entry(entry, index);
	{
		MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
		lsm_entries = lappend(lsm_entries, entry);
		MemoryContextSwitchTo(old_context);
	}
	entry->am_id = index->rd_rel->relam;
	index->rd_rel->relam = BTREE_AM_OID;
	LWLockRelease(lsm_map_lock);
	return btbuild(heap, index, indexInfo);
}

// Grab released locks for merger to proceed
static void lsm_reacquire_locks(void)
{
	if (lsm_released_locks)
	{
		ListCell *cell;
		foreach (cell, lsm_released_locks)
		{
			Oid index_oid = lfirst_oid(cell);
			/*
			 *		LockRelationOid
			 *
			 * Lock a relation given only its OID.  This should generally be used
			 * before attempting to open the relation's relcache entry.
			 * https://github.com/postgres/postgres/blob/master/src/backend/storage/lmgr/lmgr.c#L103
			 */

			LockRelationOid(index_oid, RowExclusiveLock);
		}
		list_free(lsm_released_locks);
		lsm_released_locks = NULL;
	}
}

/*
	Insert in active top index, on overflow swap active indices and start merge
	rel->indexRel;
*/
static bool lsm_insert(
	Relation rel, Datum *values, bool *isNull, ItemPointer ht_ctid,
	Relation heapRel, IndexUniqueCheck checkUnique,
	bool indexUnchanged,
	IndexInfo *indexInfo)
{
	lsm_entry *entry = lsm_get_entry(rel);
	int active_index;
	uint64 n_merges;
	Relation index;
	Oid save_am;
	bool overflow;
	int top_index_size = entry->top_index_size ? entry->top_index_size : lsm_top_index_size;
	bool is_initialized = true;

	// Get current active index
	// elog(LOG, "lsm_insert called");
	SpinLockAcquire(&entry->spinlock);
	active_index = entry->active_index;
	if (entry->top_indices[active_index])
		entry->access_count[active_index]++;
	else
		is_initialized = false;
	n_merges = entry->n_merges;
	SpinLockRelease(&entry->spinlock);

	if (!is_initialized)
	{
		bool response;
		save_am = rel->rd_rel->relam;
		rel->rd_rel->relam = BTREE_AM_OID;
		response = btinsert(rel, values, isNull, ht_ctid, heapRel, checkUnique, indexUnchanged, indexInfo);
		rel->rd_rel->relam = save_am;
		return response;
	}

	// insert in top index
	index = index_open(entry->top_indices[active_index], RowExclusiveLock);
	index->rd_rel->relam = BTREE_AM_OID;
	save_am = index->rd_rel->relam;
	btinsert(index, values, isNull, ht_ctid, heapRel, checkUnique, indexUnchanged, indexInfo);

	index_close(index, RowExclusiveLock);
	index->rd_rel->relam = save_am;

	overflow = !entry->merge_in_progress								  /* don't check for overflow if merge already initiated */
			   && (entry->n_inserts % LSM_CHK_TOP_IDX_SIZE_INTERVAL == 0) /* periodic check */
			   && (RelationGetNumberOfBlocks(index) * (BLCKSZ / 1024) > top_index_size);

	SpinLockAcquire(&entry->spinlock);
	// start merge if not already in progress
	if (overflow && !entry->merge_in_progress && entry->n_merges == n_merges)
	{
		Assert(entry->active_index = active_index);
		entry->merge_in_progress = true;
		entry->active_index ^= 1; /* swap active indices */
		entry->n_merges++;
	}

	Assert(entry->access_count[active_index] > 0);
	entry->access_count[active_index] -= 1;
	entry->n_inserts++;
	if (entry->merge_in_progress)
	{
		LOCKTAG tag;
		SET_LOCKTAG_RELATION(tag, MyDatabaseId, entry->top_indices[1 - active_index]);
		// Holding lock on non-active relation prevents bgworker from truncating this index
		if (LockHeldByMe(&tag, RowExclusiveLock))
		{

			/* Copy indices and release lock periodically */
			if (!lsm_copying || (entry->n_inserts % LSM_CHK_TOP_IDX_SIZE_INTERVAL == 0))
			{
				LockRelease(&tag, RowExclusiveLock, false);
				lsm_released_locks = lappend_oid(lsm_released_locks, entry->top_indices[1 - active_index]);
			}
		}

		/* if all inserts in previous active index are done we can start merge */
		if (entry->active_index != active_index && entry->access_count[active_index] == 0)
		{
			entry->start_merge = true;
			if (entry->compactor == NULL)
			{
				lsm_launch_worker(entry);
			}
			SetLatch(&entry->compactor->procLatch);
		}
	}
	SpinLockRelease(&entry->spinlock);

	/* We have to require released locks because othervise CopyFrom will produce warning */
	if (lsm_copying && lsm_released_locks)
	{
		pg_usleep(1);
		lsm_reacquire_locks();
	}

	return false;
}

static IndexScanDesc lsm_beginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	lsm_scan_desc *lsd;

	/* no order by operators */
	Assert(norderbys == 0);
	// elog(LOG, "Started index scan");
	scan = RelationGetIndexScan(rel, nkeys, norderbys);
	scan->xs_itupdesc = RelationGetDescr(rel);
	lsd = (lsm_scan_desc *)palloc(sizeof(lsm_scan_desc));
	lsd->entry = lsm_get_entry(rel);
	lsd->sortKeys = lsm_build_sortkeys(rel);
	// elog(LOG, "Started Checking top indices");
	for (int i = 0; i < 2; i++)
	{
		if (lsd->entry->top_indices[i])
		{
			lsd->top_indices[i] = index_open(lsd->entry->top_indices[i], AccessShareLock);
			lsd->scan[i] = btbeginscan(lsd->top_indices[i], nkeys, norderbys);
		}
		else
		{
			lsd->top_indices[i] = NULL;
			lsd->scan[i] = NULL;
		}
	}
	// elog(LOG, "Done Checking top indices");
	lsd->scan[2] = btbeginscan(rel, nkeys, norderbys);
	for (int i = 0; i < 3; i++)
	{
		if (lsd->scan[i])
		{
			lsd->eoi[i] = false;
			lsd->scan[i]->xs_want_itup = true;
			lsd->scan[i]->parallel_scan = NULL;
		}
	}
	lsd->unique = rel->rd_options ? ((lsm_options *)rel->rd_options)->unique : false;
	lsd->curr_index = -1;
	scan->opaque = lsd;

	return scan;
}

static void lsm_rescan(IndexScanDesc scan, ScanKey scanKey, int nscankeys, ScanKey orderbys, int norderbys)
{
	lsm_scan_desc *lsd = (lsm_scan_desc *)scan->opaque;
	lsd->curr_index = -1;
	// elog(LOG, "LSM rescan called");
	for (int i = 0; i < 3; i++)
	{
		if (lsd->scan[i])
		{
			btrescan(lsd->scan[i], scanKey, nscankeys, orderbys, norderbys);
			lsd->eoi[i] = false;
		}
	}
	// elog(LOG, "Done LSM rescaning");
}

static void lsm_endscan(IndexScanDesc scan)
{
	lsm_scan_desc *lsd = (lsm_scan_desc *)scan->opaque;
	// elog(LOG, "LSM endscan called");
	for (int i = 0; i < 3; i++)
	{
		if (lsd->scan[i])
		{
			btendscan(lsd->scan[i]);
			if (i < 2)
				index_close(lsd->top_indices[i], AccessShareLock);
		}
	}
	pfree(lsd);
	// elog(LOG, "Done LSM endscan");
}

static bool lsm_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	lsm_scan_desc *lsd = (lsm_scan_desc *)scan->opaque;
	int min = -1;
	int curr = lsd->curr_index;
	/* start with active index then merging then base */
	int try_index_order[3] = {lsd->entry->active_index, 1 - lsd->entry->active_index, 2};

	/* btree indices are not lossy */
	scan->xs_recheck = false;
	// elog(LOG, "LSM gettuple called");
	if (curr >= 0)
		lsd->eoi[curr] = !_bt_next(lsd->scan[curr], dir); /* move forward current index */

	for (int j = 0; j < 3; j++)
	{
		int i = try_index_order[j];
		BTScanOpaque bto = (BTScanOpaque)lsd->scan[i]->opaque;
		lsd->scan[i]->xs_snapshot = scan->xs_snapshot;
		if (!lsd->eoi[i] && !BTScanPosIsValid(bto->currPos))
		{
			lsd->eoi[i] = !_bt_first(lsd->scan[i], dir);
			if (!lsd->eoi[i] && lsd->unique && scan->numberOfKeys == scan->indexRelation->rd_index->indnkeyatts)
			{
				/* If index is marked as unique and we perform lookup using all index keys,
				 * then we can stop after locating first occurrence.
				 * If make it possible to avoid lookups of all three indexes.
				 */
				elog(DEBUG1, "Lsm3: lookup %d indices", j + 1);
				while (++j < 3) /* prevent search of all remanining indexes */
				{
					lsd->eoi[try_index_order[j]] = true;
				}
				min = i;
				break;
			}
		}
		if (!lsd->eoi[i])
		{
			if (min < 0)
			{
				min = i;
			}
			else
			{
				int result = lsm_compare_index_tuples(lsd->scan[i], lsd->scan[min], lsd->sortKeys);
				if (result == 0)
				{
					/* Duplicate: it can happen during merge when same tid is both in top and base index */
					lsd->eoi[i] = !_bt_next(lsd->scan[i], dir); /* just skip one of entries */
				}
				else if ((result < 0) == ScanDirectionIsForward(dir))
				{
					min = i;
				}
			}
		}
	}
	if (min < 0) /* all indices traversed but didn't find match */
		return false;

	scan->xs_heaptid = lsd->scan[min]->xs_heaptid; /* copy TID */
	if (scan->xs_want_itup)
		scan->xs_itup = lsd->scan[min]->xs_itup;
	lsd->curr_index = min;
	return true;
}

static int64 lsm_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	lsm_scan_desc *lsd = (lsm_scan_desc *)scan->opaque;
	int64 ntids = 0;
	for (int i = 0; i < 3; i++)
	{
		if (lsd->scan[i])
		{
			lsd->scan[i]->xs_snapshot = scan->xs_snapshot;
			ntids += btgetbitmap(lsd->scan[i], tbm);
		}
	}
	return ntids;
}

Datum lsm_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amoptsprocnum = BTOPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = 0;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = lsm_build;
	amroutine->ambuildempty = btbuildempty;
	amroutine->aminsert = lsm_insert;
	amroutine->ambulkdelete = btbulkdelete;
	amroutine->amvacuumcleanup = btvacuumcleanup;
	amroutine->amcanreturn = btcanreturn;
	amroutine->amcostestimate = btcostestimate;
	amroutine->amoptions = lsm_get_options;
	amroutine->amproperty = btproperty;
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = btvalidate;
	amroutine->amadjustmembers = btadjustmembers;
	amroutine->ambeginscan = lsm_beginscan;
	amroutine->amrescan = lsm_rescan;
	amroutine->amgettuple = lsm_gettuple;
	amroutine->amgetbitmap = lsm_getbitmap;
	amroutine->amendscan = lsm_endscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
	Acess methods for nbtree wrapper for disabling inserts
*/

/* We do not need to load data in top top index: just initialize index metadata */
static IndexBuildResult *
lsm_build_empty(Relation heap, Relation index, IndexInfo *indexInfo)
{
	/* Taken from : https://github.com/postgres/postgres/blob/master/src/backend/access/nbtree/nbtree.c#L152 */
	Page metapage;

	/* Construct metapage. */
	metapage = (Page)palloc(BLCKSZ);
	_bt_initmetapage(metapage, BTREE_METAPAGE, 0, _bt_allequalimage(index, false));

	RelationGetSmgr(index);

	PageSetChecksumInplace(metapage, BTREE_METAPAGE);
	smgrextend(index->rd_smgr, MAIN_FORKNUM, BTREE_METAPAGE,
			   (char *)metapage, true);
	log_newpage(&index->rd_smgr->smgr_rnode.node, MAIN_FORKNUM,
				BTREE_METAPAGE, metapage, true);
	smgrimmedsync(index->rd_smgr, MAIN_FORKNUM);

	RelationCloseSmgr(index);

	return (IndexBuildResult *)palloc0(sizeof(IndexBuildResult));
}

static bool
lsm_dummy_insert(Relation rel, Datum *values, bool *isnull,
				 ItemPointer ht_ctid, Relation heapRel,
				 IndexUniqueCheck checkUnique,
				 bool indexUnchanged,
				 IndexInfo *indexInfo)
{
	return false;
}

Datum lsm_nbtree_wrapper(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amoptsprocnum = BTOPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = true;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = 0;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = lsm_build_empty;
	amroutine->ambuildempty = btbuildempty;
	amroutine->aminsert = lsm_dummy_insert;
	amroutine->ambulkdelete = btbulkdelete;
	amroutine->amvacuumcleanup = btvacuumcleanup;
	amroutine->amcanreturn = btcanreturn;
	amroutine->amcostestimate = btcostestimate;
	amroutine->amoptions = lsm_get_options;
	amroutine->amproperty = btproperty;
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = btvalidate;
	amroutine->ambeginscan = btbeginscan;
	amroutine->amrescan = btrescan;
	amroutine->amgettuple = btgettuple;
	amroutine->amgetbitmap = btgetbitmap;
	amroutine->amendscan = btendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
	Utility hook handling creation and deletion of lsm indices
	Process utility hook ref :
	https://github.com/postgres/postgres/blob/master/contrib/pg_stat_statements/pg_stat_statements.c#L1080
*/
static void lsm_process_utility(PlannedStmt *plannedStmt,
								const char *queryString,
								bool readOnlyTree,
								ProcessUtilityContext context,
								ParamListInfo params,
								QueryEnvironment *queryEnv,
								DestReceiver *dest,
								QueryCompletion *qc)
{
	Node *parseTree = plannedStmt->utilityStmt;
	/*
		https://github.com/postgres/postgres/blob/master/src/include/nodes/parsenodes.h
	*/
	DropStmt *drop = NULL; // DROP <Object_type>
	/* https://github.com/postgres/postgres/blob/master/src/include/catalog/objectaddress.h#L22 */
	ObjectAddresses *objects_dropped = NULL;
	List *dropped_oids = NULL;
	ListCell *cell;

	lsm_entries = NULL;
	lsm_copying = false;
	// elog(LOG, "LSM process_utility called");
	if (IsA(parseTree, DropStmt))
	{
		drop = (DropStmt *)parseTree;
		if (drop->removeType == OBJECT_INDEX)
		{ // Removing index
			foreach (cell, drop->objects)
			{
				/* https://github.com/postgres/postgres/blob/master/src/include/nodes/primnodes.h#L55 */
				RangeVar *range_var = makeRangeVarFromNameList((List *)lfirst(cell));
				Relation index = relation_openrv(range_var, ExclusiveLock);
				if (index->rd_indam->ambuild == lsm_build)
				{ // lsm type index
					lsm_entry *entry = lsm_get_entry(index);
					if (objects_dropped == NULL)
					{
						objects_dropped = new_object_addresses();
					}
					// add the top two indices to dropped objects
					for (int i = 0; i < 2; i++)
					{
						if (entry->top_indices[i])
						{
							ObjectAddress obj_addr;
							obj_addr.classId = RelationRelationId;
							obj_addr.objectId = entry->top_indices[i];
							obj_addr.objectSubId = 0;
							add_exact_object_address(&obj_addr, objects_dropped);
						}
					}
					dropped_oids = lappend_oid(dropped_oids, RelationGetRelid(index));
				}
				relation_close(index, ExclusiveLock);
			}
		}
	}
	else if (IsA(parseTree, CopyStmt))
	{
		lsm_copying = true;
	}
	// elog(LOG, "Calling prev utility hook now");
	// Call previous process utility hooks
	(prev_process_utility_hook ? prev_process_utility_hook : standard_ProcessUtility)(
		plannedStmt,
		queryString,
		readOnlyTree,
		context,
		params,
		queryEnv,
		dest,
		qc);

	// new lsm indices might have been created (handled by standard utility hook)
	// so we now create the corresponding top indices
	if (lsm_entries)
	{
		foreach (cell, lsm_entries)
		{
			lsm_entry *entry = (lsm_entry *)lfirst(cell);
			Oid top_indices[2];
			if (IsA(parseTree, IndexStmt))
			{
				IndexStmt *stmt = (IndexStmt *)parseTree;
				char *idx_name = stmt->idxname;
				char *am = stmt->accessMethod;

				// create top 2 indices
				for (int i = 0; i < 2; i++)
				{
					if (stmt->concurrent)
					{
						PushActiveSnapshot(GetTransactionSnapshot());
					}
					stmt->accessMethod = "lsm_nbtree_wrapper";
					stmt->idxname = psprintf("%s_top%d", get_rel_name(entry->base), i);
					/* https://github.com/postgres/postgres/blob/master/src/backend/commands/indexcmds.c#L488 */
					top_indices[i] = DefineIndex(entry->heap, stmt, InvalidOid, InvalidOid, InvalidOid, false, false, false, false, true).objectId;
				}
				stmt->accessMethod = am;
				stmt->idxname = idx_name;
			}
			else
			{
				for (int i = 0; i < 2; i++)
				{
					top_indices[i] = entry->top_indices[i];
					if (top_indices[i] == InvalidOid)
					{
						char* topidxname = psprintf("%s_top%d", get_rel_name(entry->base), i);
						top_indices[i] = get_relname_relid(topidxname, get_rel_namespace(entry->base));
						if (top_indices[i] == InvalidOid)
						{
							elog(ERROR, "Lsm3: failed to lookup %s index", topidxname);
						}
					}
				}
			}
			if (ActiveSnapshotSet())
			{
				PopActiveSnapshot();
			}
			CommitTransactionCommand();
			StartTransactionCommand();
			/* prevent planner from using these indices */
			for (int i = 0; i < 2; i++)
			{
				index_set_state_flags(top_indices[i], INDEX_DROP_CLEAR_VALID);
			}
			// set top index oids in entry
			SpinLockAcquire(&entry->spinlock);
			for (int i = 0; i < 2; i++)
			{
				entry->top_indices[i] = top_indices[i];
			}
			SpinLockRelease(&entry->spinlock);
			{
				// set am to lsm (was set to btree on creation)
				Relation index = index_open(entry->base, AccessShareLock);
				index->rd_rel->relam = entry->am_id;
				index_close(index, AccessShareLock);
			}
		}
		list_free(lsm_entries);
		lsm_entries = NULL;
	}
	else if (objects_dropped)
	{
		// Remove dropped indices from lsm_map and correspoding auxiliary indices
		performMultipleDeletions(objects_dropped, drop->behavior, 0);
		LWLockAcquire(lsm_map_lock, LW_EXCLUSIVE);
		foreach (cell, dropped_oids)
		{
			hash_search(lsm_map, &lfirst_oid(cell), HASH_REMOVE, NULL);
		}
		LWLockRelease(lsm_map_lock);
	}
}

static void lsm_executor_finish(QueryDesc *queryDesc)
{
	lsm_reacquire_locks();
	lsm_copying = false;
	if (prev_executor_finish_hook)
		prev_executor_finish_hook(queryDesc);
	else
		standard_ExecutorFinish(queryDesc);
}

void _PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "LSM : Extension must be loaded via shared_preload_librarires");
	}
	// Define max #indices and size of top_index
	/* https://github.com/postgres/postgres/blob/master/src/backend/utils/misc/guc.c#L4983 */
	DefineCustomIntVariable("lsm.top_index_size",
							"Size of top index",
							NULL,
							&lsm_top_index_size,
							64 * 1024,
							BLCKSZ / 1024,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("lsm.max_indices",
							"Max #indices",
							NULL,
							&lsm_max_indices,
							1024,
							1,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	// set relation options
	lsm_relopt_kind = add_reloption_kind();

	add_int_reloption(lsm_relopt_kind, "top_index_size",
					  "Size of top index",
					  0, 0, INT_MAX, AccessExclusiveLock);
	add_int_reloption(lsm_relopt_kind, "fillfactor",
					  "btree fill factor",
					  BTREE_DEFAULT_FILLFACTOR, BTREE_MIN_FILLFACTOR, 100, ShareUpdateExclusiveLock);
	add_real_reloption(lsm_relopt_kind, "vacuum_cleanup_index_scale_factor",
					   "btree cleanup factor", -1, 0.0, 1e10, ShareUpdateExclusiveLock);
	add_bool_reloption(lsm_relopt_kind, "deduplicate_items",
					   "Enable \"deduplicate\" feature", true, AccessExclusiveLock);

	// set hooks
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = lsm_shmem_startup;
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = lsm_shmem_request;
#else
	lsm_shmem_request();
#endif
	prev_process_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = lsm_process_utility;

	prev_executor_finish_hook = ExecutorFinish_hook;
	ExecutorFinish_hook = lsm_executor_finish;
}


Datum
lsm_get_merge_count(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	Relation index = index_open(relid, AccessShareLock);
	lsm_entry* entry = lsm_get_entry(index);
	index_close(index, AccessShareLock);
	if (entry == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_INT64(entry->n_merges);
}


Datum
lsm_start_merge(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	Relation index = index_open(relid, AccessShareLock);
	lsm_entry* entry = lsm_get_entry(index);
	index_close(index, AccessShareLock);

	SpinLockAcquire(&entry->spinlock);
	if (!entry->merge_in_progress)
	{
		entry->merge_in_progress = true;
		entry->active_index ^= 1;
		entry->n_merges += 1;
		if (entry->access_count[1-entry->active_index] == 0)
		{
			entry->start_merge = true;
			if (entry->compactor == NULL) /* lazy start of bgworker */
			{
				lsm_launch_worker(entry);
			}
			SetLatch(&entry->compactor->procLatch);
		}
	}
	SpinLockRelease(&entry->spinlock);
	PG_RETURN_NULL();
}

Datum
lsm_wait_for_merge(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	Relation index = index_open(relid, AccessShareLock);
	lsm_entry* entry = lsm_get_entry(index);
	index_close(index, AccessShareLock);

	while (entry->merge_in_progress)
	{
		pg_usleep(1000000); /* one second */
	}
	PG_RETURN_NULL();
}

Datum
lsm_get_top_index_size(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	Relation index = index_open(relid, AccessShareLock);
	lsm_entry* entry = lsm_get_entry(index);
	index_close(index, AccessShareLock);
	PG_RETURN_INT64((uint64)lsm_get_index_size(entry->top_indices[entry->active_index])*BLCKSZ);
}
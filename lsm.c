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

PG_FUNCTION_INFO_V1(lsm_handler);
PG_FUNCTION_INFO_V1(lsm_nbtree_wrapper);

extern void _PG_init(void);
extern void _PG_fini(void);

static HTAB *lsm_map;		 // maps relation Oid to control structure (lsm_entry)
static LWLock *lsm_map_lock; // lock for access to lsm_map
static List *lsm_released_locks;
static List *lsm_entries;

// relation option kind for our index
static relopt_kind lsm_relopt_kind;

// configurations
static int lsm_max_indices;
static int lsm_top_index_size;

// Previous hooks
static ProcessUtility_hook_type prev_process_utility_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
// Only for vers >= 15
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static ExecutorFinish_hook_type prev_executor_finish_hook = NULL;

// background daemon termination flag
static volatile bool lsm_terminate;

// Our custom shared memory request function (hook) to reserve space for control data of all the indices
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
	entry->top_indices[0] = entry->top_indices[1] = InvalidOid;
	// rd_index is a tuple describing this index and we use it to fetch Oid of index
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
		// set Oids of top indices, named as relname_top{n}
		for (int i = 0; i < 2; i++)
		{
			// psprintf similar to sprintf but returns buffer with string
			char *top_idx_name = psprintf("%s_top%d", relname, i);
			// gets the relation Oid from its name and namespace
			entry->top[i] = get_relname_relid(top_idx_name, RelationGetNamespace(index));
			// couldn't fetch
			if (entry->top[i] == InvalidOid)
				elog(ERROR, "LSM: failed to lookup index %s", top_idx_name);
		}
		// active index is one with more entries from the two
		entry->active_index = lsm_get_index_size(entry->top[0]) >= lsm_get_index_size(entry->top[1]) ? 0 : 1;
	}
	LWLockRelease(lsm_map_lock);
	return entry;
}

// Truncate top index to 0 tuples
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
	*/
	index_build(heap, index, indexInfo, true, false);
	index_close(index, AccessExclusiveLock);
	table_close(heap, AccessShareLock);
}

// Dunno about this
#if PG_VERSION_NUM >= 140000
#define INSERT_FLAGS UNIQUE_CHECK_NO, false
#else
#define INSERT_FLAGS false
#endif

// constructing
static bytea *lsm_get_options(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"fillfactor", RELOPT_TYPE_INT, offsetof(BTOptions, fillfactor)},
		{"vacuum_cleanup", RELOPT_TYPE_REAL, offsetof(BTOptions, vacuum_cleanup_index_scale_factor)},
		{"deduplicate_items", RELOPT_TYPE_BOOL, offsetof(BTOptions, deduplicate_items)},
		{"top_index_size", RELOPT_TYPE_INT, offsetof(lsm_options, top_index_size)},
		{"unique", RELOPT_TYPE_BOOL, offsetof(lsm_options, unique)}};
	// build_reloptions parses above and returns a struct with those parsed options
	return (bytea *)build_reloptions(reloptions, validate, lsm_relopt_kind, sizeof(lsm_options), tab, lengthof(tab));
}

// builds index tuple comparator context
static SortSupport lsm_build_sortkeys(Relation index)
{
	int key_size = IndexRelationGetNumberOfKeyAttributes(index);
	SortSupport sortKeys = (SortSupport)palloc0(key_size * sizeof(SortSupportData));
	/*
		Builds an insert scan key.
		A scan key is the internal representation of a WHERE clause of the form index_key operator constant,
		where the index key is one of the columns of the index and the operator is one of the members of the
		operator family associated with that index column.
	*/
	BTScanInsert insert_key = _bt_mkscankey(index, NULL);
	Oid save_am = index->rd_rel->relam;
	index->rd_rel->relam = BTREE_AM_OID;

	for (int i = 0; i < key_size; i++)
	{
		SortSupport sortKey = &sortKeys[i];
		ScanKey scanKey = &insert_key->scankeys[i];
		int16 strategy;

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = scanKey->sk_collation;
		sortKey->ssup_nulls_first = (scanKey->sk_flag & SK_BT_NULLS_FIRST) != 0;
		sortKey->ssup_attno = scanKey->sk_attno;
		// no abbreviation support
		sortKey->abbreviate = false;
		Assert(sortKey->ssup_attno != 0);

		strategy = (scanKey->sk_flags & SK_BT_DESC) != 0 ? BTGreaterStrategyNumber : BTLessStrategyNumber;

		// this needs am to B-tree hence we set it above
		PrepareSortSupportFromIndexRel(index, strategy, sortKey);
	}
	index->rd_rel->relam = save_am;
	return sortKeys;
}

static int lsm_compare_index_tuples(IndexScanDesc scan1, IndexScanDesc scan2, SortSupport sortKeys)
{
	int n_keys = IndexRelationGetNumberOfKeyAttributes(scan1->indexRelation);

	for (int i=1;i<=n_keys;i++){
		Datum datum[2];
		bool isNumm[2];
		int result;

		datum[0] = index_getattr(scan1->xs_itup, i, scan1->xs_itupdesc, &isNull[0]);
		datum[1] = index_getattr(scan2->xs_itup, i, scan2->xs_itupdesc, &isNull[1]);
		// sortKey is basically comparator
		result = ApplySortComparator(datum[0], isNull[0], datum[1], isNull[1], &sortKeys[i-1]);

		if (result)
			return result;
	}
	return ItemPointerCompare(&scan1->xs_heaptid, &scan2->xs_heaptid);
}

static void static IndexBuildResult *
lsm_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	index->rd_rel->relam = BTREE_AM_OID;
	return btbuild(heap, index, indexInfo);
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
	amroutine->amcanunique = true;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = true;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = true;
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = lsm_build;
	amroutine->ambuildempty = btbuildempty;
	amroutine->aminsert = btinsert;
	amroutine->ambulkdelete = btbulkdelete;
	amroutine->amvacuumcleanup = btvacuumcleanup;
	amroutine->amcanreturn = btcanreturn;
	amroutine->amcostestimate = btcostestimate;
	amroutine->amoptions = btoptions;
	amroutine->amproperty = btproperty;
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = btvalidate;
	amroutine->amadjustmembers = btadjustmembers;
	amroutine->ambeginscan = btbeginscan;
	amroutine->amrescan = btrescan;
	amroutine->amgettuple = btgettuple;
	amroutine->amgetbitmap = btgetbitmap;
	amroutine->amendscan = btendscan;
	amroutine->ammarkpos = btmarkpos;
	amroutine->amrestrpos = btrestrpos;
	amroutine->amestimateparallelscan = btestimateparallelscan;
	amroutine->aminitparallelscan = btinitparallelscan;
	amroutine->amparallelrescan = btparallelrescan;

	PG_RETURN_POINTER(amroutine);
}

void _PG_init(void)
{
}
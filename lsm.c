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
static bool lsm_inside_copy;

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
	entry->n_merges = 0;
	entry->n_inserts = 0;
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

// compare index tuples
static int lsm_compare_index_tuples(IndexScanDesc scan1, IndexScanDesc scan2, SortSupport sortKeys)
{
	int n_keys = IndexRelationGetNumberOfKeyAttributes(scan1->indexRelation);

	for (int i = 1; i <= n_keys; i++)
	{
		Datum datum[2];
		bool isNumm[2];
		int result;

		datum[0] = index_getattr(scan1->xs_itup, i, scan1->xs_itupdesc, &isNull[0]);
		datum[1] = index_getattr(scan2->xs_itup, i, scan2->xs_itupdesc, &isNull[1]);
		// sortKey is basically comparator
		result = ApplySortComparator(datum[0], isNull[0], datum[1], isNull[1], &sortKeys[i - 1]);

		if (result)
			return result;
	}
	return ItemPointerCompare(&scan1->xs_heaptid, &scan2->xs_heaptid);
}

/*
	Access Methods
*/

static IndexBuildResult *
lsm_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	bool found;
	lsm_entry *entry;
	LWLockAcquire(lsm_map_lock, LW_EXCLUSIVE);
	elog(LOG, "lsm build %s", index->rd_rel->relname.data);
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
	if (lsm_released_locks){
		ListCell* cell;
		foreach(cell, lsm_released_locks){
			Oid index_oid = lfirst_oid(cell);
			LockRelationOid(index_oid, RowExclusiveLock);
		}
		list_free(lsm_released_locks);
		lsm_released_locks = NULL;
	}
}

/*
	Insert in active top index, on overflow swap active indices and start merge
*/
static bool lsm_insert(
	Relation rel, Datum *values, bool *isNull, ItemPointer ht_ctid,
	Relation heapRel, IndexUniqueCheck checkUnique,
#if PG_VERSION_NUM >= 140000
	bool indexUnchanged,
#endif
	IndexInfo *indexInfo)
{
	lsm_entry* entry = lsm_get_entry(index);
	int active_index;
	uint64 n_merges;
	Relation index;
	Oid save_am;
	bool overflow;
	int top_index_size = entry->top_index_size ? entry->top_index_size : lsm_top_index_size;
	bool is_initialized = true;

	// Get current active index
	SpinLockAcquire(&entry->spinlock);
	active_index = entry->active_index;
	if (entry->top_indices[active_index])
		entry->access_count[active_index]++;
	else
		is_initialized = false;
	n_merges = entry->n_merges;
	SpinLockRelease(&entry->spinlock);

	if (!is_initialized){
		bool response;
		save_am = rel->rd_rel->relam;
		rel->rd_rel->relam = BTREE_AM_OID;
		response = btinsert(rel, values, isNull, ht_ctid, heapRel, checkUnique,
#if PG_VERSION_NUM >= 14000
						indexUnchanged,
#endif
						indexInfo);
		index->rd_rel->relam = save_am;
		return response;
	}

	// insert in top index
	index = index_open(entry->top[active_index], RowExclusiveLock);
	index->rd_rel->relam = BTREE_AM_OID;
	save_am = index->rd_rel->relam;
	btinsert(index, values, isNull, ht_ctid, heapRel, checkUnique,
#if PG_VERSION_NUM>=140000
			 indexUnchanged,
#endif
			 indexInfo);

	index_close(index, RowExclusiveLock);
	index->rd_rel->relam = save_am;

	overflow = !entry->merge_in_progress /* don't check for overflow if merge already initiated */	
			&& (entry->n_inserts % LSM_CHK_TOP_IDX_SIZE_INTERVAL == 0)	/* periodic check */
			&& (RelationGetNumberOfBlocks(index) * (BLCKSZ/1024) > top_index_size);

	SpinLockAcquire(&entry->spinlock);
	// start merge if not already in progress
	if (overflow && !entry->merge_in_progress && entry->n_merges == n_merges){
		Assert(entry->active_index = active_index);
		entry->merge_in_progress = true;
		entry->active_index ^= 1;	/* swap active indices */
		entry->n_merges++;
	}

	Assert(entry->access_count[active_index] > 0);
	entry->access_count[active_index] -= 1;
	entry->n_inserts++;
	if (entry->merge_in_progress){
		LOCKTAG tag;
		SET_LOCKTAG_RELATION(tag, MyDatabaseId, entry->top[1-active_index]);
		// Holding lock on non-active relation prevents bgworker from truncating this index
		if (LockHeldByMe(&tag, RowExclusiveLock)){
			/* Copy locks all indexes and hold this locks until end of copy.
			 * We can not just release lock, because otherwise CopyFrom produces
			 * "you don't own a lock of type" warning.
			 * So just try to periodically release this lock and let merger grab it.
			 */
			/* release lock periodically */
			if (!lsm_inside_copy || (entry->n_inserts % LSM_CHK_TOP_IDX_SIZE_INTERVAL  == 0)){
				LockRelease(&tag, RowExclusiveLock, false);
				lsm_released_locks = lappend_oid(lsm_released_locks, entry->top[1-active_index]);
			}
		}

		/* if all inserts in previous active index are done we can start merge */
		if (entry->active_index != active_index && entry->access_count[active_index] == 0){
			entry->start_merge = true;
			if (entry->compactor == NULL){

			}
			// SetLatch(&entry->compactor->procLatch);
		}
	}
	SpinLockRelease(&entry->spinlock);

	/* We have to require released locks because othervise CopyFrom will produce warning */
	if (lsm_inside_copy && lsm_released_locks){
		pg_usleep(1);
		lsm_reacquire_locks();
	}

	return false;
}

static IndexScanDesc lsm_beginscan(Relation rel, int nkeys, int norderbys){
	IndexScanDesc scan;
	lsm_scan_desc* lsd;
	int i;

	/* no order by operators */
	Assert(norderbys == 0);

	scan = RelationGetIndexScan(rel, nkeys, norderbys);
	scan->xs_itupdesc = RelationGetDescr(rel);
	lsd = (lsm_scan_desc*)palloc(sizeof(lsm_scan_desc));
	lsd->entry = lsm_get_entry(rel);
	lsd->sortKeys = lsm_build_sortkeys(rel);

	for (int i=0;i<2;i++){
		if (lsd->entry->top[i]){
			lsd->top_indices[i] = index_open(lsd->entry->top_indices[i], AccessShareLock);
			lsd->scan[i] = btbeginscan(lsd->top_indices[i], nkeys, norderbys);
		} else {
			lsd->top_indices[i] = NULL;
			lsd->scan[i] = NULL;
		}
	}

	lsd->scan[2] = btbeginscan(rel, nkeys, norderbys);
	for (int i=0;i<3;i++){
		if (lsd->scan[i]){
			lsd->eoi[i] = false;
			lsd->scan[i]->xs_want_itup = true;
			lsd->scan[i]->parallel_scan = NULL;
		}
	}
	lsd->unique = rel->rd_options ? ((lsm_options*)rel->rd_options)->unique : false;
	lsd->curr_index = -1;
	scan->opaque = lsd;

	return scan;
}

static void lsm_rescan(IndexScanDesc scan, ScanKey scanKey, int nscankeys
					, ScanKey orderbys, int norderbys)
{
	lsm_scan_desc* lsd = (lsm_scan_desc*) scan->opaque;
	lsd->curr_index = -1;
	for (int i=0;i<3;i++){
		if (lsd->scan[i]){
			btreescan(lsd->scan[i], scankey, nscankeys, orderbys, norderbys);
			lsd->eoi[i] = false;
		}
	}
}

static void lsm_endscan(IndexScanDesc scan)
{
	lsm_scan_desc* lsd = (lsm_scan_desc*)scan->opaque;

	for (int i=0;i<3;i++){
		if (lsd->scan[i]){
			btendscan(lsd->scan[i]);
			if (i < 2)
				index_close(lsd->top_indices[i], AccessShareLock);
		}
	}
	pfree(lsd);
}

static bool lsm_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	lsm_scan_desc* lsd = (lsm_scan_desc*)scan->opaque;
	int min = -1;
	int curr = lsd->curr_index;
	/* start with active index then merging then base */
	int try_index_order[3] = {lsd->entry->active_index, 1-lsd->entry->active_index, 2};

	/* btree indices are not lossy */
	scan->xs_recheck = false;

	if (curr >= 0) 
		lsd->eoi[curr] = !_bt_next(lsd->scan[curr], dir); /* move forward current index */

	for (int j=0;j<3;j++){
		int i = try_index_order[j];
		BTScanOpaque bto = (BTScanOpaque)lsd->scan[i]->opaque;
		lsd->scan[i]->xs_snapshot = scan->xs_snapshot;
		if (!lsd->eoi[i] && !BTScanPosIsValid(bto->currPos)){
			lsd->eoi[i] = !_bt_first(lsd->scan[i], dir);
			if (!lsd->eoi[i] && lsd->unique && scan->numberOfKeys == scan->indexRelation->rd_index->indnkeyatts)
			{
				/* If index is marked as unique and we perform lookup using all index keys,
				 * then we can stop after locating first occurrence.
				 * If make it possible to avoid lookups of all three indexes.
				 */
				elog(DEBUG1, "Lsm3: lookup %d indices", j+1);
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
			if (min < 0){
				min = i;
			} else {
				int result = lsm_compare_index_tuples(lsm->scan[i], lsm->scan[min], lsm->sortKeys);
				if (result == 0)
				{
					/* Duplicate: it can happen during merge when same tid is both in top and base index */
					lsm->eoi[i] = !_bt_next(lsm->scan[i], dir); /* just skip one of entries */
				}
				else if ((result < 0) == ScanDirectionIsForward(dir))
				{
					min = i;
				}
			}
		}
	}
	if (min < 0)	/* all indices traversed but didn't find match */
		return false;
	
	scan->xs_heaptid = lsd->scan[min]->xs_heaptid;	/* copy TID */
	if (scan->xs_want_itup)
		scan->xs_itup = lsd->scan[min]->xs_itup;
	lsd->curr_index = min;	
	return true;
}

static int64 lsm_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	lsm_scan_desc* lsd = (lsm_scan_desc*)scan->opaque;
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
	amroutine->amoptions = lsm_options;
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
	Page		metapage;

	/* Construct metapage. */
	metapage = (Page) palloc(BLCKSZ);
	_bt_initmetapage(metapage, BTREE_METAPAGE, 0, _bt_allequalimage(index, false));

#if PG_VERSION_NUM>=150000
	RelationGetSmgr(index);
#else
	RelationOpenSmgr(index);
#endif

	/*
	 * Write the page and log it.  It might seem that an immediate sync would
	 * be sufficient to guarantee that the file exists on disk, but recovery
	 * itself might remove it while replaying, for example, an
	 * XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE record.  Therefore, we need
	 * this even when wal_level=minimal.
	 */
	PageSetChecksumInplace(metapage, BTREE_METAPAGE);
	smgrextend(index->rd_smgr, MAIN_FORKNUM, BTREE_METAPAGE,
			   (char *) metapage, true);
#if PG_VERSION_NUM>=160000
	log_newpage(&index->rd_smgr->smgr_rlocator.locator, MAIN_FORKNUM,
				BTREE_METAPAGE, metapage, true);
#else
	log_newpage(&index->rd_smgr->smgr_rnode.node, MAIN_FORKNUM,
				BTREE_METAPAGE, metapage, true);
#endif
	/*
	 * An immediate sync is required even if we xlog'd the page, because the
	 * write did not go through shared_buffers and therefore a concurrent
	 * checkpoint may have moved the redo pointer past our xlog record.
	 */
	smgrimmedsync(index->rd_smgr, MAIN_FORKNUM);
	RelationCloseSmgr(index);

	return (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));
}

static bool
lsm_dummy_insert(Relation rel, Datum *values, bool *isnull,
				  ItemPointer ht_ctid, Relation heapRel,
				  IndexUniqueCheck checkUnique,
#if PG_VERSION_NUM>=140000
				  bool indexUnchanged,
#endif
				  IndexInfo *indexInfo)
{
	return false;
}

Datum
lsm_nbtree_wrapper(PG_FUNCTION_ARGS)
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
	amroutine->amoptions = lsm_options;
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

/* Utility hook handling creation of lsm indices */
static void lsm_process_utitlity(PlannedStmt *plannedStmt,
								const char *queryString,
#if PG_VERSION_NUM>=150000
					 bool readOnlyTree,
#endif
					 ProcessUtilityContext context,
					 ParamListInfo paramListInfo,
					 QueryEnvironment *queryEnvironment,
					 DestReceiver *destReceiver,
#if PG_VERSION_NUM>=130000
					 QueryCompletion *completionTag
#else
	                 char *completionTag
#endif
){

}

void _PG_init(void)
{
}
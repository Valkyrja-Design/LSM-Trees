#define LSM_CHK_TOP_IDX_SIZE_INTERVAL 1 << 15   // interval after which to check the size of top index

/*
    Control struct for LSM indices for each relation
*/
typedef struct
{
    Oid base;                        // Oid of base index
    Oid heap;                        // Oid of indexed relation
    Oid top_indices[2];              // Oid of 2 top indices
    int access_count[2];             // Access counter
    int active_index;                // Current active index from the two top indices
    uint64 n_merges;                 // #merges since database start
    uint64 n_inserts;                // #inserts since database start
    volatile bool start_merge;       // start compaction of top index with base index
    volatile bool merge_in_progress; // merge is in progress?
    PGPROC *compactor;               // compactor daemon
    Oid db_id;                       // database ID
    Oid user_id;                     // user ID
    Oid am_id;                       // lsm access method Oid
    int top_index_size;              // size of top index
    slock_t spinlock;                // Spinlock to synchronize access
} lsm_entry;

typedef struct
{
    lsm_entry *entry;        // lsm control struct
    Relation top_indices[2]; // Top 2 index relations
    SortSupport sortKeys;    // Context for comparing index tuples
    IndexScanDesc scan[3];   // Scan descriptors for the 3 indices
    bool eoi[3];             // true if end of index was reached           
    int curr_index;          // index from which last tuple was selected
	bool           unique;     /* Whether index is "unique" and we can stop scan after locating first occurrence */
} lsm_scan_desc;

typedef struct
{
    BTOptions nbtree_opts; // Standard B-tree options
    int top_index_size;    // Max size of top index
    bool        unique;			/* Index may not contain duplicates. We prohibit unique constraint for Lsm index
                                 * because it can not be enforced. But presence of this index option allows to optimize
								 * index lookup: if key is found in active top index, do not search other two indexes.
                                 */

} lsm_options;
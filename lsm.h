#define LSM_CHK_TOP_IDX_SIZE_INTERVAL 1 << 15   // interval after which to check the size of top index

/*
    Control struct for LSM indexes for each relation
*/
typedef struct
{
    Oid base;                        // Oid of base index
    Oid heap;                        // Oid of indexed relation
    Oid top_indices[2];              // Oid of 2 top indices
    int active_index;                // Current active index from the two top indices
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
    bool unique;             
    int curr_index;          // index from which last tuple was selected
} lsm_scan_desc;

typedef struct
{
    BTOptions nbtree_opts; // Standard B-tree options
    int top_index_size;    // Max size of top index
    bool unique;
} lsm_options;
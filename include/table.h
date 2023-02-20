// Copyright 2014 Carnegie Mellon University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "common.h"
#include "alloc_malloc.h"
#include "alloc_dynamic.h"
#include "mica_partition.h"
#include "linux/list.h"

MEHCACHED_BEGIN

#define MEHCACHED_MAX_KEY_LENGTH (255)
#define MEHCACHED_MAX_VALUE_LENGTH (1048575)


#ifndef MEHCACHED_NO_EVICTION
// #define MEHCACHED_ITEMS_PER_BUCKET (7)
#define MEHCACHED_ITEMS_PER_BUCKET (150)
#else
#define MEHCACHED_ITEMS_PER_BUCKET (7)
// #define MEHCACHED_ITEMS_PER_BUCKET (15)
#endif

// do move-to-head if when (item's distance from tail) >= (pool size) * mth_threshold
// 0.0: full LRU; 1.0: full FIFO
#define MEHCACHED_MTH_THRESHOLD_FIFO (1.0)
#define MEHCACHED_MTH_THRESHOLD_LRU (0.0)

#define MEHCACHED_SINGLE_ALLOC

#ifdef MEHCACHED_COLLECT_STATS
#define MEHCACHED_STAT_INC(table, name) do { __sync_add_and_fetch(&(table)->stats.name, 1); } while (0)
#define MEHCACHED_STAT_DEC(table, name) do { __sync_sub_and_fetch(&(table)->stats.name, 1); } while (0)
#else
#define MEHCACHED_STAT_INC(table, name) do { (void)table; } while (0)
#define MEHCACHED_STAT_DEC(table, name) do { (void)table; } while (0)
#endif

typedef enum _MEHCACHED_RESULT
{
    MEHCACHED_OK = 0,
    MEHCACHED_ERROR,
    MEHCACHED_FULL,
    MEHCACHED_EXIST,
    MEHCACHED_NOT_FOUND,
    MEHCACHED_PARTIAL_VALUE,
    MEHCACHED_NOT_PROCESSED,
} MEHCACHED_RESULT;

struct mehcached_bucket
{
    uint32_t version;   // XXX: is uint32_t wide enough?
    uint32_t next_extra_bucket_index;   // 1-base; 0 = no extra bucket
    // 一个 bucket 包含 15 个index entry， 并且每个存储 bucket 都有固定数量的 index entry （可在源代码中配置；在我们的原型中为 15 个，正好占用两个缓存行）。
    uint64_t item_vec[MEHCACHED_ITEMS_PER_BUCKET]; 

    // 16: tag (1-base)
    //  8: alloc id
    // 40: item offset
    // item == 0: empty item

    #define MEHCACHED_TAG_MASK (((uint64_t)1 << 16) - 1)
    #define MEHCACHED_TAG(item_vec) ((item_vec) >> 48)

#ifndef MEHCACHED_SINGLE_ALLOC
    #define MEHCACHED_ALLOC_ID_MASK (((uint64_t)1 << 8) - 1)
    #define MEHCACHED_ALLOC_ID(item_vec) (((item_vec) >> 40) & MEHCACHED_ALLOC_ID_MASK)
#else
    #define MEHCACHED_ALLOC_ID(item_vec) (0LU)
#endif

#ifndef MEHCACHED_SINGLE_ALLOC
    #define MEHCACHED_ITEM_OFFSET_MASK (((uint64_t)1 << 40) - 1)
#else
    #define MEHCACHED_ITEM_OFFSET_MASK (((uint64_t)1 << 48) - 1)
#endif
    #define MEHCACHED_ITEM_OFFSET(item_vec) ((item_vec) & MEHCACHED_ITEM_OFFSET_MASK)

#ifndef MEHCACHED_SINGLE_ALLOC
    #define MEHCACHED_ITEM_VEC(tag, alloc_id, item_offset) (((uint64_t)(tag) << 48) | ((uint64_t)(alloc_id) << 40) | (uint64_t)(item_offset))
#else
    #define MEHCACHED_ITEM_VEC(tag, alloc_id, item_offset) (((uint64_t)(tag) << 48) | (uint64_t)(item_offset))
#endif
};

struct mehcached_item
{
    struct mehcached_alloc_item alloc_item;

    uint32_t kv_length_vec; // key_length: 8, value_length: 24; kv_length_vec == 0: empty item

    #define MEHCACHED_KEY_MASK (((uint32_t)1 << 8) - 1)
    #define MEHCACHED_KEY_LENGTH(kv_length_vec) ((kv_length_vec) >> 24)

    #define MEHCACHED_VALUE_MASK (((uint32_t)1 << 24) - 1)
    #define MEHCACHED_VALUE_LENGTH(kv_length_vec) ((kv_length_vec) & MEHCACHED_VALUE_MASK)

    #define MEHCACHED_KV_LENGTH_VEC(key_length, value_length) (((uint32_t)(key_length) << 24) | (uint32_t)(value_length))

    // the rest is meaningful only when kv_length_vec != 0
    /* WGT begin */
    // #define MICA_MAX_NODE_NUMS 10
    // size_t mapping_id[MICA_MAX_NODE_NUMS];
    // uintptr_t value_addr[MICA_MAX_NODE_NUMS];
    size_t mapping_id;
    uintptr_t remote_value_addr;
    /* WGT end */

    uint32_t expire_time;
    uint64_t key_hash;
    uint8_t data[0];
};

#define MEHCACHED_MAX_POOLS (16)

struct mehcached_table
{
#ifdef MEHCACHED_ALLOC_POOL
    struct mehcached_pool alloc[MEHCACHED_MAX_POOLS];
    uint8_t alloc_id_mask;
    uint64_t mth_threshold;
#endif
#ifdef MEHCACHED_ALLOC_MALLOC
    struct mehcached_malloc alloc;
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
    struct mehcached_dynamic alloc;
#endif

    struct mehcached_bucket *buckets;
    struct mehcached_bucket *extra_buckets; // = (buckets + num_buckets); extra_buckets[0] is not used because index 0 indicates "no more extra bucket"

    uint8_t concurrent_access_mode;

    uint32_t num_buckets;
    uint32_t num_buckets_mask;
    uint32_t num_extra_buckets;
    size_t mapping_id;

    struct
    {
        uint32_t lock;
        uint32_t head;   // 1-base; 0 = no extra bucket
    } extra_bucket_free_list MEHCACHED_ALIGNED(64);

    uint8_t rshift;

#ifdef MEHCACHED_COLLECT_STATS
    struct
    {
        size_t count;
        size_t set_nooverwrite;
        size_t set_new;
        size_t set_inplace;
        size_t set_evicted;
        size_t get_found;
        size_t get_notfound;
        size_t test_found;
        size_t test_notfound;
        size_t delete_found;
        size_t delete_notfound;
        size_t cleanup;
        size_t move_to_head_performed;
        size_t move_to_head_skipped;
        size_t move_to_head_failed;
    } stats;
#endif
} MEHCACHED_ALIGNED(64);

struct mehcached_prefetch_state
{
    struct mehcached_table *table;
    struct mehcached_bucket *bucket;
    uint64_t key_hash;
};

typedef enum _MEHCACHED_OPERATION
{
    MEHCACHED_NOOP_READ = 0,
    MEHCACHED_NOOP_WRITE,
    MEHCACHED_ADD,
    MEHCACHED_SET,
    MEHCACHED_GET,
    MEHCACHED_TEST,
    MEHCACHED_DELETE,
    MEHCACHED_INCREMENT,
} MEHCACHED_OPERATION;

// wgt: 增加get返回的各种状态
typedef enum MICA_GET_STATUS
{
    MICA_GET_SUCESS = 0,
    MICA_NO_KEY,
    MICA_VERSION_IS_DIRTY,
    MICA_GET_PARTIAL
} MICA_GET_STATUS;

struct mehcached_request
{
    // 0
    uint8_t operation;  // of enum MEHCACHED_OPERATION type
    uint8_t result;     // of enum MEHCACHED_RESULT type
    // 2
    uint16_t reserved0;
    // 4
    uint32_t kv_length_vec;
    // 8
    uint64_t key_hash;
    // 16
    uint32_t expire_time;
    // 20
    uint32_t reserved1;
    // 24
};

extern struct mehcached_table table_o;;

static
void
mehcached_print_bucket(const struct mehcached_bucket *bucket);

static
void
mehcached_print_buckets(const struct mehcached_table *table);

static
void
mehcached_print_stats(const struct mehcached_table *table);

static
void
mehcached_reset_table_stats(struct mehcached_table *table);

static
uint32_t
mehcached_calc_bucket_index(const struct mehcached_table *table, uint64_t key_hash);

static
uint16_t
mehcached_calc_tag(uint64_t key_hash);

static
void
mehcached_set_item(struct mehcached_item *item, uint64_t key_hash, const uint8_t *key, uint32_t key_length, const uint8_t *value, uint32_t value_length, uint32_t expire_time, struct mehcached_item *main_item);

static
void
mehcached_set_item_value(struct mehcached_item *item, const uint8_t *value, uint32_t value_length, uint32_t expire_time, struct mehcached_item *main_item);

static
bool
mehcached_compare_keys(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len);

static bool
mehcached_compare_keys_warpper(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len);

static
void
mehcached_cleanup_all(uint8_t current_alloc_id, struct mehcached_table *table);

static
void
mehcached_prefetch_table(struct mehcached_table *table, uint64_t key_hash, struct mehcached_prefetch_state *out_prefetch_state);

static
void
mehcached_prefetch_alloc(struct mehcached_prefetch_state *in_out_prefetch_state);

bool
mehcached_get(uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length, uint8_t *out_value, size_t *in_out_value_length, uint32_t *out_expire_time, bool readonly, bool get_true_value, MICA_GET_STATUS *get_status);

bool
mid_mehcached_get_warpper(uint8_t current_alloc_id MEHCACHED_UNUSED, struct mehcached_table *table, uint64_t key_hash,
                            const uint8_t *key, size_t key_length, uint8_t *out_value, size_t *in_out_value_length,
                            uint32_t *out_expire_time, bool readonly MEHCACHED_UNUSED, bool get_true_value, MICA_GET_STATUS *get_status);
                            
static
bool
mehcached_test(uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length);

struct mehcached_item *
mehcached_set(uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash,
 const uint8_t *key, size_t key_length, const uint8_t *value, 
 size_t value_length, uint32_t expire_time, bool overwrite,
 bool* is_update, bool *is_maintable, struct mehcached_item * main_item);

struct mehcached_item *
midd_mehcached_set_warpper(uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash,\
                const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length,\
                uint32_t expire_time, bool overwrite, bool *is_update, bool *is_maintable, struct mehcached_item * main_item);
static
bool
mehcached_delete(uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length);

static
bool
mehcached_increment(uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length, uint64_t increment, uint64_t *out_new_value, uint32_t expire_time);

static
void
mehcached_process_batch(uint8_t current_alloc_id, struct mehcached_table *table, struct mehcached_request *requests, size_t num_requests, const uint8_t *in_data, uint8_t *out_data, size_t *out_data_length, bool readonly);

static
void
mehcached_table_reset(struct mehcached_table *table);

void
mehcached_table_init(struct mehcached_table *table, size_t num_buckets, size_t num_pools, size_t pool_size, bool concurrent_table_read, bool concurrent_table_write, bool concurrent_alloc_write, size_t table_numa_node, size_t alloc_numa_nodes[], double mth_threshold);

static
void
mehcached_table_free(struct mehcached_table *table);

bool 
get_table_init_state();

void 
set_table_init_state(bool val);

// WGT ADD

struct midd_value_header
{
	uint64_t version;
	uint64_t value_count;
	uint8_t data[0];
};

struct midd_value_tail
{
    uint64_t version;
    volatile bool dirty;     // 我们将脏标志位放在value末尾，最后被更新
};

struct midd_key_tail
{
	uint64_t version;
	uint64_t key_count;
	uint8_t data[0];
};


#define VALUE_HEADER_LEN    (sizeof(struct midd_value_header))
#define VALUE_TAIL_LEN      (sizeof(struct midd_value_tail))
#define KEY_TAIL_LEN      (sizeof(struct midd_key_tail))
#define VALUE_TAIL_ADDR(base, key_length, true_value_length) (base + key_length + VALUE_HEADER_LEN + true_value_length)
#define VALUE_START_ADDR(base, key_length) (base + key_length + VALUE_HEADER_LEN)
#define WARPPER_KEY_LEN(true_key_len) (true_key_len + KEY_TAIL_LEN)
#define WARPPER_VALUE_LEN(true_value_len) (VALUE_HEADER_LEN + true_value_len + VALUE_TAIL_LEN)
struct mehcached_item * 
find_item(struct mehcached_table *table, uint64_t key_hash, const uint8_t* key, size_t key_length);

size_t mehcached_find_item_index_warpper(const struct mehcached_table *table, struct mehcached_bucket *bucket, uint64_t key_hash, uint16_t tag, const uint8_t *key, size_t key_length, struct mehcached_bucket **located_bucket);

extern struct list_head nic_send_list[PARTITION_MAX_NUMS];

uint8_t * item_get_key_addr(struct mehcached_item *item);
uint8_t * item_get_value_addr(struct mehcached_item *item);
void HexDump(const char *buf,int len,int addr);

uint64_t get_offset_by_item(struct mehcached_table *table, struct mehcached_item * item);
struct mehcached_item * get_item_by_offset(struct mehcached_table *table, uint64_t item_offset);
void value_get_true_value(uint8_t*true_value, uint8_t* value, size_t value_length);

#define GET_TRUE_VALUE_ADDR(value) ( (uint8_t*)value +  VALUE_HEADER_LEN)
#define GET_TRUE_VALUE_LEN(value_length) (value_length - VALUE_HEADER_LEN - VALUE_TAIL_LEN)
#define GET_TRUE_KEY_LEN(key_length) (key_length - KEY_TAIL_LEN)

extern struct mehcached_table *main_table;
extern struct mehcached_table *log_table;

MEHCACHED_END

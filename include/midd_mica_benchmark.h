/*
 * @Author: your name
 * @Date: 2022-03-09 20:03:05
 * @LastEditTime: 2022-03-09 20:04:28
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: /LineKV/include/midd_mica_benchmark.h
 */


#define ACCESS_NUM __access_num

extern int current_credict;

extern int __access_num;
// extern int rand_num[TEST_KV_NUM];
extern int read_num, update_num;
extern enum WORK_LOAD_DISTRIBUTED workload_type;
extern uint64_t set_counts, get_counts;
extern int partition_set_count[PARTITION_MAX_NUMS];

extern int get_is_more;
extern int main_node_is_readable;
extern struct test_kv kvs_group[TEST_KV_NUM];
extern int op_gaps[4];
extern int little_idx;
extern int end_round;
extern bool is_all_set_all_get;
extern int avg_partition_count_num;
extern size_t SERVER_ID;

extern int * read_num_penalty;
extern double penalty_rw_rate;
void pick_zipfian(double pf[], int rand_num[], int max_num);
void pick_uniform(double pf[], int rand_num[], int max_num);

enum WORK_LOAD_DISTRIBUTED
{
    UNIFORM,
    ZIPFIAN
};

struct test_kv * generate_test_data(size_t key_offset, size_t val_offset, size_t value_length, size_t kv_nums);
struct test_kv * generate_test_data_YCSB(size_t key_offset, size_t val_offset, size_t value_length, size_t kv_nums);
bool  cmp_item_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value);
void dump_value_by_addr(const uint8_t * value, size_t value_length);
bool cmp_item_all_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value);


#define TYPE_BOX int8_t

struct BOX{
	TYPE_BOX* array;
	int needRTT;
};

// void DO_READ();
struct BOX* intial_box(int current_node_number, int total_node_number, int kind);
void update_box(TYPE_BOX value, struct BOX * box);
//int finish_read(TYPE_BOX * value_array, int array_length, struct BOX* box);
void print_box(struct BOX* box);

extern struct dhmp_msg** set_msgs_group;
extern struct timespec start_through, end_through;
extern long long int total_through_time;
extern bool partition_count_set_done_flag[PARTITION_MAX_NUMS];
extern int partition_set_count[PARTITION_MAX_NUMS];
extern int partition_get_count[PARTITION_MAX_NUMS + 1];
extern int read_dirty_count[PARTITION_MAX_NUMS];

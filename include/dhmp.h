#ifndef DHMP_H
#define DHMP_H

#include "dhmp_mica_shm_common.h"

#define DHMP_CACHE_POLICY
#define DHMP_MR_REUSE_POLICY

#include <x86intrin.h> 

#define DHMP_SERVER_DRAM_TH ((uint64_t)1024*1024*1024*1)

#define DHMP_SERVER_NODE_NUM 100

#define DHMP_DEFAULT_SIZE 256
#define DHMP_DEFAULT_POLL_TIME 800000000

#define DHMP_MAX_OBJ_NUM 40000
#define DHMP_MAX_CLIENT_NUM 100

#define PAGE_SIZE 4096
#define NANOSECOND (1000000000)

#define DHMP_RTT_TIME (6000)
#define DHMP_DRAM_RW_TIME (260)

#define max(a,b) (a>b?a:b)
#define min(a,b) (a>b?b:a)


#define DHMP_MR_REUSE_POLICY
#ifdef DHMP_MR_REUSE_POLICY
#define RDMA_SEND_THREASHOLD 2097152
#endif

#define POST_SEND_BUFFER_SIZE 1024*1024*128



// #define TEST_CPU_BUSY_WORKLOAD
#define MIRROR_NODE_NUM 1
// #define TEST_ON_CHT


#define MAIN_NODE_ID 0
#define REPLICA_NODE_HEAD_ID (1+ MIRROR_NODE_NUM)
 #define REPLICA_NODE_NUMS (server_instance->node_nums - MIRROR_NODE_NUM -1)
 #define REPLICA_NODE_TAIL_ID (REPLICA_NODE_HEAD_ID + REPLICA_NODE_NUMS -1)


#define TEST_KV_NUM 500000


//#define WAIT_RETURN
#define ChainRep
#define ReadFromLeader


enum dhmp_node_class {
	MAIN,
	MIRROR,
	REPLICA,
	HEAD,
	TAIL,

	CLIENT
};
#define request_base 64
#define IS_MAIN(type)       (type & (1 << MAIN) )
#define IS_MIRROR(type)     (type & (1 << MIRROR) )
#define IS_REPLICA(type)    (type & (1 << REPLICA) )
// head 是副本节点中的头节点（因为严格意义上来说主节点是头节点）
#define IS_HEAD(type) 		(type & (1 << HEAD) )
// #define IS_MIDDLE(type) (type & (1 << MIDDLE) )
#define IS_TAIL(type) 		(type & (1 << TAIL) )
#define IS_CLIENT(type) 	(type & (1 << CLIENT) )

#define SET_MAIN(type) 	   ( type = (type | (1 << MAIN)   ) )
#define SET_MIRROR(type)   ( type = (type | (1 << MIRROR) ) )
#define SET_REPLICA(type)  ( type = (type | (1 << REPLICA) ) )
#define SET_HEAD(type)     ( type = (type | (1 << HEAD)   ) )
// #define SET_MIDDLE(type) ( type = (type | (1 << MIDDLE) ) )
#define SET_TAIL(type)     ( type = (type | (1 << TAIL)   ) )
#define SET_CLIENT(type)   ( type = (type | (1 << CLIENT)   ) )


enum dhmp_msg_type{
	
	// DHMP_MSG_SEND_REQUEST,
	// DHMP_MSG_SEND_RESPONSE,
	DHMP_MSG_CLOSE_CONNECTION,

	/* WGT: add new msg type */
	DHMP_MICA_SEND_INFO_REQUEST,
	DHMP_MICA_SEND_INFO_RESPONSE,
};


enum ack_info_type{
	MICA_INIT_ADDR_ACK,
};

enum ack_info_state{
	MICA_ACK_INIT_ADDR_OK,
	MICA_ACK_INIT_ADDR_NOT_OK,
};

enum mica_send_info_type{
	MICA_GET_CLIMR_REQUEST,
	MICA_GET_CLIMR_RESPONSE,
	MICA_ACK_REQUEST,
	MICA_ACK_RESPONSE,
	MICA_SERVER_GET_CLINET_NODE_ID_REQUEST,
	MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE,
	MICA_SET_REQUEST,
	MICA_SET_RESPONSE,

	MICA_GET_REQUEST,
	MICA_GET_RESPONSE,

	MICA_REPLICA_UPDATE_REQUEST,
	MICA_REPLICA_UPDATE_RESPONSE,

	MICA_SET_REQUEST_TEST,
	MICA_SET_RESPONSE_TEST,

	MICA_GET_P2P_MR_REQUEST,
	MICA_GET_P2P_MR_RESPONSE,

	DHMP_MICA_DIRTY_GET_REQUEST,
	DHMP_MICA_DIRTY_GET_RESPONSE,
		
	GET_TOMASTER_REQUEST,
        GET_TOMASTER_RESPONSE,

};

enum middware_state{
	middware_INIT,
	middware_WAIT_MAIN_NODE,
	middware_WAIT_SUB_NODE,
	middware_WAIT_MATE_DATA,

};

enum request_state{
	RQ_INIT_STATE,
	RQ_BUFFER_STATE,
};

enum response_state
{
	RS_INIT_READY,
	RS_INIT_NOREADY,
	RS_BUFFER_READY,
	RS_BUFFER_NOREADY,
};

/*struct dhmp_msg:use for passing control message*/
struct dhmp_msg{
	enum dhmp_msg_type msg_type;
	size_t data_size;		

	void *data;
	struct list_head list_anchor;
	struct dhmp_transport * trans;
	int recv_partition_id;
	int main_thread_set_id;
	int partition_id;
};

/*struct dhmp_addr_info is the addr struct in cluster*/
struct dhmp_addr_info{
	int read_cnt;
	int write_cnt;
	int node_index;
	bool write_flag;
	struct ibv_mr dram_mr;
	struct ibv_mr nvm_mr;
	struct hlist_node addr_entry;
};
struct dhmp_dram_info{
	void *nvm_addr;
	struct ibv_mr dram_mr;
};


struct post_datagram
{
	struct   post_datagram* req_ptr;	    	
	struct   post_datagram* resp_ptr;			
	int      node_id;							
	enum     mica_send_info_type info_type;		
	size_t   info_length;						
	volatile bool  done_flag;					
};
#define HEADER_LEN sizeof(struct post_datagram)
#define DATAGRAM_ALL_LEN(len) ((HEADER_LEN) + len)
#define DATA_ADDR(start_addr, offset) ((char *)start_addr +  HEADER_LEN + offset)


struct dhmp_mica_get_cli_MR_request
{
	struct replica_mappings  *info_revoke_ptr;	
};
struct dhmp_mica_get_cli_MR_response
{
	struct replica_mappings  resp_all_mapping;	 
};
struct dhmp_mica_ack_request
{
	struct ibv_mr leader_mr[4];
	enum ack_info_type ack_type;	
};
struct dhmp_mica_ack_response
{
	enum ack_info_state ack_state;		 	 
};

struct dhmp_get_nodeID_request
{
	int node_id;	
};
struct dhmp_get_nodeID_response
{	
	int resp_node_id;	 	 
};

/*
uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash,\
                const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length,\
                uint32_t expire_time, bool overwrite
*/
struct dhmp_mica_set_request
{
	uint8_t current_alloc_id;
	// struct mehcached_table *table; // don't needed
	struct dhmp_mica_set_request * job_callback_ptr;
	uint64_t	key_hash;
	size_t 		key_length;
	size_t 		value_length;
	// const uint8_t *key;		// don't needed
	// const uint8_t *value;	// don't needed
	uint32_t 	expire_time;
	bool 		overwrite;
	bool		is_update;
	bool 		is_success;					

	// bool		replica_1;
	// bool		replica_2;

	size_t 		out_mapping_id;				
	uintptr_t   out_value_addr;				

	size_t	tag;	

	int partition_id;
	int count_num;
	 


	uint8_t data[0];
};

struct dhmp_mica_set_response
{
	size_t 		out_mapping_id;
	uintptr_t   value_addr;
	uint64_t 	key_hash;
	size_t 		key_length;
	int 		partition_id;
	int 		tag;
	bool 		is_success;
	uint8_t 	key_data[0];
};

struct set_requset_pack
{
	struct post_datagram * req_ptr;
	struct dhmp_mica_set_request * req_info_ptr;
};

struct dhmp_mica_get_request
{
	uint8_t current_alloc_id;
	struct dhmp_mica_get_request * job_callback_ptr;
	uint64_t key_hash;
	size_t   key_length;
	size_t   peer_max_recv_buff_length;
	int   	partition_id;
	size_t	tag;	
	struct dhmp_mica_get_response * get_resp;
	uint8_t data[0];		
};


struct dhmp_mica_recv_trans_data
{
	struct dhmp_transport* rdma_trans;
	struct dhmp_msg* msg;
};

struct dhmp_mica_get_response
{
	size_t 	 out_value_length; 	
	uint32_t out_expire_time;	
	int 	 partition_id;
	MICA_GET_STATUS status;

	/* for aysnc recv_wr */
	struct dhmp_mica_recv_trans_data trans_data;
	void	* msg_buff_addr;	
	uint8_t  out_value[0];		
};


struct dhmp_mica_get_reuse_ptr
{
	void *req_base_ptr;
	struct dhmp_mica_get_response *resp_ptr;
};


struct dhmp_write_request
{
	struct dhmp_transport* rdma_trans;
	struct ibv_mr* mr;
	void* local_addr;
	size_t length;
	uintptr_t remote_addr;
	bool is_imm;
};


struct dhmp_update_request
{
	struct mehcached_item * item;
	uint64_t item_offset;
	int partition_id;
	size_t tag;	
	struct dhmp_write_request write_info;
	struct list_head sending_list;
	uint64_t key_hash;
};


struct dhmp_update_notify_request
{

	int partition_id;
	size_t tag;	
	uint64_t item_offset;
	uint64_t key_hash;
};

struct dhmp_update_notify_response
{
	int partition_id;
	size_t tag;	
	bool is_success;
};

extern pthread_mutex_t buff_init_lock; 
extern int wait_work_counter;
extern int wait_work_expect_counter;

void dump_mr(struct ibv_mr * mr);
int dhmp_rdma_write_packed (struct dhmp_write_request * write_req ,  size_t item_offset);
void mica_replica_update_notify(uint64_t item_offset, int partition_id, int tag , uint64_t key_hash);
extern volatile bool replica_is_ready;


#define S_BASE   1000000000
#define MS_BASE  1000000
#define US_BASE  1000
#define NS_BASE  1

#define TIMEOUT_LIMIT_S 1
#define TIMEOUT_LIMIT_MS 500

#define DEFINE_STACK_TIMER() 	struct timespec start, end;
#define MICA_TIME_COUNTER_INIT() clock_gettime(CLOCK_MONOTONIC, &start);					


#define MICA_TIME_LIMITED(tag, limit)										\
	{															\
		clock_gettime(CLOCK_MONOTONIC, &end);			    	\
		long long mica_total_time_ns = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec)); \
		{														\
			if (mica_total_time_ns / MS_BASE > limit)	\
			{													\
				ERROR_LOG("tag : [%d] TIMEOUT!, exit", tag);	\
				Assert(false);									\
			}													\
		}while(0);												\
	}while(0);

#define MICA_TIME_COUNTER_CAL(msg_str)							\
	{															\
		clock_gettime(CLOCK_MONOTONIC, &end);			    	\
		long long mica_total_time_ns = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec)); \
	}while(0);

/*
if (IS_MAIN(server_instance->server_type))				\
	ERROR_LOG("[%s] exec time is [%lld] ns", msg_str, mica_total_time_ns); 	\
*/


#define MICA_TIME_COUNTER_CAL_PRINTF(msg_str)					\
	{															\
		clock_gettime(CLOCK_MONOTONIC, &end);			    	\
		long long mica_total_time_ns = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec)); \
		printf("[%s] exec time is [%lld] us\n", msg_str, mica_total_time_ns / US_BASE); 	\
	}while(0);


extern struct mehcached_table *log_table;


struct test_kv
{
	uint8_t * key;
	uint8_t * value;
	uint8_t * get_value[PARTITION_MAX_NUMS];
	size_t true_key_length;
	size_t true_value_length;
	uint64_t key_hash;
	struct mehcached_item * item;	// 如果有
};

void dump_value_by_addr(const uint8_t * value, size_t value_length);

#define TABLE_POOL_SIZE 4294967296// (1024*1024*1024*2)//838860800
#define TABLE_BUCKET_NUMS 1024*1024*4
#define INIT_DHMP_CLIENT_BUFF_SIZE 1024*1024*8

bool 
main_node_broadcast_matedata(struct dhmp_mica_set_request  * req_info,
							  struct post_datagram * req_msg,
							  size_t total_length);

void main_node_broadcast_matedata_wait(struct dhmp_mica_set_request  * req_info, 
										int partition_id,
										struct mehcached_item * item);
int init_mulit_server_work_thread();


typedef struct distrubute_job_thread_init_data
{
    enum dhmp_msg_type thread_type;
    int partition_id;
}thread_init_data;


extern int thread_num;
extern int __test_size;
extern int __write_avg_num;
#define MAX_CQ_NUMS 100

void* busy_wait_cq_handler(void* data);

void init_busy_wait_rdma_buff(struct p2p_mappings * busy_wait_rdma_p2p[PARTITION_MAX_NUMS]);


struct dhmp_mica_get_p2p_MR_info_RQ
{
	struct ibv_mr p2p_mr;
	void *p2p_addr;
};


#define MAIN_LOG_DEBUG_THROUGHOUT 
// #define START_COUNT 1000
// #define END_COUNT 4000

void dhmp_send_request_handler(struct dhmp_transport* rdma_trans,
									struct dhmp_msg* msg, 
									bool * is_async,
									 __time_t time_start1, 
									 __syscall_slong_t time_start2,
									 bool is_cq_thread);

void distribute_partition_resp(int partition_id, struct dhmp_transport* rdma_trans, struct dhmp_msg* msg,  __time_t time_start1, __syscall_slong_t time_start2,bool is_cq_thread);
extern struct dhmp_msg** get_msgs_group;
pid_t gettid();

extern struct dhmp_msg* get_msg_readonly[PARTITION_MAX_NUMS];
extern void* penalty_addr;
extern double pf_partition[TEST_KV_NUM];
extern int *rand_num_partition[PARTITION_MAX_NUMS];
extern int *write_num_partition[PARTITION_MAX_NUMS];
extern int penalty_partition_count[PARTITION_MAX_NUMS];
void exit_print_status();

//#define THROUGH_TEST

 #define MAIN_LOG_DEBUG_LATENCE

#define for_stable 20




#define WHALE 0

#define CLINET_NUMS 1



//
//#define UD_META

#define ReadMsg 10000

extern char workloadf;
extern char workloadd;
extern char workloade;

extern int startwork;
extern struct dhmp_msg * read_list[PARTITION_MAX_NUMS][ReadMsg];

extern double workload_WriteRate;
extern int count_r[PARTITION_MAX_NUMS];
extern int count_w[PARTITION_MAX_NUMS];
extern struct ibv_mr leader_mr[4], local_mr[4];
extern struct dhmp_server *server_instance;

extern size_t max_num;
extern int set_workload_OK;
extern struct ibv_mr leader_mr[4];

extern struct BOX* box[PARTITION_MAX_NUMS];
extern int rand_num_partition_index[PARTITION_MAX_NUMS];
extern int write_num_partition_index[PARTITION_MAX_NUMS];

#define MICA_DEFAULT_VALUE_LEN (__test_size+64)


#endif

#define _GNU_SOURCE 1
#include "mica_partition.h"

#include "shm_private.h"
#include "table.h"
#include "dhmp_client.h"
#include "dhmp_log.h"
#include "dhmp_dev.h"
#include "dhmp_server.h"
#include "mid_rdma_utils.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"
#include "table.h"
#include "util.h"
#include "alloc_dynamic.h"
#include "nic.h"
#include "dhmp.h"
#include "dhmp_top_api.h"
#include "midd_mica_benchmark.h"
#include "hash.h"
#include <linux/unistd.h>
pid_t gettid(void){ return syscall(__NR_gettid); }

uint64_t  lhd_bit_locks[PARTITION_MAX_NUMS];


int need_read_round[PARTITION_MAX_NUMS];
struct ibv_mr leader_mr[4], local_mr[4];

volatile bool replica_is_ready = false;
struct timespec start_through, end_through;
struct timespec start_set_g, end_set_g;
struct timespec start_r, end_r;
uint64_t set_counts = 0, get_counts=0, op_counts=0;
long long int total_set_time = 0, total_get_time =0;
long long int total_set_latency_time = 0;
long long int total_read_latency_time = 0;
long long int total_through_time = 0;

static struct dhmp_msg * make_basic_msg(struct dhmp_msg * res_msg, struct post_datagram *resp);
static void dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, 
										bool *is_async, __time_t time_start1,
										__syscall_slong_t time_start2);

 struct dhmp_msg * read_list[PARTITION_MAX_NUMS][ReadMsg];
struct BOX* box[PARTITION_MAX_NUMS];

struct mica_work_context mica_work_context_mgr[2];
unsigned long long  __partition_nums;

int avg_partition_count_num=0;
int partition_set_count[PARTITION_MAX_NUMS];
int partition_get_count[PARTITION_MAX_NUMS + 1];
bool partition_count_set_done_flag[PARTITION_MAX_NUMS]; 

int rand_num_partition_index[PARTITION_MAX_NUMS] = {0}; 
int write_num_partition_index[PARTITION_MAX_NUMS] = {0};


int partition_count_num=0;

#define PARTITION_MAX_CREDICT 500
struct dhmp_mica_set_request * partition_credict[PARTITION_MAX_NUMS][PARTITION_MAX_CREDICT];
char partition_credict_index[PARTITION_MAX_NUMS];


size_t SERVER_ID= (size_t)-1;

struct list_head partition_local_send_list[PARTITION_MAX_NUMS];   
struct list_head main_thread_send_list[PARTITION_MAX_NUMS];
struct list_head work_list[PARTITION_MAX_NUMS];
uint64_t partition_work_nums[PARTITION_MAX_NUMS];
pthread_t busy_cpu_workload_threads[PARTITION_MAX_NUMS][PARTITION_MAX_NUMS];

void* mica_work_thread(void *data);
void* mica_busy_cpu_workload_work_thread(void *data);

int get_req_partition_id(struct post_datagram *req);
int get_resp_partition_id(struct post_datagram *req);

static void __dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id);
static void __dhmp_send_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id, bool * need_post_recv, bool * is_set);


static void  dhmp_set_response_handler(struct dhmp_msg* msg);
static struct post_datagram *  dhmp_mica_get_MR_request_handler(struct dhmp_transport* rdma_trans, struct post_datagram *req);

struct dhmp_msg** get_msgs_group;
struct dhmp_msg* get_msg_readonly[PARTITION_MAX_NUMS];
static void  check_get_op(struct dhmp_msg* msg, size_t partition_id);
void* penalty_addr;
int penalty_partition_count[PARTITION_MAX_NUMS];
double pf_partition[TEST_KV_NUM];
int *rand_num_partition[PARTITION_MAX_NUMS];
int *write_num_partition[PARTITION_MAX_NUMS];
double penalty_rw_rate;
int penalty_count[PARTITION_MAX_NUMS]={0};
int read_dirty_count[PARTITION_MAX_NUMS]={0};

void
dhmp_mica_dirty_get_request_handler(struct post_datagram *req, size_t partition_id)
{
	penalty_partition_count[partition_id]++;
	int err=0;
	struct dhmp_msg msg;
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_task *send_task_ptr;
	struct post_datagram *resq_to_replica;
	struct dhmp_transport * peer_node_trans = find_connect_server_by_nodeID(req->node_id, partition_id);
	size_t resp_len = sizeof(struct post_datagram ) + sizeof(struct dhmp_mica_set_response) + __test_size;

	resq_to_replica =(struct post_datagram *) malloc(resp_len);
	resq_to_replica->req_ptr = req->req_ptr;	
	resq_to_replica->node_id = server_instance->server_id;
	resq_to_replica->done_flag = false;
	resq_to_replica->info_type = DHMP_MICA_DIRTY_GET_RESPONSE;

	msg.msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
	msg.data_size = resp_len;	// 发送的时候带上 value
	msg.data= resq_to_replica;

	send_task_ptr=dhmp_send_task_create(peer_node_trans, &msg, partition_id);
	if(!send_task_ptr)
	{
		ERROR_LOG("create recv task error.");
		return ;
	}
	memset ( &send_wr, 0, sizeof ( send_wr ) );
	send_wr.wr_id= ( uintptr_t ) send_task_ptr;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.opcode=IBV_WR_SEND;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	sge.addr= ( uintptr_t ) send_task_ptr->sge.addr;
	sge.length=send_task_ptr->sge.length;
	sge.lkey=send_task_ptr->sge.lkey;
	err=ibv_post_send ( peer_node_trans->qp, &send_wr, &bad_wr );
	if ( err )
		ERROR_LOG ( "ibv_post_send error[%d]. [%s]" , err, strerror(err));
	free(resq_to_replica);	 
}

static
void
partition_lock(volatile uint64_t * lock)
{
	while (1)
	{
		if (__sync_bool_compare_and_swap(lock, 0UL, 1UL))
			break;
	}
}

static
void
partition_unlock(volatile uint64_t *lock)
{
	memory_barrier();
	*lock= 0UL;
}

static void
get_tomaster_request_handler(struct dhmp_transport* rdma_trans,
                                                                                                        struct dhmp_msg* msg)
{
        struct post_datagram *resp,* req;

        struct dhmp_msg resp_msg, req_msg;
 void * base;
        req = (struct post_datagram *)(msg->data);
		struct dhmp_mica_set_request  * req_info;
		req_info = (struct dhmp_mica_set_request *)((char *)req + sizeof(struct post_datagram));
        size_t resp_len = sizeof(struct post_datagram) + 320;
        base = malloc(resp_len);
    memset(base, 0 , resp_len);
resp = (struct post_datagram *)base;
        {
                resp->req_ptr  = req->req_ptr;
                resp->node_id  = req->node_id; //server_instance->server_id;
                resp->info_type = GET_TOMASTER_RESPONSE;

                resp_msg.msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
                resp_msg.data_size = resp_len;
                resp_msg.data= resp;

				
                dhmp_post_send(find_connect_read_server_by_nodeID(req->node_id,req_info->partition_id), &resp_msg, req_info->partition_id);

        }
		free(resp);
}

static void
get_tomaster_respond_handler(struct dhmp_transport* rdma_trans,
                                                                                                        struct dhmp_msg* msg)
{
        struct post_datagram *resp,* req;
        void  * req_info;

        struct dhmp_msg resp_msg, req_msg;

        resp = (struct post_datagram *)(msg->data);
void*   data = (struct dhmp_mica_set_request *)((char *)(msg->data) + sizeof(struct post_datagram));

        {
                req = resp->req_ptr;
				if(resp->node_id != server_instance->server_id)
					return;
                req->done_flag = true;
                struct dhmp_mica_set_request  * info1 =(void*)req + sizeof(struct post_datagram) ;
        }

}



static struct post_datagram * 
dhmp_mica_get_cli_MR_request_handler(struct dhmp_transport* rdma_trans,
									struct post_datagram *req)
{
	struct post_datagram *resp;
	struct dhmp_mica_get_cli_MR_request  * req_info;
	struct dhmp_mica_get_cli_MR_response * resp_req;
	struct dhmp_device * dev = dhmp_get_dev_from_server();
	size_t resp_len = sizeof(struct dhmp_mica_get_cli_MR_response);
	Assert(resp_len < SINGLE_POLL_RECV_REGION);

 
	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	INFO_LOG("resp length is %u", DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));

	req_info  = (struct dhmp_mica_get_cli_MR_request *) DATA_ADDR(req, 0);
	resp_req  = (struct dhmp_mica_get_cli_MR_response *) DATA_ADDR(resp, 0);

 
	resp_req->resp_all_mapping.node_id = server_instance->server_id;
	resp_req->resp_all_mapping.used_mapping_nums = get_mapping_nums();
	resp_req->resp_all_mapping.first_inited = false;

	if (false == get_table_init_state())
	{
		mehcached_shm_lock();
		if (false == get_table_init_state() )
		{
		 
			struct mehcached_table *table = &table_o;
			size_t numa_nodes[] = {(size_t)-1};

		 
			mehcached_table_init(table, TABLE_BUCKET_NUMS, 1, TABLE_POOL_SIZE, true, true, true, \
								numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
			Assert(table);

			resp_req->resp_all_mapping.first_inited = true;
			INFO_LOG("replica node[%d] first init finished!, caller is node[%d]", \
						server_instance->server_id, req->node_id);
			
			set_table_init_state(true);
		}
		mehcached_shm_unlock();
	}

	copy_mapping_info( (void*) resp_req->resp_all_mapping.mehcached_shm_pages);
	copy_mapping_mrs_info(&resp_req->resp_all_mapping.mrs[0]);

	memcpy(local_mr, resp_req->resp_all_mapping.mrs, sizeof(struct ibv_mr) *4 );

	ERROR_LOG("local_mr addr is  %p %p %p %p",local_mr[0].addr,local_mr[1].addr ,local_mr[2].addr ,local_mr[3].addr);

	// dump_mr(&resp_req->resp_all_mapping.mrs[1]);

	resp->req_ptr  = req->req_ptr;		    		 
	resp->resp_ptr = resp;							 
	resp->node_id  = server_instance->server_id;	 
	resp->info_type = MICA_GET_CLIMR_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						 

	return resp;
}

// 镜像节点调用该函数初始化自己的内存区域
static struct post_datagram * 
dhmp_mica_get_Mirr_MR_request_handler(struct dhmp_transport* rdma_trans,
									struct post_datagram *req)
{
	struct post_datagram *resp;
	struct dhmp_mica_get_cli_MR_request  * req_info;
	struct dhmp_mica_get_cli_MR_response * resp_req;
	struct dhmp_device * dev = dhmp_get_dev_from_server();
	size_t resp_len = sizeof(struct dhmp_mica_get_cli_MR_response);

	// Mirror arg
	struct dhmp_mr local_mr;
	int reval;

	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	INFO_LOG("resp length is %u", DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));

	req_info  = (struct dhmp_mica_get_cli_MR_request *) DATA_ADDR(req, 0);
	resp_req  = (struct dhmp_mica_get_cli_MR_response *) DATA_ADDR(resp, 0);

 
	resp_req->resp_all_mapping.node_id = server_instance->server_id;

	reval = dhmp_memory_register(dev->pd, &local_mr, 1024*1024);
	if (reval)
	{
		ERROR_LOG("dhmp_memory_register");
		exit(-1);
	}

 
	memcpy(&(resp_req->resp_all_mapping.mirror_mr), local_mr.mr, sizeof(struct ibv_mr));
	resp_req->resp_all_mapping.mirror_virtual_addr = local_mr.addr;

	resp->req_ptr  = req->req_ptr;		    		 
	resp->resp_ptr = resp;							 
	resp->node_id  = server_instance->server_id;	 
	resp->info_type = MICA_GET_CLIMR_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						 

	return resp;
}


static void 
dhmp_mica_get_cli_MR_response_handler(struct dhmp_transport* rdma_trans,
													struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req = (struct post_datagram *) (resp->req_ptr); 

	struct dhmp_mica_get_cli_MR_response *resp_info = \
			(struct dhmp_mica_get_cli_MR_response *) DATA_ADDR(resp, 0);

	struct dhmp_mica_get_cli_MR_request *req_info = \
		(struct dhmp_mica_get_cli_MR_request *) DATA_ADDR(req, 0);

	memcpy(req_info->info_revoke_ptr, &resp_info->resp_all_mapping, sizeof(struct replica_mappings));

 

	resp->req_ptr->done_flag = true;
}

static struct post_datagram * 
dhmp_ack_request_handler(struct dhmp_transport* rdma_trans,
							struct post_datagram *req)
{
 
	struct post_datagram *resp;
	struct dhmp_mica_ack_request * req_data = (struct dhmp_mica_ack_request *) DATA_ADDR(req, 0);
	struct dhmp_mica_ack_response * resp_data = (struct dhmp_mica_ack_response *) req_data;
	enum ack_info_state resp_ack_state;

	/*!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!*/
	/*!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!*/
	/*!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!*/
	nic_thread_ready= true;

	memcpy(leader_mr, req_data->leader_mr, sizeof(struct ibv_mr) *4 );
	ERROR_LOG("Get leader %p %p", leader_mr[0].addr,leader_mr[1].addr);

	switch (req_data->ack_type)
	{
	case MICA_INIT_ADDR_ACK:
		mehcached_shm_lock();
		if (get_table_init_state() == false || replica_is_ready == false)
			resp_ack_state = MICA_ACK_INIT_ADDR_NOT_OK;
		else
			resp_ack_state = MICA_ACK_INIT_ADDR_OK;

		if (IS_REPLICA(server_instance->server_type) &&
			!IS_TAIL(server_instance->server_type) && 
			nic_thread_ready == false)
			resp_ack_state = MICA_ACK_INIT_ADDR_NOT_OK;

		mehcached_shm_unlock();
		break;
	default:
		break;
	}

	resp = req;											
	resp->node_id	  = server_instance->server_id;
	resp->resp_ptr    = resp;
	resp->info_type   = MICA_ACK_RESPONSE;
	resp->info_length = sizeof(struct dhmp_mica_ack_response);


 
	resp_data->ack_state = resp_ack_state;
	return resp;
}

static void 
dhmp_ack_response_handler(struct dhmp_transport* rdma_trans,
													struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req  = (struct post_datagram *) resp->req_ptr;

	struct dhmp_mica_ack_request * req_info = \
			(struct dhmp_mica_ack_request *) DATA_ADDR(req, 0);
	struct dhmp_mica_ack_response *resp_info = \
			(struct dhmp_mica_ack_response *) DATA_ADDR(resp, 0);

 
	req_info->ack_type = resp_info->ack_state;

	req->done_flag = true;
}


static struct post_datagram * 
dhmp_node_id_request_handler(struct post_datagram *req)
{
 
	struct post_datagram *resp;
	struct dhmp_get_nodeID_response * resp_data = \
			(struct dhmp_get_nodeID_response *) DATA_ADDR(req, 0);

	resp = req;											
	resp->node_id	  = server_instance->server_id;
	resp->resp_ptr    = resp;
	resp->info_type   = MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE;
	resp->info_length = sizeof(struct dhmp_get_nodeID_response);

	resp_data->resp_node_id = server_instance->server_id;

	return resp;
}

static void 
dhmp_node_id_response_handler(struct dhmp_transport* rdma_trans,
										struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req = resp->req_ptr; 

	struct dhmp_get_nodeID_response *resp_info = \
			(struct dhmp_get_nodeID_response *) DATA_ADDR(resp, 0);

	struct dhmp_get_nodeID_request *req_info = \
			(struct dhmp_get_nodeID_request *) DATA_ADDR(req, 0);

	req_info->node_id = resp_info->resp_node_id;

	resp->req_ptr->done_flag = true;
}

static void
dhmp_mica_get_request_handler_by_addr(void * key_addr, size_t key_length, size_t expect_length)
{
	int re;
	void * out_value;
	size_t in_out_value_length = expect_length;
	uint32_t out_expire_time;
	MICA_GET_STATUS get_status;
	struct mehcached_table *table = &table_o;
	uint64_t key_hash = hash(key_addr, key_length);
	out_value = malloc(expect_length);
	re = mehcached_get(0,
						table,
						key_hash,
						key_addr,
						key_length,
						out_value,
						&in_out_value_length,
						&out_expire_time,
						false, 
						false,
						&get_status);	 
	free(out_value);
}


struct post_datagram *resp_read; 


static struct post_datagram *
dhmp_mica_get_request_handler(struct post_datagram *req, size_t partition_id)
{

	struct dhmp_mica_get_request  * req_info;
	struct dhmp_mica_get_response * set_result;
	size_t resp_len;
	bool re;
	MICA_GET_STATUS get_status;
	int random_get_id;
	void * key_addr;
	void * value_addr;
	int partition_get_id;

	req_info  = (struct dhmp_mica_get_request *) DATA_ADDR(req, 0);
	key_addr = (void*)req_info->data;
	partition_get_id = partition_get_count[req_info->partition_id];

	if (IS_REPLICA(server_instance->server_type))
		Assert(replica_is_ready == true);


	struct mehcached_table *table = &table_o;
	uint8_t *out_value;
	size_t in_out_value_length ;
	uint32_t out_expire_time;
	resp_len = sizeof(struct dhmp_mica_get_response) + req_info->peer_max_recv_buff_length;
	if(resp_read == NULL)
		resp_read = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	memset(resp_read, 0, DATAGRAM_ALL_LEN(resp_len));
	set_result = (struct dhmp_mica_get_response *) DATA_ADDR(resp_read, 0);
	value_addr = (void*)set_result + offsetof(struct dhmp_mica_get_response, out_value);
	out_value = value_addr;
	in_out_value_length = (size_t)MICA_DEFAULT_VALUE_LEN;
	int scannum = 1;
	if(workloade)
	{
		scannum = 3;
	}
	while(scannum--)
	{
		re = mehcached_get(req_info->current_alloc_id,
							table,
							req_info->key_hash+scannum,
							key_addr,
							req_info->key_length,
							out_value,
							&in_out_value_length,
							&out_expire_time,
							false, 
							false,
							&get_status);	 
		if (re== false)
		{
			set_result->out_expire_time = 0;
			set_result->out_value_length = (size_t)-1;
			 	}
		else
		{
			set_result->out_expire_time = out_expire_time;
			set_result->out_value_length = in_out_value_length;
	 	}
	}
	set_result->status = get_status;
	set_result->partition_id = req_info->partition_id;
	
	resp_read->req_ptr  = req->req_ptr;		    		 
	resp_read->resp_ptr = resp_read;						 
	resp_read->node_id  = server_instance->server_id;	 
	resp_read->info_type = MICA_GET_RESPONSE;
	resp_read->info_length = resp_len;
	resp_read->done_flag = false;

	partition_get_count[req_info->partition_id]++;
	//ERROR_LOG("Get: tag is [%d] partition_id [%d]",req_info->tag, req_info->partition_id);
	return resp_read ; 
}

static void 
dhmp_get_response_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req = resp->req_ptr; 
	struct dhmp_mica_get_response * get_re_info;

	struct dhmp_mica_get_response *resp_info = \
			(struct dhmp_mica_get_response *) DATA_ADDR(resp, 0);

	struct dhmp_mica_get_request *req_info = \
			(struct dhmp_mica_get_request *) DATA_ADDR(req, 0);

	get_re_info 					= req_info->get_resp;
	get_re_info->out_value_length   = resp_info->out_value_length;
	get_re_info->out_expire_time    = resp_info->out_expire_time;
	get_re_info->status 			= resp_info->status;

	get_re_info->trans_data.msg = msg;
	get_re_info->trans_data.rdma_trans = rdma_trans;

	if (resp_info->out_value_length != (size_t) -1)
	{
		void * value_addr =  (void*)(resp_info->out_value);
		// memcpy((void*)(get_re_info->out_value), value_addr, resp_info->out_value_length );
		get_re_info->msg_buff_addr = value_addr;

		INFO_LOG("Node [%d] GET key_hash [%lx] from node[%d]!, is success!, tag is [%ld]", \
			server_instance!=NULL ? server_instance->server_id : client_mgr->self_node_id, req_info->key_hash, resp->node_id, req_info->tag);
	}
	else
	{
		ERROR_LOG("Node [%d] GET key_hash [%lx] from node[%d]!, is FAILED!, tag is [%ld]", \
			server_instance!=NULL ? server_instance->server_id : client_mgr->self_node_id, req_info->key_hash, resp->node_id, req_info->tag);
	}

	resp->req_ptr->done_flag = true;
}

static struct post_datagram * 
dhmp_mica_update_notify_request_handler(struct post_datagram *req, int partition_id)
{
	Assert(IS_REPLICA(server_instance->server_type));
	struct post_datagram *resp=NULL;
	struct dhmp_update_notify_request  * req_info;
	// struct dhmp_update_notify_response * set_result;
	// size_t resp_len = sizeof(struct dhmp_update_notify_response);

 
	struct mehcached_item * update_item;
	struct mehcached_table *table = &table_o;
    struct midd_value_header* value_base;
    // uint8_t* value_data;
    // struct midd_value_tail* value_tail;
	size_t key_align_length;
	size_t value_len, true_value_len;
	uint64_t item_offset;

	if (IS_REPLICA(server_instance->server_type))
		Assert(replica_is_ready == true);

	req_info  = (struct dhmp_update_notify_request *) DATA_ADDR(req, 0);
	update_item = get_item_by_offset(table, req_info->item_offset);

#ifdef INFO_DEBUG
	WARN_LOG("[dhmp_mica_update_notify_request_handler] get tag [%ld]", req_info->tag);
#endif
	key_align_length = MEHCACHED_ROUNDUP8(MEHCACHED_KEY_LENGTH(update_item->kv_length_vec));
	value_len = MEHCACHED_VALUE_LENGTH(update_item->kv_length_vec);
	true_value_len = value_len - VALUE_HEADER_LEN - VALUE_TAIL_LEN;

	item_offset = get_offset_by_item(table, update_item);
	Assert(item_offset == req_info->item_offset);

    value_base = (struct midd_value_header*)(update_item->data + key_align_length);
    // value_data = update_item->data + key_align_length + VALUE_HEADER_LEN;
    // value_tail = (struct midd_value_tail*) VALUE_TAIL_ADDR(update_item->data , key_align_length, true_value_len);

#ifdef INFO_DEBUG
	WARN_LOG("Node [%d] recv update value, now value version is [%ld]", \
				server_instance->server_id, value_base->version);
#endif

	if(box[req_info->partition_id] != NULL )
	    {
	        int i =0;
//	                 for(;i < ((__test_size/request_base));i++)
	                 {
	            box[req_info->partition_id]->array[req_info->key_hash] = 0;
	        }
	    }
 
	if (!IS_TAIL(server_instance->server_type))
	{
	 
        makeup_update_request(update_item, item_offset, (uint8_t *)value_base, value_len, req_info->tag, req_info->partition_id,req_info->key_hash);
 
	}

	return resp;
}

static void 
dhmp_mica_update_notify_response_handler(struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req = resp->req_ptr; 
	struct dhmp_update_notify_response *resp_info = \
			(struct dhmp_update_notify_response *) DATA_ADDR(resp, 0);

	WARN_LOG("[dhmp_mica_update_notify_response_handler] get tag [%d]", resp_info->tag);
	INFO_LOG("Node [%d] get update response!" , server_instance->server_id);
	resp->req_ptr->done_flag = true;
}

 
static void __dhmp_send_request_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id, bool * need_post_recv, bool *is_set)
{
	struct dhmp_msg res_msg;
	//struct dhmp_device * dev;
	struct post_datagram *req;
	struct post_datagram *resp;
	struct timespec start, end; 

	req = (struct post_datagram*)msg->data;
	*need_post_recv = true;
	*is_set=false;

 
	switch (req->info_type)
	{
		case MICA_ACK_REQUEST:
			//INFO_LOG ( "Recv [MICA_ACK_REQUEST] from node [%d]",  req->node_id);	
			resp = dhmp_ack_request_handler(rdma_trans, req);
			break;	
		case MICA_GET_CLIMR_REQUEST:
///			INFO_LOG ( "Recv [MICA_GET_CLIMR_REQUEST] from node [%d-%d],%p",  req->node_id,rdma_trans->partition_id,rdma_trans);	
			resp = dhmp_mica_get_MR_request_handler(rdma_trans, req);
			break;
		case MICA_SERVER_GET_CLINET_NODE_ID_REQUEST:
//			INFO_LOG ( "Recv [MICA_SERVER_GET_CLINET_NODE_ID_REQUEST] from node [%d-%d] %p",  req->node_id,rdma_trans->partition_id,rdma_trans);
			resp = dhmp_node_id_request_handler(req);
			break;
		case MICA_SET_REQUEST:
			*is_set=true;
 	
			dhmp_mica_set_request_handler(rdma_trans, req, partition_id);
			if (!IS_MAIN(server_instance->server_type))
			{
				volatile uint64_t * lock;
				lock = &lhd_bit_locks[partition_id];
				partition_lock(lock);
				need_read_round[partition_id]++;
				partition_unlock(lock);
			//	ERROR_LOG("need_read_round[%d]== %d",partition_id,need_read_round[partition_id]);
			}
 
			if (server_instance->server_id <REPLICA_NODE_HEAD_ID)
				*need_post_recv = false;
			return; 	 
		case MICA_GET_REQUEST:
 	
			resp = dhmp_mica_get_request_handler(req, partition_id);
 
			*need_post_recv = false;
			return;  
		case MICA_REPLICA_UPDATE_REQUEST:
 
			dhmp_mica_update_notify_request_handler(req, partition_id);
			return;
		case MICA_SET_REQUEST_TEST:
			// do nothing
			//ERROR_LOG("MICA_SET_REQUEST_TEST");
			resp = (struct post_datagram *) malloc(sizeof(struct post_datagram ));
			memset(resp, 0, sizeof(struct post_datagram ));
			resp->info_type = MICA_SET_RESPONSE_TEST;
			resp->node_id = MAIN;
			resp->req_ptr = req->req_ptr;
			resp->info_length = 0;
			partition_id=0;
			break;
		case MICA_GET_P2P_MR_REQUEST:

			break;
		case  DHMP_MICA_DIRTY_GET_REQUEST:
			// ERROR_LOG ( "Recv [DHMP_MICA_DIRTY_GET_REQUEST] from node [%d]",  req->node_id);
			dhmp_mica_dirty_get_request_handler(req, partition_id);
			return;
		default:
			ERROR_LOG("Unknown request info_type %d", req->info_type);
			exit(0);
			break;
	}

	res_msg.msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
	res_msg.data_size = DATAGRAM_ALL_LEN(resp->info_length);
	res_msg.data= resp;
	//INFO_LOG("Send response msg length is [%u] KB", res_msg.data_size / 1024);

 	
	dhmp_post_send(rdma_trans, &res_msg, partition_id);
	//clock_gettime(CLOCK_MONOTONIC, &end);
	//total_get_time = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec));
	//ERROR_LOG("[dhmp_post_send]   time is [%lld] ns", total_get_time);
send_clean:
 
	if (req->info_type != MICA_ACK_RESPONSE &&
	    req->info_type != MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE)
		free((void*) resp);

	return ;
}

 
static void 
__dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id)
{
	struct post_datagram *resp;
	resp = (struct post_datagram*)(msg->data);
	DEFINE_STACK_TIMER();

	switch (resp->info_type)
	{
		case MICA_ACK_RESPONSE:
			INFO_LOG ( "Recv [MICA_ACK_RESPONSE] from node [%d-%d] %p",  resp->node_id,rdma_trans->partition_id,rdma_trans);
			dhmp_ack_response_handler(rdma_trans, msg);
			break;	
		case MICA_GET_CLIMR_RESPONSE:
			INFO_LOG ( "Recv [MICA_GET_CLIMR_RESPONSE] from node [%d-%d] %p",  resp->node_id,rdma_trans->partition_id,rdma_trans);
			dhmp_mica_get_cli_MR_response_handler(rdma_trans, msg);
			break;
		case MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE:
			INFO_LOG ( "Recv [MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE] from node [%d-%d] %p",  resp->node_id,rdma_trans->partition_id,rdma_trans);
			dhmp_node_id_response_handler(rdma_trans, msg);
			break;
		case MICA_SET_RESPONSE:
			//INFO_LOG ( "Recv [MICA_SET_RESPONSE] from node [%d]",  resp->node_id);
			dhmp_set_response_handler(msg);
			break;
		case MICA_GET_RESPONSE:
			//INFO_LOG ( "Recv [MICA_GET_RESPONSE] from node [%d]",  resp->node_id);
			dhmp_get_response_handler(rdma_trans, msg); // 需要 rdma_trans 进行 recv_wr 的卸载
			break;		
		case MICA_REPLICA_UPDATE_RESPONSE:
			//INFO_LOG ( "Recv [MICA_REPLICA_UPDATE_RESPONSE] from node [%d]",  resp->node_id);
			// dhmp_mica_update_notify_response_handler(msg);
			break;				
		case MICA_SET_RESPONSE_TEST:
			((struct post_datagram *)(msg->data))->req_ptr->done_flag=true;
			break;
		default:
			break;
	}
}

/**
 *  函数前缀没有双下划线的函数是单线程执行的
 */
void dhmp_wc_recv_handler(struct dhmp_transport* rdma_trans, 
							struct dhmp_msg* msg,
							bool *is_async, 
							__time_t time_start1, 
							__syscall_slong_t time_start2)
{
	switch(msg->msg_type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			dhmp_send_request_handler(rdma_trans, msg, is_async, time_start1, time_start2, true);
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			dhmp_send_respone_handler(rdma_trans, msg, is_async, time_start1, time_start2);
			break;

		case DHMP_MSG_CLOSE_CONNECTION:
			rdma_disconnect(rdma_trans->cm_id);
			*is_async = false;
			break;
		default:
			*is_async = false;
			break;
	}
}

// 多线程执行
void __dhmp_wc_recv_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, size_t partition_id, bool *need_post_recv, bool *is_set)
{
	switch(msg->msg_type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			__dhmp_send_request_handler(rdma_trans, msg, partition_id, need_post_recv, is_set);
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			*need_post_recv = true;
			__dhmp_send_respone_handler(rdma_trans, msg, PARTITION_NUMS);
			break;
		case DHMP_MSG_CLOSE_CONNECTION:
			ERROR_LOG("DHMP_MSG_CLOSE_CONNECTION! exit");
			exit(-1);
			rdma_disconnect(rdma_trans->cm_id);
			break;
		default:
			break;
	}
}

static struct dhmp_msg *
make_basic_msg(struct dhmp_msg * res_msg, struct post_datagram *resp)
{
	res_msg->msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
	res_msg->data_size = DATAGRAM_ALL_LEN(resp->info_length);
	res_msg->data= resp;
	return res_msg;
}

bool 
main_node_broadcast_matedata(struct dhmp_mica_set_request  * req_info,
							  struct post_datagram * req_msg,
							  size_t total_length)
{
	DEFINE_STACK_TIMER();
    int nid, reval;
	uint32_t partition_id = req_info->partition_id;
	struct dhmp_transport* trans;

	req_info->count_num = (size_t)REPLICA_NODE_NUMS;
	req_msg->node_id = MAIN;
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;


 
	struct dhmp_task write_task;
	struct ibv_sge sge;
	struct ibv_send_wr send_wr,*bad_wr=NULL; 
	for (nid = MAIN_NODE_ID +1; nid < REPLICA_NODE_HEAD_ID; nid++)
 	{
		write_task.done_flag = false;
		write_task.is_imm = true;
		mirror_node_mapping[nid][partition_id].in_used_flag = 1;

		trans = find_connect_server_by_nodeID(nid, partition_id);
		memset(&send_wr, 0, sizeof(struct ibv_send_wr));
		send_wr.opcode=IBV_WR_RDMA_WRITE_WITH_IMM; 
		send_wr.imm_data  = htonl((uint32_t)(req_info->partition_id)); 
		send_wr.wr_id= ( uintptr_t ) &write_task;
		send_wr.sg_list=&sge;
		send_wr.num_sge=1;
		send_wr.send_flags=IBV_SEND_SIGNALED;
		send_wr.wr.rdma.remote_addr = (uintptr_t)(mirror_node_mapping[nid][partition_id].mirror_virtual_addr);  // WGT
		send_wr.wr.rdma.rkey  		= mirror_node_mapping[nid][partition_id].mirror_mr.rkey;

		sge.addr  =	(uintptr_t)(trans->send_mr.addr);
		sge.length=	req_info->value_length * CLINET_NUMS; 
		sge.lkey  =	trans->send_mr.mr->lkey;
		reval=ibv_post_send ( trans->qp, &send_wr, &bad_wr );
		if ( reval )
		{
			ERROR_LOG("ibv_post_send error[%d], reason is [%s]", errno,strerror(errno));
			exit(-1);
		}
 	}
	if(REPLICA_NODE_NUMS == 0) return true;
	//while (!write_task.done_flag);

 
	trans = find_connect_server_by_nodeID(REPLICA_NODE_HEAD_ID, partition_id);
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));
	struct dhmp_msg msg;
	struct dhmp_task *send_task_ptr;
	msg.data = req_msg;
	msg.data_size = total_length * CLINET_NUMS;
	msg.msg_type = DHMP_MICA_SEND_INFO_REQUEST;
	send_task_ptr=dhmp_send_task_create(trans, &msg, partition_id);
	if(!send_task_ptr)
	{
		ERROR_LOG("create recv task error.");
		return ;
	}
	send_wr.wr_id= ( uintptr_t ) send_task_ptr;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.opcode=IBV_WR_SEND;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	sge.addr= ( uintptr_t ) send_task_ptr->sge.addr;
	sge.length=send_task_ptr->sge.length;
	sge.lkey=send_task_ptr->sge.lkey;
	nid = REPLICA_NODE_HEAD_ID;
#ifdef UD_META
#else
    for (nid = REPLICA_NODE_HEAD_ID; nid <= REPLICA_NODE_TAIL_ID; nid++)
#endif
    {
		trans = find_connect_server_by_nodeID(nid, partition_id);

		volatile uint64_t * lock;
		lock = &lhd_bit_locks[partition_id];
		partition_lock(lock);

		reval = ibv_post_send ( trans->qp, &send_wr, &bad_wr );
		if ( reval )
		{
			ERROR_LOG("ibv_post_send error[%d], reason is [%s]", errno,strerror(errno));
			exit(-1);
		}
		partition_unlock(lock);

    }
 
	return true;
}

void main_node_broadcast_matedata_wait(struct dhmp_mica_set_request  * req_info, 
										int partition_id,
										struct mehcached_item * item)
{
	struct timespec start_l, end_l;
  
	int nid;
	for(nid = MAIN_NODE_ID+1; nid < REPLICA_NODE_HEAD_ID;nid++)
		while(mirror_node_mapping[nid][partition_id].in_used_flag == 1);
	 
	while(req_info->count_num != 0);
	 

	item->mapping_id 		= req_info->out_mapping_id;
	item->remote_value_addr = req_info->out_value_addr;
}

int init_mulit_server_work_thread()
{
	
	bool recv_mulit_threads_enable=true;
	int i, retval;
	cpu_set_t cpuset;
	memset(&(mica_work_context_mgr[0]), 0, sizeof(struct mica_work_context));
	memset(&(mica_work_context_mgr[1]), 0, sizeof(struct mica_work_context));
	memset(partition_count_set_done_flag, 0, sizeof(bool) * PARTITION_MAX_NUMS);

	for (i=0; i<PARTITION_NUMS; i++)
	{
		CPU_ZERO(&cpuset);
			CPU_SET(i, &cpuset);
		thread_init_data *data = (thread_init_data *) malloc(sizeof(thread_init_data));
		data->partition_id = i;
		data->thread_type = (enum dhmp_msg_type) DHMP_MICA_SEND_INFO_REQUEST;

		INIT_LIST_HEAD(&main_thread_send_list[i]);
		INIT_LIST_HEAD(&work_list[i]);
		INIT_LIST_HEAD(&partition_local_send_list[i]);

		retval=pthread_create(&(mica_work_context_mgr[DHMP_MICA_SEND_INFO_REQUEST].threads[i]), NULL, mica_work_thread, (void*)data);
		if(retval)
		{
			ERROR_LOG("pthread create error.");
			return -1;
		}
		// 绑核
		retval = pthread_setaffinity_np(mica_work_context_mgr[DHMP_MICA_SEND_INFO_REQUEST].threads[i], sizeof(cpu_set_t), &cpuset);
		if (retval != 0)
			handle_error_en(retval, "pthread_setaffinity_np");

#ifdef TEST_CPU_BUSY_WORKLOAD
		if (SERVER_ID == 3)
		{
			int j=0;
			//if (i == 2 || i== 4)
			{
				for (j=0; j<1; j++)
				{
					retval=pthread_create(&busy_cpu_workload_threads[i][j], NULL, mica_busy_cpu_workload_work_thread, (void*)data);
					if(retval)
					{
						ERROR_LOG("pthread create error.");
						return -1;
					}
					retval = pthread_setaffinity_np(busy_cpu_workload_threads[i][j], sizeof(cpu_set_t), &cpuset);
					if (retval != 0)
						handle_error_en(retval, "pthread_setaffinity_np");
				}
			}
		}
#endif
		INFO_LOG("set affinity cpu [%d] to thread [%d]", i, i);
	}
}

void* mica_busy_cpu_workload_work_thread(void *data)
{
	struct timespec start, end;
	long long mica_total_time_ns;
	pid_t pid = gettid();
	pthread_t tid = pthread_self();
	ERROR_LOG("Pid [%d] Tid [%ld]", pid, tid);
	while(true)
	{
		clock_gettime(CLOCK_MONOTONIC, &start);
		clock_gettime(CLOCK_MONOTONIC, &end);
		mica_total_time_ns = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec)); \
	}
}


int  get_req_partition_id(struct post_datagram *req)
{
	switch (req->info_type)
	{
		case DHMP_MICA_DIRTY_GET_REQUEST:
		case MICA_SET_REQUEST:
			return ((struct dhmp_mica_set_request *) DATA_ADDR(req, 0))->partition_id; 
		case MICA_GET_REQUEST:
			return ((struct dhmp_mica_get_request *) DATA_ADDR(req, 0))->partition_id; 
		case MICA_REPLICA_UPDATE_REQUEST:
			return ((struct dhmp_update_notify_request *) DATA_ADDR(req, 0))->partition_id;
		case MICA_SET_REQUEST_TEST:
			return 0;
		default:
			ERROR_LOG("Unsupport partition request info_type %d", req->info_type);
			Assert(false);
			break;
	}
}

int  get_resp_partition_id(struct post_datagram *req)
{
	switch (req->info_type)
	{
		case DHMP_MICA_DIRTY_GET_RESPONSE:
		case MICA_SET_RESPONSE:
			return ((struct dhmp_mica_set_response *) DATA_ADDR(req, 0))->partition_id; 
		case MICA_GET_RESPONSE:
			return ((struct dhmp_mica_get_response  *) DATA_ADDR(req, 0))->partition_id; 
		case MICA_REPLICA_UPDATE_RESPONSE:
			return ((struct dhmp_update_notify_response *) DATA_ADDR(req, 0))->partition_id;
		default:
			ERROR_LOG("Unsupport partition response info_type %d", req->info_type);
			Assert(false);
			break;
	}
}

void distribute_partition_resp(int partition_id, struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, __time_t time_start1, __syscall_slong_t time_start2,bool is_cq_thread)
{
	DEFINE_STACK_TIMER();
	struct mica_work_context * mgr;
	volatile uint64_t * lock;
	struct post_datagram * req = (struct post_datagram*)(msg->data);
	long long time1,time2,time3, time4=0;
	int retry_count=0;

	switch (msg->msg_type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			mgr = &mica_work_context_mgr[0];
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			mgr = &mica_work_context_mgr[1];
			break;
		default:
			ERROR_LOG("Unkown type");;
			Assert(false);
			break;
	}
	lock = &(mgr->bit_locks[partition_id]);

		// while(partition_work_nums[partition_id] != 0);
		if(!is_cq_thread)
		{ //init
			list_add(&msg->list_anchor,  &work_list[partition_id]);
			partition_work_nums[partition_id]++;
		}
		else
		{

			if(server_instance->server_id > 0)
			{
				bool need_post_recv = true, is_set=false;
				__dhmp_wc_recv_handler(msg->trans, msg, partition_id, &need_post_recv, &is_set);
				partition_work_nums[partition_id]++;
				if (need_post_recv)
				{
					dhmp_post_recv(msg->trans, msg->data - sizeof(enum dhmp_msg_type) - sizeof(size_t), msg->recv_partition_id);
					free(container_of(&(msg->data), struct dhmp_msg , data));	
				}
			}
			else{

			retry_count=0;
			partition_lock(lock);
		//Assert(msg->list_anchor.next != LIST_POISON1 && msg->list_anchor.prev!= LIST_POISON2);
			list_add(&msg->list_anchor,  &main_thread_send_list[partition_id]);
			partition_work_nums[partition_id]++;
		//Assert(msg->list_anchor.next != LIST_POISON1 && msg->list_anchor.prev!= LIST_POISON2);
			partition_unlock(lock);
			}
		}
}

/***
* value_array is read operations between two writes
* array_length is the length of value_array
* 
* return the number of finished read in this term
*/
static int finish_read(struct BOX* box_item, int partition_id, double need_read, int current_read, struct dhmp_transport* rdma_trans)
{
	int i,j;
	bool need_post_recv = true, is_set=false;
	struct dhmp_msg* msg=NULL, *temp_msg=NULL;
	j = 1;
	if(need_read < 1) return 0;
	if(box_item == NULL)
	{ 
		{
			while(1)
			{
				int offset = 0;
			if(workloadd != 1)
				offset = partition_id+100;
			//	msg = read_list[partition_id][(current_read + j)%ReadMsg];
			msg = read_list[partition_id][write_num_partition[partition_id][(current_read + j+offset)%max_num]%ReadMsg];
				struct post_datagram *req = (struct post_datagram*)msg->data;
				dhmp_mica_get_request_handler(req, partition_id);
				j++;
				if(j > need_read)return j-1;
			}
		}

	}
		while(1)
		{
			int offset = 0;
			if(workloadd != 1)
				offset = partition_id+100;
 
// ERROR_LOG("read index = %d",rand_num_partition[partition_id][(current_read + j)%max_num]);
			msg = read_list[partition_id][write_num_partition[partition_id][(current_read + j+offset)%max_num]%ReadMsg];
//		msg = read_list[partition_id][(current_read + j)%ReadMsg];
			int  read_key,test;
			struct post_datagram * req = (struct post_datagram*)msg->data;
        		struct dhmp_mica_get_request  * req_info= (struct dhmp_mica_get_request *) DATA_ADDR(req, 0);

        		read_key = req_info->key_hash;
 
			while(box_item->array[read_key] == 1 && workload_WriteRate != (double)150)
			{
				uint64_t key = read_key;
					#ifdef ReadFromLeader
 
                                {
                                        size_t total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_set_request) + __test_size;
                                        void * base = malloc(total_length);
                                        memset(base, 0 , total_length);
                                        struct post_datagram * req_msg  = (struct post_datagram *) base;
                                        req_msg->node_id = (int)server_instance->server_id;
                                        req_msg->req_ptr = req_msg;
                                        req_msg->done_flag = false;
                                        req_msg->info_type = GET_TOMASTER_REQUEST;
                                        req_msg->info_length = sizeof(struct dhmp_mica_set_request);

                                        struct dhmp_mica_set_request * req_data = (struct dhmp_mica_set_request *)((char *)base + sizeof(struct post_datagram));
                                        req_data->current_alloc_id = -1;
                                        req_data->key_hash = key;
                                        req_data->key_length = __test_size;
                                        req_data->partition_id = partition_id;
                                        void *data_addr = (void*)req_data + offsetof(struct dhmp_mica_set_request, data);

                                        struct dhmp_msg* msg1 = (struct dhmp_msg*)malloc(sizeof(struct dhmp_msg));
                                        msg1->data = base;
                                    msg1->data_size = total_length;
                                    msg1->msg_type = DHMP_MICA_SEND_INFO_REQUEST;
                                    INIT_LIST_HEAD(&msg1->list_anchor);
                                    msg1->trans = rdma_trans;
                                    msg1->recv_partition_id = partition_id;
                                    msg1->partition_id = partition_id;

                                    dhmp_post_send(msg1->trans, msg1, partition_id);
                                        while(req_msg->done_flag == false) ;
 
						box_item->array[read_key] = 0;

						penalty_partition_count[partition_id]++;
						penalty_count[partition_id] ++;
                        }
                        #endif
			}
            dhmp_mica_get_request_handler(req, partition_id);
			j++;
			if(j > need_read)
				return j-1;
		}
	return 0;
}


long long int la_temp;
int revise_time;

void* mica_work_thread(void *data)
    {
	thread_init_data * init_data = (thread_init_data*) data;
	int partition_id =  init_data->partition_id;
	DEFINE_STACK_TIMER();
	int  i, j;
	enum dhmp_msg_type type;
	volatile uint64_t * lock;
	struct mica_work_context * mgr;
	struct timespec start_g, end_g, start_l, end_l;
	long long time1,time2,time3, time4=0;
	struct dhmp_msg* msg=NULL, *temp_msg=NULL;
	struct dhmp_msg* msg1=NULL, *temp_msg1=NULL;
	int retry_time=0;
	pid_t pid = gettid();
	pthread_t tid = pthread_self();
	int thread_set_counts = 0;
	int thread_get_counts =0;
	int need_read=0;
	struct BOX* box_item;
	if (server_instance->server_id != 0)
	{
		box_item= intial_box((int)(server_instance->server_id), server_instance->config.nets_cnt, 0);
		if(box_item == NULL)
		{
			ERROR_LOG("Init box failed!");
			exit(-1);
		}
	}
	else
		box_item=NULL;

	box[init_data->partition_id] = box_item ;
	INFO_LOG("Pid [%d] Tid [%ld]", pid, tid);
	type = init_data->thread_type;
	partition_set_count[partition_id] = 0;
	partition_get_count[partition_id] = 0;
	penalty_partition_count[partition_id] = 0;
	read_dirty_count[partition_id] = 0;
	switch (type)
	{
		case DHMP_MICA_SEND_INFO_REQUEST:
			mgr = &mica_work_context_mgr[0];
			break;
		case DHMP_MICA_SEND_INFO_RESPONSE:
			mgr = &mica_work_context_mgr[1];
			break;
		default:
			break;
	}
	lock = &(mgr->bit_locks[partition_id]);
 


	ERROR_LOG("Server work thread [%d] launch,[%d]!", partition_id,avg_partition_count_num);
int jd = 0;

double  need_read_count= 0,read_per_update;
                int read_in_this_term = 0;
        read_per_update = workload_WriteRate ;
        thread_get_counts = 0;

if(workload_WriteRate == (double)99 || workload_WriteRate == (double)150)
revise_time = 300;
else if (workload_WriteRate == (double)0)
revise_time = 0;
else
	revise_time = 200;
long long int gap=0;
 
if(server_instance->server_id == MAIN_NODE_ID)
{	
	while(set_workload_OK == 0) ;
	ERROR_LOG("Server work thread [%d] launch,[%d]!", partition_id,avg_partition_count_num);
	partition_credict_index[partition_id] = 0;
	for(i=0;i<PARTITION_MAX_CREDICT;i++)
	{
		partition_credict[partition_id][i] = NULL;
	}	

partition_work_nums[partition_id] = 0;


 
	list_for_each_entry_safe(msg, temp_msg, &(work_list[partition_id]), list_anchor)
	{
	usleep(revise_time);
		/**
 * 		 * handle cq info before execute request 
 * 		 		 * 
 * 		 		 		 */


#ifdef MAIN_LOG_DEBUG_LATENCE
struct timespec start_g[PARTITION_MAX_NUMS], end_g[PARTITION_MAX_NUMS];
		if (server_instance->server_id == 0 
 &&			partition_id == 0)
		{
				partition_count_num++;
				clock_gettime(CLOCK_MONOTONIC, &(start_g[partition_id]));	
		}
#endif


		if(partition_work_nums[partition_id])
		{
			partition_lock(lock);
			if (!list_empty(&main_thread_send_list[partition_id]))
			{
				list_replace(&main_thread_send_list[partition_id], &partition_local_send_list[partition_id]); 
				INIT_LIST_HEAD(&main_thread_send_list[partition_id]);   
				partition_work_nums[partition_id] = 0;
				partition_unlock(lock);
			}
			else
			{
				partition_unlock(lock);
			}
		}
		list_for_each_entry_safe(msg1, temp_msg1, &(partition_local_send_list[partition_id]), list_anchor)
		{
			Assert(msg1->list_anchor.next != LIST_POISON1 && msg1->list_anchor.prev!= LIST_POISON2);
			bool need_post_recv = true, is_set=false;

			__dhmp_wc_recv_handler(msg1->trans, msg1, partition_id, &need_post_recv, &is_set);
			list_del_init(&msg1->list_anchor);

			if (need_post_recv)
			{
				dhmp_post_recv(msg1->trans, msg1->data - sizeof(enum dhmp_msg_type) - sizeof(size_t), msg1->recv_partition_id);
				free(container_of(&(msg1->data), struct dhmp_msg , data));	// #define container_of(ptr, type, member)
			}
			if (is_set)
			{
				ERROR_LOG("mica_work_thread need consider cq requests");
				exit(-1);
			}
		}

		INIT_LIST_HEAD(&partition_local_send_list[partition_id]);

		/**
 * 		 * execute request 
 * 		 		 * 
 * 		 		 		 */
		bool need_post_recv = true, is_set=false;
		__dhmp_wc_recv_handler(msg->trans, msg, partition_id, &need_post_recv, &is_set);
		list_del_init(&msg->list_anchor);

		if (need_post_recv)
		{
			dhmp_post_recv(msg->trans, msg->data - sizeof(enum dhmp_msg_type) - sizeof(size_t), msg->recv_partition_id);
			free(container_of(&(msg->data), struct dhmp_msg , data));	
		}

		if (is_set)
		{
			long long int latency;
			struct timespec start, end;
			if ( partition_id == 0)
				clock_gettime(CLOCK_MONOTONIC, &start);
			{
				need_read_count += read_per_update;
				read_in_this_term = finish_read(box_item, partition_id, need_read_count, thread_get_counts,msg->trans);
                                                thread_get_counts += read_in_this_term;
                                                need_read_count = need_read_count - (double)read_in_this_term;
				
			}
			if ( partition_id == 0)
			{
				clock_gettime(CLOCK_MONOTONIC, &end);
				latency = (((end.tv_sec * 1000000000) + end.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec));
				total_read_latency_time += latency;
			 
			}
		}
		thread_set_counts++;
#ifdef MAIN_LOG_DEBUG_LATENCE
	if (server_instance->server_id == 0 
 &&			partition_id == 0)
		{
			{
				long long int latency;
				clock_gettime(CLOCK_MONOTONIC, &(end_g[partition_id]));
				latency= ((((end_g[partition_id].tv_sec * 1000000000) + end_g[partition_id].tv_nsec) - ((start_g[partition_id].tv_sec * 1000000000) + start_g[partition_id].tv_nsec)));
		if(partition_count_num >= for_stable)
				total_set_latency_time += latency;
				partition_count_num++;
				gap++;
				if (partition_count_num == avg_partition_count_num || partition_count_num % 1000== 0)
				{
					la_temp = total_set_latency_time;
					double result2 = la_temp / US_BASE;
					double result1 = result2/gap;
					gap = 0;
					printf("*****************************************************************\n");
					printf("		Write Latency = [%.2lf] us, Write Throughput = [%.3lf] Mops\n", result1, PARTITION_NUMS *(server_instance->config.nets_cnt)/result1);

			total_set_latency_time = 0;
				}
			}
		}
#endif
	}
if (partition_id == 0)
{
	double result1 = total_read_latency_time/(double)US_BASE;
	result1 = result1/ (double)(thread_get_counts);
	printf("*****************************************************************\n");
	printf("		Read Latency = [%.2lf]us Read Throughput = [%.3lf] Mops\n", result1, PARTITION_NUMS *(server_instance->config.nets_cnt)/result1);
}

ERROR_LOG("send over read=[%d] set = [%d]",thread_get_counts,thread_set_counts);
		while (true)
		{
		}
	}
	else
	{
		struct dhmp_transport* trans = NULL;
	//if(server_instance->server_id >= REPLICA_NODE_HEAD_ID)
		while(trans == NULL)
		{	
			trans = find_connect_client_by_nodeID(0, partition_id + PARTITION_NUMS);
			sleep(1);
		}
while(true)
		{
			int is_need=0;
			while(need_read_round[partition_id] || is_need > 0)
			{
				volatile uint64_t * lock;
				lock = &lhd_bit_locks[partition_id];
				if(need_read_round[partition_id] > 0)
				{
					partition_lock(lock);
					need_read_round[partition_id]--;
					partition_unlock(lock);
					need_read_count += read_per_update;
				}

				if(partition_id == 0)
						clock_gettime(CLOCK_MONOTONIC, &(start_r));
 
				read_in_this_term = finish_read(box_item, partition_id, need_read_count, thread_get_counts, trans);
                                thread_get_counts += read_in_this_term;
                                need_read_count = need_read_count - (double)read_in_this_term;
 
				if(partition_id == 0 && read_in_this_term > 0)
						{
						clock_gettime(CLOCK_MONOTONIC, &(end_r));
						long long int latency;
						latency= ((((end_r.tv_sec * 1000000000) + end_r.tv_nsec) - ((start_r.tv_sec * 1000000000) + start_r.tv_nsec)));
						total_read_latency_time += latency;
 
						}
				is_need = need_read_count;
	
			}
}





}
}

static struct post_datagram * 
dhmp_mica_get_MR_request_handler(struct dhmp_transport* rdma_trans,
									struct post_datagram *req)
{
	if (IS_MIRROR(server_instance->server_type))
		return dhmp_mica_get_Mirr_MR_request_handler(rdma_trans, req);
	else if (IS_REPLICA(server_instance->server_type))
		return dhmp_mica_get_cli_MR_request_handler(rdma_trans, req);
	else
	{
#ifdef RTT_TEST
		return dhmp_mica_get_Mirr_MR_request_handler(rdma_trans, req);
#endif
		exit(-1);
	}
}

#define MAIN_LOG_DEBUG

int countbad = 0;
size_t
dhmp_mica_main_replica_set_request_handler(struct dhmp_transport* rdma_trans, struct post_datagram *req)
{
	DEFINE_STACK_TIMER();
	// MICA_TIME_COUNTER_INIT();
	struct post_datagram *resp, *peer_req_datagram;
	struct dhmp_mica_set_request  * req_info;
	struct dhmp_mica_set_response * set_result;
	size_t resp_len = sizeof(struct dhmp_mica_set_response);
	struct mehcached_item * item;
	struct mehcached_table *table = &table_o;
	bool is_update, is_maintable = true;
	struct dhmp_msg resp_msg;
	struct dhmp_msg *resp_msg_ptr;
	size_t reuse_length; 
	int reval;
	int set_id;
	int i;

	void * key_addr;
	void * value_addr;

	if (IS_REPLICA(server_instance->server_type))
		Assert(replica_is_ready == true);

	req_info  = (struct dhmp_mica_set_request *) DATA_ADDR(req, 0);
	peer_req_datagram = req->req_ptr;
	reuse_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_set_request) + req_info->key_length;
	key_addr = (void*)req_info->data;	

 
	if (IS_REPLICA(server_instance->server_type))
		value_addr = (void*) 0x1;
	else
		value_addr = (void*)key_addr + req_info->key_length - VALUE_HEADER_LEN;

 
	if (!IS_HEAD(server_instance->server_type) &&
		!IS_MIRROR(server_instance->server_type) &&
		!IS_MAIN(server_instance->server_type))
		resp_len += req_info->key_length;	
 
	int partition_id = req_info->partition_id;
	//ERROR_LOG("thread [%d] rdna [%p] set [%d] keylength=[%d]",partition_id,rdma_trans,req_info->key_hash,req_info->key_length);

	if (IS_MAIN(server_instance->server_type))
	{
		while(1)
		{
			if(partition_credict[partition_id][partition_credict_index[partition_id]] == NULL)
				break;
			int temp,temp_flag;	
 			temp_flag = 0;
 			for(temp = 1;temp < REPLICA_NODE_HEAD_ID;temp++)
 				temp_flag = temp_flag + mirror_node_mapping[temp][partition_id].in_used_flag;
 			if(temp_flag != 0)
				continue;
			if(partition_credict[partition_id][partition_credict_index[partition_id]]->count_num == 0) 
				break;
		}
		partition_credict[partition_id][partition_credict_index[partition_id]] = req_info;
		partition_credict_index[partition_id] = (partition_credict_index[partition_id] + 1) % current_credict;
		
		//INFO_LOG("main node broadcast metadataing");
		main_node_broadcast_matedata(req_info, req, reuse_length);
	}

	// MICA_TIME_COUNTER_INIT();
	//
		if(box[partition_id] != NULL )
	{	
		int i =0;
               //         for(;i < (__test_size/request_base);i++)
                        {
				box[partition_id]->array[req_info->key_hash] = 1;
				}
	}
//	if(partition_id == 4)
///		ERROR_LOG("thread [%d] set [%d] ",partition_id,req_info->key_hash);
	for (i=0; i<CLINET_NUMS; i++)
	{
		item = mehcached_set(req_info->current_alloc_id,
							table,
							req_info->key_hash,
							key_addr,
							req_info->key_length,
							value_addr,
							req_info->value_length,
							req_info->expire_time,
							req_info->overwrite,
							&is_update,
							&is_maintable,
							NULL);
	//ERROR_LOG(" item addr is %p   item->mapping_id = %d offset = %p", item->data, item->mapping_id,get_offset_by_item(main_table, item));
	}

//		ERROR_LOG("receive write %d",req_info->value_length);

	// if (req_info->partition_id==0)
	// 	MICA_TIME_COUNTER_CAL("[set]->[mehcached_set]");
	if (IS_REPLICA(server_instance->server_type))
	{
		Assert(is_maintable == true);
		
	}

 
	partition_set_count[req_info->partition_id]++;
	set_id = partition_set_count[req_info->partition_id];
	if (!IS_MAIN(server_instance->server_type))
	{
		struct dhmp_task *send_task_ptr;
		struct ibv_send_wr send_wr,*bad_wr=NULL;
		struct ibv_sge sge;

		resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
		memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));
		set_result = (struct dhmp_mica_set_response *) DATA_ADDR(resp, 0);
		set_result->partition_id = req_info->partition_id;
	if(penalty_count[req_info->partition_id] >0 )
		{
			set_result->tag = -1;
			penalty_count[req_info->partition_id] --;
		}
		else
			set_result->tag = 0;
		if (item != NULL)
		{
		 
			set_result->value_addr = (uintptr_t ) item_get_value_addr(item);
			set_result->out_mapping_id = item->mapping_id;
			set_result->is_success = true;
			// INFO_LOG("MICA node [%d] get set request, set key_hash is \"%lx\",  mapping id is [%u] , value addr is [%p]", \
							server_instance->server_id, req_info->key_hash, set_result->out_mapping_id, set_result->value_addr);
		}
		else
		{
			ERROR_LOG("MICA node [%d] get set request, set key_hash is \"%lx\", set key FAIL!", \
					server_instance->server_id, req_info->key_hash);
			set_result->out_mapping_id = (size_t) - 1;
			set_result->is_success = false;
		//	Assert(false);
		}

		 
		resp->req_ptr  = peer_req_datagram;		    		 
		resp->resp_ptr = resp;					 
		resp->node_id  = server_instance->server_id;	 
		resp->info_type = MICA_SET_RESPONSE;
		resp->info_length = resp_len;
		resp->done_flag = false;						 
		
		resp_msg.msg_type = DHMP_MICA_SEND_INFO_RESPONSE;
		resp_msg.data_size = DATAGRAM_ALL_LEN(resp->info_length);
		resp_msg.data= resp;
		send_task_ptr=dhmp_send_task_create(rdma_trans, &resp_msg, req_info->partition_id);
		if(!send_task_ptr)
		{
			ERROR_LOG("create recv task error.");
			return ;
		}
		memset ( &send_wr, 0, sizeof ( send_wr ) );
		send_wr.wr_id= ( uintptr_t ) send_task_ptr;
		send_wr.sg_list=&sge;
		send_wr.num_sge=1;
		send_wr.opcode=IBV_WR_SEND;
		send_wr.send_flags=IBV_SEND_SIGNALED;
		sge.addr= ( uintptr_t ) send_task_ptr->sge.addr;
		sge.length=send_task_ptr->sge.length;
		sge.lkey=send_task_ptr->sge.lkey;
		reval = ibv_post_send ( rdma_trans->qp, &send_wr, &bad_wr );

		if (reval)
		{
			ERROR_LOG("ibv_post_send error[%d], reason is [%s]", errno,strerror(errno));
			exit(-1);
		}
 
	}
	else
	{
		size_t item_offset;

#ifdef WAIT_RETURN
		if (item != NULL)
			main_node_broadcast_matedata_wait(req_info, req_info->partition_id, item);
		else
		{
			ERROR_LOG("MICA node [%d] get set request, set key_hash is \"%lx\", set key FAIL!", \
					server_instance->server_id, req_info->key_hash);
			exit(-1);
		}
#endif
		if (is_maintable)
			item_offset = get_offset_by_item(main_table,item);
		else
			item_offset = get_offset_by_item(log_table, item);
#ifdef ChainRep
		if(REPLICA_NODE_NUMS > 0)
		 makeup_update_request(item, item_offset,\
		 						(uint8_t*)item_get_value_addr(item), \
		 						MEHCACHED_VALUE_LENGTH(item->kv_length_vec),\
		 						req_info->tag,
		 						req_info->partition_id,req_info->key_hash);
#endif
	}

	if (partition_set_count[req_info->partition_id] == avg_partition_count_num)
	{
		partition_count_set_done_flag[req_info->partition_id] = true;
	}

//	INFO_LOG("key_hash is %lx, val_len is %lu, addr is %p ", req_info->key_hash, req_info->value_length, key_addr);
	return req_info->partition_id;
}

static size_t 
dhmp_mica_mirror_set_request_handler(struct dhmp_transport* rdma_trans, uint32_t partition_id)
{
	struct post_datagram *resp;
	struct dhmp_mica_set_response * set_result;
	size_t resp_len = sizeof(struct dhmp_mica_set_response);
	struct mehcached_item * item;
	struct mehcached_table *table = &table_o;
	bool is_update, is_maintable = true;
	struct dhmp_msg resp_msg;
	struct dhmp_msg *resp_msg_ptr;

	resp = (struct post_datagram *) malloc(DATAGRAM_ALL_LEN(resp_len));
	memset(resp, 0, DATAGRAM_ALL_LEN(resp_len));
	set_result = (struct dhmp_mica_set_response *) DATA_ADDR(resp, 0);
	set_result->is_success = true;
	set_result->partition_id = (int)partition_id;
	if(penalty_count[partition_id]>0 )
	{
		penalty_count[partition_id]--;
		set_result->tag = -1;
	}
	else
		set_result->tag = 0;
	
 
	resp->req_ptr  = NULL;		    		 
	resp->resp_ptr = resp;							 
	resp->node_id  = server_instance->server_id;	 
	resp->info_type = MICA_SET_RESPONSE;
	resp->info_length = resp_len;
	resp->done_flag = false;						 

	partition_set_count[partition_id]++;
#ifdef TEST_ON_CHT
	if(IS_MIRROR(server_instance->server_type))
             {struct timespec start_through, end_through;
clock_gettime(CLOCK_MONOTONIC, &start_through); 
                 char * commu_buffer = malloc( CLINET_NUMS * __test_size);
                char * value_addr = malloc( CLINET_NUMS * __test_size);
              int i;
			snprintf(value_addr,__test_size, "hello world");
               for (i=0; i<CLINET_NUMS; i++)
                  memcpy(commu_buffer + i * __test_size,value_addr,__test_size);
             free(commu_buffer);
              free(value_addr);
clock_gettime(CLOCK_MONOTONIC, &end_through); 
				total_through_time = ((((end_through.tv_sec * 1000000000) + end_through.tv_nsec) - ((start_through.tv_sec * 1000000000) + start_through.tv_nsec)));
			//	ERROR_LOG("set op  time is [%d] us", total_through_time / 1000);
            }
#endif       


	resp_msg_ptr = make_basic_msg(&resp_msg, resp);
	dhmp_post_send(rdma_trans, resp_msg_ptr, (size_t)partition_id);

if(box[partition_id] != NULL )
{
int i =0;
                        // for(;i < ((__test_size/request_base));i++)
                         {
		box[partition_id]->array[write_num_partition[partition_id][write_num_partition_index[partition_id]]] = 0;
		write_num_partition_index[partition_id] = (write_num_partition_index[partition_id] + 1) % max_num;
		box[partition_id]->array[write_num_partition[partition_id][write_num_partition_index[partition_id]]] = 1;
//	if(partition_id == 0)
//		ERROR_LOG("thread[%d], set [%d] and clean the last",partition_id, write_num_partition[partition_id][write_num_partition_index[partition_id]]);	
}
	}

/*	if (!IS_MAIN(server_instance->server_type))
	{
		volatile uint64_t * lock;
		lock = &lhd_bit_locks[partition_id];
		partition_lock(lock);
		need_read_round[partition_id]++;
		partition_unlock(lock);
	}
*/
	return (size_t)partition_id;
}

size_t
dhmp_mica_set_request_handler(struct dhmp_transport* rdma_trans, struct post_datagram *req, uint32_t imm_data)
{
if(workloadf == 1)
{
	size_t key = (size_t)imm_data;
	size_t except_length = __test_size + VALUE_HEADER_LEN + VALUE_TAIL_LEN;
	dhmp_mica_get_request_handler_by_addr(&key, sizeof(size_t), except_length);
}
	if (IS_MAIN(server_instance->server_type) || 
		IS_REPLICA(server_instance->server_type))
		return dhmp_mica_main_replica_set_request_handler(rdma_trans, req);
	else if (IS_MIRROR(server_instance->server_type))
		return dhmp_mica_mirror_set_request_handler(rdma_trans, imm_data);
	else
	{
		ERROR_LOG("dhmp_mica_set_request_handler");
		exit(-1);
	}

	return (size_t) -1;
}

static void
dhmp_set_client_response_handler(struct post_datagram *resp, 
									struct post_datagram *req, 
									struct dhmp_mica_set_response *resp_info, 
									struct dhmp_mica_set_request  *req_info)
{
	req_info->out_mapping_id = resp_info->out_mapping_id;
	req_info->out_value_addr = resp_info->value_addr;
	req_info->is_success = resp_info->is_success;		
	resp->req_ptr->done_flag = true;
}

static void
dhmp_set_server_response_handler(struct post_datagram *resp, 
									struct post_datagram *req, 
									struct dhmp_mica_set_response *resp_info, 
									struct dhmp_mica_set_request  *req_info)
{
	DEFINE_STACK_TIMER();
	
	if (IS_MAIN(server_instance->server_type))
	{
		if(resp_info->tag == -1)
		{
			penalty_count[resp_info->partition_id] = 1;
		}
		if (resp->node_id < REPLICA_NODE_HEAD_ID)
		{
		
			//INFO_LOG("Main node get Mirror node set response"); 
			// __sync_fetch_and_add(&mirror_node_mapping[resp_info->partition_id].in_used_flag, 1);
			mirror_node_mapping[resp->node_id][resp_info->partition_id].in_used_flag = 0; 
			//ERROR_LOG("---MIRROR_NODE_ response to main node,pid [%d]", resp_info->partition_id);
			return;
		}
		else
		{
 
			if (resp->node_id == REPLICA_NODE_HEAD_ID)
			{
				req_info->out_mapping_id = resp_info->out_mapping_id;
				req_info->out_value_addr = resp_info->value_addr;
				req_info->is_success = resp_info->is_success;	
				if (req_info->out_mapping_id == (size_t)-1)
				{
					ERROR_LOG("Main node set node[%d] tag[%d] failed!", REPLICA_NODE_HEAD_ID, req_info->tag);
					exit(-1);
				}
#ifdef UD_META
				req_info->count_num = req_info->count_num - REPLICA_NODE_NUMS +1;
#endif
			} 
			req_info->count_num--; 
			// resp->req_ptr->done_flag = true;
			return;
		}
	}
	else if (IS_REPLICA(server_instance->server_type))
	{
		Assert(replica_is_ready == true);
 
		uint64_t key_hash = resp_info->key_hash;
		size_t key_length = resp_info->key_length;
		uint8_t * key_addr = (uint8_t*)resp_info->key_data;
		struct mehcached_item * target_item;
		struct mehcached_table *table = &table_o; // 副本节点只有一个 table

	 
		return;
	}
}

static void 
dhmp_set_response_handler(struct dhmp_msg* msg)
{
	struct post_datagram *resp = (struct post_datagram *) (msg->data); 
	struct post_datagram *req  = resp->req_ptr; 
	struct dhmp_mica_set_response *resp_info = (struct dhmp_mica_set_response *) DATA_ADDR(resp, 0);
	struct dhmp_mica_set_request  *req_info = (struct dhmp_mica_set_request *) DATA_ADDR(req, 0);

	if (server_instance == NULL)
		dhmp_set_client_response_handler(resp, req, resp_info, req_info);
	else
		dhmp_set_server_response_handler(resp, req, resp_info, req_info);

	// INFO_LOG("Node [%d] recv set resp from node[%d]!, is success!", \
			server_instance!=NULL ? server_instance->server_id : client_mgr->self_node_id,resp->node_id);
}

static void 
dhmp_send_respone_handler(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg, bool *is_async, __time_t time_start1, __syscall_slong_t time_start2)
{
	struct post_datagram *req = (struct post_datagram*)(msg->data);
	struct timespec end;
 
	if (server_instance == NULL)
	{
		if (req->info_type == MICA_GET_RESPONSE)
			*is_async = true;	 
		else
			*is_async = false;
		__dhmp_send_respone_handler(rdma_trans, msg, PARTITION_NUMS);
		return;
	}

	switch (req->info_type)
	{
		case MICA_ACK_RESPONSE:
		case MICA_GET_CLIMR_RESPONSE:
		case MICA_SERVER_GET_CLINET_NODE_ID_RESPONSE:
		case MICA_REPLICA_UPDATE_RESPONSE:

		case MICA_GET_RESPONSE:
		case MICA_SET_RESPONSE:
		case MICA_SET_RESPONSE_TEST:
			 
			__dhmp_send_respone_handler(rdma_trans, msg, PARTITION_NUMS);
			*is_async = false;
			break;
		 case GET_TOMASTER_RESPONSE:
                        get_tomaster_respond_handler(rdma_trans, msg);
					*is_async = false;
                        break;

			 
		default:
			ERROR_LOG("Unknown request info_type %d  %d", req->info_type  , GET_TOMASTER_RESPONSE);
			break;
	}

	return ;
}

 
void dhmp_send_request_handler(struct dhmp_transport* rdma_trans,
								struct dhmp_msg* msg, 
								bool * is_async,
								__time_t time_start1, 
								__syscall_slong_t time_start2, 
								bool is_cq_thread)
{
	int i;
	struct post_datagram *req = (struct post_datagram*)(msg->data);
	struct timespec end;
	bool is_get=true;
	bool is_need_post_recv=true;
	bool temp;

	switch (req->info_type)
	{
		case GET_TOMASTER_REQUEST:
                get_tomaster_request_handler(rdma_trans, msg);
                *is_async = false;
                        break;
		case MICA_ACK_REQUEST:
		case MICA_GET_CLIMR_REQUEST:
		case MICA_SERVER_GET_CLINET_NODE_ID_REQUEST:
		case MICA_REPLICA_UPDATE_REQUEST:
		case MICA_SET_REQUEST_TEST:
			 
			__dhmp_send_request_handler(rdma_trans, msg, PARTITION_NUMS, &is_need_post_recv, &temp);
			// msg->main_thread_set_id = -1;
			// distribute_partition_resp(0,  rdma_trans, msg,0, 0);
			*is_async = false;
			Assert(is_need_post_recv == true);
			break;
		case MICA_SET_REQUEST:
			if (server_instance->server_id ==0 && !is_cq_thread)
			{
				set_counts++;
				is_get=false;
			}

			if (server_instance->server_id !=0 && is_cq_thread)
			{
				set_counts++;
				is_get=false;
			}

		case MICA_GET_REQUEST: 
#ifdef THROUGH_TEST
			if (server_instance->server_id !=0 && is_cq_thread && set_counts ==for_stable)
				clock_gettime(CLOCK_MONOTONIC, &start_through); 
#endif
			if (server_instance->server_id ==0 && !is_cq_thread && !is_get)
				msg->main_thread_set_id = set_counts;  
			
			if (server_instance->server_id !=0 && is_cq_thread &&  !is_get)
				msg->main_thread_set_id = set_counts; 
 
			distribute_partition_resp(get_req_partition_id(req), rdma_trans, msg, time_start1, time_start2,is_cq_thread);

#ifdef THROUGH_TEST 
			if ( (server_instance->server_id == 0 && set_counts == update_num && !is_cq_thread) )
			{
				bool done_flag;
				while (true)
				{
					done_flag = true;
					for (i=0; i<PARTITION_NUMS; i++)
						done_flag &= partition_count_set_done_flag[i];
					
					if (done_flag)
						break;
				}
				total_through_time = ((((end_through.tv_sec * 1000000000) + end_through.tv_nsec) - ((start_through.tv_sec * 1000000000) + start_through.tv_nsec)));
				ERROR_LOG("set op count [%d], total op count [%d] total time is [%d] us", set_counts, __access_num - for_stable, total_through_time / 1000);
				size_t total_ops_num=0, total_get_ops_num=0, total_penalty_num=0;

				for (i=0; i<(int)PARTITION_NUMS; i++)
				{
					ERROR_LOG("partition[%d] set count [%d]",i, partition_set_count[i]);
					total_ops_num+=partition_set_count[i];
				}
				for (i=0; i<(int)PARTITION_NUMS+1; i++)
				{
					ERROR_LOG("partition[%d] get count [%d]",i, partition_get_count[i]);
					total_get_ops_num+=partition_get_count[i];
				}
				for (i=0; i<(int)PARTITION_NUMS+1; i++)
				{
					ERROR_LOG("penalty_partition_count[%d] get count [%d]",i, penalty_partition_count[i]);
					total_penalty_num+=penalty_partition_count[i];
				}
			}
#endif
			*is_async = true;
			break;
		default:
			ERROR_LOG("Unknown request info_type %d %d", req->info_type, MICA_SET_REQUEST);
//			exit(0);
			break;
	}

	return ;
}

void
exit_print_status()
{
	int i;
	size_t total_ops_num=0, total_get_ops_num=0, total_penalty_num=0;
	double result1 = total_read_latency_time/ US_BASE;
	result1 = result1/ (double)(partition_get_count[0]);
	printf("*****************************************************************\n");
	printf("			Read Latency = [%.2lf]us Read Throughput = [%.3lf] Mops\n", result1, PARTITION_NUMS *(server_instance->config.nets_cnt)/result1);
	for (i=0; i<(int)PARTITION_NUMS; i++)
	{
		ERROR_LOG("partition[%d] set count [%d]",i, partition_set_count[i]);
		total_ops_num+=partition_set_count[i];
	}
	for (i=0; i<(int)PARTITION_NUMS; i++)
	{
		ERROR_LOG("partition[%d] get count [%d]",i, partition_get_count[i]);
		total_get_ops_num+=partition_get_count[i];
	}
	for (i=0; i<(int)PARTITION_NUMS; i++)
	{
		ERROR_LOG("penalty_partition_count[%d] get count [%d]",i, penalty_partition_count[i]);
		total_penalty_num+=penalty_partition_count[i];
	}
}



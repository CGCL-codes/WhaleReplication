#include "dhmp.h"
#include "dhmp_log.h"
#include "dhmp_config.h"
#include "dhmp_context.h"
#include "dhmp_dev.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"
#include "dhmp_client.h"
#include "dhmp_server.h"
#include "dhmp_log.h"
#include "mid_rdma_utils.h"
#include "dhmp_top_api.h"

bool 
dhmp_post_send_info(size_t target_id, void * data, size_t length, struct dhmp_transport *specify_trans, size_t partition_id);


void
micaserver_get_cliMR(struct replica_mappings  *resp_mapping_ptr, size_t target_id)
{
ERROR_LOG("micaserver_get_cliMR");
	void * base;
	struct post_datagram *req_msg;
	struct dhmp_mica_get_cli_MR_request *req_data;
	size_t total_length = 0;
	bool re;

	// size_t target_id = server_instance->server_id + 1;
	// Assert(target_id != server_instance->node_nums);


	total_length = DATAGRAM_ALL_LEN(sizeof(struct dhmp_mica_get_cli_MR_request));
	base = malloc(total_length); 
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_mica_get_cli_MR_request *) DATA_ADDR(base, 0);


#ifdef RTT_TEST
	if (server_instance == NULL)
		req_msg->node_id = 1;
	else
#endif
		req_msg->node_id = server_instance->server_id;	
	req_msg->req_ptr = req_msg;
	req_msg->resp_ptr = NULL;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_GET_CLIMR_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_mica_get_cli_MR_request);


	req_data->info_revoke_ptr = resp_mapping_ptr;
	INFO_LOG("resp_all_mapping_ptr is [%p]", resp_mapping_ptr);

	if (!dhmp_post_send_info(target_id, base, total_length, NULL, 0))
	{
		ERROR_LOG("POST_SEND ERROR! target_id is [%d], info_length is [%u]", target_id, total_length);
		return;
	}

	DEFINE_STACK_TIMER();
	MICA_TIME_COUNTER_INIT();
	while(req_msg->done_flag == false)
		MICA_TIME_LIMITED(0, 100*TIMEOUT_LIMIT_MS);

out:
	free(base);
	return;
}


enum ack_info_state
mica_basic_ack_req(size_t target_id, enum ack_info_type ack_type, bool block, struct ibv_mr* leader_mr)
{
ERROR_LOG("mica_basic_ack_req [%d]",target_id);
	void * base;
	struct post_datagram *req_msg;
	struct dhmp_mica_ack_request *req_data;
	size_t total_length = 0;
	bool re;
	enum ack_info_state resp_state;

	
	// 构造报文
	total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_ack_request);
	base = malloc(total_length); 
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_mica_ack_request *)((char *)base + sizeof(struct post_datagram));

	memcpy(req_data->leader_mr, leader_mr, 4*sizeof(struct ibv_mr));

	// 填充公共报文
	req_msg->node_id = server_instance->server_id;	 // 向对端发送自己的 node_id 用于身份辨识
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_ACK_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_mica_ack_request);

	// 填充私有报文
	req_data->ack_type = ack_type;
sleep(1);
	if (!dhmp_post_send_info(target_id, base, total_length, NULL, 0))
	{
		ERROR_LOG("POST_SEND ERROR! target_id is [%d], info_length is [%u]", target_id, total_length);
		return -1;
	}

	if (block)
	{
		DEFINE_STACK_TIMER();
		MICA_TIME_COUNTER_INIT();
		while(req_msg->done_flag == false)
			MICA_TIME_LIMITED(0, TIMEOUT_LIMIT_MS);
	}


	resp_state = req_data->ack_type;
	free(base);
	return resp_state;
}

int
mica_ask_nodeID_req(struct dhmp_transport* new_rdma_trans)
{
//ERROR_LOG("mica_ask_nodeID_req");
	void * base;
	struct post_datagram *req_msg;
	struct dhmp_get_nodeID_request *req_data;
	size_t total_length = 0;
	int result = -1;


	total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_get_nodeID_request);
	base = malloc(total_length); 
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_get_nodeID_request *)((char *)base + sizeof(struct post_datagram));


	req_msg->node_id = server_instance->server_id;	
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_SERVER_GET_CLINET_NODE_ID_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_get_nodeID_request);


	req_data->node_id = server_instance->server_id;

	if (!dhmp_post_send_info(-1, base, total_length, new_rdma_trans, 0))
		return -1;

	DEFINE_STACK_TIMER();
	MICA_TIME_COUNTER_INIT();
	while(req_msg->done_flag == false)
		MICA_TIME_LIMITED(0, 100*TIMEOUT_LIMIT_MS);
	result = req_data->node_id;
	free(base);
	return result;
}

/*
uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash,\
                const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length,\
                uint32_t expire_time, bool overwrite

*/
size_t
mica_set_reuse(size_t target_id, size_t self_id, size_t tag, void* base, size_t total_length, size_t partition_id)
{
	return mica_set_remote(0,0,NULL,0,NULL,0,0,0,
							true,
							NULL,
							target_id,
							false,
							self_id,
							tag,
							true,
							base,
							total_length,
							partition_id);
}

size_t
mica_set_remote(uint8_t current_alloc_id,  uint64_t key_hash, const uint8_t *key, 
				size_t key_length, const uint8_t *value, size_t value_length,
                uint32_t expire_time, bool overwrite, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id,
				bool is_update,
				size_t self_node_id,
				size_t tag,
				bool re_use_datagram,
				void* req_base,
				size_t __total_length,
				size_t __partition_id)
{
	void * base;
	struct post_datagram *req_msg;
	struct dhmp_mica_set_request *req_data;
	size_t total_length = 0;
	size_t re_mapping_id=0;
	int partition_id;
	size_t tmp_key;

	// HexDump((char*)key, (int) (key_length + value_length), (size_t)key);
	if (!re_use_datagram)
	{

		if (target_id < REPLICA_NODE_HEAD_ID)
			total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_set_request) + key_length + value_length;
		else
			total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_set_request) + key_length;
		
		base = malloc(total_length); 

		req_msg  = (struct post_datagram *) base;
		req_data = (struct dhmp_mica_set_request *)((char *)base + sizeof(struct post_datagram));


		req_msg->node_id = self_node_id;	
		req_msg->req_ptr = req_msg;
		req_msg->done_flag = false;
		req_msg->info_type = MICA_SET_REQUEST;
		req_msg->info_length = sizeof(struct dhmp_mica_set_request);


		req_data->count_num = 0;
		req_data->current_alloc_id = current_alloc_id;
		req_data->expire_time = expire_time;
		req_data->key_hash = key_hash;
		req_data->key_length = key_length;
		req_data->value_length = value_length;	
		req_data->overwrite = overwrite;
		req_data->is_update = is_update;
		req_data->tag = tag;
		// req_data->partition_id = (int) (*((size_t*)key)  % (PARTITION_NUMS));
		tmp_key = *((size_t*)(key));
		tmp_key = tmp_key>>16;
		req_data->partition_id = ((int) tmp_key) % ((int)PARTITION_NUMS);
		
		memcpy(&(req_data->data), key, GET_TRUE_KEY_LEN(key_length));		// copy key


		Assert(req_data->partition_id>=0 && req_data->partition_id < PARTITION_NUMS);

		if (target_id < REPLICA_NODE_HEAD_ID)
			memcpy(((char*)(&(req_data->data)) + key_length), value, GET_TRUE_VALUE_LEN(value_length));

		partition_id = PARTITION_NUMS;
	}
	else
	{
		Assert(is_async == true);
		base = req_base;
		total_length = __total_length;
		partition_id = __partition_id;
	}

	if (!dhmp_post_send_info(target_id, base, total_length, NULL, partition_id))
		return (size_t) -1;

	if (is_async == false)
	{
		DEFINE_STACK_TIMER();
		MICA_TIME_COUNTER_INIT();
		while(req_msg->done_flag == false)
			MICA_TIME_LIMITED(tag, TIMEOUT_LIMIT_MS);

		if (req_data->is_success == false)
		{
			ERROR_LOG("remote set node [%d] is falied!", req_msg->node_id );
			Assert(false);
		}

		re_mapping_id = req_data->out_mapping_id;
		free(base);
	}
	else if (!re_use_datagram) 
	{
		req_callback_ptr->req_ptr = req_msg;
		req_callback_ptr->req_info_ptr = req_data;
		return (size_t) 0;
	}

	return re_mapping_id;
}

size_t
mica_set_remote_warpper(uint8_t current_alloc_id,  
				const uint8_t* no_header_key, uint64_t key_hash,size_t true_key_length, 
				const uint8_t* no_header_value, size_t true_value_length,
                uint32_t expire_time, bool overwrite, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id,
				bool is_update,
				size_t self_node_id,
				size_t tag)
{
	return mica_set_remote(current_alloc_id, key_hash, 
				no_header_key, 
				true_key_length + KEY_TAIL_LEN, 
				no_header_value, 
				VALUE_HEADER_LEN + true_value_length  + VALUE_TAIL_LEN,
				expire_time, overwrite, is_async, req_callback_ptr, target_id,is_update, self_node_id, tag, false, NULL, 0, PARTITION_NUMS);
}

void
mica_get_remote(uint8_t current_alloc_id,  uint64_t key_hash, const uint8_t *key, 
				size_t key_length, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id,
				size_t self_node_id,
				size_t expect_length,
				size_t tag,
				struct dhmp_mica_get_reuse_ptr *reuse_ptr)
{
	void * base;
	void * data_addr;
	struct post_datagram *req_msg;
	struct dhmp_mica_get_request *req_data;
	size_t total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_get_request) + key_length;
	struct dhmp_mica_get_response* get_resp;
	size_t tmp_key;


	if (reuse_ptr->resp_ptr == NULL)
	{
		get_resp = (struct dhmp_mica_get_response*) malloc(sizeof(struct dhmp_mica_get_response) + expect_length);
		reuse_ptr->resp_ptr = get_resp;
	}
	else
		get_resp = reuse_ptr->resp_ptr;
		// memset 0 ?

	if (reuse_ptr->req_base_ptr == NULL)
	{
		base = malloc(total_length);
		reuse_ptr->req_base_ptr = base;
	}
	else
		base = reuse_ptr->req_base_ptr;

	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_mica_get_request *)((char *)base + sizeof(struct post_datagram));


	req_msg->node_id = self_node_id;	 
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_GET_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_mica_get_request);


	req_data->current_alloc_id = current_alloc_id;
	req_data->key_hash = key_hash;
	req_data->key_length = key_length;
	req_data->get_resp = get_resp;
	req_data->peer_max_recv_buff_length = expect_length;
	
	//req_data->partition_id = (int) (*((size_t*)key)  % (PARTITION_NUMS));
	tmp_key = *((size_t*)(key));
	tmp_key = tmp_key>>16;
	req_data->partition_id = ((int) tmp_key) % ((int)PARTITION_NUMS);
	
	req_data->tag = tag;
	data_addr = (void*)req_data + offsetof(struct dhmp_mica_get_request, data);
	memcpy(data_addr, key, GET_TRUE_KEY_LEN(key_length));		// copy key


	if (!dhmp_post_send_info(target_id, base, total_length, NULL, PARTITION_NUMS))
	{
		free(reuse_ptr->resp_ptr);
		reuse_ptr->resp_ptr = NULL;
		return;
	}

	if (is_async == false)
	{
		DEFINE_STACK_TIMER();
		MICA_TIME_COUNTER_INIT();
		while(req_msg->done_flag == false)
			MICA_TIME_LIMITED(0, TIMEOUT_LIMIT_MS);

		// free(base);
	}
	else
	{
		assert(req_callback_ptr!=NULL);

		assert(false);
	}

	return;
}

void
mica_get_remote_warpper(uint8_t current_alloc_id,  uint64_t key_hash, const uint8_t *key, 
				size_t key_length, 
				bool is_async, 
				struct set_requset_pack * req_callback_ptr,
				size_t target_id,
				size_t self_node_id,
				size_t expect_length,
				size_t tag,
				struct dhmp_mica_get_reuse_ptr *reuse_ptrs)
{
	mica_get_remote(current_alloc_id,  key_hash, key, 
				key_length + KEY_TAIL_LEN, 
				is_async, 
				req_callback_ptr,
				target_id,
				self_node_id,
				expect_length,
				tag,
				reuse_ptrs);
}

void 
mica_replica_update_notify(uint64_t item_offset, int partition_id, int tag, uint64_t key_hash)
{
	//INFO_LOG("mica_replica_update_notify offset is [%ld]", item_offset);
	void * base;
	void * data_addr;
	struct post_datagram *req_msg;
	struct dhmp_update_notify_request *req_data;
	size_t total_length = 0;
	size_t target_id;

	if (IS_MAIN(server_instance->server_type))
		target_id = REPLICA_NODE_HEAD_ID;
	else if (IS_REPLICA(server_instance->server_type))
		target_id = server_instance->server_id + 1;
	else
		Assert(false);


	int partition_id_server;
	if(server_instance->server_id > 0)
	partition_id_server = 0;
	else 
		partition_id_server = PARTITION_MAX_NUMS;
	struct dhmp_transport *rdma_trans =NULL;
    rdma_trans = find_connect_server_by_nodeID(target_id, partition_id_server);
	

	total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_update_notify_request);
	base = malloc(total_length); 
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_update_notify_request *)((char *)base + sizeof(struct post_datagram));


	req_msg->node_id = server_instance->server_id;	
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_REPLICA_UPDATE_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_update_notify_request);


	req_data->item_offset = item_offset;
	req_data->partition_id = partition_id;
	req_data->tag = tag;
	req_data->key_hash = key_hash;


	//WARN_LOG("[mica_replica_update_notify] send key [%ld]", key_hash);
	if (!dhmp_post_send_info(-1, base, total_length, rdma_trans, partition_id))
	{
		ERROR_LOG("Send failed!");
		Assert(false);
	}


/*
	DEFINE_STACK_TIMER();
	MICA_TIME_COUNTER_INIT();
	while(req_msg->done_flag == false)
		MICA_TIME_LIMITED(tag, TIMEOUT_LIMIT_MS);
*/
	free(base);
}

bool 
dhmp_post_send_info(size_t target_id, void * data, size_t length, struct dhmp_transport *specify_trans, size_t partition_id)
{
	struct dhmp_transport *rdma_trans=NULL;
	struct dhmp_msg msg;

	if (target_id == -1)
	{
		rdma_trans = specify_trans;
		Assert(rdma_trans != NULL);
	}
	else
	{
		
		if(server_instance->server_id)
			partition_id = 0;
		rdma_trans = find_connect_server_by_nodeID(target_id, partition_id);

		if(!rdma_trans)
		{
			ERROR_LOG("don't exist remote server_instance. target_id=[%d]", target_id);
			return true;
		}
	}
	if(rdma_trans->trans_state!=DHMP_TRANSPORT_STATE_CONNECTED)
	{
	//	INFO_LOG("ERROR! Transport is not CONNECTED!, now state is \"%s\"", dhmp_printf_connect_state(rdma_trans->trans_state));
		return false;
	}
	/*build malloc request msg*/
	msg.msg_type = DHMP_MICA_SEND_INFO_REQUEST;
	msg.data_size = length;
	msg.data= data;

	Assert(length < SINGLE_NORM_RECV_REGION);

	dhmp_post_send(rdma_trans, &msg, partition_id);
out:
	return true;
}

void busy_wait_rdmawrite_send()
{
	
}

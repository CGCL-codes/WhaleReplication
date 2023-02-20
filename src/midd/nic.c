#include "dhmp.h"
#include "common.h"
#include "table.h"
#include "shm.h"
#include "dhmp_log.h"
#include "dhmp_transport.h"
#include "dhmp_server.h"
#include "dhmp_mica_shm_common.h"
#include "nic.h"
#include "util.h"

#include "mica_partition.h"

struct list_head tmp_send_list;   
struct list_head nic_send_list[PARTITION_MAX_NUMS];
uint64_t work_nums[PARTITION_MAX_NUMS];
static volatile uint64_t nic_sendQ_lock[PARTITION_MAX_NUMS];

static void memory_barrier();
volatile bool nic_thread_ready = false;

 
volatile int nic_list_length = 0;

struct ibv_mr * 
mehcached_get_mapping_self_mr(struct replica_mappings * mappings, size_t mapping_id)
{
	return &mappings->mrs[mapping_id];
}

void
nic_sending_queue_lock(int lock_id)
{
	while (1)
	{
		if (__sync_bool_compare_and_swap((volatile uint64_t *)(&nic_sendQ_lock[lock_id]), 0UL, 1UL))
			break;
	}
}

void
nic_sending_queue_unlock(int lock_id)
{
	memory_barrier();
	*(volatile uint64_t *)(&nic_sendQ_lock[lock_id]) = 0UL;
}

 
void
makeup_update_request(struct mehcached_item * item, uint64_t item_offset, const uint8_t *value, uint32_t value_length, size_t tag, int partition_id,  uint64_t key_hash)
{

    struct list_head * _new, * head, * next;
    size_t next_id;
    struct dhmp_update_request * up_req;
    int nic_partition_id;

    if (IS_MAIN(server_instance->server_type))
        next_id = REPLICA_NODE_HEAD_ID;
    else
        next_id = server_instance->server_id + 1;

    up_req = (struct dhmp_update_request *) malloc(sizeof(struct dhmp_update_request));

  
    int partition_id_server;
	if(server_instance->server_id > 0)
	partition_id_server = 0;
	else 
		partition_id_server = PARTITION_MAX_NUMS;
    up_req->write_info.rdma_trans = find_connect_server_by_nodeID(next_id, partition_id_server);
    up_req->write_info.mr = &next_node_mappings->mrs[item->mapping_id];
    //dump_mr(up_req->write_info.mr);
    up_req->write_info.local_addr = (void*)value;
    up_req->write_info.length = value_length;
    up_req->write_info.remote_addr = item->remote_value_addr;
	//ERROR_LOG("remote %p",item->remote_value_addr);
    up_req->item_offset = item_offset;
    up_req->item = item;
    up_req->partition_id = partition_id;
    up_req->tag = tag;
	up_req->key_hash = key_hash;
    // while(nic_thread_ready == false);
//	ERROR_LOG("makeup_update_request [%d]=%d %d",partition_id,key_hash, server_instance->server_id);


#ifdef NIC_MULITI_THREAD
    nic_partition_id = up_req->partition_id;
#else
    nic_partition_id = 0;
#endif
     nic_sending_queue_lock(nic_partition_id);
     // memory_barrier();
     list_add(&up_req->sending_list,  &nic_send_list[nic_partition_id]);
     work_nums[nic_partition_id]++;
     nic_sending_queue_unlock(nic_partition_id);

}

 
void * main_node_nic_thread(void * args)
{
    int partition_id = (int) (uintptr_t)args;
    int i;

    if (partition_id == 0)
    {
        for (i=0 ;i<PARTITION_NUMS; i++)
            nic_sendQ_lock[i] = 0UL;
    }

    pthread_detach(pthread_self());
    INIT_LIST_HEAD(&nic_send_list[partition_id]);
    INIT_LIST_HEAD(&tmp_send_list);
    work_nums[partition_id] = 0;

    nic_thread_ready = true;
    INFO_LOG("Node [%d] start nic thread!", server_instance->server_id);
    for(;;)
    {
        struct list_head *iter_node, *temp_node;
        int re;
        struct dhmp_update_request * send_req=NULL, * temp_send_req=NULL;

     
        if (work_nums[partition_id] == 0)
            continue;

 
        nic_sending_queue_lock(partition_id);
        //memory_barrier();
        list_replace(&nic_send_list[partition_id], &tmp_send_list); 
        INIT_LIST_HEAD(&nic_send_list[partition_id]);   
		work_nums[partition_id] = 0;
        nic_sending_queue_unlock(partition_id);

        /**
         * list_for_each_entry_safe - iterate over list of given type safe against removal of list entry
         * @pos:	the type * to use as a loop cursor.
         * @n:		another type * to use as temporary storage
         * @head:	the head for your list.
         * @member:	the name of the list_struct within the struct.
         */
        list_for_each_entry_safe(send_req, temp_send_req, &tmp_send_list, sending_list)
        {
            void * key = item_get_key_addr(send_req->item);

             re = dhmp_rdma_write_packed(&send_req->write_info, send_req->item_offset);
             if (re == -1)
             {
                 ERROR_LOG("NIC dhmp_rdma_write_packed error!,exit");
                 Assert(false);
             }
           
             mica_replica_update_notify(send_req->item_offset, send_req->partition_id, send_req->tag,  send_req->key_hash);


            list_del(&send_req->sending_list);

  
            free(send_req);
        }

     
        INIT_LIST_HEAD(&tmp_send_list);
    }
    pthread_exit(0);
}

void set_main_node_thread_addr(void* (**p)(void*))
{
    *p = main_node_nic_thread;
}

void set_replica_node_thread_addr(void* (**p)(void*))
{
    *p = main_node_nic_thread;
}

void resp_filter(struct dhmp_mica_set_request  * req_info)
{
	void * key_addr   = (void*)req_info->data;
	void * value_addr = (void*)key_addr + req_info->key_length;
}

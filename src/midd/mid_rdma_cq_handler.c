#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <arpa/inet.h>

#include "dhmp.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"
#include "dhmp_client.h"
#include "dhmp_log.h"
#include "dhmp_dev.h"
#include "dhmp_server.h"
#include "midd_mica_benchmark.h"


struct dhmp_cq* dcq_array[MAX_CQ_NUMS];
int total_cq_nums=0;
bool cq_thread_is_launch=false;
volatile bool cq_thread_stop_flag=true;
static void dhmp_wc_success_handler(struct ibv_wc* wc);
static void dhmp_wc_error_handler(struct ibv_wc* wc);

void dhmp_comp_channel_handler()
{
	// struct dhmp_cq* dcq =(struct dhmp_cq*) data;
	struct ibv_cq* cq;
	void* cq_ctx;
	struct ibv_wc wc;
	int err=0, i;

	while(cq_thread_stop_flag == true);

	while(true)
	{
		for (i=0; i<MAX_CQ_NUMS; i++)
		{
			if (dcq_array[i] == NULL)
				continue;
			
			if(ibv_poll_cq(dcq_array[i]->cq, 1, &wc))
			{
				if(wc.status==IBV_WC_SUCCESS)
{
					dhmp_wc_success_handler(&wc);
}
				else
					dhmp_wc_error_handler(&wc);
			}
	continue;
			if (server_instance->server_id != 0 && 
				!is_all_set_all_get && 
				set_counts == update_num)
			{
				if (set_counts ==1)
					clock_gettime(CLOCK_MONOTONIC, &start_through);  
				else
				{
					int i;
					bool done_flag;
					while (true)
					{
						done_flag = true;
						for (i=0; i<PARTITION_NUMS; i++)
							done_flag &= partition_count_set_done_flag[i];
						
						if (done_flag)
							break;
					}
					clock_gettime(CLOCK_MONOTONIC, &end_through); 
					total_through_time = ((((end_through.tv_sec * 1000000000) + end_through.tv_nsec) - ((start_through.tv_sec * 1000000000) + start_through.tv_nsec)));
					ERROR_LOG("set op count [%d], total op count [%d] total time is [%d] us", set_counts, __access_num, total_through_time / 1000);
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
						ERROR_LOG("read_dirty_count[%d] get count [%d]",i, read_dirty_count[i]);
						total_penalty_num+=read_dirty_count[i];
					}
					
				}
			}
		}
	}
}


static void dhmp_wc_success_handler(struct ibv_wc* wc)
{
	DEFINE_STACK_TIMER();
	struct dhmp_task *task_ptr;
	struct dhmp_transport *rdma_trans;
	struct dhmp_msg *msg;
	struct post_datagram *req_datagram;
	// struct dhmp_msg msg;
	
	bool is_async = false;
	size_t peer_partition_id = (size_t)-1;
	int recv_partition_id;
	
	//DEFINE_STACK_TIMER();
	task_ptr=(struct dhmp_task*)(uintptr_t)wc->wr_id;
	rdma_trans=task_ptr->rdma_trans;
	recv_partition_id= task_ptr->partition_id;
	switch(wc->opcode)
	{
		case IBV_WC_SEND:
			break;

		case IBV_WC_RECV_RDMA_WITH_IMM: 
			Assert(IS_MIRROR(server_instance->server_type));
			msg = (struct dhmp_msg *) malloc(sizeof(struct dhmp_msg) + sizeof(struct post_datagram));
			req_datagram = (struct post_datagram *)((char*)msg + sizeof(struct dhmp_msg));
			msg->msg_type = DHMP_MICA_SEND_INFO_REQUEST;
			msg->data_size = 0;
			msg->data = req_datagram;

			msg->partition_id = task_ptr->partition_id;

	
			INIT_LIST_HEAD(&msg->list_anchor);
			msg->trans = rdma_trans;
			peer_partition_id = (size_t)ntohl(wc->imm_data);
			req_datagram->info_type = MICA_SET_REQUEST;
			int id = peer_partition_id % PARTITION_NUMS;

		

			distribute_partition_resp(id, rdma_trans, msg, start.tv_sec, start.tv_nsec,true);

 
			dhmp_post_recv(rdma_trans, task_ptr->sge.addr, recv_partition_id);
			//  = dhmp_mica_set_request_handler(rdma_trans, NULL, ntohl(wc->imm_data));

			break;
		case IBV_WC_RECV:
			msg = (struct dhmp_msg *) malloc(sizeof(struct dhmp_msg));
			/*read the msg content from the task_ptr sge addr*/
			msg->msg_type=*(enum dhmp_msg_type*)task_ptr->sge.addr;
			msg->data_size=*(size_t*)(task_ptr->sge.addr+sizeof(enum dhmp_msg_type));
			msg->data= task_ptr->sge.addr + sizeof(enum dhmp_msg_type) + sizeof(size_t);

			INIT_LIST_HEAD(&msg->list_anchor);
			msg->trans = rdma_trans;
			msg->recv_partition_id = recv_partition_id;
			msg->partition_id = task_ptr->partition_id;
			// if (msg->recv_partition_id == 0)
			// 	MICA_TIME_COUNTER_CAL("IBV_WC_RECV init");
			dhmp_wc_recv_handler(rdma_trans, msg, &is_async, 0, 0);

			if (! is_async)
			{
				dhmp_post_recv(rdma_trans, task_ptr->sge.addr, recv_partition_id);
				free(msg);
			}
			break;

		case IBV_WC_RDMA_WRITE:
#ifdef DHMP_MR_REUSE_POLICY
		
#endif
			// task_ptr->addr_info->write_flag=false;
			task_ptr->done_flag=true;
			break;
		case IBV_WC_RDMA_READ:
			task_ptr->done_flag=true;
			break;
		default:
			ERROR_LOG("unknown opcode:%s",
			            dhmp_wc_opcode_str(wc->opcode));
			break;
	}
}

/**
 *	dhmp_wc_error_handler:handle the error work completion.
 */
static void dhmp_wc_error_handler(struct ibv_wc* wc)
{
	if(wc->status==IBV_WC_WR_FLUSH_ERR)
	{
		// INFO_LOG("work request flush, retry.....");
	}
	else
	{
		ERROR_LOG("wc status is [%s], byte_len is [%u], opcode is [%s]", \
				ibv_wc_status_str(wc->status), wc->byte_len, dhmp_wc_opcode_str(wc->opcode));
		return;
	}

}

void* busy_wait_cq_handler(void* data)
{
	pid_t pid = gettid();
	pthread_t tid = pthread_self();
	ERROR_LOG("Pid [%d] Tid [%ld]", pid, tid);
	// struct dhmp_cq* dcq = (struct dhmp_cq* )data;
	dhmp_comp_channel_handler();
}

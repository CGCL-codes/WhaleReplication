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


static struct dhmp_send_mr* dhmp_get_mr_from_send_list(struct dhmp_transport* rdma_trans, void* addr, int length);

void dhmp_post_recv(struct dhmp_transport* rdma_trans, void *addr, size_t partition_id);
void dhmp_post_all_recv(struct dhmp_transport *rdma_trans);

/*
 *	two sided RDMA operations
 */
void dhmp_post_recv(struct dhmp_transport* rdma_trans, void *addr, size_t partition_id)
{
	struct ibv_recv_wr recv_wr, *bad_wr_ptr=NULL;
	struct ibv_sge sge;
	struct dhmp_task *recv_task_ptr;
	int err=0;

	if(rdma_trans->trans_state>DHMP_TRANSPORT_STATE_CONNECTED)
		return ;
	
	recv_task_ptr=dhmp_recv_task_create(rdma_trans, addr, partition_id);
	if(!recv_task_ptr)
	{
		ERROR_LOG("create recv task error.");
		return ;
	}
	
	recv_wr.wr_id=(uintptr_t)recv_task_ptr;
	recv_wr.next=NULL;
	recv_wr.sg_list=&sge;
	recv_wr.num_sge=1;

	sge.addr=(uintptr_t)recv_task_ptr->sge.addr;
	sge.length=recv_task_ptr->sge.length;
	sge.lkey=recv_task_ptr->sge.lkey;
	
	err=ibv_post_recv(rdma_trans->qp, &recv_wr, &bad_wr_ptr);
	if(err)
		ERROR_LOG("ibv post recv error, the reason is %s", strerror(errno));
	
}

/**
 *	dhmp_post_all_recv:loop call the dhmp_post_recv function
 */
void dhmp_post_all_recv(struct dhmp_transport *rdma_trans)
{
	int i, single_region_size=0,j;

	if(rdma_trans->is_poll_qp)
		single_region_size=SINGLE_POLL_RECV_REGION;
	else
		single_region_size=SINGLE_NORM_RECV_REGION;
	
	// DEBUG_LOG("post recv nums is %d", RECV_REGION_SIZE/single_region_size);
//	for (j=0; j<PARTITION_NUMS+1; j++)
	{
		for(i=0; i<RECV_REGION_SIZE/single_region_size; i++)
			dhmp_post_recv(rdma_trans, rdma_trans->recv_mr.addr+i*single_region_size, j);
	}
}



void dhmp_post_send(struct dhmp_transport* rdma_trans, struct dhmp_msg* msg_ptr, size_t partition_id)
{
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_task *send_task_ptr;
	int err=0;
		
	send_task_ptr=dhmp_send_task_create(rdma_trans, msg_ptr, partition_id);
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

	err=ibv_post_send ( rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
		ERROR_LOG ( "ibv_post_send error." );
}

/*
one side rdma
*/



int dhmp_rdma_read_after_write ( struct dhmp_transport* rdma_trans, struct dhmp_addr_info *addr_info, struct ibv_mr* mr, void* local_addr, int length)
{
return 0;
}

int dhmp_rdma_read(struct dhmp_transport* rdma_trans, struct ibv_mr* mr, void* local_addr, int length, 
						off_t offset)
{
return 0;
}

// WGT
int dhmp_rdma_write (struct dhmp_transport* rdma_trans,
						struct ibv_mr* mr, 
						void* local_addr, 
						size_t length,
						uintptr_t remote_addr,
						bool is_imm ,
						 size_t item_offset)
{
	long long get_mr_time=0;
	struct timespec start_time, end_time;
	struct dhmp_task* write_task;
	struct ibv_send_wr send_wr,*bad_wr=NULL;
	struct ibv_sge sge;
	struct dhmp_send_mr* smr=NULL;
	int err=0;
	
	//clock_gettime(CLOCK_MONOTONIC, &start_time);
	smr=dhmp_get_mr_from_send_list(rdma_trans, local_addr, (int)length);
	//clock_gettime(CLOCK_MONOTONIC, &end_time);
	//get_mr_time += ((end_time.tv_sec * 1000000000) + end_time.tv_nsec) - ((start_time.tv_sec * 1000000000) + start_time.tv_nsec);
	//INFO_LOG("get_mr_time: %ld", get_mr_time);
						
	write_task=dhmp_write_task_create(rdma_trans, smr, (int)length);
	if(!write_task)
	{
		ERROR_LOG("allocate memory error.");
		return -1;
	}
	// write_task->addr_info=addr_info;
	memset(&send_wr, 0, sizeof(struct ibv_send_wr));

	if (is_imm)
	{
		send_wr.opcode=IBV_WR_RDMA_WRITE_WITH_IMM;
		send_wr.imm_data   = htonl(0x1234);
		//mirror_node_mapping->in_used_flag = true;
	}
	else
		send_wr.opcode=IBV_WR_RDMA_WRITE;

	send_wr.wr_id= ( uintptr_t ) write_task;
	send_wr.sg_list=&sge;
	send_wr.num_sge=1;
	send_wr.send_flags=IBV_SEND_SIGNALED;
	uintptr_t temp = local_addr-local_mr[1].addr;
	if((uintptr_t) local_addr > (uintptr_t) local_mr[1].addr)
		send_wr.wr.rdma.remote_addr= ( uintptr_t ) mr->addr + local_addr - local_mr[1].addr ; 
	else
		send_wr.wr.rdma.remote_addr = ( uintptr_t ) mr->addr;
			
	//ERROR_LOG("item-offset == %p localdata= %p, remote mr=%p, local-database=%p pre-real%p",send_wr.wr.rdma.remote_addr,local_addr,mr->addr,local_addr-local_mr[1].addr,send_wr.wr.rdma.remote_addr-remote_addr);
//	send_wr.wr.rdma.remote_addr= remote_addr;  // WGT
//	ERROR_LOG("remote_addr == %p",remote_addr);
	send_wr.wr.rdma.rkey=mr->rkey;


	//ERROR_LOG("write remote_addr = %p  mr addr = %p",send_wr.wr.rdma.remote_addr,mr->addr );

	sge.addr= ( uintptr_t ) write_task->sge.addr;
	sge.length=write_task->sge.length;
	sge.lkey=write_task->sge.lkey;

	// INFO_LOG("dhmp_rdma_write addr is [%p]", write_task->sge.addr);
#ifdef DHMP_MR_REUSE_POLICY
	if (length <= RDMA_SEND_THREASHOLD)
		memcpy(write_task->sge.addr, local_addr, length);
#endif

	err=ibv_post_send ( rdma_trans->qp, &send_wr, &bad_wr );
	if ( err )
	{
		ERROR_LOG("ibv_post_send error");
		exit(-1);
		goto error;
	}
	// DEFINE_STACK_TIMER();
	// MICA_TIME_COUNTER_INIT();
	while (!write_task->done_flag);
		// MICA_TIME_LIMITED(0, TIMEOUT_LIMIT_MS);
	// DEBUG_LOG("after read_mr[%d] addr content is %s", rdma_trans->node_id, client_mgr->read_mr[rdma_trans->node_id]->mr->addr);

#ifdef DHMP_MR_REUSE_POLICY
	if (length > RDMA_SEND_THREASHOLD)
	{
#endif
		while (!write_task->done_flag);
		ibv_dereg_mr(smr->mr);
		free(smr);

#ifdef DHMP_MR_REUSE_POLICY
	}
#endif
	return 0;
error:
	return -1;
}

/**
 *	return the work completion operation code string.
 */
const char* dhmp_wc_opcode_str(enum ibv_wc_opcode opcode)
{
	switch(opcode)
	{
		case IBV_WC_SEND:
			return "IBV_WC_SEND";
		case IBV_WC_RDMA_WRITE:
			return "IBV_WC_RDMA_WRITE";
		case IBV_WC_RDMA_READ:
			return "IBV_WC_RDMA_READ";
		case IBV_WC_COMP_SWAP:
			return "IBV_WC_COMP_SWAP";
		case IBV_WC_FETCH_ADD:
			return "IBV_WC_FETCH_ADD";
		case IBV_WC_BIND_MW:
			return "IBV_WC_BIND_MW";
		case IBV_WC_RECV:
			return "IBV_WC_RECV";
		case IBV_WC_RECV_RDMA_WITH_IMM:
			return "IBV_WC_RECV_RDMA_WITH_IMM";
		default:
			return "IBV_WC_UNKNOWN";
	};
}

static struct dhmp_send_mr* dhmp_get_mr_from_send_list(struct dhmp_transport* rdma_trans, void* addr, int length)
{
	struct dhmp_send_mr* res, * tmp;
	void* new_addr = NULL;

	res = (struct dhmp_send_mr*)malloc(sizeof(struct dhmp_send_mr));
	if (!res)
	{
		ERROR_LOG("allocate memory error.");
		return NULL;
	}

#ifdef DHMP_MR_REUSE_POLICY

	if (length > RDMA_SEND_THREASHOLD)
	{
#endif

		res->mr = ibv_reg_mr(rdma_trans->device->pd,
			addr, length, IBV_ACCESS_LOCAL_WRITE);
		if (!res->mr)
		{
			ERROR_LOG("ibv register memory error.");
			goto error;
		}

#ifdef DHMP_MR_REUSE_POLICY
	}
	else
	{

		pthread_mutex_lock(&client_mgr->mutex_send_mr_list);
		list_for_each_entry(tmp, &client_mgr->send_mr_list, send_mr_entry)
		{
			if (tmp->mr->length >= length) 
				break;
		}


		if ((&tmp->send_mr_entry) == (&client_mgr->send_mr_list))
		{
			pthread_mutex_unlock(&client_mgr->mutex_send_mr_list);
			new_addr = malloc(length);
			if (!new_addr)
			{
				ERROR_LOG("allocate memory error.");
				goto error;
			}

			res->mr = ibv_reg_mr(rdma_trans->device->pd,
				new_addr, length, IBV_ACCESS_LOCAL_WRITE);
			if (!res->mr)
			{
				ERROR_LOG("ibv reg memory error.");
				free(new_addr);
				goto error;
			}
		}
		else
		{

			free(res);
			res = tmp;
			list_del(&res->send_mr_entry);
			pthread_mutex_unlock(&client_mgr->mutex_send_mr_list);
		}
	}
#endif

	return res;

error:
	free(res);
	return NULL;
}

int dhmp_rdma_write_packed (struct dhmp_write_request * write_req, size_t item_offset)
{
	return dhmp_rdma_write(write_req->rdma_trans, write_req->mr, \
			write_req->local_addr, write_req->length, write_req->remote_addr, false, item_offset);	
}

int dhmp_rdma_write_mica_warpper (struct dhmp_transport* rdma_trans,
						struct mehcached_item * item,
						struct ibv_mr* mr, 
						size_t length,
						void* remote_addr,
						bool is_imm)
{
}

#define _GNU_SOURCE 1
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <arpa/inet.h>

#include "dhmp.h"
#include "dhmp_transport.h"
#include "dhmp_server.h"
#include "dhmp_log.h"
#include "dhmp_client.h"
#include "midd_mica_benchmark.h"

static int trans_thread_idx=1;
void dhmp_event_channel_handler(int fd, void* data);
int dhmp_transport_listen(struct dhmp_transport* rdma_trans, int listen_port)
{
	int retval=0, backlog;
	struct sockaddr_in addr;

	retval=rdma_create_id(rdma_trans->event_channel,
	                        &rdma_trans->cm_id,
	                        rdma_trans, RDMA_PS_TCP);
	if(retval)
	{
		ERROR_LOG("rdma create id error.");
		return retval;
	}

	memset(&addr, 0, sizeof(addr));
	addr.sin_family=AF_INET;
	addr.sin_port=htons(listen_port);

	retval=rdma_bind_addr(rdma_trans->cm_id,
	                       (struct sockaddr*) &addr);
	if(retval)
	{
		ERROR_LOG("rdma bind addr error.");
		goto cleanid;
	}

	backlog=10;
	retval=rdma_listen(rdma_trans->cm_id, backlog);
	if(retval)
	{
		ERROR_LOG("rdma listen error.");
		goto cleanid;
	}

	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_LISTEN;
	INFO_LOG("rdma listening on port %d",
	           ntohs(rdma_get_src_port(rdma_trans->cm_id)));

	return retval;

cleanid:
	rdma_destroy_id(rdma_trans->cm_id);
	rdma_trans->cm_id=NULL;

	return retval;
}

static int dhmp_port_uri_transfer(struct dhmp_transport* rdma_trans,
										const char* url, int port)
{
	struct sockaddr_in peer_addr;
	int retval=0;

	memset(&peer_addr,0,sizeof(peer_addr));
	peer_addr.sin_family=AF_INET;
	peer_addr.sin_port=htons(port);

	retval=inet_pton(AF_INET, url, &peer_addr.sin_addr);
	if(retval<=0)
	{
		ERROR_LOG("IP Transfer Error.");
		goto out;
	}

	memcpy(&rdma_trans->peer_addr, &peer_addr, sizeof(struct sockaddr_in));

out:
	return retval;
}


int dhmp_transport_connect(struct dhmp_transport* rdma_trans,
                             const char* url, int port)
{
	int retval=0;
	if(!url||port<=0)
	{
		ERROR_LOG("url or port input error.");
		return -1;
	}

	retval=dhmp_port_uri_transfer(rdma_trans, url, port);
	if(retval<0)
	{
		ERROR_LOG("rdma init port uri error.");
		return retval;
	}

	/*rdma_cm_id dont init the rdma_cm_id's verbs*/
	retval=rdma_create_id(rdma_trans->event_channel,
						&rdma_trans->cm_id,
						rdma_trans, RDMA_PS_TCP);
	if(retval)
	{
		ERROR_LOG("rdma create id error.");
		goto clean_rdmatrans;
	}

	retval=rdma_resolve_addr(rdma_trans->cm_id, NULL,
							(struct sockaddr*) &rdma_trans->peer_addr,
							ADDR_RESOLVE_TIMEOUT);


	if(retval)
	{
		ERROR_LOG("RDMA Device resolve addr error.");
		goto clean_cmid;
	}
	
	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_CONNECTING;
	return retval;

clean_cmid:
	rdma_destroy_id(rdma_trans->cm_id);

clean_rdmatrans:
	rdma_trans->cm_id=NULL;

	return retval;
}

/*
 *	get the cq because send queue and receive queue need to link it
 */
struct dhmp_cq* dhmp_cq_get(struct dhmp_device* device, struct dhmp_context* ctx)
{
	struct dhmp_cq* dcq;
	int retval,flags=0, i;

	dcq=(struct dhmp_cq*) calloc(1,sizeof(struct dhmp_cq));
	if(!dcq)
	{
		ERROR_LOG("allocate the memory of struct dhmp_cq error.");
		return NULL;
	}

	dcq->comp_channel=ibv_create_comp_channel(device->verbs);
	if(!dcq->comp_channel)
	{
		ERROR_LOG("rdma device %p create comp channel error.", device);
		goto cleanhcq;
	}

	flags=fcntl(dcq->comp_channel->fd, F_GETFL, 0);
	if(flags!=-1)
		flags=fcntl(dcq->comp_channel->fd, F_SETFL, flags|O_NONBLOCK);
	if(flags==-1)
	{
		ERROR_LOG("set hcq comp channel fd nonblock error.");
		goto cleanchannel;
	}

	dcq->ctx=ctx;
	
	/*
	retval=dhmp_context_add_event_fd(dcq->ctx,
									EPOLLIN,
									dcq->comp_channel->fd,
									dcq, dhmp_comp_channel_handler);
	*/

	dcq->cq=ibv_create_cq(device->verbs, 100000, dcq, dcq->comp_channel, 0);
	if(!dcq->cq)
	{
		ERROR_LOG("ibv create cq error.");
		goto cleaneventfd;
	}

	retval=ibv_req_notify_cq(dcq->cq, 0);
	if(retval)
	{
		ERROR_LOG("ibv req notify cq error.");
		goto cleaneventfd;
	}

	dcq->device=device;

	
	ctx->cq_id = total_cq_nums;
	dcq->cq_id = ctx->cq_id;
	dcq_array[ctx->cq_id] = dcq;
	total_cq_nums++;

	return dcq;

cleaneventfd:
	dhmp_context_del_event_fd(ctx, dcq->comp_channel->fd);

cleanchannel:
	ibv_destroy_comp_channel(dcq->comp_channel);

cleanhcq:
	free(dcq);

	return NULL;
}

/*
 *	create the qp resource for the RDMA connection
 */
int dhmp_qp_create(struct dhmp_transport* rdma_trans)
{
	int retval=0;
	struct ibv_qp_init_attr qp_init_attr;
	struct dhmp_cq* dcq;

	dcq=dhmp_cq_get(rdma_trans->device, rdma_trans->ctx);
	if(!dcq)
	{
		ERROR_LOG("dhmp cq get error.");
		return -1;
	}

	memset(&qp_init_attr,0,sizeof(qp_init_attr));
	qp_init_attr.qp_context=rdma_trans;
	qp_init_attr.qp_type=IBV_QPT_RC;
	qp_init_attr.send_cq=dcq->cq;
	qp_init_attr.recv_cq=dcq->cq;

	qp_init_attr.cap.max_send_wr=15000;
	qp_init_attr.cap.max_send_sge=1;

	qp_init_attr.cap.max_recv_wr=15000;
	qp_init_attr.cap.max_recv_sge=1;

	retval=rdma_create_qp(rdma_trans->cm_id,
	                        rdma_trans->device->pd,
	                        &qp_init_attr);
	if(retval)
	{
		ERROR_LOG("rdma create qp error.");
		goto cleanhcq;
	}

	rdma_trans->qp=rdma_trans->cm_id->qp;
	rdma_trans->dcq=dcq;

	return retval;

cleanhcq:
	free(dcq);
	return retval;
}


static void dhmp_qp_release(struct dhmp_transport* rdma_trans)
{
	if(rdma_trans->qp)
	{
		dcq_array[rdma_trans->dcq->cq_id] = NULL;
		total_cq_nums--;

		ibv_destroy_qp(rdma_trans->qp);
		ibv_destroy_comp_channel(rdma_trans->dcq->comp_channel);
		ibv_destroy_cq(rdma_trans->dcq->cq);
		// ibv_dealloc_pd(rdma_trans->dcq->device->pd);	
		// dhmp_context_del_event_fd(rdma_trans->ctx, rdma_trans->dcq->comp_channel->fd);
		free(rdma_trans->dcq);
		rdma_trans->dcq=NULL;
	}
}


int dhmp_event_channel_create(struct dhmp_transport* rdma_trans)
{
	int flags,retval=0;

	rdma_trans->event_channel=rdma_create_event_channel();
	if(!rdma_trans->event_channel)
	{
		ERROR_LOG("rdma create event channel error.");
		return -1;
	}

	flags=fcntl(rdma_trans->event_channel->fd, F_GETFL, 0);
	if(flags!=-1)
		flags=fcntl(rdma_trans->event_channel->fd,
		              F_SETFL, flags|O_NONBLOCK);

	if(flags==-1)
	{
		retval=-1;
		ERROR_LOG("set event channel nonblock error.");
		goto clean_ec;
	}

	dhmp_context_add_event_fd(rdma_trans->ctx,
								EPOLLIN,
	                            rdma_trans->event_channel->fd,
	                            rdma_trans->event_channel,
	                            dhmp_event_channel_handler);
	return retval;

clean_ec:
	rdma_destroy_event_channel(rdma_trans->event_channel);
	return retval;
}
struct dhmp_transport* dhmp_transport_create(struct dhmp_context* ctx, 
													struct dhmp_device* dev,
													bool is_listen,
													bool is_poll_qp,
													int peer_node_id)
{
	int i;
	struct dhmp_transport *rdma_trans;
	int err=0;
	rdma_trans=(struct dhmp_transport*)malloc(sizeof(struct dhmp_transport));
	memset(rdma_trans, 0, sizeof(struct dhmp_transport));
	if(!rdma_trans)
	{
		ERROR_LOG("allocate memory error");
		return NULL;
	}
	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_INIT;
	rdma_trans->ctx=ctx;
	rdma_trans->device=dev;
	rdma_trans->dram_used_size=rdma_trans->nvm_used_size=0;
	// 新增的 rdma_trans 标识，如果为true则表示该 trans 是一个 server监听trans
	rdma_trans->is_listen = is_listen;
	rdma_trans->node_id = peer_node_id;
	rdma_trans->trans_mid_state = middware_INIT;
	rdma_trans->recv_mr_lock = 0UL;
	rdma_trans->send_mr_lock = 0UL;

	err=dhmp_event_channel_create(rdma_trans);
	if(err)
	{
		ERROR_LOG("dhmp event channel create error");
		goto out;
	}

	

	if(!is_listen)
	{
		// for (i=0; i<PARTITION_NUMS+1; i++)
		// {
			err=dhmp_memory_register(dev->pd,
									&(rdma_trans->send_mr),
									SEND_REGION_SIZE);
			if(err)
				goto out_event_channel;

			err=dhmp_memory_register(dev->pd,
									&(rdma_trans->recv_mr),
									RECV_REGION_SIZE);
			if(err)
				goto out_send_mr;
		// }

		rdma_trans->is_poll_qp=is_poll_qp;
	}
	
	return rdma_trans;
out_send_mr:
	// for (i=0; i<PARTITION_NUMS+1; i++)
	// {
		ibv_dereg_mr(rdma_trans->send_mr.mr);
		free(rdma_trans->send_mr.addr);
	// }

out_event_channel:
	dhmp_context_del_event_fd(rdma_trans->ctx, rdma_trans->event_channel->fd);
	rdma_destroy_event_channel(rdma_trans->event_channel);
	
out:
	free(rdma_trans);
	return NULL;
}

/**
 *	dhmp_destroy_source: destroy the used RDMA resouces
 */
static void dhmp_destroy_source(struct dhmp_transport* rdma_trans)
{
	int i;
	// for (i=0; i<PARTITION_NUMS+1; i++)
	// {
		if(rdma_trans->send_mr.addr)
		{
			ibv_dereg_mr(rdma_trans->send_mr.mr);
			free(rdma_trans->send_mr.addr);
		}

		if(rdma_trans->recv_mr.addr)
		{
			ibv_dereg_mr(rdma_trans->recv_mr.mr);
			free(rdma_trans->recv_mr.addr);
		}
	// }

	rdma_destroy_qp(rdma_trans->cm_id);
	dhmp_context_del_event_fd(rdma_trans->ctx, rdma_trans->dcq->comp_channel->fd);
	dhmp_context_del_event_fd(rdma_trans->ctx, rdma_trans->event_channel->fd);
}


static int on_cm_addr_resolved(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{
	int retval=0;

	retval=rdma_resolve_route(rdma_trans->cm_id, ROUTE_RESOLVE_TIMEOUT);
	if(retval)
	{
		ERROR_LOG("RDMA resolve route error.");
		return retval;
	}
	return retval;
}

static int on_cm_route_resolved(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{
	// struct rdma_conn_param conn_param;
	int i, retval=0;

	retval=dhmp_qp_create(rdma_trans);
	if(retval)
	{
		ERROR_LOG("hmr qp create error.");
		return retval;
	}

	// memset(&conn_param, 0, sizeof(conn_param));
	memset(&rdma_trans->connect_params, 0, sizeof(rdma_trans->connect_params));

	rdma_trans->connect_params.retry_count=100;
	rdma_trans->connect_params.rnr_retry_count=200;
	rdma_trans->connect_params.responder_resources = 1;
	rdma_trans->connect_params.initiator_depth = 1;

	retval=rdma_connect(rdma_trans->cm_id, &rdma_trans->connect_params);
	if(retval)
	{
		ERROR_LOG("rdma connect error.");
		goto cleanqp;
	}
	//INFO_LOG("on_cm_route_resolved success!");
	dhmp_post_all_recv(rdma_trans);
	return retval;

cleanqp:
	dhmp_qp_release(rdma_trans);
	rdma_trans->ctx->stop=1;
	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_ERROR;
	return retval;
}


static int on_cm_connect_request(struct rdma_cm_event* event, 
										struct dhmp_transport* rdma_trans)
{
	struct dhmp_transport* new_trans,*normal_trans;
	struct rdma_conn_param conn_param;
	int i,retval=0;
	char* peer_addr;
	normal_trans=find_connect_by_socket(&event->id->route.addr.dst_sin);
	if(normal_trans)
	{
		// 来自同一客户端ip地址的重复的连接，丢弃？
		ERROR_LOG("rdma pear connect repeat error.");
		return -1;
	}

	new_trans=dhmp_transport_create(rdma_trans->ctx, rdma_trans->device,
									false, false, -1);
	if(!new_trans)
	{
		ERROR_LOG("rdma trans process connect request error.");
		return -1;
	}

	new_trans->link_trans=NULL;
	new_trans->cm_id=event->id;
	event->id->context=new_trans;
	
	retval=dhmp_qp_create(new_trans);
	if(retval)
	{
		ERROR_LOG("dhmp qp create error.");
		goto out;
	}

	if(normal_trans)
	{
		normal_trans->link_trans=new_trans;
		new_trans->link_trans=normal_trans;
	}

	memset(&conn_param, 0, sizeof(conn_param));
	conn_param.retry_count=100;
	conn_param.rnr_retry_count=200;
	conn_param.responder_resources = 1;
	conn_param.initiator_depth = 1;
	
	retval=rdma_accept(new_trans->cm_id, &conn_param);
	if(retval)
	{
		ERROR_LOG("rdma accept error.");
		return -1;
	}
	
	new_trans->trans_state=DHMP_TRANSPORT_STATE_CONNECTING;
	dhmp_post_all_recv(new_trans);


	pthread_mutex_lock(&server_instance->mutex_client_list);
new_trans->partition_id = server_instance->cur_connections ;
	if(IS_MIRROR(server_instance->server_type))
new_trans->node_id = 0;
	++server_instance->cur_connections;
	list_add_tail(&new_trans->client_entry, &server_instance->client_list);
	pthread_mutex_unlock(&server_instance->mutex_client_list);
	ERROR_LOG("get %d-th trans %p",server_instance->cur_connections,new_trans);
	return retval;

out:
	free(new_trans);
	return retval;
}

static int on_cm_established(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{
	int retval=0;

	memcpy(&rdma_trans->local_addr,
			&rdma_trans->cm_id->route.addr.src_sin,
			sizeof(rdma_trans->local_addr));

	memcpy(&rdma_trans->peer_addr,
			&rdma_trans->cm_id->route.addr.dst_sin,
			sizeof(rdma_trans->peer_addr));

	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_CONNECTED;

#ifdef RTT_TEST
	if (server_instance != NULL)
	{
		dcq_array[0] = rdma_trans->dcq;
		total_cq_nums++;
		cpu_set_t cpuset;
		pthread_t cq_thread;
		CPU_ZERO(&cpuset);
		if (SERVER_ID < 4)
			CPU_SET(PARTITION_NUMS+1, &cpuset);
		else
			CPU_SET(PARTITION_NUMS+1+20, &cpuset);
		retval=pthread_create(&cq_thread, NULL, busy_wait_cq_handler, NULL);
		if(retval)
			handle_error_en(retval, "pthread_setaffinity_np");
		retval = pthread_setaffinity_np(cq_thread, sizeof(cpu_set_t), &cpuset);
		if (retval != 0)
			handle_error_en(retval, "pthread_setaffinity_np");
		cq_thread_stop_flag = false;
	}
#endif
	

	return retval;
}

static int on_cm_disconnected(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{
	ERROR_LOG("unexpected disconnect!");
	exit_print_status();
	exit(-1);
	dhmp_destroy_source(rdma_trans);
	rdma_trans->trans_state = DHMP_TRANSPORT_STATE_DISCONNECTED;

	if(server_instance!=NULL && 
		!rdma_trans->is_listen &&
		!rdma_trans->is_active)
	{

		pthread_mutex_lock(&server_instance->mutex_client_list);
		--server_instance->cur_connections;
		list_del(&rdma_trans->client_entry);
		pthread_mutex_unlock(&server_instance->mutex_client_list);
	}
	return 0;
}




static int on_cm_error(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{
	ERROR_LOG("unexpected disconnect!");
	Assert(false);
	dhmp_destroy_source(rdma_trans);
	rdma_trans->trans_state=DHMP_TRANSPORT_STATE_ERROR;
	if(server_instance!=NULL)
	{
		pthread_mutex_lock(&server_instance->mutex_client_list);
		--server_instance->cur_connections;
		list_del(&rdma_trans->client_entry);
		pthread_mutex_unlock(&server_instance->mutex_client_list);
	}
	return 0;
}


static int on_cm_rejected(struct rdma_cm_event* event, struct dhmp_transport* rdma_trans)
{

	rdma_trans->trans_state = DHMP_TRANSPORT_STATE_REJECT;
	return 0;
}

int free_trans(struct dhmp_transport* rdma_trans)
{
	
	int i;
	int node_id = rdma_trans->node_id;

	// undo dhmp_transport_create
	// undo dhmp_memory_register
	// for (i=0; i<PARTITION_NUMS+1; i++)
	// {
		ibv_dereg_mr(rdma_trans->send_mr.mr);
		if(rdma_trans->send_mr.addr)
			free(rdma_trans->send_mr.addr);

		ibv_dereg_mr(rdma_trans->recv_mr.mr);
		if (rdma_trans->recv_mr.addr)
			free(rdma_trans->recv_mr.addr);
	// }

	// undo on_cm_route_resolved
	dhmp_qp_release(rdma_trans);

	// undo dhmp_transport_connect
	rdma_destroy_id(rdma_trans->cm_id);

	// rdma_trans->
	dhmp_context_del_event_fd(rdma_trans->ctx, rdma_trans->event_channel->fd);

	// undo dhmp_event_channel_create
	if(rdma_trans->event_channel)
		rdma_destroy_event_channel(rdma_trans->event_channel);

	// undo malloc
	// free(rdma_trans);
	// rdma_trans = NULL;
	
//	INFO_LOG("Free rdma_trans with server_instance [%d]-th", node_id);
//	INFO_LOG("rdma_trans state is %d",  rdma_trans->trans_state);
	return 0;
}

/*
 *	the function use for handling the event of event channel
 */
int dhmp_handle_ec_event(struct rdma_cm_event* event)
{
	int retval=0;
	struct dhmp_transport* rdma_trans;
	
	rdma_trans=(struct dhmp_transport*) event->id->context;

//	INFO_LOG("XXXX dhmp_transport cm event [%s],status:[%d], node_id: [%d], is_listen [%d] ",
	//           rdma_event_str(event->event),event->status , rdma_trans->node_id, rdma_trans->is_listen);

	switch(event->event)
	{
		case RDMA_CM_EVENT_ADDR_RESOLVED:
			retval=on_cm_addr_resolved(event, rdma_trans);
			break;
		case RDMA_CM_EVENT_ROUTE_RESOLVED:
			retval=on_cm_route_resolved(event, rdma_trans);
			break;
		case RDMA_CM_EVENT_CONNECT_REQUEST:
			retval=on_cm_connect_request(event,rdma_trans);
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			retval=on_cm_established(event,rdma_trans);
			break;
		case RDMA_CM_EVENT_DISCONNECTED:
			retval=on_cm_disconnected(event,rdma_trans);
			break;
		case RDMA_CM_EVENT_CONNECT_ERROR:
			retval=on_cm_error(event, rdma_trans);
			break;
		// WGT
		case RDMA_CM_EVENT_REJECTED:
		//	INFO_LOG("occur \"RDMA_CM_EVENT_REJECTED\" error.");
			retval=on_cm_rejected(event, rdma_trans);
			break;
		case RDMA_CM_EVENT_ADDR_ERROR:
			ERROR_LOG("occur \"RDMA_CM_EVENT_ADDR_ERROR\" error.");
			rdma_trans->trans_state = DHMP_TRANSPORT_STATE_ADDR_ERROR;
			retval=-1;
			break;
		default:
			ERROR_LOG("occur the other error.");
			retval=-1;
			break;
	};
	return retval;
}



void dhmp_event_channel_handler(int fd, void* data)
{
	struct rdma_event_channel* ec=(struct rdma_event_channel*) data;
	struct rdma_cm_event* event,event_copy;
	int retval=0;

	event=NULL;
	while(( retval=rdma_get_cm_event(ec, &event) ==0))
	{
		memcpy(&event_copy, event, sizeof(*event));

		/*
		 * note: rdma_ack_cm_event function will clear event content
		 * so need to copy event content into event_copy.
		 */
		rdma_ack_cm_event(event);

		if(dhmp_handle_ec_event(&event_copy))
			break;
	}

	if(retval && errno!=EAGAIN)
	{
		ERROR_LOG("rdma get cm event error.");
	}
}

struct dhmp_transport* 
find_connect_by_socket(struct sockaddr_in *sock)
{
	char cur_ip[INET_ADDRSTRLEN], travers_ip[INET_ADDRSTRLEN];
	struct dhmp_transport *rdma_trans=NULL, *res_trans=NULL;
	struct in_addr in=sock->sin_addr;
	int cur_ip_len,travers_ip_len;
	
	inet_ntop(AF_INET, &(sock->sin_addr), cur_ip, sizeof(cur_ip));
	cur_ip_len=strlen(cur_ip);
	
	pthread_mutex_lock(&server_instance->mutex_client_list);
	list_for_each_entry(rdma_trans, &server_instance->client_list, client_entry)
	{
		inet_ntop(AF_INET, &(rdma_trans->peer_addr.sin_addr), travers_ip, sizeof(travers_ip));
		travers_ip_len=strlen(travers_ip);
		
		if(memcmp(cur_ip, travers_ip, max(cur_ip_len,travers_ip_len))==0)
		{
			if (rdma_trans->peer_addr.sin_port == sock->sin_port)
			{
				INFO_LOG("find the same connection.");
				res_trans=rdma_trans;
				break;
			}
		}
	}
	pthread_mutex_unlock(&server_instance->mutex_client_list);

	return res_trans;
}


struct dhmp_transport* 
find_connect_server_by_nodeID(int node_id, int thread_num)
{
	int trans_id = 0;
	for (trans_id =0; trans_id<DHMP_SERVER_NODE_NUM; trans_id ++)
	{
		if (client_mgr->connect_trans[trans_id][thread_num] != NULL)
		{
			if (client_mgr->connect_trans[trans_id][thread_num]->node_id == node_id)
			{
				if (client_mgr->connect_trans[node_id][thread_num]->trans_state == DHMP_TRANSPORT_STATE_CONNECTED)
					return client_mgr->connect_trans[node_id][thread_num];
				else
				{
					ERROR_LOG("target node trans [%d-%d] is disconnect!", node_id,thread_num);
					return NULL;
				}
			}
		}
	}
	ERROR_LOG("target node trans [%d-%d] does not exist!", node_id,thread_num);
	return NULL;
}

struct dhmp_transport* 
find_connect_read_server_by_nodeID(int node_id, int thread_num)
{
	int trans_id = 0;
	for (trans_id =0; trans_id<DHMP_SERVER_NODE_NUM; trans_id ++)
	{
		if (client_mgr->read_connect_trans[trans_id][thread_num] != NULL)
		{
			if (client_mgr->read_connect_trans[trans_id][thread_num]->node_id == node_id)
			{
				if (client_mgr->read_connect_trans[node_id][thread_num]->trans_state == DHMP_TRANSPORT_STATE_CONNECTED)
					return client_mgr->read_connect_trans[node_id][thread_num];
				else
				{
					ERROR_LOG("target node trans [%d-%d] is disconnect!", node_id,thread_num);
					return NULL;
				}
			}
		}
	}
	ERROR_LOG("target node trans [%d-%d] does not exist!", node_id,thread_num);
	return NULL;
}

struct dhmp_transport* 
find_connect_client_by_nodeID(int node_id, int thread_num)
{
	struct dhmp_transport *rdma_trans=NULL, * re_trans = NULL;
	//pthread_mutex_lock(&server_instance->mutex_client_list);
	list_for_each_entry(rdma_trans, &server_instance->client_list, client_entry)
	{
		if (rdma_trans->node_id == node_id && rdma_trans->partition_id == thread_num)
		{
			re_trans = rdma_trans;
			break;
		}
	}
	//pthread_mutex_unlock(&server_instance->mutex_client_list);
	return re_trans;
}

// 返回客户端与头节点的trans

int client_find_server_id()
{
	int i;
	for(i=0; i<client_mgr->config.nets_cnt; i++)
	{
		if(client_mgr->connect_trans[i] != NULL)
			return i;
	}
	return -1;
}

int find_next_node(int id)
{
	if(id >= client_mgr->config.nets_cnt-1)
		return -1;
	return  id + 1;
}

const char *
dhmp_printf_connect_state(enum dhmp_transport_state state)
{
	switch (state)
	{
		case DHMP_TRANSPORT_STATE_INIT:
			return "DHMP_TRANSPORT_STATE_INIT";
			break;
		case DHMP_TRANSPORT_STATE_LISTEN:
			return "DHMP_TRANSPORT_STATE_LISTEN";
			break;
		case DHMP_TRANSPORT_STATE_CONNECTING:
			return "DHMP_TRANSPORT_STATE_CONNECTING";
			break;
		case DHMP_TRANSPORT_STATE_CONNECTED:
			return "DHMP_TRANSPORT_STATE_CONNECTED";
			break;
		case DHMP_TRANSPORT_STATE_DISCONNECTED:
			return "DHMP_TRANSPORT_STATE_DISCONNECTED";
			break;
		case DHMP_TRANSPORT_STATE_RECONNECT:
			return "DHMP_TRANSPORT_STATE_RECONNECT";
			break;
		case DHMP_TRANSPORT_STATE_CLOSED:
			return "DHMP_TRANSPORT_STATE_CLOSED";
			break;
		case DHMP_TRANSPORT_STATE_DESTROYED:
			return "DHMP_TRANSPORT_STATE_DESTROYED";
			break;
		case DHMP_TRANSPORT_STATE_ERROR:
			return "DHMP_TRANSPORT_STATE_ERROR";
			break;
		case DHMP_TRANSPORT_STATE_REJECT:
			return "DHMP_TRANSPORT_STATE_REJECT";
			break;
		case DHMP_TRANSPORT_STATE_ADDR_ERROR:
			return "DHMP_TRANSPORT_STATE_ADDR_ERROR";
			break;
		default:
			INFO_LOG("UNKONW connect state!");
			return NULL;
	}
}

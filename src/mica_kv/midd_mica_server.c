#include "mehcached.h"
#include "hash.h"
#include "dhmp.h"
#include "dhmp_log.h"
#include "dhmp_hash.h"
#include "dhmp_config.h"
#include "dhmp_context.h"
#include "dhmp_dev.h"
#include "dhmp_transport.h"
#include "dhmp_task.h"

#include "dhmp_client.h"
#include "dhmp_server.h"
#include "dhmp_init.h"
#include "mid_rdma_utils.h"
#include "dhmp_top_api.h"
#include "nic.h"

#include "midd_mica_benchmark.h"
#include "mica_partition.h"


void new_main_test_through();

pthread_t nic_thread[PARTITION_MAX_NUMS];
void* (*main_node_nic_thread_ptr) (void* );
void* (*replica_node_nic_thread_ptr) (void* );
void test_set(struct test_kv * kvs);

int startwork;

struct dhmp_msg* all_access_set_group;

int *partition_req_count_array;

void generate_local_get_mgs();
void generate_local_get_mgs_handler(size_t read_per_node);
void set_workloada_server();

int set_workload_OK;

double workload_WriteRate;
char workloadf;
char workloadd;
char workloade;


#define TOTAL_OPT_NUMS 2000
int main(int argc,char *argv[])
{
    // 初始化集群 rdma 连接
    int i;
    int nic_thread_num;
    size_t numa_nodes[] = {(size_t)-1};;
    const size_t page_size = 4*1024UL;
	const size_t num_numa_nodes = 2;
    const size_t num_pages_to_try = 16384;
    const size_t num_pages_to_reserve = 16384 - 2048;   // give 2048 pages to dpdk
    
    INFO_LOG("Server argc is [%d]", argc);
	main_node_is_readable = true;
    Assert(argc==8 || argc==5);
    if(argc == 5)
    {
    for (i = 0; i<argc; i++)
    {
        if (i==1)
        {
            SERVER_ID = (size_t)(*argv[i] - '0');
        }
        else if (i==2)
        {
            __partition_nums = (unsigned long long) atoi(argv[i]);
            Assert(__partition_nums >0 && __partition_nums < PARTITION_MAX_NUMS);
        }
        else if (i==3)
        {
            is_ubuntu = atoi(argv[i]);
        }
        else if (i==4)
        {
            if(strcmp(argv[i], "workloada") == 0)
            {
                workload_WriteRate = 1;
            }
            else if(strcmp(argv[i], "workloadb") == 0)
            {
                workload_WriteRate = 19;
            }
            else if(strcmp(argv[i], "workloadc") == 0)
            {
                workload_WriteRate = 150;
            }
            else if(strcmp(argv[i], "workloadd") == 0)
            {
                workload_WriteRate = 19;
                workloadd = 1;
            }
            else if(strcmp(argv[i], "workloade") == 0)
            {
                workload_WriteRate = 19;
                workloade =  1;
            }
            else if(strcmp(argv[i], "workloadf") == 0)
            {
                workload_WriteRate = 1;
                workloadf = 1;
            }
            else
            {
                ERROR_LOG(" workload[a-f]!");
                exit(0);
            }
        }
    }
    __test_size = 64;
    workload_type=ZIPFIAN;
	current_credict = 1;
    }
    else
    for (i = 0; i<argc; i++)
	{
        if (i==1)
        {
            SERVER_ID = (size_t)(*argv[i] - '0');
            INFO_LOG("Server node_id is [%d]", SERVER_ID);
        }
        else if (i==2)
        {
            __partition_nums = (unsigned long long) atoi(argv[i]);
            Assert(__partition_nums >0 && __partition_nums < PARTITION_MAX_NUMS);
            INFO_LOG("Server __partition_nums is [%d]", __partition_nums);
        }
        else if (i==3)
        {
            is_ubuntu = atoi(argv[i]);
            INFO_LOG("Server is_ubuntu is [%d]", is_ubuntu);
        }
        else if (i==4)
        {
            __test_size = atoi(argv[i]);
            INFO_LOG(" __test_size is [%d]", __test_size);
        }
        else if (i==5)
        {
            if (strcmp(argv[i], "uniform") == 0)
            {
                INFO_LOG(" workload_type is [%s]", argv[i]);
                workload_type=UNIFORM;
            }
            else if (strcmp(argv[i], "zipfian") == 0)
            {
                INFO_LOG(" workload_type is [%s]", argv[i]);
                workload_type=ZIPFIAN;
            }
            else
            {
                ERROR_LOG("Unkown workload!");
                exit(0);
            }
        }
        else if (i==6)
        {
            current_credict = atoi(argv[i]);
            INFO_LOG("current_credict is [%d]",current_credict );
        }
        else if (i==7)
        {
            if(strcmp(argv[i], "0.5") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
				workload_WriteRate = 1;
                read_num = 5;
                update_num = 5;
                penalty_rw_rate = 0.5;
            }
            else if(strcmp(argv[i], "1.0") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
				workload_WriteRate = 0;
                read_num = 0;
                update_num = TOTAL_OPT_NUMS;
                is_all_set_all_get = true;
                penalty_rw_rate=1.0;
            }
            else if(strcmp(argv[i], "0.75") == 0)
            {
				workload_WriteRate = (double)1/(double)3;
                INFO_LOG(" RW_TATE is [%s] %lf", argv[i],workload_WriteRate);
                read_num = 1;
                update_num =3;
                penalty_rw_rate = 0.75;
            }
            else if(strcmp(argv[i], "0.0") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
				workload_WriteRate = 150;
                read_num = 6000;
                update_num = 1;     
                is_all_set_all_get = true;
            }
            else if(strcmp(argv[i], "0.2") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
				workload_WriteRate = 4;
                read_num = 20;
                update_num = 5;
                penalty_rw_rate = 0.2;
                main_node_is_readable = true;
            }
            else if(strcmp(argv[i], "0.01") == 0)
            {
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
				workload_WriteRate = 99;
                read_num = 99;
                update_num = 1;
                main_node_is_readable = true;
                penalty_rw_rate = 0.01;
            }
            else if(strcmp(argv[i], "0.05") == 0)
            {
				workload_WriteRate = 19;
                INFO_LOG(" RW_TATE is [%s]", argv[i]);
                read_num = 19;
                update_num = 1;
                main_node_is_readable = true;
                penalty_rw_rate = 0.05;
            }
            else
            {
                ERROR_LOG("Write rate = [0.0 0.01 0.05 0.2 0.5 0.75 1.0]!");
                exit(0);
            }

            if (update_num < 1)
            {
                ERROR_LOG("update_num not enought exit!");
                exit(-1);
            }
	    }
    }
	
#ifdef NIC_MULITI_THREAD
    nic_thread_num = PARTITION_NUMS;
#else
    nic_thread_num = 1;
#endif


	mehcached_shm_init(page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);


    server_instance = dhmp_server_init(SERVER_ID);
    // if (main_node_is_readable)
    //     available_r_node_num = server_instance->config.nets_cnt;
    // else
        // main_node_is_readable=false;

    little_idx=-1;      
	 __test_size = server_instance->config.nets_cnt * __test_size;
                update_num = PARTITION_NUMS * TOTAL_OPT_NUMS;
		if(workload_WriteRate == (double)150)
		{
			update_num = PARTITION_NUMS*200;
	}
			read_num = workload_WriteRate *  (double)update_num;
        __access_num = read_num + update_num;
        ERROR_LOG("Total op num is [%d] ,read_op is [%d], set_op is [%d]", __access_num, read_num, update_num);
		



 startwork = 0;
	init_mulit_server_work_thread();
    // op_gap;
    partition_req_count_array = (int*) malloc(sizeof(int) * PARTITION_NUMS);
    memset(partition_req_count_array, 0, sizeof(int) * PARTITION_NUMS);
    client_mgr = dhmp_client_init(INIT_DHMP_CLIENT_BUFF_SIZE, false);
    Assert(server_instance);
    Assert(client_mgr);
    avg_partition_count_num = update_num /(int) PARTITION_NUMS;


    generate_test_data((size_t)0, (size_t)1, (size_t)__test_size , (size_t)TEST_KV_NUM);
//    if (!is_all_set_all_get)
        generate_local_get_mgs_handler((size_t) ReadMsg);

    next_node_mappings = (struct replica_mappings *) malloc(sizeof(struct replica_mappings));
    memset(next_node_mappings, 0, sizeof(struct replica_mappings));

    if (IS_MAIN(server_instance->server_type))
    {
        if (server_instance->node_nums < 3)
        {
            ERROR_LOG("The number of cluster is not enough");
            exit(0);
        }
        Assert(server_instance->server_id == 0);
    }


    if (IS_MAIN(server_instance->server_type))
    {
        mehcached_table_init(main_table, TABLE_BUCKET_NUMS, 1, TABLE_POOL_SIZE, true, true, true,\
             numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
        mehcached_table_init(log_table, TABLE_BUCKET_NUMS, 1, TABLE_POOL_SIZE, true, true, true,\
             numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);

        memset(mirror_node_mapping, 0, sizeof(struct replica_mappings) * PARTITION_MAX_NUMS*MIRROR_NODE_NUM);
        Assert(main_table);
    }

    if (IS_MIRROR(server_instance->server_type))
    {
        mehcached_table_init(main_table, TABLE_BUCKET_NUMS, 1, TABLE_POOL_SIZE, true, true, true,\
             numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
        Assert(main_table);
    }


	if (IS_MAIN(server_instance->server_type))
    {
		MID_LOG("Node [%d] do MAIN node init work", server_instance->server_id);
        // 向所有副本节点发送 main_table 初始化请求
        Assert(server_instance->server_id  < REPLICA_NODE_HEAD_ID);
		size_t nid;
		size_t init_nums = 0;
		bool* inited_state = (bool *) malloc(sizeof(bool) * server_instance->node_nums);
		memset(inited_state, false, sizeof(bool) * server_instance->node_nums);
		// for (nid = REPLICA_NODE_HEAD_ID; nid <= REPLICA_NODE_TAIL_ID; nid++)

		//size_t mapping_nums = get_mapping_nums();
        struct ibv_mr* leader_mr = malloc(4 * sizeof(struct ibv_mr));
        copy_mapping_mrs_info(leader_mr);
        ERROR_LOG("leader_mr = %p %p %p %p",leader_mr->addr, leader_mr[1].addr,  leader_mr[2].addr, leader_mr[3].addr);
		memcpy(&local_mr[1],&leader_mr[1],sizeof(struct ibv_mr));
	

  
		init_nums = 1;
         while (init_nums < REPLICA_NODE_HEAD_ID)
         {
             micaserver_get_cliMR(&mirror_node_mapping[init_nums][0], init_nums);
             for (i=1; i < (int)PARTITION_NUMS; i++)
                 memcpy(&mirror_node_mapping[init_nums][i], &mirror_node_mapping[init_nums][0], sizeof(struct replica_mappings));
 			init_nums++;
         }
         init_nums = 0;

		if(REPLICA_NODE_NUMS > 0)
             micaserver_get_cliMR(next_node_mappings, REPLICA_NODE_HEAD_ID);


		while (init_nums < REPLICA_NODE_NUMS)
		{
			enum ack_info_state ack_state;
            init_nums = 0;
			ERROR_LOG("replica %d %d",REPLICA_NODE_NUMS,init_nums);
			for (nid = REPLICA_NODE_HEAD_ID; nid <= REPLICA_NODE_TAIL_ID; nid++) 
			{
				if (inited_state[nid] == false)
				{
					ack_state = mica_basic_ack_req(nid, MICA_INIT_ADDR_ACK, true, leader_mr);
					if (ack_state == MICA_ACK_INIT_ADDR_OK)
					{
						inited_state[nid] = true;
						init_nums++;
					}
                    else
                    {
                        // retry
                        if (ack_state != MICA_ACK_INIT_ADDR_NOT_OK)
                        {
                            ERROR_LOG("replica node [%d] unkown error!", nid);
                            Assert(false);
                        }
                    }
				}
                else
                    init_nums++;
			}
		}

         for (i=0; i<nic_thread_num; i++)
         {
             int64_t nic_id = (int64_t)i;
             int retval = pthread_create(&nic_thread[i], NULL, main_node_nic_thread, (void*)nic_id);
             if(retval)
             {
                 ERROR_LOG("pthread create error.");
                 return -1;
             }
        }
		ERROR_LOG("---------------------------MAIN node init finished!------------------------------");

    }

	if (IS_MIRROR(server_instance->server_type))
	{
		
		MID_LOG("Node [%d] is mirror node, don't do any init work", server_instance->server_id);
        ERROR_LOG("---------------------------MIRROR node init finished!---------------------------");
	}


	if(IS_REPLICA(server_instance->server_type))
    {

        struct dhmp_transport *rdma_trans=NULL;
        int expected_connections;
        int active_connection = 0;
        // size_t up_node;
    
        if (!IS_TAIL(server_instance->server_type))
        {
            if (REPLICA_NODE_NUMS> 1)
            {
   
                size_t target_id = server_instance->server_id + 1;
                Assert(target_id != server_instance->node_nums);

                micaserver_get_cliMR(next_node_mappings, target_id);


             for (i=0; i<nic_thread_num; i++)
             {
                 int64_t nic_id = (int64_t)i;
                 int retval = pthread_create(&nic_thread[i], NULL, main_node_nic_thread, (void*)nic_id);
                 if(retval)
                 {
                     ERROR_LOG("pthread create error.");
                     return -1;
                 }
             }
            MID_LOG("Node [%d] is started nicthread and get cliMR!", server_instance->server_id);
			}
        }

            expected_connections = (int)PARTITION_NUMS *2+ 1;  // 非头节点需要被动接受主节点和上游节点的连接
        
        while (true)
        {
            pthread_mutex_lock(&server_instance->mutex_client_list);
            if (server_instance->cur_connections == expected_connections)
            {
                list_for_each_entry(rdma_trans, &server_instance->client_list, client_entry)
                {
					
					
				//	if(active_connection < 2) {active_connection++; continue;} 
                    if (rdma_trans->node_id == -1)
                    {

                        int peer_id = mica_ask_nodeID_req(rdma_trans);
                        if (peer_id == -1)
                        {
                            pthread_mutex_unlock(&server_instance->mutex_client_list);
                            break;
                        }
                       active_connection++;
                        rdma_trans->node_id = peer_id;
                    }
                    INFO_LOG("Replica node [%d] get \"dhmp client\" (MICA MAIN node or upstream node) id is [%d], success!", \
                                server_instance->server_id, rdma_trans->node_id);
                }
            }
            pthread_mutex_unlock(&server_instance->mutex_client_list);

            if (active_connection == expected_connections)
                break;
//			active_connection = 0;
        }

        while(get_table_init_state() == false);

  
        replica_is_ready = true;
        ERROR_LOG("---------------------------Replica Node [%d] init finished!---------------------------", server_instance->server_id);
    }

#ifdef MAIN_LOG_DEBUG_THROUGHOUT
    if (IS_MAIN(server_instance->server_type))
    {
		sleep(5);
        set_workloada_server();
    }

#endif

    pthread_join(server_instance->ctx.epoll_thread, NULL);
    return 0;
}


struct dhmp_msg * 
pack_test_set_resq(struct test_kv * kvs, int tag, int partition_id)
{
    void * base;
    struct dhmp_msg* msg;
	struct post_datagram *req_msg;
	struct dhmp_mica_set_request *req_data;
    size_t key_length  = kvs->true_key_length +  KEY_TAIL_LEN;
    size_t value_length= kvs->true_value_length + VALUE_HEADER_LEN + VALUE_TAIL_LEN;
    size_t total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_set_request) + key_length + value_length;
    msg = (struct dhmp_msg*)malloc(sizeof(struct dhmp_msg));
	base = malloc(total_length); 
	memset(base, 0 , total_length);
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_mica_set_request *)((char *)base + sizeof(struct post_datagram));
	

	req_msg->node_id = MAIN;	
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_SET_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_mica_set_request);


	req_data->current_alloc_id = 0;
	req_data->expire_time = 0;
	req_data->key_hash = kvs->key_hash;
	req_data->key_length = key_length;
	req_data->value_length = value_length;	
	req_data->overwrite = true;
	req_data->is_update = false;
	req_data->tag = (size_t)tag;

    // req_data->partition_id = (int) (*((size_t*)kvs->key)  % (PARTITION_NUMS));
    
	req_data->partition_id = partition_id;

    partition_req_count_array[req_data->partition_id]++;

	memcpy(&(req_data->data), kvs->key, kvs->true_key_length);		// copy key
    memcpy(( (void*)&(req_data->data) + key_length), kvs->value,  kvs->true_value_length);	

    msg->data = base;
    msg->data_size = total_length;
    msg->msg_type = DHMP_MICA_SEND_INFO_REQUEST;
    INIT_LIST_HEAD(&msg->list_anchor);
    msg->trans = NULL;
    msg->recv_partition_id = -1;
    msg->partition_id = req_data->partition_id;

    Assert(msg->list_anchor.next != LIST_POISON1 && msg->list_anchor.prev!= LIST_POISON2);
    return msg;
}

struct dhmp_msg* 
pack_test_get_resq(struct test_kv * kvs, int tag, size_t expect_length, int partition_id)
{
  	void * base;
	void * data_addr;
    struct dhmp_msg* msg;
	struct post_datagram *req_msg;
	struct dhmp_mica_get_request *req_data;
    struct dhmp_mica_get_response* get_resp;
    size_t key_length = kvs->true_key_length+KEY_TAIL_LEN;
	size_t total_length = sizeof(struct post_datagram) + sizeof(struct dhmp_mica_get_request) + key_length;


    msg = (struct dhmp_msg*)malloc(sizeof(struct dhmp_msg));
    base = malloc(total_length);
    memset(base, 0 , total_length);
    // get_resp = (struct dhmp_mica_get_response*) malloc(sizeof(struct dhmp_mica_get_response) + expect_length);
    get_resp = NULL;
	req_msg  = (struct post_datagram *) base;
	req_data = (struct dhmp_mica_get_request *)((char *)base + sizeof(struct post_datagram));


	req_msg->node_id = (int)server_instance->server_id;	 
	req_msg->req_ptr = req_msg;
	req_msg->done_flag = false;
	req_msg->info_type = MICA_GET_REQUEST;
	req_msg->info_length = sizeof(struct dhmp_mica_get_request);


	req_data->current_alloc_id = 0;
	req_data->key_hash = kvs->key_hash;
	req_data->key_length = key_length;
	req_data->get_resp = get_resp;
	req_data->peer_max_recv_buff_length = (size_t)expect_length;

	req_data->partition_id = partition_id;

	req_data->tag = (size_t)tag;
	data_addr = (void*)req_data + offsetof(struct dhmp_mica_get_request, data);
	memcpy(data_addr, kvs->key, GET_TRUE_KEY_LEN(key_length));		// copy key

    msg->data = base;
    msg->data_size = total_length;
    msg->msg_type = DHMP_MICA_SEND_INFO_REQUEST;
    INIT_LIST_HEAD(&msg->list_anchor);
    msg->trans = NULL;
    msg->recv_partition_id = -1;
    msg->partition_id = req_data->partition_id;
    Assert(msg->list_anchor.next != LIST_POISON1 && msg->list_anchor.prev!= LIST_POISON2);
    return msg;
}


size_t max_num;
void generate_local_get_mgs_handler(size_t read_per_node)
{
     max_num = (size_t)update_num > read_per_node ? (size_t)update_num : read_per_node;

	struct dhmp_msg* msg ;
 	int i,j,idx;
//	for(i=0;i<PARTITION_NUMS;i++)read_list[i] = (struct dhmp_msg**)malloc(sizeof(struct dhmp_msg*) * (read_per_node/PARTITION_NUMS+5));
	
     int count[PARTITION_MAX_NUMS]= {0};
	for(j=0;j<(int)PARTITION_NUMS;j++)
	{
     for (i=0; i<(int)read_per_node; i++)
     {
         idx = i % TEST_KV_NUM; /* idx = rand() % TEST_KV_NUM;*/
         msg = pack_test_get_resq(&kvs_group[idx], i, (size_t)__test_size + VALUE_HEADER_LEN + VALUE_TAIL_LEN,j);
		read_list[j][count[j]%ReadMsg] = msg;
         count[j]++;
     }
	}
     for(i=0;i<(int)PARTITION_NUMS;i++)
         ERROR_LOG("generate read[%d] [%d]",i,count[i]);


    switch (workload_type)
    {
        case UNIFORM:
			for(i=0; i<(int)PARTITION_NUMS;i++)
			{
				rand_num_partition[i] =  (int *)malloc(sizeof(int) * (size_t)max_num);
				pick_uniform(pf_partition, rand_num_partition[i] , (int)max_num);
				write_num_partition[i] = (int *)malloc(sizeof(int) * (size_t)max_num);
				pick_uniform(pf_partition, write_num_partition[i] , (int)max_num);
			}
            break;
        case ZIPFIAN:
            for(i=0; i<(int)PARTITION_NUMS;i++)
            {
                rand_num_partition[i] =  (int *)malloc(sizeof(int) * (size_t)max_num);
                write_num_partition[i] = (int *)malloc(sizeof(int) * (size_t)max_num);

                pick_zipfian(pf_partition, rand_num_partition[i] , (int)max_num);
                pick_zipfian(pf_partition, write_num_partition[i] , (int)max_num);
            }
            break;
        default:
            ERROR_LOG("Unkown!");
            break;
    }
}

void set_workloada_server()
{
	int i,j = 0;
    int idx;
    int set_workload_max_nums = TEST_KV_NUM;
    for(j = 0;j<(int)PARTITION_NUMS;j++)
    {
        set_msgs_group = (struct dhmp_msg**) malloc( (size_t)(update_num/PARTITION_NUMS) * sizeof(void*));
        for (i=0; i< (update_num/PARTITION_NUMS);i++)
        {
            idx = i % set_workload_max_nums;
            set_msgs_group[i] = pack_test_set_resq(&kvs_group[write_num_partition[j][idx]], i,j);
            set_msgs_group[i]->partition_id = j;
            bool is_async;
            dhmp_send_request_handler(NULL, set_msgs_group[i], &is_async, 0, 0, false);
        }

    }
    
    set_workload_OK = 1;
}




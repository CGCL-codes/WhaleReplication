/*
 * @Descripttion: 
 * @version: 
 * @Author: sueRimn
 * @Date: 2021-04-25 17:46:17
 * @LastEditors: Please set LastEditors
 * @LastEditTime: 2022-03-09 19:54:09
 */
#ifndef DHMP_CLIENT_H
#define DHMP_CLIENT_H

#include "dhmp_context.h"
#include "dhmp_config.h"
#define DHMP_CLIENT_HT_SIZE 50000

struct dhmp_client{
	/* data */
	size_t self_node_id;
	bool	is_test_clinet;
	struct dhmp_context ctx;
	struct dhmp_config config;
	struct list_head dev_list;

	struct dhmp_transport *connect_trans[DHMP_SERVER_NODE_NUM][PARTITION_MAX_NUMS+1];
	struct dhmp_transport *read_connect_trans[DHMP_SERVER_NODE_NUM][PARTITION_MAX_NUMS];
	/*store the dhmp_addr_entry hashtable*/

	//pthread_mutex_t mutex_ht;
	struct hlist_head addr_info_ht[DHMP_CLIENT_HT_SIZE];
	pthread_mutex_t mutex_send_mr_list;
	struct list_head send_mr_list;

	int fifo_node_index;	/*use for node select*/
	int conn_index;	/*指出当前可用connect_trans数组中的位置*/

	pthread_t work_thread1;
	pthread_t work_thread2;
	pthread_t work_thread3;
	pthread_t work_thread4;
	pthread_t work_thread5;
	pthread_t work_thread6;

	pthread_mutex_t mutex_work_list;
	pthread_mutex_t mutex_asyn_work_list;
	struct list_head work_list;
	struct list_head work_asyn_list;

	struct dhmp_send_mr* read_mr[DHMP_SERVER_NODE_NUM][PARTITION_MAX_NUMS];
};

extern struct dhmp_client *client_mgr;
extern size_t CLINET_ID;
#endif



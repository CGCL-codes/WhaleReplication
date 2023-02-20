#ifndef DHMP_CONFIG_H
#define DHMP_CONFIG_H

#define DHMP_CONFIG_FILE_NAME "config.xml"
// ADD 
// #define DHMP_CLIENT_CONFIG_FILE_NAME "config_cli.xml"
// #define DHMP_SERVER_CONFIG_FILE_NAME "server_config.xml"


#define DHMP_ADDR_LEN 18
#define DHMA_NIC_NAME_LEN 10

#include "dhmp.h"

struct dhmp_net_info{
	char	nic_name[DHMA_NIC_NAME_LEN];
	char	addr[DHMP_ADDR_LEN];
	int		port;
};

/*nvm simulate infomation*/
struct dhmp_simu_info{
	int rdelay;
	int wdelay;
	int knum;
};

struct dhmp_mem_info
{
	int dram_node;
	int nvm_node;
};


struct dhmp_config{
	struct dhmp_net_info net_infos[DHMP_SERVER_NODE_NUM];
	struct dhmp_simu_info simu_infos[DHMP_SERVER_NODE_NUM];
	struct dhmp_mem_info mem_infos[DHMP_SERVER_NODE_NUM];
	int curnet_id;  //store the net_infos index of curnet
	int nets_cnt;  //current include total server nodes
	char watcher_addr[DHMP_ADDR_LEN];
	int watcher_port;
	
	// add
	int is_self;
};

/**
 *	app read the config.xml infomation, 
 *	analyse the client configuration include log level 
 *	and the servers configuration 
 */

int dhmp_config_init ( struct dhmp_config* config_ptr, bool is_client);
extern void dhmp_set_curnode_id ( struct dhmp_config* config_ptr, bool is_ubuntu);
#define MAX_PORT_NUMS 100
extern int used_id[MAX_PORT_NUMS];
extern int used_nums ;

extern bool is_ubuntu;
#endif


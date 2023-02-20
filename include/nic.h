#ifndef NIC_H
#define NIC_H

// nic.c
void set_main_node_thread_addr(void* (**p) (void*));
void set_replica_node_thread_addr(void* (**p)(void*));

void * main_node_nic_thread(void * args);
volatile bool nic_thread_ready;
#endif
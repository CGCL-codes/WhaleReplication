# Whale

Modern storage systems typically replicate data on multiple servers to provide high reliability and availability. However, most commercially-deployed datastores often fail to offer low latency, high throughput, and strong consistency at the same time. This paper presents Whale, a Remote Direct Memory Access (RDMA) based primary-backup replication system for in-memory datastores. Whale achieves both low latency and strong consistency by decoupling metadata multicasting from data replication for all backup nodes, and using an optimistic commitment mechanism to respond to client write requests earlier. Whale achieves high throughput by propagating writes from the primary node to backup nodes asynchronously via RDMA-optimized chain replication. To further reduce the cost of data replication, we design a log-structured datastore to fully exploit the advantages of one-sided RDMA and Persistent Memory (PM).  

More detailed instructions on code compiling, running, and testing are elaborated in the following.


Code Structure, Dependencies, Compiling, and Running
------------
## 1. Code Structure
 1. `LineKV/` contains the source code on client side  
 2. `LineKV/bin` contains the executable files on client side, which are generated from `LineKV/test`, and the datasets for PageRank
 3. `LineKV/include` contains the header files needed by client side   
 4. `LineKV/src/midd` contains the source code of RDMA connection, RDMA replication tasks generating and processing  
 5. `LineKV/src/mica` contains the source code of datastore


## 2. External Dependencies  
Before running Whale codes, it's essential that you have already install dependencies listing below.

* g++ (GCC) 4.8.5
* numactl-devel
* ndctl
* [RDMA driver](https://www.mellanox.com/products/infiniband-drivers/linux/mlnx_ofed) (Packages, such as libibverbs/librdmacm/etc. , may need to be installed for RDMA driver)  

## 3. Compiling 
To run Whale, the entire project needs to be compiled. The project requires some hardware and software supports and configurations, such as the network setup and NVM use modules.

**3.1 Environment Setting**    

&#160; &#160; &#160; &#160;First, the <kbd>config.xml</kbd> need to be configured according to the run-time environment.  
```
[server node] cd LineKV
[server node] vim config.xml
```
&#160; &#160; &#160; &#160;In the <kbd>config.xml</kbd>，the following information should be configured on the client and server sides.  
```
<server id="x">         //server node ID, node 0 is the primary leader node  
    <nic_name>ib0</nic_name>        //RDMA Card Name by ifconfig command
    <addr>xxx.xxx.xxx.xxx</addr>        //Server's RDMA Card Address
    <port>39300</port>          //Server's listen port
</server>
```

To use NVM as memory at the server, this project uses NVM in APP Direct Mode and configures it as NUMA node using ndctl. The way to use NVM can be referred to a blog [How To Extend Volatile System Memory (RAM) using Persistent Memory on Linux](https://stevescargall.com/2019/07/09/how-to-extend-volatile-system-memory-ram-using-persistent-memory-on-linux/?tdsourcetag=s_pctim_aiomsg). The project use NVM in a NUMA node by default. ( Please configuring the numa node in LineKV/include/numa_malloc_header.h .)


**3.2 Compiling** 

The following commands are used to compile the entire project:
```
[server node] cd LineKV
[server node] ./rebuild
```

The executable <kbd>mica</kbd> and <kbd>mmap_test</kbd> programs are generated in the folder <kbd>bin/</kbd>.

**3.3 More**

With Ubuntu system, the <kbd>bits/sigset.h</kbd> header should be changed to <kbd>bits/types/__sigset_t.h</kbd> in <kbd>include/sys/epoll.h</kbd> for correct compiling. It also allowed to revise and use the shell file:
```
[server node] cd LineKV
[server node] vim send_all.sh
[server node] ./send_all.sh
```
To send the executable programs to other server nodes and run them.

## 4. Running   
The executable files can be found in the directories `LineKV/bin` directory. It is recommand to start all server nodes at the same time for correctness.
```
[server node] cd LineKV
[server node] taskset -c [CPU Number]-[CPU Number] ./bin/mica [Server ID] [Thread Number] [Is ubuntu?] [Value Size] [Workload uniform/zipfian] [Credict] [Write Rates]
```

For example, the following show the commands on server node 0 (which is running ubuntu OS) to request objects of 64 Byte with 6 threads. The workload is write-only and follows an uniform distribution. No extend request is allowed (Credict is 1) and the replication system is bound to CPU0-CPU19. 
```
[server node] taskset -c 0-19 ./bin/mica 0 6 1 64 uniform 1 1.0
```
  
## 5. Performance Test 

**5.1 YCSB**  
Whale can run YCSB as Macro-benchmarks to evaluate the performance of Whale.  
|  Workload  | Explanation  |
|  ----  | ----  |
| WorkloadA | Read : Write = 50 : 50 |
| WorkloadB | Read : Write = 95 : 5 |
| WorkloadC | Read : Write = 100 : 0|
| WorkloadD | Read : Insert = 95 : 5 |
| WorkloadE | Scan(Scan_range = 3) : Write = 95 : 5 |
| WorkloadF | Read : Read-Modify-Write = 50 : 50 |

The following commands are used to run different workloads of YCSB:
```
[server node] cd LineKV
[server node] taskset -c [CPU Number]-[CPU Number] ./bin/mica [Server ID] [Thread Number] [Is ubuntu?] [workload[a-f]] 
```
&#160; &#160; &#160; &#160;For example, the following command performs workload A with default configuration:
```
[server node] taskset -c 0-19 ./bin/mica 0 6 1 workloada
```  
**5.2 Software-Defined Datastores**  
Whale enables various datastore configurations for different users. Specifically, the replication protocol and the corresponding topology of storage nodes can be softwaredefined according to users’ SLAs. To configure the datastores, some macro definitions in file `LineKV/include/dhmp.h` need to be specified. The following list shows the macro definitions and the corresponding systems.


|  Macro Definition  | Explanation |
|  ----  | ----  |
| #define MAIN_NODE_ID | node id of primary_leader node (default 0) |
| #define MIRROR_NODE_NUM | numbers of mirror_leader nodes |



**5.3 CPU Load**  
To evaluate the impact of CPU load on performance of the replication systems, the following macro definitions in `LineKV/include/dhmp.h` need to be set.

|  Macro Definition   | Explanation |
|  ----  | ----  |
| #define TEST_CPU_BUSY_WORKLOAD  | Adding CPU Load |








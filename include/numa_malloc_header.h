#ifndef NUMA_MALLOC

    #define NUMA_MALLOC(length)	  numa_alloc_onnode(length, 1);	

#endif

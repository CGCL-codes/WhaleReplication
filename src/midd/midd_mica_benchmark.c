/*
 * @Author: your name
 * @Date: 2022-03-09 20:00:04
 * @LastEditTime: 2022-03-09 21:04:46
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: /LineKV/src/mica_kv/benchmark.c
 */

#include "hash.h"
#include "dhmp.h"
#include "dhmp_log.h"
#include "midd_mica_benchmark.h"

int current_credict = 0;

int __test_size;
int __write_avg_num=0;
int __access_num=0;
int read_num, update_num;
int end_round=0;
int op_gaps[4];
int little_idx;
bool is_all_set_all_get =false;

int main_node_is_readable;
enum WORK_LOAD_DISTRIBUTED workload_type;
struct test_kv kvs_group[TEST_KV_NUM];

const double A = 1.3;  
const double C = 1.0;  
//double pf[TEST_KV_NUM]; 
// int rand_num[TEST_KV_NUM]={0};

int * read_num_penalty=NULL;

 
void generate_zipfian(double pf[], size_t nums)
{
    int i;
    double sum = 0.0;
 
    for (i = 0; i < nums; i++)
        sum += C/pow((double)(i+2), A);

    for (i = 0; i < nums; i++)
    {
        if (i == 0)
            pf[i] = C/pow((double)(i+2), A)/sum;
        else
            pf[i] = pf[i-1] + C/pow((double)(i+2), A)/sum;
    }
}

 
void pick_zipfian(double pf[], int rand_num[], int max_num)
{
	int i, index;

    generate_zipfian(pf, TEST_KV_NUM);

    srand(time(0));
    for ( i= 0; i < max_num; i++)
    {
        index = 0;
        double data = (double)rand()/RAND_MAX; 
        while (index<(TEST_KV_NUM-1)&&data > pf[index])   
            index++;
		rand_num[i]=index;
       // printf("%d ", rand_num[i]);
    }
    //printf("\n");
//    sleep(1);
}

void pick_uniform(double pf[], int rand_num[], int max_num)
{
	int i, rand_idx, tmp;

    for (i=0 ;i<max_num; i++)
        rand_num[i] = i % TEST_KV_NUM;

    srand(time(0));
    for ( i= 0; i < max_num; i++)
    {
        rand_idx = rand() % max_num; 
        tmp = rand_num[i];
        rand_num[i] = rand_num[rand_idx];
        rand_num[rand_idx] = tmp;
    }
}

struct test_kv *
generate_test_data(size_t key_offset, size_t val_offset, size_t value_length, size_t kv_nums)
{
    size_t i,j;
    int partition_id;
	int kv_count[PARTITION_MAX_NUMS];
    for(i = 0;i < PARTITION_NUMS;i++)
        kv_count[i] = 0;
    //struct test_kv *kvs_group;
    //kvs_group = (struct test_kv *) malloc(sizeof(struct test_kv) * kv_nums);
    memset(kvs_group, 0, sizeof(struct test_kv) * kv_nums);

    for (i = 0; i < kv_nums; i++)
    {
        size_t key = i;

        kvs_group[i].true_key_length = sizeof(key);
        kvs_group[i].true_value_length = value_length;
        kvs_group[i].key = (uint8_t *)malloc(kvs_group[i].true_key_length);
        kvs_group[i].value = (uint8_t*) malloc(kvs_group[i].true_value_length);

        // 注意我们 get 回来的数据需要  考虑到 header 和 tail 的大小
        for (j=0; j<1; j++) // 暂时只开一个缓冲区
            kvs_group[i].get_value[j] = (uint8_t*) malloc(kvs_group[i].true_value_length + VALUE_HEADER_LEN + VALUE_TAIL_LEN);

        memset(kvs_group[i].value, (int)(i+val_offset), kvs_group[i].true_value_length);
        memcpy(kvs_group[i].key, &key, kvs_group[i].true_key_length);
		kvs_group[i].key_hash = hash(kvs_group[i].key, kvs_group[i].true_key_length );
        partition_id = (kvs_group[i].key_hash)  % (PARTITION_NUMS);
		j = i % (PARTITION_NUMS);
		if(j == partition_id)
			kv_count[partition_id] ++;
    }
for(i = 0;i < PARTITION_NUMS;i++)
        ERROR_LOG("&&&&&&&&&&    kv_bucket[%d] = %d", i,kv_count[i]);
    return kvs_group;
}

int count_r[PARTITION_MAX_NUMS]= {0};
int count_w[PARTITION_MAX_NUMS]= {0};

struct test_kv *
generate_test_data_YCSB(size_t key_offset, size_t val_offset, size_t value_length, size_t kv_nums)
{
    generate_test_data(key_offset, val_offset, (size_t)__test_size , (size_t)TEST_KV_NUM);

    set_msgs_group = (struct dhmp_msg**) malloc( (size_t)(100000) * sizeof(void*));

    FILE *fp;
  fp = fopen("/home/hdlu/LineKV/atrace.txt", "r");
  if(fp == NULL){
      printf("open file error");
          exit(0);
  }
  char s[40];
  struct dhmp_msg* msg ;
  char delim[] = " ";
  uint32_t i = 0, writes = 0;
  char opcode = -1;
  int idx, partition_id;
  bool is_async;
  while(fgets(s, 40, fp)){
      char *tmp;
      char *str1, *str2;
          tmp = strtok(s, delim);
          str1 = tmp;
          int j = 0;
      while(tmp != NULL){
          if(j == 1){
            str2 = tmp;
        }
          tmp = strtok(NULL, delim);
                  j++;
      }

      if(atoi(str1) == 0){
          opcode = 0; 
      }else if(atoi(str1) == 1){
          opcode = 1;
      }


      int len = strlen(str2);
      char str2_key[8];
      strcpy(str2_key, str2 + len - 8);
      idx = (atol(str2_key)) % TEST_KV_NUM;
//	count_r[14]++;ERROR_LOG("count{%d] [%d] %d",count_r[14] ,opcode,idx);
      if(opcode == 0 )
      {
          msg = pack_test_get_resq(&kvs_group[idx], 0, (size_t)__test_size + VALUE_HEADER_LEN + VALUE_TAIL_LEN);
          partition_id = idx % PARTITION_NUMS;
          read_list[partition_id][count_r[partition_id]%10000] = msg;
          count_r[partition_id]++;
      }
      else if(opcode == 1)
      {
		if(kv_nums == 111)
		{	
          msg  = (struct dhmp_msg*)pack_test_set_resq(&kvs_group[idx], count_w[partition_id]);
		 set_msgs_group[count_w[partition_id]] = msg;
		ERROR_LOG("count =%p  %p",msg,set_msgs_group[count_w[partition_id]]);
          dhmp_send_request_handler(NULL, set_msgs_group[count_w[partition_id]], &is_async, 0, 0, false);
		}
          count_w[partition_id]++;
      }
          
  }
  fclose(fp);
  for(i=0;i<PARTITION_NUMS;i++)
  {
	count_r[i] = count_r[i]/4;
    ERROR_LOG("generate read[%d]=[%d]",i,count_r[i]);
    ERROR_LOG("generate write[%d]=[%d]",i,count_w[i]);
    }
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
	startwork = 1;
}

 
bool 
cmp_item_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value)
{
    bool re = true;
    if (a_value_length != b_value_length)
    {
        ERROR_LOG("MICA value length error! %lu != %lu", a_value_length, b_value_length);
        re= (false);
    }

#ifdef DUMP_MEM
    size_t off = 0;
    bool first = false, second = false, second_count=0;
    for (off = 0; off < b_value_length; off++)
    {
        if (a_out_value[off] != b_out_value[off])
        {
            if (first == false)
            {
                first = true;
                size_t tp = off - 16;
                for (; tp < off; tp++)
                    printf("%ld, %hhu, %hhu\n", tp, a_out_value[tp], b_out_value[tp]);
            }
   
            printf("%ld, %hhu, %hhu\n", off, a_out_value[off], b_out_value[off]);
        }
        else
        {
            if (first == true && second == false && second_count < 16)
            {
                printf("%ld, %hhu, %hhu\n", off, a_out_value[off], b_out_value[off]);
                second_count ++;
                if (second_count == 16)
                    second = true;
            }
        }
    }
#endif

    if (memcmp(a_out_value, b_out_value, b_value_length) != 0 )
    {
        ERROR_LOG("value context error! %p, %p, len is %lu", a_out_value, b_out_value, b_value_length);
        re=  (false);
    }

    return re;
}

void dump_value_by_addr(const uint8_t * value, size_t value_length)
{
    uint64_t header_v, tail_v, value_count;
    
    bool dirty;

    header_v = *(uint64_t*) value;
    value_count = *(uint64_t*) (value + sizeof(uint64_t));
    tail_v = *(uint64_t*) (value + 2*sizeof(uint64_t) + GET_TRUE_VALUE_LEN(value_length));
    dirty = *(bool*)(value + 3*sizeof(uint64_t) + GET_TRUE_VALUE_LEN(value_length));

    // INFO_LOG("value header_v is %lu, value_count is %lu, tail_v is %lu, dirty is %d", header_v,value_count,tail_v, dirty);
#ifdef DUMP_VALUE
    const uint8_t * value_base  = (value + 2 * sizeof(uint64_t));
    HexDump(value_base, (int)(GET_TRUE_VALUE_LEN(value_length)), (int) value_base);
#endif
}


bool 
cmp_item_all_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value)
{
    bool re = true;
    if (a_value_length != b_value_length)
    {
        ERROR_LOG("MICA value length error! %lu != %lu", a_value_length, b_value_length);
        re= (false);
    }
    size_t off = 0;
    for (off = 0; off < b_value_length; off++)
    {
        if (a_out_value[off] != b_out_value[off])
        {
            // 打印 unsigned char printf 的 格式是 %hhu
            printf("%ld, %hhu, %hhu\n", off, a_out_value[off], b_out_value[off]);
        }
    }
    if (memcmp(a_out_value, b_out_value, b_value_length) != 0 )
    {
        ERROR_LOG("value context error! %p, %p, len is %lu", a_out_value, b_out_value, b_value_length);

        // dump_value_by_addr(a_out_value, a_value_length);
        // dump_value_by_addr(b_out_value, b_value_length);
        re=  (false);
    }

    if (memcmp(GET_TRUE_VALUE_ADDR(a_out_value), GET_TRUE_VALUE_ADDR(b_out_value), GET_TRUE_VALUE_LEN(b_value_length)) != 0 )
    {
        ERROR_LOG("true value error!");
        re=  (false);
    }
    return re;
}

 
struct dhmp_msg** set_msgs_group;
 
 
struct BOX* intial_box(int current_node_number, int total_node_number, int kind)
{
	if(current_node_number == 0)
		return NULL;
	
	struct BOX * box = (struct BOX*) malloc(sizeof(struct BOX));
	box->array = (TYPE_BOX *)malloc(TEST_KV_NUM * sizeof(TYPE_BOX));
	memset(box->array, 0, TEST_KV_NUM * sizeof(TYPE_BOX));
	ERROR_LOG("init box");
	return box; 
}

 
void update_box(TYPE_BOX value, struct BOX * box)
{
	return;
}



void print_box(struct BOX* box)
{
}

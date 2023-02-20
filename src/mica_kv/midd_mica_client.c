/*
 * @Author: your name
 * @Date: 2022-03-09 19:58:18
 * @LastEditTime: 2022-03-09 21:23:04
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: /LineKV/src/mica_kv/midd_mica_client.c
 */
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
#include "dhmp_init.h"
#include "mid_rdma_utils.h"
#include "dhmp_top_api.h"

#include "midd_mica_benchmark.h"

bool ica_cli_get(struct test_kv *kv_entry, void *user_buff, size_t *out_value_length, size_t target_id, size_t tag);
struct test_kv * kvs;


// 
bool
mica_cli_get(struct test_kv *kv_entry, void *user_buff, size_t *out_value_length, size_t target_id, size_t tag)
{

}




int main(int argc,char *argv[])
{
 
    return 0;
}
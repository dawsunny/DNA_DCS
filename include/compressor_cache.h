/*compressor cache header file*/

#ifndef __COMPRESSOR_CACHE_H_
#define __COMPRESSOR_CACHE_H_

#include "dcs_const.h"
#include "compressor_op.h"
#include "compressor_con.h"


struct container_list{
    dcs_u64_t container_id;
    struct container_list *next;
};
typedef struct container_list container_list_t;

struct cache_sha_info{
    dcs_u8_t sha[20];
    dcs_u64_t container_id;
    dcs_u32_t offset;
};
typedef struct cache_sha_info sha_cache_t;

struct sha_cache_table{
    dcs_u32_t sha_num[TOTAL_CACHE_NUM/EACH_BLOCK_NUM];
    pthread_rwlock_t cache_lock[TOTAL_CACHE_NUM/EACH_BLOCK_NUM];
    sha_cache_t *sha_cache[TOTAL_CACHE_NUM/EACH_BLOCK_NUM];
};
typedef struct sha_cache_table cache_table_t;

#define TMP_BLOCK_NUM 10
#define BLOCK_NEED 10

struct tmp_sha_table{
    dcs_u32_t block_num;
    dcs_u32_t bucket_id[TMP_BLOCK_NUM];
    dcs_u32_t sha_num[TMP_BLOCK_NUM];
    pthread_rwlock_t cache_lock[TMP_BLOCK_NUM];
    sha_cache_t *sha_cache[TMP_BLOCK_NUM];
};
typedef struct tmp_sha_table tmp_table_t;

#define DATA_CACHE_SIZE 10

struct data_cache_table{
    dcs_u32_t con_num;
    dcs_u32_t con_size[DATA_CACHE_SIZE];
    dcs_u64_t container_id[DATA_CACHE_SIZE];
    dcs_s8_t  *data_con[DATA_CACHE_SIZE];
    pthread_rwlock_t cache_lock[DATA_CACHE_SIZE];
};
typedef struct data_cache_table data_cache_t;

/*init container list*/
dcs_s32_t container_list_init();
/*init cache hash table*/
dcs_s32_t cache_table_init();
/*data cache init*/
dcs_s32_t data_cache_init();
/*dcslication*/
dcs_datapos_t *dcslication(sha_array_t *chunk_info,
                               dcs_s8_t  *datap,
                               dcs_u32_t chunk_num,
                               dcs_u64_t container_id,
                               dcs_thread_t *threadp);

dcs_datapos_t *get_dcslication_info(sha_array_t *chunk_info,
                                        dcs_u32_t chunk_num,
                                        dcs_u64_t container_id);

dcs_u32_t cache_delete(dcs_u8_t *sha, dcs_u64_t container_id);
/*query the cache*/
dcs_datapos_t *cache_query(dcs_u8_t *sha);
/*search  the cache list juge weather the container is in cache list*/
dcs_s32_t search_cache_list(dcs_u64_t container_id);
/*cache the fp*/
dcs_s32_t cache_fp(dcs_u64_t container_id);
/*insert fp into cache*/
dcs_s32_t insert_cache(con_sha_t con_sha, dcs_u32_t *tmp_num, dcs_u64_t container_id);
/*clean the cache bucket*/
dcs_s32_t clean_cache_bucket(dcs_u32_t bucket_id);
/*clean tmp cache*/
dcs_s32_t clean_tmp_cache();
/*get bucket power*/
dcs_u32_t get_bucket_power();
/*get data container*/
dcs_s8_t *get_data_container(dcs_u64_t  container_id, dcs_u32_t *size);
/*update data cache*/
dcs_s32_t update_data_cache(dcs_u32_t thread_id);


extern data_cache_t    *data_cache;

#endif

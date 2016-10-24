/*master cache head file*/

#ifndef __MASTER_CACHE_H_
#define __MASTER_CACHE_H_

struct master_cache{
    dcs_u8_t *sha;
    dcs_u32_t sha_num;
    pthread_mutex_t cache_lock;
};
typedef struct master_cache master_cache_t;

#define MAX_CACHE_NUM 1000

extern master_cache_t sha_cache[MAX_CACHE_NUM];

/*init master cache*/
dcs_s32_t __dcs_master_cache_init();
/*insert FP to cache table*/
dcs_s32_t __dcs_master_cache_insert(dcs_u8_t *sha, dcs_u32_t sha_num);


#endif

/*compressor index head file*/

#ifndef __COMPRESSOR_INDEX_H_
#define __COMPRESSOR_INDEX_H_

#include "bloom.h"
#include "dcs_const.h"

#define EXTEN_HASH_SIZE (1024*1024)
#define BUCKET_NUM      16

/*index element*/
struct sha_con{
    dcs_u8_t sha[20];
    dcs_u32_t ref_count;
    dcs_u64_t con_id;
};
typedef struct sha_con sha_con_t;

/*hash block*/
struct hash_type{
    dcs_u64_t block_id;
    pthread_rwlock_t block_lock;
    dcs_u32_t sha_num;
    dcs_u32_t block_depth;
    sha_con_t   sha_conid[INDEX_BLOCK_SIZE];
    struct hash_type *next;
};
typedef struct hash_type index_block_t;

struct exten_hash{
    dcs_u32_t total_block_num;//by zhj
    dcs_u32_t split_depth;
    pthread_rwlock_t depth_lock;
    index_block_t *index_block[EXTEN_HASH_SIZE];
};
typedef struct exten_hash exten_hash_t;

/* by zhj **** begin struct******struct for store info of indes block*/
typedef struct {
    dcs_u64_t block_id;
    dcs_u64_t bucket_id;
    dcs_u32_t sha_num;
    dcs_u32_t block_depth;
    sha_con_t   sha_conid[INDEX_BLOCK_SIZE];
} save_info_t;
/*by zhj******** struct   over***********/

//extern BLOOM *bf;
//extern pthread_rwlock_t bloom_lock;
extern exten_hash_t *fp_index;
extern pthread_mutex_t fp_file_lock;
//#define bloom_path "/root/sugon-863/DDSS/compressor/bf"
//#define HASH_FUNC_NUM 3

dcs_s32_t __dcs_fp_index_reload();
/*init extendible hash*/
dcs_s32_t __dcs_compressor_index_init();
/*init the bloom filter*/
dcs_s32_t __dcs_compressor_bloom_init();
dcs_s32_t __dcs_tier_bloom_init();
/*query the sample index*/
container_t *dcs_query_index(dcs_u8_t *sha, dcs_s32_t sha_num);
/*insert the sha and container id into extendible hash table*/
dcs_s32_t dcs_insert_index(dcs_u8_t *sha, dcs_s32_t *ref_count, dcs_s32_t sha_num, dcs_u64_t container_id);
dcs_s32_t dcs_delete_index(dcs_datamap_t *mapinfo, dcs_s32_t *chunk_num);
/*insert a key into index*/
dcs_s32_t index_insert(dcs_u8_t *sha, dcs_u64_t container_id, dcs_u32_t ref_count);
/*get block id from the split depth and bitmap query*/
dcs_u32_t get_block_id(dcs_u8_t *sha, dcs_u32_t index_depth);
/*update bitmap */
dcs_s32_t update_bmap(dcs_u32_t block_id);
dcs_s32_t reset_bmap(dcs_u32_t block_id);
/*query the extendible hash index*/
dcs_u32_t index_query(dcs_u8_t *sha, dcs_u64_t *container_id);
dcs_u32_t index_delete(dcs_u8_t *sha, dcs_u64_t container_id);
/*bloom update
 * update the bloom filter use hook gotten from the contaienr
 */
dcs_s32_t bloom_update(dcs_u8_t *sha, dcs_s32_t sha_num);
/**query the bloom filter*/
dcs_s32_t bloom_query(dcs_u8_t *tmpsha);
/*hash function for bloom filter*/
dcs_s32_t bloom_delete(dcs_u8_t *tmpsha);
dcs_u64_t hashfunc(dcs_u8_t *sha, dcs_s32_t i);

/*********** by zhj *******************/
dcs_s32_t save_fp_index();
dcs_s32_t resync_disk_fp_index(dcs_s32_t id);
dcs_s32_t read_fp_index();
dcs_u32_t disk_index_query(dcs_u8_t *sha, dcs_u64_t *container_id);
dcs_u32_t disk_index_delete(dcs_u8_t *sha, dcs_u64_t container_id, dcs_u32_t *todelete);
/********** by zhj ******************/

#endif


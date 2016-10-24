/*compressor container header file*/

#ifndef __COMPRESSOR_CON_H_
#define __COMPRESSOR_CON_H_

#include "dcs_const.h"
#include "amp_list.h"
#define CONTAINER_ID_LEN 12

//modify by bxz
//struct list_head datacon_queue;
//struct list_head fpcon_queue;
//struct list_head update_queue;
extern struct list_head datacon_queue;
extern struct list_head fpcon_queue;
extern struct list_head update_queue;
extern pthread_mutex_t datacon_queue_lock;
extern pthread_mutex_t fpcon_queue_lock; 
extern pthread_mutex_t update_queue_lock;
extern sem_t datacon_sem;
extern sem_t fpcon_sem;
extern sem_t update_sem;
extern struct list_head  container_recycle_list;
extern pthread_mutex_t container_recycle_lock;

struct conid_block{
    dcs_u64_t con_id;
    time_t timestamp;
    struct list_head con_list;
};
typedef struct conid_block con_block_t;

struct con_shainfo{
    dcs_u8_t sha[20];
    dcs_u32_t offset;
};
typedef struct con_shainfo con_sha_t;

struct sha_type{
    dcs_u8_t sha[20];
};
typedef struct sha_type sha_t;

struct data_container_type{
    dcs_s8_t *con;
    //dcs_u32_t size
    dcs_u32_t offset;
    clock_t     timestamp; 
    dcs_u64_t container_id;
    struct list_head con_list;
};
typedef struct data_container_type data_container_t;

struct fp_container_type{
    //con_sha_t *con;
    //dcs_u32_t size;
    sha_t *con;
    dcs_u32_t offset[8*(CONTAINER_SIZE)/DCS_CHUNK_SIZE];
    //dcs_u32_t ref_count[8*(CONTAINER_SIZE)/DCS_CHUNK_SIZE];
    dcs_u32_t chunk_num;
    dcs_u64_t container_id;
    struct list_head con_list;
};
typedef struct fp_container_type fp_container_t;

struct container_table{
    fp_container_t *fp_con[DCS_COMPRESSOR_THREAD_NUM];
    data_container_t *data_con[DCS_COMPRESSOR_THREAD_NUM];
    pthread_rwlock_t con_lock[DCS_COMPRESSOR_THREAD_NUM];
};
typedef struct container_table container_table_t;

extern container_table_t *con_table;

/*init container hash table which is used for cache the not full contaienr*/
dcs_s32_t container_table_init();
/*query the container buf*/
dcs_datapos_t *query_container_buf(dcs_u8_t *sha);
/*add data to container*/
dcs_datapos_t *add_to_container(dcs_s8_t *datap, 
                                  dcs_u32_t chunksize, 
                                  dcs_thread_t *threadp,
                                  dcs_u8_t *sha);
/*queue the full data container*/
void __queue_datacon(data_container_t *data_container);
/*queue the responding fp container*/
void __queue_fpcon(fp_container_t *fp_container);
/*write data container to disk*/
dcs_s32_t compressor_data_wb(data_container_t *data_con);
/*write fp container to disk*/
dcs_s32_t compressor_fp_wb(fp_container_t *fp_con);
dcs_s32_t compressor_update(fp_container_t *fp_con);
/*get data container name including path and filename*/
dcs_s8_t *get_datacon_name(dcs_u64_t container_id);
/*get fp container name including path and filename*/
dcs_s8_t *get_fpcon_name(dcs_u64_t container_id);
/*read the data container from disk*/
dcs_s8_t *read_container(dcs_u64_t container_id, dcs_u32_t * size);
/*compare two sha string*/
dcs_s32_t shastr_cmp(dcs_u8_t *sha1, dcs_u8_t *sha2);
/*read the container from buf*/
dcs_s8_t *read_container_buf(dcs_u64_t container_id, dcs_u32_t * size);
/*compressor answer the disk usage to server*/
dcs_s32_t __dcs_compressor_answer(amp_request_t *req);

dcs_s32_t dcs_compressor_data_fp_sync(data_container_t *data_con, fp_container_t *fp_con);
dcs_s32_t dcs_compressor_data_fp_trunc(data_container_t *data_con, fp_container_t *fp_con);
dcs_u64_t get_container_id();
dcs_s32_t container_dir_init();
dcs_s32_t global_conid_init();
#endif

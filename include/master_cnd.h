/*master configuration header file*/

#ifndef __MASTER_CND_H_
#define __MASTER_CND_H_

#include"bloom.h"

extern BLOOM               **bf;
extern pthread_mutex_t     bloom_lock[DCS_COMPRESSOR_NUM];

extern tier_bloom          **tbf;

extern amp_comp_context_t  *master_comp_context;
extern dcs_u32_t         master_this_id;
extern dcs_u32_t         master_chunk_type;
extern dcs_u32_t         master_chunk_size;
extern dcs_u32_t         master_rout_type;
extern struct list_head    request_queue;
extern pthread_mutex_t     request_queue_lock;
extern sem_t               request_sem;

//#define bloom_path "/tmp/master/bloom/"

/*parse paramatter of master*/
dcs_s32_t __dcs_master_parse_paramatter(dcs_s32_t argc, 
                                            dcs_s8_t **argv);
/*init the connection*/
dcs_s32_t __dcs_master_init_com();
/*request queue*/
dcs_s32_t __queue_req(amp_request_t *req);
/*allocbuf for recieve request from server*/
dcs_s32_t __master_allocbuf(void *msgp, 
                amp_u32_t *niov, amp_kiov_t **iov);
/*master freebuf*/
void __master_freebuf(amp_u32_t niov, amp_kiov_t *iov);
/*init background process*/
inline int  __dcs_daemonlize();
/*init bloom filter*/
dcs_s32_t __dcs_bloom_init();
dcs_s32_t __dcs_master_tier_bloom_init();
dcs_s32_t __tier_bloom_local_reload();
/*get bloom fileter name*/
void get_bloom_filename(dcs_s8_t **bloom_name);
/*display hash function name*/
void usage();


#endif

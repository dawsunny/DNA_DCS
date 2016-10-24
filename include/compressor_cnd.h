/*compressor configuration header file*/

#ifndef __COMPRESSOR_CND_H_
#define __COMPRESSOR_CND_H_

#include "dcs_debug.h"
#include "amp.h"

extern amp_comp_context_t  *compressor_comp_context;
extern dcs_u32_t         compressor_this_id;
extern dcs_u32_t         power;
extern pthread_mutex_t     request_queue_lock;
extern sem_t               request_sem;
extern dcs_u64_t         container;
extern pthread_mutex_t     container_lock;
extern dcs_u32_t         daemonlize;
//modify by bxz
//struct list_head request_queue;
extern struct list_head request_queue;


/*parse paramatter of compressor*/
dcs_s32_t __dcs_compressor_parse_paramatter(dcs_s32_t argc, dcs_s8_t **argv);
/*init the connection*/
//dcs_s32_t __dcs_compressor_init_com();
dcs_s32_t __compressor_com_init();
/*request queue*/
dcs_s32_t __queue_req(amp_request_t *req);
/*allocbuf for recieve request from client*/
dcs_s32_t __compressor_allocbuf(void *msgp, 
                amp_u32_t *niov, amp_kiov_t **iov);
/*compressor freebuf*/
void __compressor_freebuf(amp_u32_t niov, amp_kiov_t *iov);
/*background process*/
inline int  __dcs_daemonlize();
/*get sample power*/
dcs_u32_t get_sample_power();
/*__queue the req*/
dcs_s32_t __queue_req(amp_request_t *req);
/*
 * init background process
 */
inline int  __dcs_daemonlize();


#endif

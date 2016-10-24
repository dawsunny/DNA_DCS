/*header file of server_cnd.c*/

#ifndef __SERVER_CND_H_
#define __SERVER_CND_H_

#include "dcs_debug.h"
#include "amp.h"

extern amp_comp_context_t  *server_comp_context;
extern dcs_u32_t         server_this_id;
extern dcs_u32_t         server_chunk_type;
extern dcs_u32_t         server_chunk_size;
extern dcs_u32_t         server_rout_type;
extern pthread_mutex_t     request_queue_lock;
extern sem_t               request_sem;
//modify by bxz
//struct list_head request_queue;
extern struct list_head request_queue;

/*parse paramatter of server*/
dcs_s32_t __dcs_server_parse_paramatter(dcs_s32_t argc, dcs_s8_t **argv);
/*init the connection*/
dcs_s32_t __dcs_server_init_com();
/*request queue*/
dcs_s32_t __queue_req(amp_request_t *req);
/*allocbuf for recieve request from client*/
dcs_s32_t __server_allocbuf(void *msgp, 
                amp_u32_t *niov, amp_kiov_t **iov);
/*allocbuf for recieve mark from master*/
dcs_s32_t __server_allocbuf1(void *msgp, 
                amp_u32_t *niov, amp_kiov_t **iov);
/*server allocbuf for compressor reply info*/
dcs_s32_t __server_allocbuf2(void *msgp, 
                amp_u32_t *niov, amp_kiov_t **iov);
/*server freebuf*/
void __server_freebuf(amp_u32_t niov, amp_kiov_t *iov);
/*background process*/
inline int  __dcs_daemonlize();


#endif

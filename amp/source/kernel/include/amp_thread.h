/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_THREAD_H_
#define __AMP_THREAD_H_

#include <amp_sys.h>
#include <amp_types.h>

//#define AMP_MAX_THREAD_NUM       (16)  /*for each type of thread*/
#define AMP_MAX_THREAD_NUM       (32)  /*for each type of thread*/

/*the number created when init module*/
//#define AMP_SRVIN_THR_INIT_NUM   (8)  
#define AMP_SRVIN_THR_INIT_NUM   (16)  
//#define AMP_SRVOUT_THR_INIT_NUM  (8)
#define AMP_SRVOUT_THR_INIT_NUM  (16)
#define AMP_RECONN_THR_INIT_NUM  (1)
#define AMP_BH_THR_NUM           (8)

/*tell service to work sem*/
extern amp_sem_t      amp_process_out_sem;
extern amp_sem_t      amp_process_in_sem;

#ifndef __KERNEL__
extern amp_sem_t      amp_reconn_sem;
extern amp_s32_t      amp_listen_sockfd;
#endif

/*current number of service thread*/
/*
 * increased by specific creation function
 */ 
extern amp_u32_t amp_srvin_thread_num;
extern amp_u32_t amp_srvout_thread_num;
extern amp_u32_t amp_reconn_thread_num;

/*thread structure*/
extern amp_thread_t  *amp_srvin_threads;
extern amp_thread_t  *amp_srvout_threads;
extern amp_thread_t  *amp_reconn_threads;

#define AMP_NET_MORNITOR_INTVL  (50)  /*seconds*/

#ifdef __KERNEL__
extern amp_thread_t  *amp_bh_threads;
extern amp_sem_t     amp_bh_sem;
extern amp_u32_t     amp_bh_thread_num;
#endif

extern amp_sem_t     amp_reconn_finalize_sem;

/*lock for pretect changing thread structure or thread number*/
extern amp_lock_t     amp_threads_lock;

/*some kernel thread callbacks*/
#ifdef __KERNEL__
int __amp_serve_out_thread (void *argv);
int __amp_serve_in_thread (void *argv);
int __amp_reconn_thread (void *argv);
int __amp_listen_thread (void *argv);
int __amp_bh_thread (void *argv);
int __amp_netmorn_thread (void *argv);
#else
void* __amp_serve_out_thread (void *argv);
void* __amp_serve_in_thread (void *argv);
void* __amp_reconn_thread (void *argv);
void* __amp_listen_thread (void *argv);
void* __amp_wake_up_reconn_thread(void *argv);
void* __amp_netmorn_thread(void *argv);
#endif
/*start and stop threads functions*/
int __amp_start_srvin_thread (int seqno);
int __amp_start_srvout_thread (int seqno);
int __amp_start_reconn_thread (int seqno);
int __amp_start_bh_thread (int seqno);
#ifdef __KERNEL__
amp_thread_t*  __amp_start_listen_thread(amp_connection_t *parent_conn);
#else
amp_thread_t*  __amp_start_listen_thread(amp_comp_context_t *ctxt);
#endif

amp_thread_t*  __amp_start_netmorn_thread(amp_comp_context_t *);

int __amp_start_srvin_threads (void);
int __amp_start_srvout_threads (void);
int __amp_start_reconn_threads (void);
int __amp_start_bh_threads (void);
int __amp_stop_reconn_thread (int seqno);

int __amp_stop_srvin_threads (void);
int __amp_stop_srvout_threads (void);
int __amp_stop_reconn_threads (void);
int __amp_stop_bh_threads (void);

int __amp_stop_listen_thread(amp_comp_context_t *);
int __amp_stop_netmorn_thread(amp_comp_context_t *);


/*create daemon for the kernel thread*/
void __amp_kdaemonize (char *str);


/*blocking signal function*/
void __amp_blockallsigs (void);

	
/*init and finalize*/
int __amp_threads_init (void);     /*tell them up*/
int __amp_threads_finalize(void);  /*tell them down*/

int __amp_recv_msg (amp_connection_t *conn, amp_message_t **retmsgp);
#endif

/*end of file*/

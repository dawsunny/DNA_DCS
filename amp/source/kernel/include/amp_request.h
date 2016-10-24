/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_REQUEST_H_
#define __AMP_REQUEST_H_

#include <amp_sys.h>
#include <amp_types.h>
#include <amp_help.h>

#define AMP_REQ_MAGIC  (0xDEFABD9C)

#define AMP_MAX_MSG_SIZE  (8192)

/*general request type*/
#define AMP_REQUEST   (0x00010000)     /*request of msg*/
#define AMP_REPLY     (0x00020000)     /*reply of msg*/

#define AMP_MSG       (0x00000001)  /*request for data transfer*/
#define AMP_DATA      (0x00000002)  /*reply for data transfer*/

#define AMP_HELLO     (0x00000100)
#define AMP_HELLO_ACK (0x00000200)



/*state of request*/
#define AMP_REQ_STATE_NORMAL   (0x00010000)  /*a normal state*/
#define AMP_REQ_STATE_RESENT   (0x00020000)  /*need resending*/
#define AMP_REQ_STATE_TIMEOUT  (0x00040000)  /*time out for previous sending*/
#define AMP_REQ_STATE_NOPROC   (0x00080000)  /*break contact with the process*/
#define AMP_REQ_STATE_INIT     (0x00100000)  /*the request is just created*/
#define AMP_REQ_STATE_ERROR    (0x00200000)  /*something wrong with sending*/

/*stage of the request sending*/
#define AMP_REQ_STAGE_MSG         (1)
#define AMP_REQ_STAGE_DATA       (2)



/*to be sent requests list*/
extern struct list_head  amp_sending_list;
extern amp_lock_t        amp_sending_list_lock;

/*waiting for reply list*/
extern struct list_head  amp_waiting_reply_list;
extern amp_lock_t        amp_waiting_reply_list_lock;


/*lock to protect freeing request and getting request from handle*/
extern amp_lock_t        amp_free_request_lock;

/*cache of request*/
#ifdef __KERNEL__
extern struct kmem_cache  *amp_request_cache;
#endif

#define AMP_FILL_REQ_HEADER(__rqhd, __sz, __tp, __pid, __sh, __axid, __st) \
do { \
	(__rqhd)->amh_magic = AMP_REQ_MAGIC; \
	(__rqhd)->amh_size = (__sz); \
       (__rqhd)->amh_type = (__tp); \
       (__rqhd)->amh_pid = (__pid); \
       (__rqhd)->amh_sender_handle = (unsigned long)(__sh); \
       (__rqhd)->amh_xid = (__axid); \
       (__rqhd)->amh_send_ts = (__st); \
} while (0);


/*xid generator*/
extern amp_u64_t  amp_xid_generator;
extern amp_lock_t  amp_xid_lock;
extern amp_u64_t  __amp_getxid(void);

/*some functions*/
int __amp_init_request(void);
int __amp_finalize_request(void);
int __amp_alloc_request(amp_request_t **);
int __amp_free_request(amp_request_t *);
amp_request_t* __amp_handle2req(amp_u64_t, amp_u64_t, amp_u32_t, amp_time_t);
int  __amp_reqheader_equal(amp_message_t*, amp_message_t *);

#endif
/*end of file*/

/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_TYPES_H_
#define __AMP_TYPES_H_

#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include "amp_sys.h"

/*generic types*/
typedef unsigned char   amp_u8_t;
typedef char            amp_s8_t;
typedef unsigned short  amp_u16_t;
typedef short           amp_s16_t;
typedef unsigned int    amp_u32_t;
typedef int             amp_s32_t;
typedef unsigned long long  amp_u64_t;
typedef long long           amp_s64_t;

/*amp time used*/
struct __amp_time {
	amp_u64_t  sec;    /*second*/
	amp_u64_t  usec;   /*micro second*/
};
typedef struct __amp_time amp_time_t;


/*data block vector*/
struct __amp_kiov {
	void        *ak_addr;
	amp_u32_t   ak_len;
	amp_u32_t   ak_offset;
	amp_u64_t   ak_flag;    /*flags indicate the attribute of this page*/
};
typedef struct __amp_kiov  amp_kiov_t;

typedef pthread_mutex_t  amp_lock_t;
typedef sem_t            amp_sem_t;

struct __amp_comp_context;
typedef struct __amp_comp_context amp_comp_context_t;

struct __amp_connection;
typedef struct __amp_connection  amp_connection_t;

struct __amp_message {
	amp_u32_t  amh_magic;
	amp_u32_t  amh_size; /*the size of the payload, NOT including this header size*/
	amp_u32_t  amh_type; /*AMP_REQUEST or AMP_REPLY*/
	amp_u32_t  amh_pid;  /*in mormal msg is who send this message, in hello msg, it's comtype*/
    amp_u32_t  amh_xid;
    amp_u32_t  amh_seqno;
	amp_u64_t  amh_sender_handle;
	//amp_u64_t  amh_xid;  /*in normal is xid of request, in hello msg, is the id of this component*/
	amp_time_t amh_send_ts;	
	struct sockaddr_in amh_addr; /*for receiving udp message*/

};
typedef struct __amp_message  amp_message_t;
#define AMP_MESSAGE_HEADER_LEN (sizeof(amp_message_t))


struct __amp_request {
	struct list_head      req_list;    
    amp_message_t        *req_msg;
    amp_message_t        *req_reply;
    amp_kiov_t           *req_iov;
    amp_comp_context_t   *req_ctxt;
    amp_connection_t     *req_conn;
    amp_sem_t             req_waitsem; /*process waits on here for finishing*/ 
	amp_lock_t            req_lock;
    
    //amp_u32_t             req_send_state;
    amp_u32_t             req_type;    /*AMP_DATA(MSG)*/
	amp_u32_t             req_msglen;     /*length of req_msg */
	amp_u32_t             req_replylen;   /*length of req_reply*/
	amp_u32_t             req_state;
	amp_u32_t             req_niov;
	amp_s32_t             req_error;       /*0 - normally send request and      
	                                                        receive reply, else something wrong*/
	amp_u32_t             req_remote_type;   /*type of dst component*/
	amp_u32_t             req_remote_id;       /*id of dst component*/
	amp_u32_t             req_need_ack;
	amp_u32_t             req_need_free;
	amp_u32_t             req_replied;
	amp_u32_t             req_refcount;
	amp_u32_t             req_stage;  /*in which stage now*/
	amp_u32_t             req_resent;  /*not 0 - add to conn when it is error, 0 - not just return*/
	//struct sockaddr_in  req_remote_addr;  /*the sock address of remote peer, used for reply in udp*/
};
typedef struct __amp_request amp_request_t;



/*type of connection structure*/
struct __amp_connection {
	struct list_head ac_list;        /*used by fs*/
	struct list_head ac_reconn_list; /*add to reconnection list*/
	struct list_head ac_dataready_list; /*add to dataready list*/
    struct sockaddr_in   ac_remote_addr;   /*combined with ipaddr and port*/
    amp_comp_context_t *ac_ctxt;  /*context to which this connection belongs to */

   	amp_u32_t        ac_state;       /*state of connection, e.g. AMP_CONN_OK */
	amp_u32_t        ac_type;        /*type of connection, e.g. AMP_CONN_TYPE_TCP */
	amp_u32_t        ac_need_reconn; /*only allow client  reconnect to server*/
	amp_u32_t        ac_remote_ipaddr;
	
    amp_u32_t        ac_remote_port;
	amp_u32_t        ac_remote_comptype; /*used by fs, AMP_CLIENT, AMP_MDS...*/
	amp_u32_t        ac_remote_id;              /*used by fs*/
	amp_u32_t        ac_this_type; /*type of this component, AMP_CLIENT, AMP_MDS...*/
	
    amp_u32_t        ac_this_id;     /*id of this component*/
	amp_u32_t        ac_this_port; /*port of this connection, used by listen connection*/
	amp_s32_t        ac_sock;
	amp_u32_t        ac_refcont;
	
    amp_u32_t        ac_sched;             /*this conn is queued for receiving*/
	amp_u32_t        ac_datard_count; /*how many times we received the data ready callback*/ 
	amp_u32_t        ac_weight;            /*wait of this connection*/
    /*something about reconnection*/
    amp_u32_t        ac_remain_times; /*how many times remain for retry*/
    amp_u64_t        ac_last_reconn;    /*the latest reconnection time, or as heartbeat time*/
    
    amp_u64_t        ac_payload;          /*how many bytes want to be sent through this connection*/
    amp_lock_t       ac_lock;            /*protect for changing this connection*/
	amp_sem_t        ac_sendsem;
	amp_sem_t        ac_recvsem;
	amp_sem_t        ac_listen_sem;   /*the accept thread sleep on this sem*/

	/*something about reconnection*/

	/*
	* the callback for queue the received request, provided by fs layer
	* if not provided, then just drop the request.
	* return value: 1 - queued successfully, else - something wrong.
	*/
	int (*ac_queue_cb) (amp_request_t *req);

	/*
	* used by server to alloc page to be used for receiving data bulk. MUST PROVIDE it.
	* this cb just alloc pages for 
	* each vector.
	* return value: 0 - alloc successfully, <0 - something wrong.
	*/
	int (*ac_allocpage_cb) (void *opaque, amp_u32_t *niov, amp_kiov_t **iov);
	void (*ac_freepage_cb)(amp_u32_t niov, amp_kiov_t *iov);
};
	
/*
 * connection info for each kind of components, its main purpose is to record the relationship with 
 * other file system component.
 */
#define AMP_SELECT_CONN_ARRAY_ALLOC_LEN   (8)
struct __conn_queue {
	pthread_mutex_t queue_lock;
	struct  list_head  queue;
	amp_connection_t   **conns;  /*for selecting*/
	amp_u32_t          total_num; /*how long of the conns array*/
	amp_s32_t          active_conn_num; /*how many eff conns in the conns*/
};
typedef struct __conn_queue conn_queue_t;

struct __amp_comp_conns {
	amp_u32_t  acc_num;  /*number of valid remote connections*/
	amp_u32_t  acc_alloced_num; /*alloced number of acc_remote_conns*/
	amp_lock_t  acc_lock;  /*lock for changing this structure*/
	//struct list_head  *acc_remote_conns;   /*remote connection table, indexed by remote component id*/	
	conn_queue_t  *acc_remote_conns;
};
typedef struct __amp_comp_conns amp_comp_conns_t;

/*thread structures*/
struct __amp_thread {
	amp_u32_t         at_seqno;    /*our sequence*/
	amp_sem_t         at_startsem;
	amp_sem_t         at_downsem;
	amp_u32_t         at_shutdown; /*1 -  to shutdown, 0 - not*/
	amp_u32_t         at_isup;     /*1 - is up, 0 - not*/
	pthread_t          at_thread_id;
	void *at_provite;  /*for provite use, as to listen thread, it's a connection structure.*/

};
typedef struct __amp_thread amp_thread_t;

#define MAX_CONN_TABLE_LEN  (8192)

/*message passing system component context*/
struct __amp_comp_context {
	amp_u32_t  acc_this_type;  /*type of this component*/
	amp_u32_t  acc_this_id;      /*id of for this component within its type*/
	amp_connection_t *acc_listen_conn;    /*listen connection for this component*/
	amp_thread_t     *acc_listen_thread;  /*threads for listen*/
    amp_thread_t     *acc_netmorn_thread;
	amp_comp_conns_t *acc_conns;               /*connections with other component*/
	amp_connection_t  **acc_conn_table; /*all connection stored in this array indexed by fd*/
	pthread_mutex_t   acc_lock;

// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------
#ifdef __AMP_LISTEN_SELECT

	fd_set            acc_readfds;
	amp_u32_t         acc_maxfd;
	
#endif
#ifdef __AMP_LISTEN_POLL

	struct pollfd  *acc_poll_list;	// by ChenZhuan at 2008-10-31
	amp_u32_t         acc_maxfd;
	
#endif
#ifdef __AMP_LISTEN_EPOLL

	amp_s32_t  acc_epfd;			// by Chen Zhuan at 2008-11-03
	
#endif
// -----------------------------------------------------------------

	amp_u32_t         acc_notifyfd;     /*used to break the select*/
	amp_u32_t         acc_srvfd;        /*server side used fd*/
};


/*storage system component type*/
enum amp_comp_type {
	AMP_CLIENT = 1,   /*client of storage system*/
	AMP_MDS = 2,        /*meta data server*/
	AMP_OSD = 3,        /*object based storage server*/
	AMP_SNBD = 4,      /*super nbd*/
	AMP_MGNT = 5,     /*management node*/
	AMP_MOD = 6,
	AMP_MPS = 7,
	AMP_COMP_MAX,   /*max number*/
};

#define AMP_MAX_COMP_TYPE  (16)


#endif

/*end of file*/

/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_CONN_H_
#define __AMP_CONN_H_

#include  <amp_sys.h>
#include  <amp_types.h>
#include  <amp_help.h>

/*state of connection*/
#define AMP_CONN_OK       (0x00000010) /*it's ok*/
#define AMP_CONN_RECOVER  (0x00000020) /*during recover*/
#define AMP_CONN_BAD      (0x00000040) /*unuseable*/
#define AMP_CONN_NOTINIT  (0x00000080) /*haven't initialized yet*/
#define AMP_CONN_CLOSE     (0x00000100)

/*state of bh*/
#define AMP_CONN_BH_DATAREADY    (0x00001000)
#define AMP_CONN_BH_STATECHANGE  (0x00002000)

#define AMP_CONN_RECONN_INTERVAL  (5)   /*after these seconds , we than do reconnection*/
#define AMP_CONN_RECONN_MAXTIMES (60)   /*max reconnection times*/

#define AMP_CONN_MAXIP_PER_NODE  (16)


/*type of connection*/
enum amp_conn_type {
	AMP_CONN_TYPE_TCP = 1,
	AMP_CONN_TYPE_UDP,
	AMP_CONN_TYPE_GM,
	AMP_CONN_TYPE_MAX,
};

/*direction of connection*/
#define AMP_CONN_DIRECTION_CONNECT  (0x00000100)
#define AMP_CONN_DIRECTION_ACCEPT   (0x00000200)
#define AMP_CONN_DIRECTION_LISTEN   (0x00000400)



/*reconnection list*/
extern struct list_head  amp_reconn_conn_list;
extern amp_lock_t        amp_reconn_conn_list_lock;

/*when data coming into this conn, the data-ready callback queue 
 * this conn to this list
 */
extern struct list_head  amp_dataready_conn_list;
extern amp_lock_t        amp_dataready_conn_list_lock;

#ifdef __KERNEL__
extern struct list_head  amp_bh_req_list;
extern amp_lock_t        amp_bh_req_list_lock;
#endif


extern amp_u32_t         amp_reconn_thread_started;

#ifdef __KERNEL__
extern struct kmem_cache *amp_conn_cache;
#endif


/*
 * hash table for resend request
 */
struct __amp_htb_entry {
	struct list_head  queue;
	amp_lock_t         lock;
};
typedef struct __amp_htb_entry amp_htb_entry_t;

#define AMP_RESEND_HTB_SIZE   (1 << 14)
extern amp_htb_entry_t   *amp_resend_hash_table;



#define AMP_CONN_ADD_INCR   (8192)   /*every time we add this  much connections in comp_conns*/


// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------
#ifndef __KERNEL__
#ifdef __AMP_LISTEN_POLL

extern int amp_poll_fd_zero( struct pollfd *poll_list, amp_u32_t poll_size );
extern int amp_poll_fd_set( amp_s32_t fd, struct pollfd *poll_list );
extern int amp_poll_fd_isset( amp_s32_t fd, struct pollfd *poll_list );
extern int amp_poll_fd_clr( amp_s32_t fd, struct pollfd *poll_list );

#endif
#ifdef __AMP_LISTEN_EPOLL

extern int amp_epoll_fd_isset( amp_s32_t fd, struct epoll_event *ev, amp_s32_t nfds );
extern int amp_epoll_fd_set( amp_s32_t fd, amp_s32_t epfd );
extern int amp_epoll_fd_clear( amp_s32_t fd, amp_s32_t epfd );

#endif
#endif
// -----------------------------------------------------------------

amp_u32_t  __amp_hash(amp_u32_t type, amp_u32_t id);
void __amp_add_resend_req(amp_request_t *req);
void __amp_remove_resend_req(amp_request_t *req);
amp_request_t * __amp_find_resend_req(amp_u32_t type, amp_u32_t id);


/*some functions*/
int __amp_init_conn(void);      /*called when module init*/
int __amp_finalize_conn(void);  /*called when module cleanup*/
int __amp_alloc_conn(amp_connection_t **conn);  /*alloc one*/
int __amp_free_conn(amp_connection_t *conn);   /*free one*/

int __amp_enqueue_conn(amp_connection_t *conn, amp_comp_context_t *ctxt);
int __amp_dequeue_conn(amp_connection_t *conn, amp_comp_context_t *ctxt);

int  __amp_select_conn(amp_u32_t type,  amp_u32_t id,  amp_comp_context_t *ctxt,  amp_connection_t **retconn);


/*to revoke related need resend requests*/
void __amp_revoke_resend_reqs(amp_connection_t *conn);
int __amp_connect_server (amp_connection_t *conn,  amp_u32_t thistype, amp_u32_t  thisid);

#ifdef __KERNEL__
int __amp_do_connection (struct socket **retsock, struct sockaddr_in *addr, amp_u32_t conn_type, amp_u32_t direction);
int __amp_accept_connection (struct socket *sockparent, amp_connection_t *childconn);
#else
int __amp_do_connection(amp_s32_t **retsock, struct sockaddr_in *addr, amp_u32_t conn_type, amp_u32_t direction);
int __amp_accept_connection (amp_s32_t *sockparent, amp_connection_t *childconn);

#endif

#ifdef __KERNEL__
int __amp_sock_write (struct socket *sock, void *bufp, amp_u32_t len);
int __amp_sock_read (struct socket *sock, void *bufp, amp_u32_t len);
#endif

#ifndef __KERNEL__
int __amp_add_to_listen_fdset (amp_connection_t *conn);
#endif

#endif

/*end of file*/

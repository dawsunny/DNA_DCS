/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/

#include <amp_conn.h>
#include <amp_request.h>
#include <amp_protos.h>
#include <amp_thread.h>

struct list_head  amp_reconn_conn_list;
amp_lock_t        amp_reconn_conn_list_lock;
struct list_head  amp_dataready_conn_list;
amp_lock_t        amp_dataready_conn_list_lock;
struct list_head  amp_bh_req_list;
amp_lock_t        amp_bh_req_list_lock;
amp_u32_t         amp_reconn_thread_started;

#ifdef __KERNEL__
struct kmem_cache  *amp_conn_cache = NULL;
#endif

amp_htb_entry_t   *amp_resend_hash_table = NULL;

/*
 * initialization 
 */ 
int
__amp_init_conn(void)
{
	amp_htb_entry_t  *bucket = NULL;
	
	INIT_LIST_HEAD(&amp_reconn_conn_list);
	amp_lock_init(&amp_reconn_conn_list_lock);

	INIT_LIST_HEAD(&amp_dataready_conn_list);
	amp_lock_init(&amp_dataready_conn_list_lock);

#ifdef __KERNEL__
	INIT_LIST_HEAD(&amp_bh_req_list);
        amp_lock_init(&amp_bh_req_list_lock);
#endif
	amp_reconn_thread_started = 0;

	amp_resend_hash_table = 
		(amp_htb_entry_t *)amp_alloc(sizeof(*amp_resend_hash_table) * AMP_RESEND_HTB_SIZE);
	if (!amp_resend_hash_table) {
		AMP_ERROR("__amp_init_conn: alloc memory for amp_resend_hash_table error\n");
		return -ENOMEM;
	}

	for(bucket = amp_resend_hash_table + AMP_RESEND_HTB_SIZE - 1; 
	      bucket >= amp_resend_hash_table;
	      bucket --) {
		INIT_LIST_HEAD(&bucket->queue);
		amp_lock_init(&bucket->lock);
	}	  

#ifdef __KERNEL__
	amp_conn_cache = kmem_cache_create ("amp_conns",
			                             sizeof(amp_connection_t),
										 0,
										 SLAB_HWCACHE_ALIGN,
										 NULL,
										 NULL);
	if (!amp_conn_cache) {
		AMP_ERROR("amp_init_conn: create conn cache error\n");
		amp_free(amp_resend_hash_table, \
			          sizeof(*amp_resend_hash_table) * AMP_RESEND_HTB_SIZE);
		return -ENOMEM;
	}
#endif

	return 0;

}

/*
 * finalize 
 */ 
int 
__amp_finalize_conn ()
{
	amp_request_t *req = NULL;
	amp_htb_entry_t *htbe = NULL;
	amp_u32_t i;

	AMP_ENTER("__amp_finalize_conn: enter\n");


	/*
	 * free resend hash table.
	 */
	AMP_DMSG("__amp_finalize_conn: check resend reqs\n");
	for (i = 0; i<AMP_RESEND_HTB_SIZE; i++) {
		htbe = amp_resend_hash_table + i;
		amp_lock(&htbe->lock);
		while (!list_empty(&htbe->queue))  {
			req = list_entry(htbe->queue.next, amp_request_t, req_list);
			list_del_init(&req->req_list);
			req->req_error = -EINTR;
			__amp_free_request(req);
			amp_sem_up(&req->req_waitsem);
		}
		amp_unlock(&htbe->lock);
	}

	amp_free(amp_resend_hash_table, sizeof(amp_htb_entry_t) * AMP_RESEND_HTB_SIZE);


	/*
	 * free request in send list.
	 */
	AMP_DMSG("__amp_finalize_conn: check sending list\n");
	amp_lock(&amp_sending_list_lock);
	while (!list_empty(&amp_sending_list)) {
		req = list_entry(amp_sending_list.next, amp_request_t, req_list);
		list_del_init(&req->req_list);
		req->req_error = -EINTR;
		__amp_free_request(req);
		amp_sem_up(&req->req_waitsem);
	}

	amp_unlock(&amp_sending_list_lock);

	/*
	 * free all waiting for reply request
	 */
	amp_lock(&amp_waiting_reply_list_lock);
	AMP_DMSG("__amp_finalize_conn: check waiting reply list\n");

	while  (!list_empty(&amp_waiting_reply_list)) {
		req = list_entry(amp_waiting_reply_list.next, amp_request_t, req_list);	
		list_del_init(&req->req_list);
		req->req_error = -EINTR;
		__amp_free_request(req);
		amp_sem_up(&req->req_waitsem);
	}
	amp_unlock(&amp_waiting_reply_list_lock);
		
	
	/*
	 * interruptible all resend request
	 */
#ifdef __KERNEL__
	if (amp_conn_cache)
		kmem_cache_destroy(amp_conn_cache);
#endif
	AMP_LEAVE("__amp_finalize_conn: leave\n");
	return 0;
}

int 
__amp_alloc_conn (amp_connection_t **retconn)
{
	amp_s32_t err = 0;
	amp_connection_t *conn;

	AMP_ENTER("__amp_alloc_conn: enter\n");

#ifdef __KERNEL__
	conn = kmem_cache_alloc (amp_conn_cache, GFP_KERNEL);
#else
	conn = (amp_connection_t *)malloc(sizeof(amp_connection_t));
#endif

	if (!conn) {
		AMP_ERROR("__amp_alloc_conn: alloc for conn error\n");
		err = -ENOMEM;
		goto EXIT;
	}
	/*
	 * Initialization
	 */ 
	memset(conn, 0, sizeof(amp_connection_t));
	INIT_LIST_HEAD(&conn->ac_list);
	INIT_LIST_HEAD(&conn->ac_reconn_list);
	INIT_LIST_HEAD(&conn->ac_dataready_list);
	INIT_LIST_HEAD(&conn->ac_protobh_list);

		
	conn->ac_state = AMP_CONN_NOTINIT;
	conn->ac_sock = NULL;
	atomic_set(&conn->ac_refcont, 1);

	amp_lock_init(&conn->ac_lock);
	amp_sem_init(&conn->ac_sendsem);
	amp_sem_init(&conn->ac_recvsem);
	amp_sem_init_locked(&conn->ac_listen_sem);

	*retconn = conn;
	
EXIT:
	AMP_LEAVE("__amp_alloc_conn: leave, conn:%p\n", conn);
	return err;
}


int 
__amp_free_conn (amp_connection_t *conn)
{
	AMP_ENTER("__amp_free_conn: enter,conn:%p, refcont:%d\n", \
                   conn, atomic_read(&conn->ac_refcont));
	if (!conn) {
		AMP_ERROR("__amp_free_conn: no connecton\n");
		goto EXIT;
	}
	//printk("[%d] amp_free_conn:before lock conn:%p\n", current->pid, conn);
	if (!atomic_read(&conn->ac_refcont)) {
		AMP_ERROR("__amp_free_conn: conn:%p, its refcont is zero now, something wrong\n", \
                conn);
	}
	amp_lock(&conn->ac_lock);
	if (!atomic_read(&conn->ac_refcont)) {
		AMP_ERROR("__amp_free_conn: conn:%p, its refcont is zero now\n", conn);
	}

	if (!atomic_dec_and_test(&conn->ac_refcont)) {
		amp_unlock(&conn->ac_lock);
		goto EXIT;
	}
	if (!list_empty(&conn->ac_list)) {
		printk("__amp_free_conn: to free chained connection:%p\n",conn);
		amp_unlock(&conn->ac_lock);
		goto EXIT;
	}
	
	INIT_LIST_HEAD(&conn->ac_list);
	INIT_LIST_HEAD(&conn->ac_reconn_list);
	INIT_LIST_HEAD(&conn->ac_dataready_list);		
	amp_unlock(&conn->ac_lock);
	memset(conn, 0, sizeof(amp_connection_t));

	//printk("__amp_free_conn: fully free conn:%p\n", conn);
#ifdef __KERNEL__
	kmem_cache_free(amp_conn_cache, conn);
#else
	free(conn);
#endif

EXIT:
	AMP_LEAVE("__amp_free_conn: leave, conn:%p\n", conn);
	return 0;
}

/*
 * queue a new created connection to the specificed context.
 */
int 
__amp_enqueue_conn(amp_connection_t *conn, amp_comp_context_t *ctxt)
{
	amp_s32_t  err = 0;
	amp_u32_t  type;
	amp_u32_t  id;
	struct list_head *head = NULL;
	conn_queue_t  *cnq = NULL;
	amp_s32_t i, j;
	amp_connection_t **orig_conns = NULL;
	amp_u32_t orig_num;
	amp_u32_t realloc_num;
	amp_u32_t realloc_size;
	
	AMP_WARNING("__amp_queue_conn: enter, conn:%p\n", conn);

	if (!ctxt)  {
		AMP_ERROR("__amp_queue_conn: no context\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!conn)  {
		AMP_ERROR("__amp_queue_conn: no connection\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!ctxt->acc_conns) {
		AMP_ERROR("__amp_queue_conn: no acc_conns in ctxt\n");
		err = -EINVAL;
		goto EXIT;
	}


	type = conn->ac_remote_comptype;
	id = conn->ac_remote_id;
	
	AMP_DMSG("__amp_queue_conn: type:%d, id:%d\n", type, id);

	if (type > AMP_MAX_COMP_TYPE) {
		AMP_ERROR("__amp_queue_conn: type(%d) is too large\n", type);
		err = -EINVAL;
		goto EXIT;
	}

	if (!ctxt->acc_conns[type].acc_remote_conns) {
		AMP_ERROR("__amp_queue_conn: no remote conns in comp_conns of type:%d\n", type);
		err = -EINVAL;
		goto EXIT;
	}


	head = &(ctxt->acc_conns[type].acc_remote_conns[id].queue);

	write_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
	cnq = &(ctxt->acc_conns[type].acc_remote_conns[id]);
	list_add_tail(&conn->ac_list, head);

	AMP_DMSG("__amp_queue_conn: before add conn, cnq:%p, active_conn_num:%d,total_num:%d\n", \
                  cnq, cnq->active_conn_num, cnq->total_num);
	for (i=0; i<=cnq->active_conn_num; i++) {
		if (!cnq->conns[i])	
			break;
	}

	AMP_DMSG("__amp_queue_conn: after search, i:%d\n", i);
	if (i > cnq->active_conn_num) {
		if (cnq->active_conn_num >= (cnq->total_num - 1)) {
			AMP_DMSG("__amp_queue_conn: need realloc the conns\n");
			orig_conns = cnq->conns;	
			orig_num = cnq->total_num;
			realloc_num = cnq->total_num + AMP_SELECT_CONN_ARRAY_ALLOC_LEN;
			realloc_size = realloc_num * sizeof(amp_connection_t *);
			AMP_DMSG("__amp_queue_conn: orig_num:%d, realloc_num:%d\n", \
                                  orig_num, realloc_num);

			cnq->conns = (amp_connection_t **)amp_alloc(realloc_size);
			if (!cnq->conns) {
				cnq->conns = orig_conns;
				AMP_ERROR("__amp_queue_conn: realloc for conns error\n");
				err = -ENOMEM;
				goto EXIT;
			}
			memset(cnq->conns, 0, realloc_size);
			for(j=0; j<orig_num; j++) 
				cnq->conns[j] = orig_conns[j];

			amp_free(orig_conns, orig_num * sizeof(amp_connection_t *));
			cnq->total_num = realloc_num;
		}
		cnq->conns[i] = conn;
		cnq->active_conn_num = i;

	} else {
		cnq->conns[i] = conn;
	}

	AMP_DMSG("__amp_queue_conn: after add conn, active_conn_num:%d, total_num:%d\n", \
                  cnq->active_conn_num, cnq->total_num);
	write_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
	
EXIT:
	AMP_WARNING("__amp_queue_conn: leave\n");
	return err;
}


/*
 * remove a connection from context
 */
int 
__amp_dequeue_conn(amp_connection_t *conn, amp_comp_context_t *ctxt)
{

	amp_s32_t  err = 0;
	amp_u32_t  type;
	amp_u32_t  id;
	amp_s32_t  i;
	conn_queue_t *cnq = NULL;
	amp_request_t *req = NULL;
	struct list_head *pos = NULL;
	struct list_head *nxt = NULL;
	
	AMP_ENTER("__amp_dequeue_conn: enter, conn:%p\n", conn);

	if (!ctxt)  {
		AMP_ERROR("__amp_dequeue_conn: no context\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!conn)  {
		AMP_ERROR("__amp_dequeue_conn: no connection\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!ctxt->acc_conns) {
		AMP_ERROR("__amp_dequeue_conn: no acc_conns in ctxt\n");
		err = -EINVAL;
		goto EXIT;
	}


	type = conn->ac_remote_comptype;
	id = conn->ac_remote_id;

	AMP_DMSG("__amp_dequeue_conn: type:%d, id:%d\n", type, id);

	if (type > AMP_MAX_COMP_TYPE) {
		AMP_ERROR("__amp_dequeue_conn: type(%d) is too large\n", type);
		err = -EINVAL;
		goto EXIT;
	}

	if (!ctxt->acc_conns[type].acc_remote_conns) {
		AMP_ERROR("__amp_dequeue_conn: no remote conns in comp_conns of type:%d\n", type);
		err = -EINVAL;
		goto EXIT;
	}

	AMP_DMSG("__amp_dequeue_conn: dequeue the conn\n");

	write_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
	cnq = &(ctxt->acc_conns[type].acc_remote_conns[id]);
	AMP_DMSG("__amp_dequeue_conn: before search, active_conn_num:%d, total_num:%d\n", \
                  cnq->active_conn_num, cnq->total_num);

	list_del_init(&conn->ac_list);
	for (i=0; i<=cnq->active_conn_num; i++) {
		if(cnq->conns[i] == conn)
			break;
	}
	AMP_DMSG("__amp_dequeue_conn: after search, i:%d\n", i);
	if (i > cnq->active_conn_num) {
		AMP_ERROR("__amp_dequeue_conn: not find the conn:%p in select queue\n", \
                           conn);
		write_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
		goto EXIT;
	}
	cnq->conns[i] = NULL;

	if (i >= cnq->active_conn_num)
		cnq->active_conn_num --;
	else {
		cnq->conns[i] = cnq->conns[cnq->active_conn_num];
		cnq->conns[cnq->active_conn_num] = NULL;
		cnq->active_conn_num --;
	}

	while((cnq->active_conn_num >= 0) 
              && (!cnq->conns[cnq->active_conn_num]))
		cnq->active_conn_num --;
	
	AMP_DMSG("__amp_dequeue_conn: active_conn_num:%d, total_num:%d\n", \
                  cnq->active_conn_num, cnq->total_num);

	if (cnq->active_conn_num == -1) {
		write_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
		amp_lock(&amp_waiting_reply_list_lock);
		list_for_each_safe(pos, nxt, &amp_waiting_reply_list) {
			req = list_entry(pos, amp_request_t, req_list);
			if ((req->req_remote_id == id) && 
                            (req->req_remote_type == type)) {
				AMP_DMSG("__amp_dequeue_conn: find a request:%p\n", req);
				if(!req->req_resent){
					AMP_DMSG("__amp_dequeue_conn: wake up request: %p\n", req);
					list_del_init(&req->req_list);
					req->req_error = -ENETUNREACH;
					__amp_free_request(req);
					amp_sem_up(&req->req_waitsem);
                                }
			}
		}
		amp_unlock(&amp_waiting_reply_list_lock);
	} else 
		write_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
	
EXIT:
	AMP_LEAVE("__amp_dequeue_conn: leave\n");
	return err;


}

/*
 * select a connection belong to specific context.
 * return value:
 *                  0 - normal and retconn contain the returned connection
 *                  1 - no connection connected with the remote peer
 *                  2 - no valid connection with the remote peer.
 *                <0 - something wrong.
 */
int 
__amp_select_conn(amp_u32_t type,  
                  amp_u32_t id,  
                  amp_comp_context_t *ctxt,  
                  amp_connection_t **retconn)
{
	amp_s32_t  err = 0;
	amp_connection_t  *conn = NULL;
	struct list_head *head = NULL;
	amp_comp_conns_t *cmp_conns = NULL;
	conn_queue_t *cnq = NULL;
	struct timeval crtv;
	amp_u32_t beginidx = 0;
	amp_u32_t i;
	amp_u32_t cur_weight = -1;

	AMP_ENTER("__amp_select_conn: enter\n");

	if (type > AMP_MAX_COMP_TYPE) {
		AMP_ERROR("__amp_select_conn: wrong type: %d\n", type);
		err = -EINVAL;
		goto EXIT;
	}

	if (!ctxt) {
		AMP_ERROR("__amp_select_conn: no context\n");
		err = -EINVAL;
		goto EXIT;
	}
	

	if (!ctxt->acc_conns) {
		AMP_ERROR("__amp_select_conn: no acc_conns in ctxt\n");
		err = 1;
		goto EXIT;
	}
	
	cmp_conns = &(ctxt->acc_conns[type]);

	if (id < 0 || id >= cmp_conns->acc_alloced_num) {
		AMP_ERROR("__amp_select_conn: wrong id:%d\n", id);
		err = -EINVAL;
		goto EXIT;

	}

	if (!cmp_conns->acc_remote_conns) {
		AMP_ERROR("__amp_select_conn: no remote conns \n");
		err = 1;
		goto EXIT;
	}

	read_lock(&(cmp_conns->acc_remote_conns[id].queue_lock));
	cnq = &(cmp_conns->acc_remote_conns[id]);
	head = &(cmp_conns->acc_remote_conns[id].queue);	
	
	if (list_empty(head)) {
		AMP_ERROR("__amp_select_conn: no connection corresponding to type:%d, id:%d\n",
			              type, id);
		err = 1;
		read_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));
		goto EXIT;
	}

	if (cnq->active_conn_num < 0) {
		AMP_ERROR("__amp_select_conn: type:%d, id:%d, active_conn_num:%d ,wrong\n", \
			   type, id, cnq->active_conn_num);
		err = 1;
		read_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));
		goto EXIT;
	}
	
	err = 2;
	*retconn = NULL;
	do_gettimeofday(&crtv);
	beginidx = crtv.tv_sec + crtv.tv_usec + (id ^ (~type));
	beginidx = beginidx % (cnq->active_conn_num + 1);
	i = beginidx;
	AMP_DMSG("__amp_select_conn: beginidx:%d, active_conn_num:%d, total_num:%d\n", \
                  beginidx, cnq->active_conn_num, cnq->total_num);

	do {
		conn = cnq->conns[i];

		if (conn) {
			amp_lock(&conn->ac_lock);
			if (conn->ac_state == AMP_CONN_OK) {
				err = 0;
				if (conn->ac_weight < cur_weight) {
					*retconn = conn;
					cur_weight = conn->ac_weight;
				}
			}
			amp_unlock(&conn->ac_lock);
		}
		i = (i + 1) % (cnq->active_conn_num + 1);

	} while (i != beginidx);
	AMP_DMSG("__amp_select_conn: selected conn:%p\n", conn);


	read_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));
	if (*retconn) {
		amp_lock(&((*retconn)->ac_lock));
		AMP_DMSG("__amp_select_conn: get connection:%p\n", \
			  *retconn);
	}

EXIT:
	AMP_LEAVE("__amp_select_conn: leave\n");
	return err;
}


/*
 * revoke all request waiting for resend to the peer corresponding to this 
 * conn.
 */
void
__amp_revoke_resend_reqs(amp_connection_t *conn)
{
	amp_request_t *req  = NULL;
	amp_u32_t type;
	amp_u32_t id;

	AMP_ENTER("__amp_revoke_resend_reqs: enter\n");

	type = conn->ac_remote_comptype;
	id = conn->ac_remote_id;

	while ((req = __amp_find_resend_req(type, id)))  {
		AMP_LEAVE("__amp_revoke_resend_reqs: find one req:%p\n", req);
		amp_lock(&req->req_lock);

		/*
		 * maybe someone else, e.g. timeout handler, will 
		 * remove it from the resend hash table.
		 */
		if (req->req_state == AMP_REQ_STATE_RESENT)  {
			__amp_remove_resend_req(req);
			req->req_state = AMP_REQ_STATE_NORMAL;
			amp_lock(&amp_sending_list_lock);
			list_add_tail(&req->req_list, &amp_sending_list);
			amp_sem_up(&amp_process_out_sem);
			amp_unlock(&amp_sending_list_lock);
		}
		amp_unlock(&req->req_lock);
	}


	AMP_LEAVE("__amp_revoke_resend_reqs: leave\n");
	return;
}

#ifdef __KERNEL__
/*
 * write to socket
 */
 
int
__amp_sock_write(struct socket * sock, void * bufp, amp_u32_t len)
{
	amp_s32_t          rc = 0;
       mm_segment_t  oldmm = get_fs();

	AMP_ENTER("__amp_sock_write: enter\n");

       while (len > 0) {
                struct iovec  iov = {
                        .iov_base = bufp,
                        .iov_len  = len
                };
                struct msghdr msg = {
                        .msg_name       = NULL,
                        .msg_namelen    = 0,
                        .msg_iov        = &iov,
                        .msg_iovlen     = 1,
                        .msg_control    = NULL,
                        .msg_controllen = 0,
                        .msg_flags      = 0
                };

                set_fs (KERNEL_DS);
                rc = sock_sendmsg (sock, &msg, iov.iov_len);
                set_fs (oldmm);
                
                if (rc < 0)
                        goto EXIT;

                if (rc == 0) {
                        AMP_ERROR ("Unexpected zero rc\n");
			   rc = -ECONNABORTED;
                }

                bufp = ((amp_s8_t *)bufp) + rc;
                len -= rc;
        }

EXIT:
	AMP_LEAVE("__amp_sock_write: leave\n ");
        return rc;
}

/*
 * read from sock
 */
int
__amp_sock_read (struct socket *sock, void *bufp, amp_u32_t len)
{
	amp_s32_t           rc = 0;
    mm_segment_t  oldmm = get_fs();

	AMP_ENTER("__amp_sock_read: enter\n");
	
       while (len > 0) {
                struct iovec  iov = {
                        .iov_base = bufp,
                        .iov_len  = len
                };
                struct msghdr msg = {
                        .msg_name       = NULL,
                        .msg_namelen    = 0,
                        .msg_iov        = &iov,
                        .msg_iovlen     = 1,
                        .msg_control    = NULL,
                        .msg_controllen = 0,
                        .msg_flags      = 0
                };

                set_fs (KERNEL_DS);
                rc = sock_recvmsg (sock, &msg, iov.iov_len, 0);
                set_fs (oldmm);
                
                if (rc < 0)
                        return (rc);

                if (rc == 0) {
					rc = -ECONNABORTED;
			   		goto EXIT;
                }

                bufp = ((char *)bufp) + rc;
                len -= rc;
        }
EXIT:
	AMP_LEAVE("__amp_sock_read: leave\n");
	
    return rc;

}
/*
 * do a connection
 */
int
__amp_do_connection (struct socket **retsock, 
                     struct sockaddr_in *addr, 
                     amp_u32_t conn_type, 
                     amp_u32_t direction)
{
	amp_s32_t  err = 0;

	AMP_ENTER("__amp_do_connection: enter\n");

	if (!AMP_HAS_TYPE(conn_type)) {
		AMP_ERROR("__amp_do_connection: no type: %d\n", conn_type);
		err = -ENOSYS;
		goto EXIT;
	}

	if (!AMP_OP(conn_type, proto_connect)) {
		AMP_ERROR("__amp_do_connection: no amp_proto_connect op to type: %d\n", conn_type);
		err = -ENOSYS;
		goto EXIT;
	}
	err = AMP_OP(conn_type, proto_connect)(NULL, \
                                               (void **)retsock, \
					       (void *)addr, \
					       direction);
	if (err < 0) 
		AMP_ERROR("__amp_do_connection: connect error, err:%d\n", err);
	else
		AMP_DMSG("__amp_do_connection: retsock:%p\n", retsock);

EXIT:
	AMP_LEAVE("__amp_do_connection: leave\n");
	return err;
}

/*
 * connect to a server.
 */ 
int 
__amp_connect_server (amp_connection_t *conn, amp_u32_t thistype, amp_u32_t thisid)
{
	amp_s32_t    err = 0;
	struct socket *sock = NULL;
	struct sockaddr_in sin;
	amp_message_t   hello_msg;


	AMP_ENTER("__amp_connect_server: enter, conn:%p\n", conn);
	AMP_ENTER("__amp_connect_server: thistype:%d, thisid:%d\n", thistype, thisid);

	if (!conn) {
		AMP_ERROR("__amp_connect_server: no conn provided\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!conn->ac_remote_ipaddr || !conn->ac_remote_port) {
		AMP_ERROR("__amp_connect_server: the address of remote comp is error\n");
		err = -EINVAL;
		goto EXIT;
	}

	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(conn->ac_remote_ipaddr);
	sin.sin_port = htons(conn->ac_remote_port);

	err = __amp_do_connection(&sock, &sin, conn->ac_type, AMP_CONN_DIRECTION_CONNECT);
	if (err < 0) {
		AMP_ERROR("__amp_connect_server: connect error\n");
		goto EXIT;
	}

	/*
	 * send hello to server.
	 */
	memset(&hello_msg, 0, sizeof(amp_message_t));
	hello_msg.amh_magic = AMP_REQ_MAGIC;
	hello_msg.amh_type = AMP_HELLO;
	hello_msg.amh_pid = thistype;
	hello_msg.amh_xid = thisid;

	err = AMP_OP(conn->ac_type, proto_sendmsg)((void *)sock, &sin, sizeof(sin), sizeof(hello_msg), &hello_msg, 0);
	if (err < 0) {
		AMP_ERROR("__amp_connect_server: write header error, err:%d\n", err);
		AMP_OP(conn->ac_type, proto_disconnect)((void *)sock);
		goto EXIT;
	}
	 
	/*
	 * receive ack from server
	 */
	err = AMP_OP(conn->ac_type, proto_recvmsg)((void*)sock, &sin, sizeof(sin),  sizeof(hello_msg), &hello_msg, 0);		
	if (err < 0) {
		AMP_ERROR("amp_connect_server: read from server error, err:%d\n", err);
		AMP_OP(conn->ac_type, proto_disconnect)((void *)sock);
		goto EXIT;
	}

	if (hello_msg.amh_type != AMP_HELLO_ACK) {
		err = -EINVAL;
		AMP_ERROR("__amp_connect_server: get a wrong hello ack, type:%d\n", hello_msg.amh_type);
		AMP_OP(conn->ac_type, proto_disconnect)((void *)sock);
		goto EXIT;
	}

	sock->sk->sk_user_data = conn;

	if (conn->ac_type == AMP_CONN_TYPE_TCP)
		conn->ac_saved_data_ready = sock->sk->sk_data_ready;
	
	conn->ac_saved_state_change = sock->sk->sk_state_change;
	conn->ac_saved_write_space = sock->sk->sk_write_space;
	

	err = AMP_OP(conn->ac_type, proto_init)((void *)sock, AMP_CONN_DIRECTION_CONNECT);
	if (err < 0) {
		AMP_ERROR("amp_connect_server: int socket error, err:%d\n", err);
		AMP_OP(conn->ac_type, proto_disconnect)((void *)sock);
		goto EXIT;
	}

	conn->ac_remote_addr = sin;
	conn->ac_sock = sock;

	sock->sk->sk_user_data = conn;
	

EXIT:

	AMP_LEAVE("__amp_connect_server: leave\n");
	return err;
}
/*
* accept a connection from client
* 
* sockparent - the listen socket
* childconn - the new conn need to be initialized after create it
*/
int 
__amp_accept_connection (struct socket *sockparent, amp_connection_t *childconn)
{
	amp_s32_t err = 0;
	amp_message_t  msghd;
	amp_u32_t conn_type;
	struct socket *childsock = NULL;
	struct sockaddr_in sin;
	amp_u32_t  slen;

	AMP_WARNING("__amp_accept_connection: enter, childconn:%p\n", childconn);

	if (!sockparent) {
		AMP_ERROR("__amp_accept_connection: no parent socket\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!childconn) {
		AMP_ERROR("__amp_accept_connection: no child conn\n");
		err = -EINVAL;
		goto EXIT;
	}
	
	if (!childconn->ac_ctxt) {
		AMP_ERROR("__amp_accept_connection: no ctxt\n");
		err = -EINVAL;
		goto EXIT;
	}
	
	conn_type = childconn->ac_type;
	

	if (conn_type != AMP_CONN_TYPE_TCP) {
		AMP_ERROR("__amp_accept_connection: not tcp, so needn't connection\n");
		err = -EINVAL;
		goto EXIT;
	}

	/*
	 * 1. accept the connection
	 */
	err = AMP_OP(conn_type, proto_connect)((void*)sockparent,\
                                               (void**)&childsock, NULL, \
                                               AMP_CONN_DIRECTION_ACCEPT);
	
	if (err < 0) {
		AMP_ERROR("__amp_accept_connection: accept error, err:%d\n", err);
		goto EXIT;
	}

	if (!childsock) {
		AMP_ERROR("__amp_accept_connection: no child sock return\n");
		err = -EPROTO;
		goto EXIT;
	}

	slen = sizeof(sin);
	err = childsock->ops->getname(childsock, (struct sockaddr *) &sin, &slen, 1);
	if (err < 0) {
		AMP_ERROR("__amp_accept_connection: get name error, err:%d\n", err);
		AMP_OP(conn_type, proto_disconnect)(&childsock);
		goto EXIT;
	}

	/*
	 * accept the hello msg.
	 */
	 err = AMP_OP(conn_type, proto_recvmsg)((void *)childsock, \
						  NULL,  \
						  0, \
						  sizeof(amp_message_t), \
						  &msghd, \
						  0);
	
	 if (err < 0) {
	 	AMP_ERROR("__amp_accept_connection: receive msg head error, err:%d\n", err);
		AMP_OP(conn_type, proto_disconnect)((void *)childsock);
		goto EXIT;
	 }

	 if (msghd.amh_magic != AMP_REQ_MAGIC) {
	 	AMP_ERROR("__amp_accept_connection: receive msg head error, err:%d\n", err);
		AMP_OP(conn_type, proto_disconnect)((void *)childsock);
		goto EXIT;
	 }

	 if (msghd.amh_type != AMP_HELLO) {
	 	AMP_ERROR("__amp_accept_connection: receive msg head error, err:%d\n", err);
		AMP_OP(conn_type, proto_disconnect)((void *)childsock);
		goto EXIT; 	

	 }

	 /*
	  * initialize the connection.
	  */
	 AMP_DMSG("__amp_accept_connection: remote_id:%lld, remote_type:%d\n", \
                   msghd.amh_xid, msghd.amh_pid);

	 childconn->ac_remote_id = msghd.amh_xid;
	 childconn->ac_remote_comptype = msghd.amh_pid;
	 childconn->ac_remote_ipaddr = ntohl(sin.sin_addr.s_addr);
	 childconn->ac_remote_port = ntohs(sin.sin_port);
	 childconn->ac_remote_addr = sin;
	 childconn->ac_need_reconn = 0;
	 childconn->ac_state = AMP_CONN_OK;
	 childconn->ac_sock = childsock;

	 childsock->sk->sk_user_data = childconn;
	 childconn->ac_saved_state_change = childsock->sk->sk_state_change;
	 childconn->ac_saved_data_ready = childsock->sk->sk_data_ready;
	 childconn->ac_saved_write_space = childsock->sk->sk_write_space;

	 err = __amp_enqueue_conn(childconn, childconn->ac_ctxt);
	 if (err < 0) {
		AMP_ERROR("__amp_accept_connection: enqueue conn error, err:%d\n", err);
		AMP_OP(conn_type, proto_disconnect)((void *)childsock);
		goto EXIT;
	 }

	/*
	 * init the socket.
	 */
	 err = AMP_OP(conn_type, proto_init)((void* )childsock, AMP_CONN_DIRECTION_ACCEPT);
	 if (err < 0) {
	 	AMP_ERROR("__amp_accept_connection: init socket error, err:%d\n", err);
		__amp_dequeue_conn(childconn, childconn->ac_ctxt);
		AMP_OP(conn_type, proto_disconnect)((void*)childsock);
		goto EXIT;
	 }
	
	 childconn->ac_state = AMP_CONN_OK;

	 /*
	  * send back ack.
	  */
	 msghd.amh_type = AMP_HELLO_ACK;
	 err = AMP_OP(conn_type, proto_sendmsg)(childsock,  \
	 	                                    NULL,  \
	 	                                    0, \
	 	                                    sizeof(amp_message_t), \
	 	                                    &msghd, \
	 	                                    0);
	 if (err < 0) {
	 	AMP_ERROR("__amp_accept_connection: send back hello ack error, err:%d\n", err);
		childconn->ac_state = AMP_CONN_BAD;
		__amp_dequeue_conn(childconn, childconn->ac_ctxt);
		AMP_OP(conn_type, proto_disconnect)((void*)childsock);
		goto EXIT;
	 }


	 err = 0; 	  

EXIT:
	AMP_WARNING("__amp_accept_connection: leave\n");
	return err;
}
#endif

/*
 * return the hash value.
 */
amp_u32_t 
__amp_hash(amp_u32_t type, amp_u32_t id)
{
	amp_u32_t  hashvalue;

	hashvalue = ((~type) ^ ~(id)) & ((type << 8) ^ (id << 7));

	hashvalue = hashvalue % AMP_RESEND_HTB_SIZE;

	return hashvalue;
}

/*
 * add a request to a resend hash table.
 */
void 
__amp_add_resend_req(amp_request_t *req)
{
	amp_u32_t  hashvalue;
	amp_htb_entry_t *htbentry = NULL;

	AMP_ENTER("__amp_add_resend_req: enter, req:%p\n", req);
	AMP_ERROR("__amp_add_resend_req: add req, remote_type:%d, remote_id:%d\n",\
                  req->req_remote_type, req->req_remote_id);

	hashvalue = __amp_hash(req->req_remote_type, req->req_remote_id);
	htbentry = amp_resend_hash_table + hashvalue;

	amp_lock(&req->req_lock);
	amp_lock(&htbentry->lock);
	list_add_tail(&req->req_list, &htbentry->queue);
	amp_unlock(&htbentry->lock);
	req->req_state = AMP_REQ_STATE_RESENT;
	amp_unlock(&req->req_lock);
	
	AMP_LEAVE("__amp_add_resend_req: leave\n");
	return;
}

/*
 * remove the request from the resend hash table.
 */
void __amp_remove_resend_req(amp_request_t *req)
{
	amp_u32_t hashvalue;
	amp_htb_entry_t *htbentry = NULL;

	AMP_ENTER("__amp_remove_resend_req: enter, req:%p\n", req);
	if (req->req_state != AMP_REQ_STATE_RESENT)  {
		AMP_WARNING("__amp_remove_resend_req: remove non resend req\n");
		goto EXIT;
	}
		

	hashvalue = __amp_hash(req->req_remote_type, req->req_remote_id);
	htbentry = amp_resend_hash_table + hashvalue;

	amp_lock(&htbentry->lock);
	list_del(&req->req_list);
	amp_unlock(&htbentry->lock);
	
EXIT:
	AMP_LEAVE("__amp_remove_resend_req: leave\n");
	return;
}

/*
 * find any resend request corresponding with this type and id.
 */
amp_request_t *
__amp_find_resend_req(amp_u32_t type, amp_u32_t id)
{
	amp_request_t *req = NULL;
	amp_u32_t hashvalue;
	amp_htb_entry_t *htbentry = NULL;
	struct list_head *pos;
	
	AMP_ENTER("__amp_find_resend_req: enter\n");
	AMP_ENTER("__amp_find_resend_req: type:%d, id:%d\n", type, id);
	
	hashvalue = __amp_hash(type, id);

	htbentry = amp_resend_hash_table + hashvalue;
	amp_lock(&htbentry->lock);

	list_for_each(pos, &htbentry->queue) {
		req = list_entry(pos, amp_request_t, req_list);
		if (req->req_remote_type != type)
			continue;
		if (req->req_remote_id != id)
			continue;

		AMP_LEAVE("__amp_find_resend_req: find one req:%p\n", req);

		goto EXIT;
	}

	req = NULL;

EXIT:
	amp_unlock(&htbentry->lock);
	AMP_LEAVE("__amp_find_resend_req: leave\n");
	return req;
}
/*end of file*/

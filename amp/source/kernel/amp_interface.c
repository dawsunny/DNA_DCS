/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef EXPORT_SYMTAB
#define EXPORT_SYMTAB
#endif

#include <amp.h>

/*
 * we build a new connection
 * 
 * param:  
 *            ctxt - context of this component;
 *            remote_type - the type of remote component
 *            remote_id - the id of remote component
 *            addr - the address of the remote peer, for listen , it's ANY;
 *            port - the port of the remote peer;
 *            conn_type - the type of connection;
 *            direction - the direction of connection.
 *            queue_req - a call back for queuing the request when received.
 *            allocpage - a call back for alloc page for the request.
 *            freepag -  a call back for free the page alloced by above callback.
 *
 * return:  0 - successfully, <0 - something wrong, return the wrong code.
 */
int 
amp_create_connection (amp_comp_context_t *ctxt,
	               amp_u32_t remote_type,
		       amp_u32_t remote_id,
		       amp_u32_t addr,
		       amp_u32_t port,
		       amp_u32_t conn_type,
		       amp_u32_t direction,
		       int (*queue_req) (amp_request_t *req),
		       int (*allocpages) (void *, amp_u32_t *, amp_kiov_t **),
		       void (*freepages)(amp_u32_t , amp_kiov_t *))
{
	amp_connection_t *conn = NULL;
	amp_s32_t  err = 0;
	amp_thread_t *threadp = NULL;
	struct socket *newsock = NULL;
	struct socket *parentsock = NULL;
	struct sockaddr_in sin;

	AMP_ENTER("amp_create_connection: enter\n");

	if (direction == AMP_CONN_DIRECTION_ACCEPT) {
		AMP_ERROR("amp_create_connection: wrong direction: %d\n", direction);
		err = -EINVAL;
		goto EXIT;
	}

	if (!ctxt) {
		AMP_ERROR("amp_create_connection: no context\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!ctxt->acc_conns) {
		AMP_ERROR("amp_create_connection: no acc_conns in context\n");
		err = -EINVAL;
		goto EXIT;
	}

/*
	if (!queue_req) {
		AMP_ERROR("amp_create_connection: no queue_req\n ");
		err = -EINVAL;
		goto EXIT;

	}

	if (!allocpage) {
		AMP_ERROR("amp_create_connection: no allocpage\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!freepage) {
		AMP_ERROR("amp_create_connection: no freepage\n");
		err = -EINVAL;
		goto EXIT;
	}
*/
	if ((conn_type != AMP_CONN_TYPE_TCP) && (conn_type != AMP_CONN_TYPE_UDP)) {
		AMP_ERROR("amp_create_connection: wrong type: %d\n", conn_type);
		err = -EINVAL;
		goto EXIT;
	}

	err = __amp_alloc_conn(&conn);
	if (err < 0) {
		AMP_ERROR("amp_create_connection: cannot alloc connection\n");
		goto EXIT;
	}

	

	conn->ac_ctxt = ctxt;	
	conn->ac_remote_ipaddr = addr;
	conn->ac_remote_port = port;
	conn->ac_queue_cb = queue_req;
	conn->ac_freepage_cb = freepages;
	conn->ac_allocpage_cb = allocpages;
	

	switch (direction)  {
		case AMP_CONN_DIRECTION_LISTEN:
			AMP_DMSG("amp_create_connection: create listen connection\n");
			sin.sin_family = AF_INET;
			sin.sin_addr.s_addr = htonl(addr);
			sin.sin_port = htons(port);			

			/*
			 * connecting.
			 */
			err = AMP_OP(conn_type, proto_connect)((void *)parentsock,
			                                       (void **)&newsock,
			                                       (void *)&sin,
			                                       direction);
			if (err < 0)  {
				__amp_free_conn(conn);
				goto EXIT;
			}

			conn->ac_saved_state_change = newsock->sk->sk_state_change;
			conn->ac_saved_data_ready = newsock->sk->sk_data_ready;
			conn->ac_saved_write_space = newsock->sk->sk_write_space;
			
			newsock->sk->sk_user_data = conn;

			/*
			 * init the socket
			 */
			err = AMP_OP(conn_type, proto_init)((void *)newsock, direction);
			if (err < 0) {
				AMP_ERROR("amp_create_connection: proto init error\n");
				__amp_free_conn(conn);
				goto EXIT;
			}
			conn->ac_sock = newsock;
			conn->ac_type = conn_type;
			conn->ac_this_type = ctxt->acc_this_type;
			conn->ac_this_id = ctxt->acc_this_id;
			conn->ac_this_port = port;

			if (conn_type == AMP_CONN_TYPE_TCP) {
				/*
				 * Create listen specific thread
				 */
				AMP_DMSG("amp_create_connection: this is a listen connection\n");
		
				threadp = __amp_start_listen_thread(conn);
				if (!threadp)  {
					AMP_OP(conn_type, proto_disconnect)(conn->ac_sock);
					__amp_free_conn(conn);
					goto EXIT;
				}
			}

			ctxt->acc_listen_conn = conn;
			ctxt->acc_listen_thread = threadp;
						
						
			break;
		case AMP_CONN_DIRECTION_CONNECT:
			if (!ctxt->acc_conns[remote_type].acc_remote_conns) {
				AMP_ERROR("amp_create_connection: no acc_remote_conns for type:%d\n", remote_type);
				err = -EINVAL;
				goto EXIT;
			}
			
			if (remote_type > AMP_MAX_COMP_TYPE) {
				AMP_ERROR("amp_create_connection: wrong remote_type:%d\n", remote_type);
				err = -EINVAL;
				goto EXIT;
			}
			
			conn->ac_type = conn_type;	
			conn->ac_remote_comptype = remote_type;
			conn->ac_remote_id = remote_id;
			conn->ac_need_reconn = 1;
			conn->ac_remain_times = AMP_CONN_RECONN_MAXTIMES;
			conn->ac_this_type = ctxt->acc_this_type;
			conn->ac_this_id = ctxt->acc_this_id;
	
			err = __amp_connect_server (conn, ctxt->acc_this_type, ctxt->acc_this_id);
			if (err < 0) {
				__amp_free_conn(conn);
				goto EXIT;
			}
			conn->ac_state = AMP_CONN_OK;
			__amp_enqueue_conn(conn, ctxt);
			
			break;
		
		default:
			AMP_ERROR("amp_create_connection: wrong connection type: %d\n", conn_type);
			__amp_free_conn(conn);
			err = -EINVAL;
			goto EXIT;
	}
	
	AMP_LEAVE("__amp_create_connection: created conn:%p\n", conn);	
	
	
EXIT:
	AMP_LEAVE("amp_create_connection: leave\n");
	return err;	

}

#if 0
int amp_send_sync (amp_comp_context_t *ctxt,
		   amp_request_t *req,
		   amp_u32_t  type,
		   amp_u32_t id,
	           amp_s32_t resent)
{
	amp_s32_t  err = 0;
	amp_message_t  *msgp = NULL;
	amp_u64_t   xid;
	amp_time_t  tm;
	struct timeval  tv;
	
	AMP_ENTER("amp_send_sync: enter\n");
	AMP_DMSG("__amp_send_sync: type:%d, id:%d\n", type, id);

	if (!req) {

		AMP_ERROR("amp_send_sync: no request\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (type > AMP_MAX_COMP_TYPE) {
		AMP_ERROR("amp_send_sync: wrong type:%d\n", type);
		err = -EINVAL;
		goto EXIT;

	}

	if (!ctxt) {
		AMP_ERROR("amp_send_sync: no context\n");
		err = -EINVAL;
		goto EXIT;
	}
	
	if (!ctxt->acc_conns) {
		AMP_ERROR("amp_send_sync: no acc_conns in context\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!ctxt->acc_conns[type].acc_remote_conns) {
		AMP_ERROR("amp_send_sync: no acc_remote_conns in this context\n");
		err = -EINVAL;
		goto EXIT;
	}

	read_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
	
	if (list_empty(&(ctxt->acc_conns[type].acc_remote_conns[id].queue))) {
		AMP_ERROR("amp_send_sync: the remote_conns is empty\n");
		err = -ENOTCONN;
		read_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
		goto EXIT;
	}

	read_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));

	req->req_remote_type = type;
	req->req_remote_id = id;
	req->req_ctxt = ctxt;
	

	if (resent)
		req->req_resent = 1;
	else 
		req->req_resent = 0;

	atomic_inc(&req->req_refcount);
	if (req->req_type & AMP_REQUEST) {
		AMP_DMSG("amp_send_sync: it's a request\n");
		if (!req->req_msg) {
			AMP_ERROR("amp_send_sync: no msg for a request\n");
			err = -EINVAL;
			goto EXIT;
		}
		msgp = req->req_msg;
		xid = __amp_getxid();
		amp_gettimeofday(&tv);
		tm.sec = tv.tv_sec;
		tm.usec = tv.tv_usec;
	
		AMP_FILL_REQ_HEADER(msgp, \
                            req->req_msglen - AMP_MESSAGE_HEADER_LEN, \
                            req->req_type, \
                            current->pid, \
                            req, \
                            xid, \
                            tm);
	} else {
	       AMP_DMSG("amp_send_sync: it's a reply\n");
	       if (!req->req_reply) {
			AMP_ERROR("amp_send_sync: no req_reply for a reply\n");
			err = -EINVAL;
			goto EXIT;
	       }
               req->req_reply->amh_size = req->req_replylen - AMP_MESSAGE_HEADER_LEN;
	       msgp = req->req_reply;
	       msgp->amh_type = req->req_type;
	}

	AMP_DMSG("[%d]amp_send_sync: req:%p, req->req_msg:%p, req->req_reply:%p\n", \
		  current->pid, req, req->req_msg, req->req_reply);
	
	AMP_DMSG("amp_send_sync: before add request\n");	
	amp_lock(&amp_sending_list_lock);
	list_add_tail(&req->req_list, &amp_sending_list);
	amp_sem_up(&amp_process_out_sem);
	amp_unlock(&amp_sending_list_lock);
	
	AMP_DMSG("amp_send_sync: request added, before down waitsem\n");

	/*
	 * now wait on the semaphore
	 */
	AMP_WARNING("amp_send_sync: before sem down\n");
	err = amp_sem_down_interruptible(&req->req_waitsem);
	if (err < 0) {
		AMP_ERROR("amp_send_sync: sem down error, err:%d\n", err);	
		goto EXIT;
	}
	
	AMP_WARNING("amp_send_sync: after down waitsem\n");

	err = req->req_error;
	
EXIT:	
	AMP_LEAVE("amp_send_sync: leave, err:%d\n", err);
	return err;
}
#endif
int amp_send_sync (amp_comp_context_t *ctxt,
		   amp_request_t *req,
		   amp_u32_t  type,
		   amp_u32_t id,
	           amp_s32_t resent)
{
	amp_s32_t  err = 0;
	amp_message_t  *msgp = NULL;
	amp_u64_t   xid;
	amp_time_t  tm;
	struct timeval  tv;
	amp_u32_t   req_type = 0;
	amp_u32_t   conn_type = 0;
	amp_u32_t   flags = 0;
	amp_u32_t   need_ack = 0;
	struct sockaddr_in  sout_addr;
	amp_u32_t   sendsize = 0;
	amp_connection_t *conn = NULL;

	AMP_ENTER("amp_send_sync: enter\n");
	AMP_DMSG("__amp_send_sync: type:%d, id:%d\n", type, id);

	if (!req) {

		AMP_ERROR("amp_send_sync: no request\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (type > AMP_MAX_COMP_TYPE) {
		AMP_ERROR("amp_send_sync: wrong type:%d\n", type);
		err = -EINVAL;
		goto EXIT;

	}

	if (!ctxt) {
		AMP_ERROR("amp_send_sync: no context\n");
		err = -EINVAL;
		goto EXIT;
	}
	
	if (!ctxt->acc_conns) {
		AMP_ERROR("amp_send_sync: no acc_conns in context\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!ctxt->acc_conns[type].acc_remote_conns) {
		AMP_ERROR("amp_send_sync: no acc_remote_conns in this context\n");
		err = -EINVAL;
		goto EXIT;
	}

	read_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
	
	if (list_empty(&(ctxt->acc_conns[type].acc_remote_conns[id].queue))) {
		AMP_ERROR("amp_send_sync: the remote_conns is empty\n");
		err = -ENOTCONN;
		read_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
		goto EXIT;
	}

	read_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));

	req->req_remote_type = type;
	req->req_remote_id = id;
	req->req_ctxt = ctxt;
	req->req_error = 0;
	

	if (resent)
		req->req_resent = 1;
	else 
		req->req_resent = 0;

	atomic_inc(&req->req_refcount);

	if (req->req_type & AMP_REQUEST) {
		AMP_DMSG("amp_send_sync: it's a request\n");
		if (!req->req_msg) {
			AMP_ERROR("amp_send_sync: no msg for a request\n");
			err = -EINVAL;
			goto EXIT;
		}
		msgp = req->req_msg;
		xid = __amp_getxid();
		amp_gettimeofday(&tv);
		tm.sec = tv.tv_sec;
		tm.usec = tv.tv_usec;
	
		AMP_FILL_REQ_HEADER(msgp, \
                            req->req_msglen - AMP_MESSAGE_HEADER_LEN, \
                            req->req_type, \
                            current->pid, \
                            req, \
                            xid, \
                            tm);
	} else {
	       AMP_DMSG("amp_send_sync: it's a reply\n");
	       if (!req->req_reply) {
			AMP_ERROR("amp_send_sync: no req_reply for a reply\n");
			err = -EINVAL;
			goto EXIT;
	       }
               req->req_reply->amh_size = req->req_replylen - AMP_MESSAGE_HEADER_LEN;
	       msgp = req->req_reply;
	       msgp->amh_type = req->req_type;
	}

	AMP_DMSG("[%d]amp_send_sync: req:%p, req->req_msg:%p, req->req_reply:%p\n", \
		  current->pid, req, req->req_msg, req->req_reply);
	
	req_type = req->req_type;

	if ((req_type != (AMP_REQUEST | AMP_MSG))  &&
		(req_type != (AMP_REQUEST | AMP_DATA)) &&
		(req_type != (AMP_REPLY | AMP_MSG)) &&
		(req_type != (AMP_REPLY | AMP_DATA))) {
		AMP_ERROR("__amp_send_sync: wrong msg type: 0x%x\n", req_type);
		req->req_error = -EINVAL;
		err = -EINVAL;
		goto EXIT;
	}

	if (req_type & AMP_REQUEST) {
                sendsize = req->req_msglen;
                if (req_type & AMP_DATA)
                        sendsize = sendsize + (4096 << req->req_niov);
        } else {
                sendsize = req->req_replylen;
                if (req_type & AMP_DATA)
                        sendsize = sendsize + (4096 << req->req_niov);
        }
SELECT_CONN:
	err = 0;
	err = __amp_select_conn(req->req_remote_type, 
		                req->req_remote_id, 
		                req->req_ctxt, 
		                &conn);
	if (err) {
		switch(err)  {
			case 2:
				AMP_WARNING("amp_send_sync: no valid conn to peer (type:%d, id:%d, err:%d)\n", \
					     req->req_remote_type, \
				             req->req_remote_id, \
				             err);
				
				if (req->req_resent) {
					__amp_add_resend_req(req);
					goto WAITSEM;
				}
			case 1:
				AMP_WARNING("amp_send_sync: no conn to peer(type:%d, id:%d, err:%d)\n", \
					     req->req_remote_type, \
					     req->req_remote_id, \
					     err);
			default:				
				req->req_error = -ENOTCONN;
				err = -ENOTCONN;
				AMP_ERROR("amp_send_sync: before free request:%p, refcount:%d\n", \
				           req, atomic_read(&req->req_refcount));
				__amp_free_request(req);
				amp_sem_up(&req->req_waitsem);
				goto EXIT;
		}
		
	}
	switch(conn->ac_state) {
		case AMP_CONN_BAD:
		case AMP_CONN_NOTINIT:
		case AMP_CONN_CLOSE:
		case AMP_CONN_RECOVER:
			amp_unlock(&conn->ac_lock);
			AMP_WARNING("amp_send_sync: conn:%p is not valid currently\n", conn);
			goto SELECT_CONN;
		default:
			break;
	}

	conn_type = conn->ac_type;       
        if ((!AMP_HAS_TYPE(conn_type)) || 
	   	(!AMP_OP(conn_type, proto_sendmsg)) || 
	   	(!AMP_OP(conn_type, proto_senddata)))  {
		AMP_WARNING("amp_send_sync: conn:%p, has no operations\n", conn);
	   	req->req_error = -ENOSYS;
		err = -ENOSYS;
		amp_unlock(&conn->ac_lock);
		goto EXIT;
        }	
	
	
        atomic_inc(&conn->ac_refcont);	      
        amp_unlock(&conn->ac_lock); 

        amp_sem_down(&conn->ac_sendsem);
        conn->ac_weight += sendsize;

        if (conn->ac_state != AMP_CONN_OK) {
		AMP_WARNING("amp_send_sync: before send, state of conn:%p is invalid:%d\n", \
                             conn, conn->ac_state);
		conn->ac_weight -= sendsize;
		amp_sem_up(&conn->ac_sendsem);
		__amp_free_conn(conn);
		goto SELECT_CONN;
        }
	/*lock request*/
	amp_lock(&req->req_lock);	
        req->req_stage = AMP_REQ_STAGE_MSG;
	if (req_type & AMP_DATA)
		flags = MSG_MORE;
	else
		flags = 0;

	sout_addr = conn->ac_remote_addr;
	/*
	 * We must add the req to waiting reply list before 
         * really sending the msg, for in smp architecture,
         * it will cause the queuing operation postponed after
         * we have received the reply from peer.
	 */
	need_ack = req->req_need_ack;
	/*
	if (need_ack) { 
		AMP_ENTER("amp_send_sync: add req:%p to waiting reply list\n", req);
		amp_lock(&amp_waiting_reply_list_lock);
		list_add_tail(&req->req_list, &amp_waiting_reply_list);
		amp_unlock(&amp_waiting_reply_list_lock);
	}
	*/

	if (req_type & AMP_REQUEST) {
		AMP_DMSG("amp_send_sync: conn:%p, send request\n", conn);
		err = AMP_OP(conn_type, proto_sendmsg)(conn->ac_sock, \
	                                           &sout_addr, \
	                                           sizeof(sout_addr), \
	                                           req->req_msglen, \
	                                           req->req_msg, \
	                                           flags);
	} else {
		AMP_DMSG("amp_send_sync: conn:%p, send reply\n", conn);
		if (conn->ac_type == AMP_CONN_TYPE_UDP)
			sout_addr = req->req_reply->amh_addr;
		
		err = AMP_OP(conn_type, proto_sendmsg)(conn->ac_sock, \
	                                               &sout_addr, \
	                                               sizeof(sout_addr), \
	                                               req->req_replylen, \
	                                               req->req_reply, \
	                                               flags);
	}
	
	if (err < 0) {
		AMP_ERROR("amp_send_sync: sendmsg error, conn:%p, req:%p, err:%d\n", \
                           conn, req, err);
		/*
		if (need_ack) {
			AMP_DMSG("amp_send_sync: send msg error, err:%d\n", err);
			amp_lock(&amp_waiting_reply_list_lock);
			list_del_init(&req->req_list);
			amp_unlock(&amp_waiting_reply_list_lock);
		}
		*/
		amp_unlock(&req->req_lock);
		goto SEND_ERROR;
	}

	if (req_type & AMP_DATA)  {
		/*stage2: send data*/

		AMP_DMSG("amp_send_sync: conn:%p, send data\n", conn);
		req->req_stage = AMP_REQ_STAGE_DATA;
		err = AMP_OP(conn_type, proto_senddata)(conn->ac_sock,
		                                        &sout_addr, \
	                                                sizeof(sout_addr), \
	                                                req->req_niov,
	                                                req->req_iov,
	                                                0);
		if (err < 0) {
			AMP_ERROR("amp_send_sync: senddata error, conn:%p, req:%p, err:%d\n",\
                                   conn, req, err);
			/*
			if (need_ack) {
				AMP_DMSG("amp_send_sync: send data error, err:%d\n", err);
				amp_lock(&amp_waiting_reply_list_lock);
				list_del_init(&req->req_list);
				amp_unlock(&amp_waiting_reply_list_lock);
			}
			*/
			amp_unlock(&req->req_lock);
			goto SEND_ERROR;
		}
	}
	conn->ac_weight -= sendsize;
	amp_sem_up(&conn->ac_sendsem);
	__amp_free_conn(conn);
	
	if (need_ack) { /*waiting for ack*/
		AMP_LEAVE("amp_send_sync: req:%p, need ack\n", req);
		amp_lock(&amp_waiting_reply_list_lock);
		list_add_tail(&req->req_list, &amp_waiting_reply_list);
		amp_unlock(&amp_waiting_reply_list_lock);

		amp_unlock(&req->req_lock);
		goto WAITSEM;
	}
	amp_unlock(&req->req_lock);
	/*
	 * do not need ack
	 */
	__amp_free_request(req);
	amp_sem_up(&req->req_waitsem);	
	err = 0;
	goto EXIT;

	/*
	 * now wait on the semaphore
	 */
WAITSEM:
	amp_sem_down(&req->req_waitsem);
	AMP_DMSG("amp_send_sync: after down waitsem, req:%p, refcount:%d\n", \
                   req, atomic_read(&req->req_refcount));
	err = req->req_error;

EXIT:	
	AMP_LEAVE("amp_send_sync: leave, err:%d\n", err);
	return err;

SEND_ERROR:
	AMP_ERROR("amp_send_sync: send error through conn:%p, err:%d\n", 
                  conn, err);

	amp_lock(&conn->ac_lock);
	AMP_DMSG("amp_send_sync: before check\n");
	
	if (conn->ac_type == AMP_CONN_TYPE_TCP)  {
		conn->ac_datard_count = 0;
		conn->ac_sched = 0;		
	}

	if (conn->ac_need_reconn) {
		/*
		 * we must add the bad connection to the reconnect list.
		 */
		if (conn->ac_state != AMP_CONN_RECOVER) {
			AMP_ERROR("amp_send_sync: set conn:%p to recover\n", conn);
			AMP_OP(conn_type, proto_disconnect)((void *)&conn->ac_sock);
			conn->ac_sock = NULL;
			conn->ac_state = AMP_CONN_RECOVER;	
			amp_lock(&amp_reconn_conn_list_lock);
			if (list_empty(&conn->ac_reconn_list))
				list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
			amp_unlock(&amp_reconn_conn_list_lock);
		} else 
			AMP_ERROR("amp_send_sync: someone else has set conn:%p to recover\n", conn);
			
	} else if (conn->ac_type == AMP_CONN_TYPE_TCP)   { 
	        /*
	         * Maybe it's in server side or it's realy need to be released, so we free it.
	         */
		if (conn->ac_state != AMP_CONN_CLOSE) {
			AMP_ERROR("amp_send_sync: set conn:%p to close\n", conn);
			conn->ac_state = AMP_CONN_CLOSE;
			amp_lock(&amp_reconn_conn_list_lock);
			if (list_empty(&conn->ac_reconn_list))
				list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
			amp_unlock(&amp_reconn_conn_list_lock);
		} else 
			AMP_ERROR("amp_send_sync: someone else has set conn:%p to close\n", conn);
	}
		
	atomic_dec(&conn->ac_refcont);
	amp_unlock(&conn->ac_lock);	
	conn->ac_weight -= sendsize;
	amp_sem_up(&conn->ac_sendsem);
	goto SELECT_CONN;

}



int amp_send_async ( amp_comp_context_t *ctxt,
		     amp_request_t *req,
		     amp_u32_t  type,
		     amp_u32_t id,
		     amp_s32_t resent)
{
	amp_s32_t  err = 0;
	amp_message_t *msgp = NULL;
	amp_u64_t xid;
	amp_time_t tm;
	struct timeval tv;
	
	AMP_ENTER("amp_send_async: enter\n");

	if (!req) {

		AMP_ERROR("amp_send_async: no request\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (type > AMP_MAX_COMP_TYPE) {
		AMP_ERROR("amp_send_async: wrong type:%d\n", type);
		err = -EINVAL;
		goto EXIT;

	}

	if (!ctxt) {
		AMP_ERROR("amp_send_async: no context\n");
		err = -EINVAL;
		goto EXIT;
	}


	if (!ctxt->acc_conns) {
		AMP_ERROR("amp_send_async: no acc_conns in context\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!ctxt->acc_conns[type].acc_remote_conns) {
		AMP_ERROR("amp_send_async: no acc_remote_conns in this context\n");
		err = -EINVAL;
		goto EXIT;
	}

	read_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
	if (list_empty(&(ctxt->acc_conns[type].acc_remote_conns[id].queue))) {
		AMP_ERROR("amp_send_async: the remote_conns is empty\n");
		read_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
		err = -ENOTCONN;
		goto EXIT;
	}
	read_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));

	req->req_remote_type = type;
	req->req_remote_id = id;
	req->req_ctxt = ctxt;
	req->req_error = 0;

	if (resent)
		req->req_resent = 1;
	else 
		req->req_resent = 0;
	
	atomic_inc(&req->req_refcount);
	if (req->req_type & AMP_REQUEST) {
		msgp = req->req_msg;
		xid = __amp_getxid();
		amp_gettimeofday(&tv);
		tm.sec = tv.tv_sec;
		tm.usec = tv.tv_usec;
	
		AMP_FILL_REQ_HEADER(msgp, \
                            req->req_msglen - AMP_MESSAGE_HEADER_LEN, \
                            req->req_type, \
                            current->pid, \
                            req, \
                            xid, \
                            tm);
	} else {
            if (!req->req_reply) {
                AMP_ERROR("amp_send_async: no req_reply  for a reply\n");
                err = -EINVAL;
                goto EXIT;
            }
            req->req_reply->amh_size = req->req_replylen - AMP_MESSAGE_HEADER_LEN;
            req->req_reply->amh_type = req->req_type;
        }
	amp_lock(&amp_sending_list_lock);
	list_add_tail(&req->req_list, &amp_sending_list);
	amp_sem_up(&amp_process_out_sem);
	amp_unlock(&amp_sending_list_lock);

		
EXIT:	
	AMP_LEAVE("amp_send_async: leave\n");
	return err;
}

amp_comp_context_t *
amp_sys_init (amp_u32_t  type, amp_u32_t id)
{
	amp_comp_context_t *ctxt = NULL;
	amp_u32_t size;
	amp_u32_t i, j;
	amp_u32_t arr_size;

	AMP_ENTER("amp_sys_init: enter, type:%d, id:%d\n", type, id);

	ctxt = (amp_comp_context_t *)amp_alloc(sizeof(amp_comp_context_t));
	if (!ctxt)  {
		AMP_ERROR("amp_sys_init: alloc context structure error, no mem\n");
		goto EXIT;
	}
	memset(ctxt, 0, sizeof(amp_comp_context_t));
	ctxt->acc_this_id = id;
	ctxt->acc_this_type = type;
	ctxt->acc_listen_conn = NULL;
	ctxt->acc_listen_thread = NULL;

	size = AMP_MAX_COMP_TYPE * sizeof(amp_comp_conns_t);
	ctxt->acc_conns = (amp_comp_conns_t *)amp_alloc(size);
	if (!ctxt->acc_conns)  {
		AMP_ERROR("amp_sys_init: alloc for acc_conns error\n");
		goto  ALLOC_CONNS_ERROR;
	}	
	memset(ctxt->acc_conns, 0, size);

	arr_size = AMP_SELECT_CONN_ARRAY_ALLOC_LEN * sizeof(amp_connection_t *);

	for (i=0; i<AMP_MAX_COMP_TYPE; i++) {
		amp_lock_init(&(ctxt->acc_conns[i].acc_lock));
		ctxt->acc_conns[i].acc_num = 0;
		ctxt->acc_conns[i].acc_remote_conns = (conn_queue_t *)amp_alloc(AMP_CONN_ADD_INCR * sizeof(conn_queue_t));
		if (!ctxt->acc_conns[i].acc_remote_conns) {
			AMP_ERROR("amp_sys_init: alloc acc_remote_conns error\n");
			goto ALLOC_REMOTE_CONNS_ERROR;
		}
		memset(ctxt->acc_conns[i].acc_remote_conns, 
			     0, 
			     AMP_CONN_ADD_INCR * sizeof(conn_queue_t));

		for (j=0; j<AMP_CONN_ADD_INCR; j++) {
			INIT_LIST_HEAD(&(ctxt->acc_conns[i].acc_remote_conns[j].queue));
			rwlock_init(&(ctxt->acc_conns[i].acc_remote_conns[j].queue_lock));
			ctxt->acc_conns[i].acc_remote_conns[j].conns = 
                           (amp_connection_t **)amp_alloc(arr_size);
			if (!ctxt->acc_conns[i].acc_remote_conns[j].conns) {
				AMP_ERROR("amp_sys_init: alloc conns for i:%d, j:%d\n", i, j);
				goto ALLOC_REMOTE_CONNS_ERROR;
			}
			memset(ctxt->acc_conns[i].acc_remote_conns[j].conns, 0, arr_size);
			ctxt->acc_conns[i].acc_remote_conns[j].total_num = AMP_SELECT_CONN_ARRAY_ALLOC_LEN;
			ctxt->acc_conns[i].acc_remote_conns[j].active_conn_num = -1;
		}
		
		ctxt->acc_conns[i].acc_alloced_num = AMP_CONN_ADD_INCR;
	}
	
	ctxt->acc_netmorn_thread = __amp_start_netmorn_thread(ctxt);
	if (!ctxt->acc_netmorn_thread) {
		AMP_ERROR("amp_sys_init: start net mornitor thread error\n");
		goto START_NETMORN_THREAD_ERROR;
	}
	
EXIT:
	AMP_LEAVE("amp_sys_init: leave\n");
	return ctxt;
START_NETMORN_THREAD_ERROR:
ALLOC_REMOTE_CONNS_ERROR:
	if (ctxt->acc_conns)  {
		for (i=0; i<AMP_MAX_COMP_TYPE; i++)  {
			if (ctxt->acc_conns[i].acc_remote_conns) {
				for (j=0; j<AMP_CONN_ADD_INCR; j++) {
					if (ctxt->acc_conns[i].acc_remote_conns[j].conns)
						amp_free(ctxt->acc_conns[i].acc_remote_conns[j].conns,\
							 arr_size);
				}
				amp_free(ctxt->acc_conns[i].acc_remote_conns, \
						   AMP_CONN_ADD_INCR * sizeof(conn_queue_t));
			}
		}
		amp_free(ctxt->acc_conns, AMP_MAX_COMP_TYPE * sizeof(amp_comp_conns_t));
	}

ALLOC_CONNS_ERROR:
	if (ctxt)
		amp_free(ctxt, sizeof(amp_comp_context_t));

	ctxt = NULL;
	goto EXIT;

}

int 
amp_disconnect_peer (amp_comp_context_t *ctxt,
                     amp_u32_t remote_type,
                     amp_u32_t remote_id,
                     amp_u32_t forall)
{
	amp_s32_t err = 0;
	amp_connection_t *conn = NULL;
	struct list_head *head = NULL;
	amp_comp_conns_t *cmp_conns = NULL;
	conn_queue_t     *cnq = NULL;
	amp_request_t    *req = NULL;
	amp_u32_t i;

	AMP_ENTER("amp_disconnect_peer: enter,type:%d,id:%d,forall:%d\n", \
                   remote_type, remote_id, forall);

	if (remote_type > AMP_MAX_COMP_TYPE) {
		AMP_ERROR("amp_disconnect_peer: wrong type: %d\n", \
                           remote_type);
		err = -EINVAL;
		goto EXIT;
	}

	if (!ctxt) {
		AMP_ERROR("amp_disconnect_peer: no context\n");
		err = -EINVAL;
		goto EXIT;
	}
	

	if (!ctxt->acc_conns) {
		AMP_ERROR("amp_disconnect_peer: no acc_conns in ctxt\n");
		err = 1;
		goto EXIT;
	}
	
	cmp_conns = &(ctxt->acc_conns[remote_type]);

	if (remote_id < 0 || remote_id >= cmp_conns->acc_alloced_num) {
		AMP_ERROR("amp_disconnect_peer: wrong id:%d\n", remote_id);
		err = -EINVAL;
		goto EXIT;
	}

	if (!cmp_conns->acc_remote_conns) {
		AMP_ERROR("amp_disconnect_peer: no remote conns \n");
		err = 1;
		goto EXIT;
	}

	read_lock(&(cmp_conns->acc_remote_conns[remote_id].queue_lock));
	cnq = &(cmp_conns->acc_remote_conns[remote_id]);
	head = &(cmp_conns->acc_remote_conns[remote_id].queue);	
	
	if (list_empty(head)) {
		AMP_ERROR("amp_disconnect_peer: no connection to type:%d, id:%d\n",\
			   remote_type, remote_id);
		err = 1;
		read_unlock(&(cmp_conns->acc_remote_conns[remote_id].queue_lock));
		goto EXIT;
	}

	if (cnq->active_conn_num < 0) {
		AMP_ERROR("amp_disconnect_peer: type:%d, id:%d, active_conn_num:%d ,wrong\n", \
			   remote_type, remote_id, cnq->active_conn_num);
		err = 1;
		read_unlock(&(cmp_conns->acc_remote_conns[remote_id].queue_lock));
		goto EXIT;
	}

	for (i=0; i<=cnq->active_conn_num; i++) {
		conn = cnq->conns[i];
		if (!conn)
			continue;

		cnq->conns[i] = NULL;
		AMP_DMSG("amp_disconnect_peer: to disconnect conn:%p\n", conn);
		amp_lock(&conn->ac_lock);

		AMP_DMSG("amp_disconnect_peer: delete from queue\n");
		list_del_init(&conn->ac_list);

		if (!list_empty(&conn->ac_reconn_list) &&
		     conn->ac_state != AMP_CONN_CLOSE) {
			AMP_DMSG("amp_disconnect_peer: conn is on reconn list\n");
			amp_lock(&amp_reconn_conn_list_lock);
			list_del_init(&conn->ac_reconn_list);
			amp_unlock(&amp_reconn_conn_list_lock);
		}
		conn->ac_state = AMP_CONN_BAD;
		amp_sem_down(&conn->ac_recvsem);
		amp_sem_down(&conn->ac_sendsem);


		if (!list_empty(&conn->ac_protobh_list)) {
			AMP_ERROR("amp_disconnect_peer: free conn:%p, protobh_list not empty\n",\
                                   conn);
		}

		AMP_DMSG("amp_disconnect_peer: call proto disconnect\n");

		if (conn->ac_sock)
			AMP_OP(conn->ac_type, proto_disconnect)(conn->ac_sock);
		else
			AMP_ERROR("amp_disconnect_peer: close conn:%p, no sock\n", \
                                   conn);
		conn->ac_sock = NULL;
		conn->ac_weight = 0;
		amp_sem_up(&conn->ac_recvsem);
		amp_sem_up(&conn->ac_sendsem);
		amp_unlock(&conn->ac_lock);
		__amp_free_conn(conn);

		if (!forall)
			break;
	}
	read_unlock(&(cmp_conns->acc_remote_conns[remote_id].queue_lock));
	if (forall) {
		AMP_DMSG("amp_disconnect_peer: to wake all resend reqs\n");
		while ((req = __amp_find_resend_req(remote_type, remote_id))) {
			AMP_DMSG("amp_disconnect_peer: find on req:%p\n", req);
			amp_lock(&req->req_lock);
			__amp_remove_resend_req(req);
			req->req_error = -ENOTCONN;
			amp_unlock(&req->req_lock);
			__amp_free_request(req);
			amp_sem_up(&req->req_waitsem);
		}
		cnq->active_conn_num = -1;
	}

EXIT:
	AMP_LEAVE("amp_disconnect_peer: leave\n");
	return err;
}

int
amp_sys_finalize (amp_comp_context_t *cmp_ctxt)
{
	amp_u32_t err = 0;
	amp_thread_t *threadp = NULL;
	struct list_head *head = NULL;
	amp_connection_t *conn = NULL;
	amp_u32_t i, j;
	amp_u32_t arr_size;
	conn_queue_t *cnq = NULL;
	
	AMP_WARNING("amp_sys_finalize: enter\n");

	if (!cmp_ctxt)  {
		AMP_ERROR("amp_sys_finalize: no component context\n");
		goto EXIT;
	}

	/*
	 * firstly stop the listen thread.
	 */
	if (cmp_ctxt->acc_listen_thread)  {
		threadp = cmp_ctxt->acc_listen_thread;
		err = __amp_stop_listen_thread(cmp_ctxt);
		if (err < 0) 
			goto EXIT;
		amp_free(threadp, sizeof(amp_thread_t));
		cmp_ctxt->acc_listen_thread = NULL;
	}

	/*
	 * get down the listen connection
	 */
	if (cmp_ctxt->acc_listen_conn) {
		AMP_ERROR("amp_sys_finalize: free listen connection: %p\n", cmp_ctxt->acc_listen_conn);
		conn = cmp_ctxt->acc_listen_conn;
		AMP_OP(conn->ac_type, proto_disconnect)(conn->ac_sock);
		__amp_free_conn(conn);
		cmp_ctxt->acc_listen_conn = NULL;

	}
	/*
         * stop net mornitor thread
         */
	err = __amp_stop_netmorn_thread(cmp_ctxt);
	if (err < 0) 
		AMP_ERROR("amp_sys_finalize: stop netmorn thread error, err:%d\n", err);
		
        

	/*
	 * get down the other connections
	 */
	amp_sem_down(&amp_reconn_finalize_sem);
	if (cmp_ctxt->acc_conns)  {
		for (i=0; i<AMP_MAX_COMP_TYPE; i++) {
			amp_lock(&(cmp_ctxt->acc_conns[i].acc_lock));
			for (j=0; j<cmp_ctxt->acc_conns[i].acc_alloced_num; j++) {
				head = &(cmp_ctxt->acc_conns[i].acc_remote_conns[j].queue);
				cnq = &(cmp_ctxt->acc_conns[i].acc_remote_conns[j]);
				while (!list_empty(head))  {
					conn = list_entry(head->next, amp_connection_t, ac_list);
					/*
					 * we must check where dose this conn hang on before we free it.
					 */
					if (!list_empty(&conn->ac_dataready_list)) {
						amp_lock(&amp_dataready_conn_list_lock);
						__amp_free_conn(conn);
						list_del_init(&conn->ac_dataready_list);
						amp_unlock(&amp_dataready_conn_list_lock);
					}

					if (!list_empty(&conn->ac_reconn_list))  {
						amp_lock(&amp_reconn_conn_list_lock);
						list_del_init(&conn->ac_reconn_list);
						amp_unlock(&amp_reconn_conn_list_lock);
					}
					list_del_init(&conn->ac_list);
					AMP_WARNING("amp_sys_finalize: disconnect conn:%p\n", conn);
					amp_lock(&conn->ac_lock);
					amp_sem_down(&conn->ac_sendsem);
					amp_sem_down(&conn->ac_recvsem);
					AMP_OP(conn->ac_type, proto_disconnect)(conn->ac_sock);
					conn->ac_sock = NULL;
					conn->ac_state = AMP_CONN_BAD;
					amp_unlock(&conn->ac_lock);
					amp_sem_up(&conn->ac_sendsem);
					amp_sem_up(&conn->ac_recvsem);
					AMP_LEAVE("amp_sys_finalize: free conn:%p, datard_count:%d\n", \
                                                   conn, conn->ac_datard_count);
			
					if (atomic_read(&conn->ac_refcont) > 1) {
						printk("amp_sys_finalize: ref of conn:%p is %d, too large\n", conn, atomic_read(&conn->ac_refcont));
						atomic_set(&conn->ac_refcont, 1);
					}	
					__amp_free_conn(conn);
					AMP_DMSG("amp_sys_finalize: conn:%p freed\n", conn);
				}
				arr_size = cnq->total_num * sizeof(amp_connection_t *);
				if (cnq->conns) 
					amp_free(cnq->conns, arr_size);
			}
			amp_unlock(&(cmp_ctxt->acc_conns[i].acc_lock));

			amp_free(cmp_ctxt->acc_conns[i].acc_remote_conns, \
			         sizeof(conn_queue_t) * cmp_ctxt->acc_conns[i].acc_alloced_num);
			
		}

		amp_free(cmp_ctxt->acc_conns, \
				   sizeof(amp_comp_conns_t) * AMP_MAX_COMP_TYPE);
		cmp_ctxt->acc_conns = NULL;

	}	
	amp_sem_up(&amp_reconn_finalize_sem);

	amp_free(cmp_ctxt, sizeof(amp_comp_context_t));

	cmp_ctxt = NULL;
	
EXIT:
	AMP_LEAVE("amp_sys_finalize: leave\n");
	return err;

}


int 
amp_config (amp_s32_t cmd, void *conf, amp_s32_t len)
{
	return 0;

}

EXPORT_SYMBOL(amp_create_connection);
EXPORT_SYMBOL(amp_send_sync);
EXPORT_SYMBOL(amp_send_async);
EXPORT_SYMBOL(amp_disconnect_peer);
EXPORT_SYMBOL(amp_sys_init);
EXPORT_SYMBOL(amp_sys_finalize);
EXPORT_SYMBOL(amp_config);


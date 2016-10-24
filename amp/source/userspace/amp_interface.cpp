/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
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
 * uplevel app should maintain a list which include conneciton,
 * and this list will check the conn per hour and reconn the disconnected conn
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
    struct sockaddr_in sin;
    amp_s32_t *fd = NULL;
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
            err = AMP_OP(conn_type, proto_connect)(NULL, (void **)&fd, (void *)&sin, direction);
            
            if (err < 0)  {
                __amp_free_conn(conn);
                goto EXIT;
            }

            err = AMP_OP(conn_type, proto_init)((void *)fd, direction);
            if (err < 0) {
                AMP_ERROR("amp_create_connection: proto init error\n");
                __amp_free_conn(conn);
                goto EXIT;
            }
            conn->ac_sock = *fd;
            if(fd)
                free(fd);
            conn->ac_type = conn_type;
            __amp_add_to_listen_fdset(conn);
            pthread_mutex_lock(&ctxt->acc_lock);
            amp_listen_sockfd = conn->ac_sock;
            pthread_mutex_unlock(&ctxt->acc_lock);

            conn->ac_this_port = port;
            ctxt->acc_listen_conn = conn;
            conn->ac_this_type = ctxt->acc_this_type;
            conn->ac_this_id = ctxt->acc_this_id;
            
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
            conn->ac_need_reconn = 1; //by weizheng 2013-11-19 reconn 
            conn->ac_remain_times = AMP_CONN_RECONN_MAXTIMES;
            conn->ac_this_type = ctxt->acc_this_type;
            conn->ac_this_id = ctxt->acc_this_id;
    
            err = __amp_connect_server (conn);
            if (err < 0) {
                __amp_free_conn(conn);
                goto EXIT;
            }
            conn->ac_state = AMP_CONN_OK;
            __amp_enqueue_conn(conn, ctxt);
            __amp_add_to_listen_fdset(conn);
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


int amp_send_sync (amp_comp_context_t *ctxt,
                   amp_request_t *req,
                   amp_u32_t  type,
                   amp_u32_t id,
                   amp_s32_t resent)
{
    amp_s32_t  err = 0;
    amp_message_t  *msgp = NULL;
    amp_u32_t   xid;
    amp_time_t  tm;
    struct timeval  tv;

    amp_u32_t   pid;
    amp_u32_t   req_type = 0;
    amp_u32_t   conn_type = 0;
    amp_u32_t   flags = 0;
    amp_u32_t   need_ack = 0;
    struct sockaddr_in  sout_addr;
    amp_u32_t   sendsize = 0;
    amp_connection_t *conn = NULL;
    
    AMP_ENTER("amp_send_sync: enter\n");
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
    
    pthread_mutex_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));

    if (list_empty(&(ctxt->acc_conns[type].acc_remote_conns[id].queue))) {
        AMP_ERROR("amp_send_sync: the remote_conns is empty\n");
        err = -ENOTCONN;
        pthread_mutex_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
        goto EXIT;
    }

    pthread_mutex_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
    
    amp_lock(&req->req_lock);
    //req->req_send_state = AMP_REQ_SEND_INIT;
    req->req_remote_type = type;
    req->req_remote_id = id;
    req->req_ctxt = ctxt;
    req->req_error = 0; 
    if (resent)
        req->req_resent = 1;
    else 
        req->req_resent = 0;
    amp_unlock(&req->req_lock);

    if (req->req_type & AMP_REQUEST) {
        AMP_DMSG("amp_send_sync: it's a request\n");
        if (!req->req_msg) {
            AMP_ERROR("amp_send_sync: no msg for a request\n");
            err = -EINVAL;
            goto EXIT;
        }
        msgp = req->req_msg;
        xid = ctxt->acc_this_id;
        amp_gettimeofday(&tv);
        tm.sec = tv.tv_sec;
        tm.usec = tv.tv_usec;
        pid = ctxt->acc_this_type;
        AMP_FILL_REQ_HEADER(msgp, \
                                    req->req_msglen - AMP_MESSAGE_HEADER_LEN, \
                                    req->req_type, \
                                    pid, \
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

    AMP_DMSG("amp_send_sync: req:%p, req->req_msg:%p, req->req_reply:%p\n", \
                 req, req->req_msg, req->req_reply);

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
            sendsize = sendsize + (4096 * req->req_niov); 
    } else {
        sendsize = req->req_replylen;
        if (req_type & AMP_DATA)
            sendsize = sendsize + (4096 * req->req_niov);
    }
SELECT_CONN:
    if(req->req_conn && req->req_conn->ac_state == AMP_CONN_OK && (req_type & AMP_REPLY)){
        conn = req->req_conn;
        amp_lock(&conn->ac_lock);
        conn->ac_weight += sendsize;
        conn->ac_last_reconn = time(NULL);
    }else{
        err = __amp_select_conn(req->req_remote_type, req->req_remote_id, sendsize, req->req_ctxt, &conn);
        if (err) {
            switch(err)  {
                case 2:
                    AMP_ERROR("amp_send_sync: no valid conn to peer (type:%d, id:%d, err:%d)\n", \
                                              req->req_remote_type, \
                                              req->req_remote_id, \
                                              err);
                
                    if (req->req_resent && (req->req_type & AMP_REQUEST)) {
                        sleep(2);
                        __amp_add_resend_req(req);
                        amp_sem_down(&req->req_waitsem);
                        err = req->req_error;
                        goto EXIT;
                    }
                case 1:
                    AMP_ERROR("amp_send_sync: no conn to peer(type:%d, id:%d, err:%d)\n", \
                                              req->req_remote_type, \
                                              req->req_remote_id, \
                                              err);
                default:
                    req->req_error = -ENOTCONN;
                    err = -ENOTCONN;
                    amp_sem_up(&req->req_waitsem);
                    goto EXIT;
            }
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
    
    //conn->ac_weight += sendsize;
    amp_unlock(&conn->ac_lock); 

    amp_sem_down(&conn->ac_sendsem);

    if (conn->ac_state != AMP_CONN_OK) {
        AMP_WARNING("amp_send_sync: before send, state of conn:%p is invalid:%d\n", conn, conn->ac_state);
        amp_lock(&conn->ac_lock);
        conn->ac_weight -= sendsize;
        amp_unlock(&conn->ac_lock);
        amp_sem_up(&conn->ac_sendsem);
        goto SELECT_CONN;
    }
    /*lock request*/
    amp_lock(&req->req_lock);
    //req->req_send_state = AMP_REQ_SEND_START;
    req->req_conn = conn;
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

    if (req_type & AMP_REQUEST) {

        AMP_DMSG("amp_send_sync: conn:%p, send request\n", conn);
        err = AMP_OP(conn_type, proto_sendmsg)(&conn->ac_sock, \
                             &sout_addr, \
                             sizeof(sout_addr), \
                             req->req_msglen, \
                             req->req_msg, \
                             flags);
    } else {
        AMP_DMSG("amp_send_sync: conn:%p, send reply\n", conn);
        if (conn->ac_type == AMP_CONN_TYPE_UDP)
            sout_addr = req->req_reply->amh_addr;

        err = AMP_OP(conn_type, proto_sendmsg)(&conn->ac_sock, \
                             &sout_addr, \
                             sizeof(sout_addr), \
                             req->req_replylen, \
                             req->req_reply, \
                             flags);
        if (err != 0)
            AMP_ERROR("amp_send_sync: sendmsg error, conn:%p, req:%p, seqno:%d,  err:%d\n", \
                                  conn, req, *((int *)(req->req_reply+AMP_MESSAGE_HEADER_LEN+8)), err);
   
    }
     
    if (err < 0) {
        AMP_ERROR("amp_send_sync: sendmsg error, conn:%p, req:%p, err:%d\n", \
                          conn, req, err);
        req->req_conn = NULL;
        amp_unlock(&req->req_lock);
        goto SEND_ERROR;
    }
    if (req_type & AMP_DATA)  {
        /*stage2: send data*/
        AMP_DMSG("amp_send_sync: conn:%p, send data\n", conn);
        req->req_stage = AMP_REQ_STAGE_DATA;
        err = AMP_OP(conn_type, proto_senddata)(&conn->ac_sock,
                             &sout_addr, \
                             sizeof(sout_addr), \
                             req->req_niov,
                             req->req_iov,
                             0);
        if (err < 0) {
            AMP_ERROR("amp_send_sync: senddata error, conn:%p, req:%p, err:%d\n",\
                                  conn, req, err);
            req->req_conn = NULL;
            amp_unlock(&req->req_lock);
            goto SEND_ERROR;
        }
    }
    //req->req_send_state = AMP_REQ_SEND_END;
    amp_unlock(&req->req_lock);

    if (need_ack) { /*waiting for ack*/
        AMP_LEAVE("amp_send_sync: req:%p, need ack\n", req);
        amp_lock(&amp_waiting_reply_list_lock);
        amp_lock(&req->req_lock);
        if(!__amp_within_waiting_reply_list(req))
            if (list_empty(&req->req_list)){
                list_add_tail(&req->req_list, &amp_waiting_reply_list);
            }
        amp_unlock(&req->req_lock);
        amp_unlock(&amp_waiting_reply_list_lock);
    }
    
    amp_lock(&conn->ac_lock);
    conn->ac_weight -= sendsize;
    amp_unlock(&conn->ac_lock);
    amp_sem_up(&conn->ac_sendsem);
    if (need_ack)
    {
        amp_sem_down(&req->req_waitsem);
        AMP_DMSG("amp_send_sync: after down waitsem\n");
        err = req->req_error;
    }else{
        amp_sem_up(&req->req_waitsem);  
        err = 0;
    }

EXIT:   
    AMP_LEAVE("amp_send_sync: leave, err:%d\n", err);
    return err;

SEND_ERROR:
    AMP_ERROR("amp_send_sync: send error through conn:%p, err:%d\n", 
                  conn, err);

    if (conn->ac_type == AMP_CONN_TYPE_TCP)  {
        amp_lock(&conn->ac_lock);
        conn->ac_datard_count = 0;
        conn->ac_sched = 0;     
        amp_unlock(&conn->ac_lock);
    }
    

    AMP_OP(conn_type, proto_disconnect)((void *)&conn->ac_sock);
    if (conn->ac_need_reconn) {
        /*
         * we must add the bad connection to the reconnect list.
         */
        if (conn->ac_state != AMP_CONN_RECOVER) {
            AMP_ERROR("amp_send_sync: set conn:%p to recover\n", conn);
            conn->ac_state = AMP_CONN_RECOVER;  
            amp_lock(&amp_reconn_conn_list_lock);
            amp_lock(&conn->ac_lock);
            if (list_empty(&conn->ac_reconn_list))
                list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
            amp_unlock(&conn->ac_lock);
            amp_unlock(&amp_reconn_conn_list_lock);
            amp_sem_up(&amp_reconn_sem);
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
            amp_lock(&conn->ac_lock);
            if (list_empty(&conn->ac_reconn_list))
                list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
            amp_unlock(&conn->ac_lock);
            amp_unlock(&amp_reconn_conn_list_lock);
            amp_sem_up(&amp_reconn_sem);
        } else 
            AMP_ERROR("amp_send_sync: someone else has set conn:%p to close\n", conn);
    }

    amp_lock(&conn->ac_lock);    
    conn->ac_weight -= sendsize;
    amp_unlock(&conn->ac_lock); 
    amp_sem_up(&conn->ac_sendsem);
    goto SELECT_CONN;
}


int amp_send_async (amp_comp_context_t *ctxt,
                    amp_request_t *req,
                    amp_u32_t  type,
                    amp_u32_t id,
                    amp_s32_t resent)
{
    amp_s32_t  err = 0;
    amp_message_t *msgp = NULL;
    amp_u32_t xid;
    amp_u32_t pid;
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

    pthread_mutex_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
    if (list_empty(&(ctxt->acc_conns[type].acc_remote_conns[id].queue))) {
        AMP_ERROR("amp_send_async: the remote_conns is empty\n");
        pthread_mutex_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
        err = -ENOTCONN;
        goto EXIT;
    }

    pthread_mutex_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));

    req->req_remote_type = type;
    req->req_remote_id = id;
    req->req_ctxt = ctxt;
    req->req_error = 0;
    if (resent)
        req->req_resent = 1;
    else 
        req->req_resent = 0;
    
    if (req->req_type & AMP_REQUEST) {
        msgp = req->req_msg;
        xid = ctxt->acc_this_id;
        amp_gettimeofday(&tv);
        tm.sec = tv.tv_sec;
        tm.usec = tv.tv_usec;
        pid = ctxt->acc_this_type;
        AMP_FILL_REQ_HEADER(msgp, \
                                    req->req_msglen - AMP_MESSAGE_HEADER_LEN, \
                                    req->req_type, \
                                    pid, \
                                    req, \
                                    xid, \
                                    tm);
    } else {
        AMP_DMSG("amp_send_async: it's a reply\n");
        if (!req->req_reply) {
            AMP_ERROR("amp_send_async: no req_reply for a reply\n");
            err = -EINVAL;
            goto EXIT;
        }
        req->req_reply->amh_size = req->req_replylen - AMP_MESSAGE_HEADER_LEN;
        msgp = req->req_reply;
        msgp->amh_type = req->req_type;
    }

    amp_lock(&amp_sending_list_lock);
    amp_lock(&req->req_lock);
    if(!__amp_within_sending_list(req))
        list_add_tail(&req->req_list, &amp_sending_list);
    amp_unlock(&req->req_lock);
    amp_unlock(&amp_sending_list_lock);
    amp_sem_up(&amp_process_out_sem);

        
EXIT:   
    AMP_LEAVE("amp_send_async: leave\n");
    return err;
}


// 2008-10-30
amp_thread_t *
__tmp (amp_comp_context_t *ctxt)
{
    amp_thread_t *threadp = NULL;

    threadp =(amp_thread_t *)amp_alloc(sizeof(amp_thread_t));
    return threadp;

}



// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------
#ifdef __AMP_LISTEN_POLL

int amp_poll_fd_zero( struct pollfd *poll_list, amp_u32_t poll_size )
{
    amp_s32_t  i;
    amp_s32_t  rc = 0;

    for( i=0; i<poll_size; ++i )
    {
        poll_list[i].fd = -1;
        poll_list[i].revents &= (~POLLIN);
        poll_list[i].revents &= (~POLLPRI);
    }
    rc = 1;
    return rc;
}

int amp_poll_fd_set( amp_s32_t fd, struct pollfd *poll_list )
{
    amp_s32_t  rc = 1;

    poll_list[fd].fd = fd;
    poll_list[fd].events = POLLIN|POLLPRI;

    return rc;
}

int amp_poll_fd_reset( amp_s32_t fd, struct pollfd *poll_list )
{
    amp_s32_t  rc = 0;
    poll_list[fd].revents &= (~POLLIN);
    poll_list[fd].revents &= (~POLLPRI);
    poll_list[fd].revents &= (~POLLOUT);
    poll_list[fd].revents &= (~POLLERR);
    poll_list[fd].revents &= (~POLLHUP);
    poll_list[fd].revents &= (~POLLNVAL);
    poll_list[fd].fd = fd;
    poll_list[fd].events = POLLIN|POLLPRI;

    rc = 1;
    return rc;
}

int amp_poll_fd_isset( amp_s32_t fd, struct pollfd *poll_list )
{
    amp_s32_t  rc = 0;

    if( poll_list[fd].fd < 0 )
    {
        rc = 0;
    }
    else if(((poll_list[fd].revents&POLLIN) == POLLIN) ||
                ((poll_list[fd].revents&POLLPRI) == POLLPRI) )
    {
        rc = 1;
    }
    return rc;
}

int amp_poll_fd_clr( amp_s32_t fd, struct pollfd *poll_list )
{
    amp_s32_t  rc = 0;

    poll_list[fd].fd = -1;
    poll_list[fd].revents &= (~POLLIN);
    poll_list[fd].revents &= (~POLLPRI);
    poll_list[fd].revents &= (~POLLOUT);
    poll_list[fd].revents &= (~POLLERR);
    poll_list[fd].revents &= (~POLLHUP);
    poll_list[fd].revents &= (~POLLNVAL);

    rc = 1;
    return rc;
}

#endif
#ifdef __AMP_LISTEN_EPOLL

int amp_epoll_fd_isset( amp_s32_t fd, struct epoll_event *ev, amp_s32_t nfds )
{
    amp_s32_t  i;
    amp_s32_t  rc = 0;

    for( i=0; i<nfds; ++i )
    {
        if( ev[i].data.fd == fd )
            return 1;
    }
    return rc;
}

int amp_epoll_fd_set( amp_s32_t fd, amp_s32_t epfd )
{
    amp_s32_t  rc = 1;
    struct epoll_event ev;

    ev.data.fd = fd;
    ev.events = EPOLLIN|EPOLLPRI;//|EPOLLET;
    
    rc = epoll_ctl( epfd, EPOLL_CTL_ADD, fd, &ev );
    if(rc == -1)
        rc = epoll_ctl( epfd, EPOLL_CTL_MOD, fd, &ev);
    return rc;
}

int amp_epoll_fd_reset( amp_s32_t fd, amp_s32_t epfd )
{
    amp_s32_t  rc = 1;
    struct epoll_event ev;

    ev.data.fd = fd;
    ev.events = EPOLLIN|EPOLLPRI;//|EPOLLET;
    rc = epoll_ctl( epfd, EPOLL_CTL_MOD, fd, &ev);
    return rc;
}

int amp_epoll_fd_clear( amp_s32_t fd, amp_s32_t epfd )
{
    amp_s32_t  rc = 1;
    struct epoll_event ev;

    ev.data.fd = fd;
    rc = epoll_ctl( epfd, EPOLL_CTL_DEL, fd, &ev);
    return rc;
}

#endif
// -----------------------------------------------------------------


amp_comp_context_t *
amp_sys_init (amp_u32_t  type, amp_u32_t id)
{
    amp_s32_t err = 0;
    amp_comp_context_t *ctxt = NULL;
    amp_u32_t size;
    amp_u32_t i, j;
    amp_s32_t pipefd[2];
    amp_thread_t *lst_threadp = NULL;
    amp_u32_t arr_size;

    AMP_ENTER("amp_sys_init: enter, type:%d, id:%d\n", type, id);
    __amp_blockallsigs ();
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
            pthread_mutex_init(&(ctxt->acc_conns[i].acc_remote_conns[j].queue_lock), NULL);
            ctxt->acc_conns[i].acc_remote_conns[j].conns = (amp_connection_t **)amp_alloc(arr_size);
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
    ctxt->acc_conn_table = (amp_connection_t **)amp_alloc(MAX_CONN_TABLE_LEN * sizeof(amp_connection_t *));
    if (!ctxt->acc_conn_table) {
        AMP_ERROR("amp_sys_init: alloc for connection table error, no mem\n");
        goto ALLOC_CONN_TABLE_ERROR;
    }
    memset(ctxt->acc_conn_table, 
               0, 
               MAX_CONN_TABLE_LEN * sizeof(amp_connection_t *));


// by Chen Zhuan at 2008-02-05
// -----------------------------------------------------------------
#ifdef __AMP_LISTEN_SELECT

    ctxt->acc_maxfd = 0;
    FD_ZERO(&ctxt->acc_readfds);

#endif
#ifdef __AMP_LISTEN_POLL

    ctxt->acc_maxfd = 0;
    ctxt->acc_poll_list = (struct pollfd *)amp_alloc( AMP_CONN_ADD_INCR * sizeof( struct pollfd ) );
    memset(ctxt->acc_poll_list,0,AMP_CONN_ADD_INCR * sizeof( struct pollfd ));
    amp_poll_fd_zero( ctxt->acc_poll_list, AMP_CONN_ADD_INCR );

#endif
#ifdef __AMP_LISTEN_EPOLL

    ctxt->acc_epfd = epoll_create( AMP_CONN_ADD_INCR );

#endif
// -----------------------------------------------------------------

    err = __amp_init_conn();
    if (err < 0) 
        goto EXIT;

    err = __amp_init_request();
    if (err < 0)  
        goto INIT_REQUEST_ERROR;

    amp_proto_interface_table_init ();
    err = pipe(pipefd);
    if (err < 0) {
        AMP_ERROR("amp_sys_init: create pipe error, err:%d\n", err);
        goto EXIT;
    }


// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------
#ifdef __AMP_LISTEN_SELECT

    FD_SET(pipefd[0], &ctxt->acc_readfds);
    if (ctxt->acc_maxfd < pipefd[0])
        ctxt->acc_maxfd = pipefd[0];

#endif
#ifdef __AMP_LISTEN_POLL

    amp_poll_fd_set( pipefd[0], ctxt->acc_poll_list );
    if (ctxt->acc_maxfd < pipefd[0])
        ctxt->acc_maxfd = pipefd[0];

#endif
#ifdef __AMP_LISTEN_EPOLL

    amp_epoll_fd_set( pipefd[0], ctxt->acc_epfd );

#endif
// -----------------------------------------------------------------

    ctxt->acc_notifyfd = pipefd[1];
    ctxt->acc_srvfd = pipefd[0];

    err = __amp_threads_init ();
    if (err < 0)
        goto INIT_THREAD_ERROR;
    
    lst_threadp = __amp_start_listen_thread(ctxt);
    if (!lst_threadp) {
        AMP_ERROR("amp_sys_init: start listen thread error\n");
        err = -1;
        goto INIT_REQUEST_ERROR;
    }
    ctxt->acc_listen_thread = lst_threadp;

    /*
     * modified by weizheng 2014-1-2, start netmorn thread, SOCKRAW is used by icmp ptotocol, not root user cannot use this function
     */
#ifdef __AMP_ICMP_NETMORN
    ctxt->acc_netmorn_thread = __amp_start_netmorn_thread(ctxt);
    if(!ctxt->acc_netmorn_thread)
    {
        AMP_ERROR("amp_sys_init: start net mornitor thread error\n");
        goto INIT_REQUEST_ERROR;
    }
#endif
 
EXIT:
    AMP_LEAVE("amp_sys_init: leave\n");
    return ctxt;

INIT_REQUEST_ERROR:
INIT_THREAD_ERROR:
ALLOC_CONN_TABLE_ERROR:
ALLOC_REMOTE_CONNS_ERROR:
    if (ctxt->acc_conns)  {
        for (i=0; i<AMP_MAX_COMP_TYPE; i++)  {
            if (ctxt->acc_conns[i].acc_remote_conns) {
                for (j=0; j<AMP_CONN_ADD_INCR; j++) {
                    if (ctxt->acc_conns[i].acc_remote_conns[j].conns)
                        amp_free(ctxt->acc_conns[i].acc_remote_conns[j].conns,arr_size);
                }
                amp_free(ctxt->acc_conns[i].acc_remote_conns, AMP_CONN_ADD_INCR * sizeof(conn_queue_t));
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
    amp_u32_t i;

    amp_s32_t   sockfd = -1;

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

    pthread_mutex_lock(&(cmp_conns->acc_remote_conns[remote_id].queue_lock));
    cnq = &(cmp_conns->acc_remote_conns[remote_id]);
    head = &(cmp_conns->acc_remote_conns[remote_id].queue); 
    
    if (list_empty(head)) {
        AMP_ERROR("amp_disconnect_peer: no connection to type:%d, id:%d\n",\
               remote_type, remote_id);
        err = 1;
        pthread_mutex_unlock(&(cmp_conns->acc_remote_conns[remote_id].queue_lock));
        goto EXIT;
    }

    if (cnq->active_conn_num < 0) {
        AMP_ERROR("amp_disconnect_peer: type:%d, id:%d, active_conn_num:%d ,wrong\n", \
               remote_type, remote_id, cnq->active_conn_num);
        err = 1;
        pthread_mutex_unlock(&(cmp_conns->acc_remote_conns[remote_id].queue_lock));
        goto EXIT;
    }

    for (i=0; i<=cnq->active_conn_num; i++) {
        conn = cnq->conns[i];
        if (!conn)
            continue;
        cnq->conns[i] = NULL;

        AMP_DMSG("amp_disconnect_peer: to disconnect conn:%p\n", conn);
        sockfd = conn->ac_sock;
        amp_lock(&conn->ac_lock);
        if(!list_empty(&conn->ac_list))
            list_del_init(&conn->ac_list);
        amp_unlock(&conn->ac_lock);

        amp_lock(&amp_reconn_conn_list_lock);
        amp_lock(&conn->ac_lock);
        if (!list_empty(&conn->ac_reconn_list) &&
                    conn->ac_state != AMP_CONN_CLOSE) {
            AMP_DMSG("amp_disconnect_peer: conn is on reconn list\n");
            list_del_init(&conn->ac_reconn_list);
        }
        conn->ac_state = AMP_CONN_BAD;
        amp_unlock(&conn->ac_lock);
        amp_unlock(&amp_reconn_conn_list_lock);

        amp_sem_down(&conn->ac_recvsem);
        amp_sem_down(&conn->ac_sendsem);

        if (conn->ac_sock >= 0)
            AMP_OP(conn->ac_type, proto_disconnect)(&conn->ac_sock);
        else
            AMP_ERROR("amp_disconnect_peer: close conn:%p, no sock\n", conn);
        
        amp_lock(&conn->ac_lock);
        conn->ac_sock = -1;
        conn->ac_weight = 0;
        amp_unlock(&conn->ac_lock);

        amp_sem_up(&conn->ac_recvsem);
        amp_sem_up(&conn->ac_sendsem);
        __amp_free_conn(conn);

        ctxt->acc_conn_table[sockfd] = NULL;

        if (!forall)
            break;
    }
    pthread_mutex_unlock(&(cmp_conns->acc_remote_conns[remote_id].queue_lock));

    if (forall) {
        AMP_DMSG("amp_disconnect_peer: to wake all resend reqs\n");
        __amp_remove_resend_reqs(conn, 1, 1);
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
    
    AMP_ENTER("amp_sys_finalize: enter\n");

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
        conn = cmp_ctxt->acc_listen_conn;
        AMP_OP(conn->ac_type, proto_disconnect)(&conn->ac_sock);
        __amp_free_conn(conn);
        cmp_ctxt->acc_listen_conn = NULL;
    }

#ifdef __AMP_ICMP_NETMORN
    err = __amp_stop_netmorn_thread(cmp_ctxt);
    if (err < 0)
        AMP_ERROR("amp_sys_finalize: stop netmorn thread error, err:%d\n", err);
#endif

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
                    amp_lock(&amp_dataready_conn_list_lock);
                    amp_lock(&conn->ac_lock);
                    if (!list_empty(&conn->ac_dataready_list)) {
                        list_del_init(&conn->ac_dataready_list);
                    }
                    amp_unlock(&conn->ac_lock);
                    amp_unlock(&amp_dataready_conn_list_lock);

                    amp_lock(&amp_reconn_conn_list_lock);
                    amp_lock(&conn->ac_lock);
                    if (!list_empty(&conn->ac_reconn_list))  {
                        list_del_init(&conn->ac_reconn_list);
                    }
                    amp_unlock(&conn->ac_lock);
                    amp_unlock(&amp_reconn_conn_list_lock);

                    amp_lock(&conn->ac_lock);
                    if(!list_empty(&conn->ac_list))
                        list_del_init(&conn->ac_list);
                    amp_unlock(&conn->ac_lock);

                    AMP_DMSG("amp_sys_finalize: disconnect conn:%p\n", conn);
                    AMP_OP(conn->ac_type, proto_disconnect)(&conn->ac_sock);
                    AMP_DMSG("amp_sys_finalize: free conn:%p, datard_count:%d\n", \
                                                 conn, conn->ac_datard_count);
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
        amp_sem_up(&amp_reconn_finalize_sem);

        amp_free(cmp_ctxt->acc_conns, sizeof(amp_comp_conns_t) * AMP_MAX_COMP_TYPE);
        cmp_ctxt->acc_conns = NULL;
    }   
    if (cmp_ctxt->acc_conn_table)
        amp_free(cmp_ctxt->acc_conn_table, MAX_CONN_TABLE_LEN * sizeof(amp_connection_t *));
    __amp_threads_finalize ();
    __amp_finalize_conn ();
    __amp_finalize_request ();
// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------

    #ifdef __amp_listen_poll
    if(cmp_ctxt->acc_poll_list){
        free(cmp_ctxt->acc_poll_list);
    }
    #endif
    #ifdef __AMP_LISTEN_EPOLL
    close( cmp_ctxt->acc_epfd );
    #endif
// -----------------------------------------------------------------
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

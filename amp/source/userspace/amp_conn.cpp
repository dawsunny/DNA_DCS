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
amp_u32_t         amp_reconn_thread_started = 0;
amp_u32_t         amp_wakeup_thread_started = 0;
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

    amp_resend_hash_table = 
        (amp_htb_entry_t *)amp_alloc(sizeof(*amp_resend_hash_table) * AMP_RESEND_HTB_SIZE);
    if (!amp_resend_hash_table) {
        AMP_ERROR("__amp_init_conn: alloc memory for amp_resend_hash_table error\n");
        return -ENOMEM;
    }

    for(bucket = amp_resend_hash_table + AMP_RESEND_HTB_SIZE - 1; 
            bucket >= amp_resend_hash_table; bucket --) {

        INIT_LIST_HEAD(&bucket->queue);
        amp_lock_init(&bucket->lock);
    }

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
            amp_lock(&req->req_lock);
            req->req_error = -EINTR;
            list_del_init(&req->req_list);
            amp_unlock(&req->req_lock);
            amp_sem_up(&req->req_waitsem);//modified by weizheng 2013-11-18
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
        amp_lock(&req->req_lock);
        list_del_init(&req->req_list);
        req->req_error = -EINTR;
        amp_unlock(&req->req_lock);
        amp_sem_up(&req->req_waitsem);
    }
    amp_unlock(&amp_sending_list_lock);


    /*
     * free all waiting for reply request
     */
    AMP_DMSG("__amp_finalize_conn: check waiting reply list\n");

    amp_lock(&amp_waiting_reply_list_lock);
    while  (!list_empty(&amp_waiting_reply_list)) {
        req = list_entry(amp_waiting_reply_list.next, amp_request_t, req_list);
        amp_lock(&req->req_lock);
        list_del_init(&req->req_list);
        req->req_error = -EINTR;
        amp_unlock(&req->req_lock);
        amp_sem_up(&req->req_waitsem);
    }
    amp_unlock(&amp_waiting_reply_list_lock);

        
    
    /*
     * interruptible all resend request
     */
    AMP_LEAVE("__amp_finalize_conn: leave\n");
    return 0;
}

int 
__amp_alloc_conn (amp_connection_t **retconn)
{
    amp_s32_t err = 0;
    amp_connection_t *conn;

    AMP_ENTER("__amp_alloc_conn: enter\n");

    conn = (amp_connection_t *)malloc(sizeof(amp_connection_t));

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

        
    conn->ac_state = AMP_CONN_NOTINIT;
    conn->ac_sock = -1;
    conn->ac_refcont = 1;
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
    AMP_ENTER("__amp_free_conn: enter,conn:%p, refcont:%d\n", conn, conn->ac_refcont);
    if (!conn) {
        AMP_ERROR("__amp_free_conn: no connecton\n");
        goto EXIT;
    }
    
    amp_lock(&conn->ac_lock);
    if (!conn->ac_refcont) {
        AMP_ERROR("__amp_free_conn: conn:%p, its refcont is zero now\n", conn);
    }
    conn->ac_refcont --;
    if (conn->ac_refcont) {
        amp_unlock(&conn->ac_lock);
        goto EXIT;
    }
    if (!list_empty(&conn->ac_list)) {
        AMP_WARNING("__amp_free_conn: to free chained connection\n");
        amp_unlock(&conn->ac_lock);
        goto EXIT;
    }
    
    INIT_LIST_HEAD(&conn->ac_list);
    INIT_LIST_HEAD(&conn->ac_reconn_list);
    INIT_LIST_HEAD(&conn->ac_dataready_list);       
    amp_unlock(&conn->ac_lock);
    memset(conn, 0, sizeof(amp_connection_t));

    AMP_LEAVE("__amp_free_conn: fully free conn:%p\n", conn);
    free(conn);

EXIT:
    AMP_LEAVE("__amp_free_conn: leave\n");
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

    
    AMP_ENTER("__amp_queue_conn: enter, conn:%p\n", conn);

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
    pthread_mutex_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
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

    pthread_mutex_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
    
EXIT:
    AMP_LEAVE("__amp_queue_conn: leave\n");
    return err;
}

/*
 * remove the invalid connection  */
int
__amp_dequeue_invalid_conn(amp_connection_t * conn, amp_comp_context_t *ctxt)
{
    conn_queue_t * cnq = NULL;
    amp_connection_t * tmp_conn = NULL;
    struct list_head * pos = NULL;
    struct list_head * nxt = NULL;
    amp_request_t * req = NULL;
    amp_u32_t type = conn->ac_remote_comptype;
    amp_u32_t id = conn->ac_remote_id;
    amp_u32_t ip = conn->ac_remote_ipaddr;
    amp_s32_t sock = conn->ac_sock;
    amp_s32_t i;
    amp_s32_t err = 0;
    amp_s32_t *sock_fd = NULL; 
    amp_s32_t  sock_num = 0;
    if(!ctxt){
        AMP_ERROR("__amp_dequeue_invalid_conn: no context\n");
        err = -EINVAL;
        goto EXIT;
    }

    if(!conn){
        AMP_ERROR("__amp_dequeue_invalid_conn: no conn\n");
        err = -EINVAL;
        goto EXIT;
    }

    if(!ctxt->acc_conns) {
        AMP_ERROR("__amp_dequeue_invalid_conn: no acc_conns in ctxt\n");
        err = -EINVAL;
        goto EXIT;
    }


    if (type > AMP_MAX_COMP_TYPE) {
        AMP_ERROR("__amp_dequeue_invalid_conn: type(%d) is too large\n", type);
        err = -EINVAL;
        goto EXIT;
    }

    if (!ctxt->acc_conns[type].acc_remote_conns) {
        AMP_ERROR("__amp_dequeue_invalid_conn: no remote conns in comp_conns of type:%d\n", type);
        err = -EINVAL;
        goto EXIT;
    }

    pthread_mutex_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
    cnq = &(ctxt->acc_conns[type].acc_remote_conns[id]);
    sock_fd = (amp_s32_t *)malloc((cnq->active_conn_num + 1) * sizeof(amp_s32_t));
    if(NULL == sock_fd){
        AMP_ERROR("amp_dequeue_invalid_conn: no mem for dequeued invalid sock\n");
        err = -ENOMEM;
        goto EXIT;
    }
    memset(sock_fd,0,sizeof(amp_s32_t)*cnq->active_conn_num);
    for(i=0; i<=cnq->active_conn_num;) {
        tmp_conn = cnq->conns[i];
        if((tmp_conn != ctxt->acc_listen_conn) &&
           (tmp_conn->ac_remote_comptype == type) &&
           (tmp_conn->ac_remote_id == id) &&
           (tmp_conn->ac_remote_ipaddr == ip)){
            AMP_ERROR("__amp_dequeue_invalid_conn: dequeue invalid conn:%p, remote_type:%d, remote_id:%d, remote_ip:%u, sock:%d\n",tmp_conn, type, id, ip, tmp_conn->ac_sock);
            sock_fd[sock_num++] = tmp_conn->ac_sock;
            amp_lock(&amp_dataready_conn_list_lock);
            amp_lock(&tmp_conn->ac_lock);
            if (!list_empty(&tmp_conn->ac_dataready_list)) {
                list_del_init(&tmp_conn->ac_dataready_list);
            }
            amp_unlock(&tmp_conn->ac_lock);
            amp_unlock(&amp_dataready_conn_list_lock);
    
            amp_lock(&tmp_conn->ac_lock);
            if(!list_empty(&tmp_conn->ac_list))
                list_del_init(&tmp_conn->ac_list);
            amp_unlock(&tmp_conn->ac_lock);
            
            amp_lock(&amp_reconn_conn_list_lock);
            amp_lock(&tmp_conn->ac_lock);
            if(!list_empty(&tmp_conn->ac_reconn_list)){
                list_del_init(&tmp_conn->ac_reconn_list);
            }
            amp_unlock(&tmp_conn->ac_lock);
            amp_unlock(&amp_reconn_conn_list_lock);
            if(tmp_conn->ac_sock != sock){
                shutdown(tmp_conn->ac_sock,SHUT_RDWR); 
                close(tmp_conn->ac_sock);
            }
            cnq->conns[i] = NULL;
            if(i >= cnq->active_conn_num){
                cnq->active_conn_num --;
                i++;
            }else {
                cnq->conns[i] = cnq->conns[cnq->active_conn_num];
                cnq->conns[cnq->active_conn_num]  = NULL;
                cnq->active_conn_num --;
            }
            ctxt->acc_conn_table[tmp_conn->ac_sock] = NULL;
        }else
            i++;
    }   
    pthread_mutex_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));

    amp_lock(&amp_waiting_reply_list_lock);
    list_for_each_safe(pos, nxt, &amp_waiting_reply_list)
    {
        req = list_entry(pos, amp_request_t, req_list);
        if(type == req->req_remote_type && id == req->req_remote_id)
        {
            amp_lock(&req->req_lock);
            list_del_init(&req->req_list);
            amp_unlock(&req->req_lock);
            if((req->req_type & AMP_REQUEST) && req->req_resent)
                __amp_add_resend_req(req); 
        }
    }
    amp_unlock(&amp_waiting_reply_list_lock);
    pthread_mutex_lock(&ctxt->acc_lock);
    for(i=0;i<sock_num;i++){
#ifdef __AMP_LISTEN_SELECT
        FD_CLR(sock_fd[i],&ctxt->acc_readfds);
#endif
#ifdef __AMP_LISTEN_POLL
        amp_poll_fd_clr(sock_fd[i], ctxt->acc_poll_list);
#endif
#ifdef __AMP_LISTEN_EPOLL
        amp_epoll_fd_clear(sock_fd[i], ctxt->acc_epfd);
#endif
    } 
    pthread_mutex_unlock(&ctxt->acc_lock);
    if(NULL != sock_fd)
        free(sock_fd);

EXIT:
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
/*
 * modified by weizheng 2013-12-12 when this function is called, the conn's sock is
 * -1, at this monment, if we use the variable, we will cause free segment  
*/
    amp_s32_t  sockfd = conn->ac_sock;

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
    pthread_mutex_lock(&ctxt->acc_lock);
#ifdef __AMP_LISTEN_SELECT
    FD_CLR(conn->ac_sock,&ctxt->acc_readfds);
#endif
#ifdef __AMP_LISTEN_POLL
    amp_poll_fd_clr(conn->ac_sock, ctxt->acc_poll_list);
#endif
#ifdef __AMP_LISTEN_EPOLL
    amp_epoll_fd_clear(conn->ac_sock, ctxt->acc_epfd);
#endif
    pthread_mutex_unlock(&ctxt->acc_lock);


    pthread_mutex_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));

    cnq = &(ctxt->acc_conns[type].acc_remote_conns[id]);

    amp_lock(&amp_dataready_conn_list_lock);
    amp_lock(&conn->ac_lock);
    if (!list_empty(&conn->ac_dataready_list)) {
        list_del_init(&conn->ac_dataready_list);
    }
    amp_unlock(&conn->ac_lock);
    amp_unlock(&amp_dataready_conn_list_lock);

    amp_lock(&conn->ac_lock);
    if(!list_empty(&conn->ac_list))
        list_del_init(&conn->ac_list);
    amp_unlock(&conn->ac_lock);
    
    amp_lock(&amp_reconn_conn_list_lock);
    amp_lock(&conn->ac_lock);
    if(!list_empty(&conn->ac_reconn_list)){
        list_del_init(&conn->ac_reconn_list);
    }
    amp_unlock(&conn->ac_lock);
    amp_unlock(&amp_reconn_conn_list_lock);

    for (i=0; i<=cnq->active_conn_num; i++) {
        if(cnq->conns[i] == conn)
            break;
    }
    AMP_DMSG("__amp_dequeue_conn: after search, i:%d\n", i);
    if (i > cnq->active_conn_num) {
        AMP_ERROR("__amp_dequeue_conn: not find the conn:%p in select queue\n", conn);
        pthread_mutex_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
        goto EXIT;
    }
    cnq->conns[i] = NULL;

    if (i >= cnq->active_conn_num)
        cnq->active_conn_num --;
    else {
        cnq->conns[i] = cnq->conns[cnq->active_conn_num];
        cnq->conns[cnq->active_conn_num]  = NULL;
        cnq->active_conn_num --;
    }

    AMP_DMSG("__amp_dequeue_conn: finished, active_conn_num:%d, total_num:%d\n", \
                 cnq->active_conn_num, cnq->total_num);
    
    ctxt->acc_conn_table[sockfd] = NULL;

    while((cnq->active_conn_num >= 0) && (!cnq->conns[cnq->active_conn_num]))
        cnq->active_conn_num --;
    
    AMP_DMSG("__amp_dequeue_conn: active_conn_num:%d, total_num:%d\n", \
                 cnq->active_conn_num, cnq->total_num);
    if(cnq->active_conn_num == -1){
        __amp_remove_resend_reqs(conn, 1, 1);
        __amp_remove_waiting_reply_reqs(conn, 1, 1);
    }else{
        __amp_remove_resend_reqs(conn, 1, 0);
        __amp_remove_waiting_reply_reqs(conn, 1, 0);
    }
 
    pthread_mutex_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
    
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
                  amp_u32_t sendsize,
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
    pthread_mutex_lock(&(cmp_conns->acc_remote_conns[id].queue_lock));

    cnq = &(cmp_conns->acc_remote_conns[id]);
    head = &(cmp_conns->acc_remote_conns[id].queue);    
    
    if (list_empty(head)) {
        AMP_ERROR("__amp_select_conn: no connection corresponding to type:%d, id:%d\n",
                          type, id);
        err = 1;
        pthread_mutex_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));
        goto EXIT;
    }
    if (cnq->active_conn_num < 0) {
        AMP_ERROR("__amp_select_conn: type:%d, id:%d, active_conn_num:%d ,wrong\n", \
                          type, id, cnq->active_conn_num);
        err = 1;
        pthread_mutex_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));
        goto EXIT;
    }

    
    err = 2;
    *retconn = NULL;
    gettimeofday(&crtv, NULL);
    beginidx = crtv.tv_sec + crtv.tv_usec + (id ^ (~type));
    beginidx = beginidx % (cnq->active_conn_num + 1);
    i = beginidx;
    AMP_DMSG("__amp_select_conn: beginidx:%d\n", beginidx);

    do {
        conn = cnq->conns[i];
        if (conn && conn->ac_state == AMP_CONN_OK) {
            err = 0;
            if (conn->ac_weight < cur_weight) {
                *retconn = conn;
                cur_weight = conn->ac_weight;
            }
        }
        i = (i + 1) % (cnq->active_conn_num + 1);

    } while (i != beginidx);
    if (*retconn) {
        amp_lock(&((*retconn)->ac_lock));
        (*retconn)->ac_weight += sendsize;
        (*retconn)->ac_last_reconn = time(NULL);
        AMP_DMSG("__amp_select_conn: get connection:%p\n", *retconn);
    }
    pthread_mutex_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));

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
        if (req->req_state == AMP_REQ_STATE_RESENT)  {
            AMP_ERROR("__amp_revoke_resend_reqs: find one req:%p\n", req);
            __amp_remove_resend_req(req);
            amp_lock(&amp_sending_list_lock);
            amp_lock(&req->req_lock);
            if(!__amp_within_sending_list(req)){
                list_add_tail(&req->req_list, &amp_sending_list);
            }
            req->req_state = AMP_REQ_STATE_NORMAL;
            amp_unlock(&req->req_lock);
            amp_unlock(&amp_sending_list_lock);
            amp_sem_up(&amp_process_out_sem);
        }
    }

    AMP_LEAVE("__amp_revoke_resend_reqs: leave\n");
    return;
}


/*
 * do a connection
 */
int
__amp_do_connection(amp_s32_t **retsock, 
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
        AMP_DMSG("__amp_do_connection: retsock: %d\n", **retsock);

EXIT:
    AMP_LEAVE("__amp_do_connection: leave\n");
    return err;
}

/*
 * connect to a server.
 */ 
int 
__amp_connect_server (amp_connection_t *conn)
{
    amp_s32_t    err = 0;
    amp_s32_t  *sock = NULL;
    struct sockaddr_in sin;
    amp_message_t   hello_msg;


    AMP_ENTER("__amp_connect_server: enter, conn:%p\n", conn);

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
    hello_msg.amh_pid = conn->ac_this_type;
    hello_msg.amh_xid = conn->ac_this_id;
    err = AMP_OP(conn->ac_type, proto_sendmsg)((void *)sock, &sin, sizeof(sin), sizeof(hello_msg), &hello_msg, 0);
    if (err < 0) {
        AMP_ERROR("__amp_connect_server: write header error, err:%d\n", err);
        AMP_OP(conn->ac_type, proto_disconnect)((void *)sock);
        amp_free(sock, sizeof(amp_s32_t));
        goto EXIT;
    }
    /*
     * receive ack from server
     */
    err = AMP_OP(conn->ac_type, proto_recvmsg)((void*)sock, &sin, sizeof(sin),  sizeof(hello_msg), &hello_msg, 0);      
    if (err < 0) {
        AMP_ERROR("amp_connect_server: read from server error, err:%d\n", err);
        AMP_OP(conn->ac_type, proto_disconnect)((void *)sock);
        amp_free(sock, sizeof(amp_s32_t));
        goto EXIT;
    }
    if (hello_msg.amh_type != AMP_HELLO_ACK) {
        err = -EINVAL;
        AMP_ERROR("__amp_connect_server: get a wrong hello ack, type:%d\n", hello_msg.amh_type);
        AMP_OP(conn->ac_type, proto_disconnect)((void *)sock);
        amp_free(sock, sizeof(amp_s32_t));
        goto EXIT;
    }

    err = AMP_OP(conn->ac_type, proto_init)((void *)sock, AMP_CONN_DIRECTION_CONNECT);
    if (err < 0) {
        AMP_ERROR("amp_connect_server: int socket error, err:%d\n", err);
        AMP_OP(conn->ac_type, proto_disconnect)((void *)sock);
        amp_free(sock, sizeof(amp_s32_t));
        goto EXIT;
    }
    conn->ac_remote_addr = sin;
    conn->ac_sock = *sock;
    amp_free(sock, sizeof(amp_s32_t));

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
__amp_accept_connection (amp_s32_t *sockparent, amp_connection_t *childconn)
{
    amp_s32_t err = 0;
    amp_message_t  msghd;
    amp_u32_t conn_type;
    amp_s32_t *childsock = NULL;
    struct sockaddr_in sin;
    amp_u32_t  slen;

    AMP_ENTER("__amp_accept_connection: enter, childconn:%p\n", childconn);

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
    err = getpeername(*childsock, (struct sockaddr *) &sin, &slen);
    if (err < 0) {
        AMP_ERROR("__amp_accept_connection: get name error, err:%d\n", err);
        AMP_OP(conn_type, proto_disconnect)((void*)childsock);
        amp_free(childsock, sizeof(amp_s32_t));
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
        amp_free(childsock, sizeof(amp_s32_t));
        goto EXIT;
    }

    if (msghd.amh_magic != AMP_REQ_MAGIC) {
        err = -1;
        AMP_ERROR("__amp_accept_connection: receive msg head error, err:%d\n", err);
        AMP_OP(conn_type, proto_disconnect)((void *)childsock);
        amp_free(childsock, sizeof(amp_s32_t));
        goto EXIT;
    }

    if (msghd.amh_type != AMP_HELLO) {
        err = -1;
        AMP_ERROR("__amp_accept_connection: receive msg head error, err:%d\n", err);
        AMP_OP(conn_type, proto_disconnect)((void *)childsock);
        amp_free(childsock, sizeof(amp_s32_t));
        goto EXIT;  

    }

    /*
     * initialize the connection.
     */
    AMP_DMSG("__amp_accept_connection: remote_id:%d, remote_type:%d\n", msghd.amh_xid, msghd.amh_pid);

    childconn->ac_remote_id = msghd.amh_xid;
    childconn->ac_remote_comptype = msghd.amh_pid;
    childconn->ac_remote_ipaddr = ntohl(sin.sin_addr.s_addr);
    childconn->ac_remote_port = ntohs(sin.sin_port);
    childconn->ac_remote_addr = sin;
    childconn->ac_need_reconn = 0;// by weizheng 2013-12-20 the server's reconn falsg, as the recomand to define, --1:reconn, 0 :no reconn
    childconn->ac_state = AMP_CONN_OK;
    childconn->ac_sock = *childsock;

    /*
     * init the socket.
     */
    err = AMP_OP(conn_type, proto_init)((void* )childsock, AMP_CONN_DIRECTION_ACCEPT);
    if (err < 0) {
        AMP_ERROR("__amp_accept_connection: init socket error, err:%d\n", err);
        AMP_OP(conn_type, proto_disconnect)((void*)childsock);
        amp_free(childsock, sizeof(amp_s32_t));
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
        AMP_OP(conn_type, proto_disconnect)((void*)childsock);
        amp_free(childsock, sizeof(amp_s32_t));
        goto EXIT;
    }
    
    amp_free(childsock, sizeof(amp_s32_t));
    err = 0;      

EXIT:
    AMP_LEAVE("__amp_accept_connection: leave\n");
    return err;
}


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

    hashvalue = __amp_hash(req->req_remote_type, req->req_remote_id);
    htbentry = amp_resend_hash_table + hashvalue;
    amp_lock(&htbentry->lock);
    amp_lock(&req->req_lock);
    if(!__amp_within_resend_req(req) && list_empty(&req->req_list))
    {
        list_add_tail(&req->req_list, &htbentry->queue);
    }
    req->req_state = AMP_REQ_STATE_RESENT;
    amp_unlock(&req->req_lock);
    amp_unlock(&htbentry->lock);

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
    amp_lock(&req->req_lock);
    if(__amp_within_resend_req(req))
        list_del_init(&req->req_list);
    amp_unlock(&req->req_lock);
    amp_unlock(&htbentry->lock);
    
EXIT:
    AMP_LEAVE("__amp_remove_resend_req: leave\n");
    return;
}

void
__amp_remove_resend_reqs(amp_connection_t *conn, amp_u32_t force, amp_u32_t no_conns)
{
    amp_htb_entry_t *htbentry = NULL;
    amp_request_t *req = NULL;
    struct list_head * pos = NULL;
    struct list_head * nxt = NULL;
    amp_u32_t remote_type = conn->ac_remote_comptype;
    amp_u32_t remote_id = conn->ac_remote_id;

    htbentry = amp_resend_hash_table + __amp_hash(remote_type, remote_id);
    amp_lock(&htbentry->lock);
    list_for_each_safe(pos, nxt, &htbentry->queue)
    {
        req = list_entry(pos, amp_request_t, req_list);
        if((!req->req_resent||force) && 
           (req->req_conn == NULL||req->req_conn == conn))
        {
            amp_lock(&req->req_lock);
            list_del_init(&req->req_list);
            if(no_conns)
                req->req_error = -ENOTCONN;
            else
                req->req_error = -ENETUNREACH;
            req->req_conn = NULL;
            amp_unlock(&req->req_lock);
            amp_sem_up(&req->req_waitsem);
        }
    }
    amp_unlock(&htbentry->lock);
    return;
}

void
__amp_remove_waiting_reply_reqs(amp_connection_t *conn, amp_u32_t force, amp_u32_t no_conns)
{
    amp_request_t *req = NULL;
    struct list_head * pos = NULL;
    struct list_head * nxt = NULL;

    amp_lock(&amp_waiting_reply_list_lock);
    if(list_empty(&amp_waiting_reply_list)){
        amp_unlock(&amp_waiting_reply_list_lock);
        return;
    }
    list_for_each_safe(pos, nxt, &amp_waiting_reply_list)
    {
        req = list_entry(pos, amp_request_t, req_list);
        if((!req->req_resent||force) && 
            (req->req_conn == NULL||req->req_conn == conn))
        {
            amp_lock(&req->req_lock);
            list_del_init(&req->req_list);
            if(no_conns)
                req->req_error = -ENOTCONN;
            else
                req->req_error = -ENETUNREACH;
            req->req_conn = NULL;
            amp_unlock(&req->req_lock);
            amp_sem_up(&req->req_waitsem);
        }
    }
    amp_unlock(&amp_waiting_reply_list_lock);
    return;
}

int 
__amp_within_waiting_reply_list(amp_request_t * req)
{
    struct list_head *pos = NULL;
    struct list_head *nxt = NULL;
    amp_request_t * tmpreq = NULL;
    amp_u32_t type = req->req_remote_type;
    amp_u32_t id = req->req_remote_id;

    list_for_each_safe(pos, nxt, &amp_waiting_reply_list) {
        tmpreq = list_entry(pos, amp_request_t, req_list);
        if(tmpreq->req_remote_id != id || tmpreq->req_remote_type != type)
            continue;
        if (tmpreq != req)
            continue;
        return 1;
    }
    return 0;
}

int 
__amp_within_resend_req( amp_request_t * req)
{
    amp_htb_entry_t *htbentry = NULL;
    struct list_head *pos = NULL;
    struct list_head *nxt = NULL;
    amp_request_t * tmpreq = NULL;
    amp_u32_t type = req->req_remote_type;
    amp_u32_t id = req->req_remote_id;

    htbentry = amp_resend_hash_table + __amp_hash(type, id);

    list_for_each_safe(pos, nxt, &htbentry->queue) {
        tmpreq = list_entry(pos, amp_request_t, req_list);
        if(tmpreq->req_remote_id != id || tmpreq->req_remote_type != type)
            continue;
        if (tmpreq != req)
            continue;
        return 1;
    }
    return 0;
}

int 
__amp_within_sending_list(amp_request_t * req)
{
    struct list_head *pos = NULL;
    struct list_head *nxt = NULL;
    amp_request_t * tmpreq = NULL;

    list_for_each_safe(pos, nxt, &amp_sending_list) {
        tmpreq = list_entry(pos, amp_request_t, req_list);
        if (tmpreq != req)
            continue;
        return 1;
    }

    return 0;
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

int
__amp_add_to_listen_fdset(amp_connection_t *conn)
{
    amp_comp_context_t *ctxt = NULL;
    amp_s32_t  err = 0;
    amp_s32_t  sockfd;
    amp_s32_t  notifyid = 1;

    AMP_DMSG("__amp_add_to_listen_fdset: enter, conn:%p, fd:%d\n",
                 conn, conn->ac_sock);
    ctxt = conn->ac_ctxt;
    if (!ctxt) {
        AMP_ENTER("__amp_add_to_listen_fdset: no context\n");
        err = -EINVAL;
        goto EXIT;
    }
    sockfd = conn->ac_sock;
    pthread_mutex_lock(&ctxt->acc_lock);
    ctxt->acc_conn_table[sockfd] = conn;


// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------
#ifdef __AMP_LISTEN_SELECT

    FD_SET(sockfd, &ctxt->acc_readfds);
    if (sockfd > ctxt->acc_maxfd)
        ctxt->acc_maxfd = sockfd;

#endif
#ifdef __AMP_LISTEN_POLL

    amp_poll_fd_set( sockfd, ctxt->acc_poll_list );
    if (sockfd > ctxt->acc_maxfd)
        ctxt->acc_maxfd = sockfd;

#endif
#ifdef __AMP_LISTEN_EPOLL

    amp_epoll_fd_set( sockfd, ctxt->acc_epfd );

#endif
// -----------------------------------------------------------------

    pthread_mutex_unlock(&ctxt->acc_lock);
    write(ctxt->acc_notifyfd, (char *)&notifyid, sizeof(amp_s32_t));
EXIT:
    AMP_LEAVE("__amp_add_to_listen_fdset: leave\n");
    return 0;
}
/*end of file*/

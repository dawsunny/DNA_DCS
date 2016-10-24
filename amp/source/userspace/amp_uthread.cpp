/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */
/*          Rongfeng Tang                      */
/***********************************************/

#include <amp_help.h>
#include <amp_thread.h>
#include <amp_conn.h>
#include <amp_request.h>
#include <amp_protos.h>


amp_sem_t amp_process_in_sem;
amp_sem_t amp_process_out_sem;
amp_sem_t amp_reconn_sem;
amp_sem_t amp_netmorn_sem;
amp_sem_t amp_reconn_finalize_sem;

amp_u32_t amp_srvin_thread_num = 0;
amp_u32_t amp_srvout_thread_num = 0;
amp_u32_t amp_reconn_thread_num = 0;
amp_u32_t amp_wakeup_thread_num = 0;

amp_thread_t  *amp_srvin_threads = NULL;
amp_thread_t  *amp_srvout_threads = NULL;
amp_thread_t  *amp_reconn_threads = NULL;
amp_thread_t  *amp_wakeup_threads = NULL;

amp_lock_t    amp_threads_lock;

amp_s32_t     amp_listen_sockfd = -1;


/* 
 * block signals
 */
void
__amp_blockallsigs ()
{
    sigset_t  thread_sigs;

    sigemptyset (&thread_sigs);
    sigaddset (&thread_sigs, SIGALRM);
    sigaddset (&thread_sigs, SIGTERM);
    sigaddset (&thread_sigs, SIGHUP);
    //sigaddset (&thread_sigs, SIGINT);
    sigaddset (&thread_sigs, SIGQUIT);
    sigaddset (&thread_sigs, SIGPIPE);
    pthread_sigmask (SIG_BLOCK, &thread_sigs, NULL);

    AMP_LEAVE("__amp_blockallsigs: leave\n");
}


/*
 * recv message from a connection.
 *
 * return value: 0 - normal, the msgp contain the nearly recved message.
 *                   <0 - something wrong.
 */
int 
__amp_recv_msg (amp_connection_t *conn, amp_message_t **retmsgp)
{
    amp_message_t  *msgp = NULL;
    amp_s8_t *bufp = NULL;
    amp_message_t  header;
    amp_u32_t conn_type;
    amp_s32_t err = -1;
    amp_u32_t  size;

    AMP_ENTER("__amp_recv_msg: enter, conn:%p\n", conn);

    if (!retmsgp) {
        AMP_ERROR("__amp_recv_msg: no return address\n");
        err = -EINVAL;
        goto EXIT;
    }

    conn_type = conn->ac_type;

    switch (conn_type)  {
        case AMP_CONN_TYPE_UDP:
            msgp = (amp_message_t *)amp_alloc(AMP_MAX_MSG_SIZE);
            if (!msgp) {
                AMP_ERROR("__amp_recv_msg: cannot alloc msgp\n");
                err = -ENOMEM;
                goto EXIT;
            }

            /*
             * receive the msg
             */
            err = AMP_OP(conn_type, proto_recvmsg)(&conn->ac_sock, 
                                     &msgp->amh_addr,
                                     sizeof(msgp->amh_addr),
                                     AMP_MAX_MSG_SIZE,
                                     msgp,
                                     0);

            
            if (err < 0) {
                amp_free(msgp, AMP_MAX_MSG_SIZE);
                goto EXIT;
            }

             err = 0;            
            break;
        case AMP_CONN_TYPE_TCP:
            /*
             * firstly receive the msg header
             */
            err = AMP_OP(conn_type, proto_recvmsg)(&conn->ac_sock, 
                                     NULL,
                                     0,
                                     sizeof(header),
                                     &header,
                                     0);


            if (err < 0) {
                AMP_ERROR("__amp_recv_msg: receive msg header error, err:%d\n", err);
                goto EXIT;
            }

            size = header.amh_size;
            AMP_DMSG("__amp_recv_msg: header.amh_size:%d\n", size);
            if (size == 0) 
                AMP_WARNING("__amp_recv_msg: the size of payload is zero\n");
            
            msgp = (amp_message_t *)amp_alloc(size + sizeof(header));
            if (!msgp) {
                AMP_ERROR("__amp_recv_msg: no memory\n");
                err = -ENOMEM;
                goto EXIT;
            }
            
            if (size)   {   
                bufp = (amp_s8_t *)msgp + sizeof(header);

//RECV_BODY:
                /*
                 * receive the remain message
                 */
                err = AMP_OP(conn_type, proto_recvmsg)(&conn->ac_sock, NULL, 0, size, bufp, 0);
                if (err < 0) {
                    AMP_ERROR("__amp_recv_msg: receive msg error, err:%d\n", err);
                    goto EXIT;
                }
            }

            msgp->amh_magic = header.amh_magic;
            msgp->amh_addr = header.amh_addr;
            msgp->amh_pid = header.amh_pid;
            msgp->amh_sender_handle = header.amh_sender_handle;
            msgp->amh_send_ts = header.amh_send_ts;
            msgp->amh_size = header.amh_size;
            msgp->amh_type = header.amh_type;
            msgp->amh_xid = header.amh_xid;
            msgp->amh_seqno = header.amh_seqno;
            err = 0;
            break;
        default:
            err = -1;
            AMP_ERROR("__amp_recv_msg: wrong conn type: %d\n", conn_type);
            goto EXIT;
    }
    *retmsgp = msgp;

EXIT:
    AMP_LEAVE("__amp_recv_msg: leave\n");
    return err;
}


/*
 * general create thread function.
 */ 
int
__amp_create_thread(void* (*thrfunc)(void *), amp_thread_t *thread)
{
    amp_s32_t err = 0;

    AMP_ENTER("__amp_create_thread: enter\n");
    
    if (!thrfunc) {
        AMP_ERROR("__amp_create_thread: no thread func\n");
        err = -EINVAL;
        goto EXIT;
    }
    
    if (!thread) {
        AMP_ERROR("__amp_create_thread: no arg\n");
        err = -EINVAL;
        goto EXIT;
    }

    err = pthread_create(&thread->at_thread_id, NULL, thrfunc, (void *)thread);
    if (err) {
        AMP_ERROR("__amp_create_thread: create thread error, err:%d\n", err);
        goto EXIT;
    }

    amp_sem_down(&thread->at_startsem);
    err = 0;
    
EXIT:
    AMP_LEAVE("__amp_create_thread: leave, err:%d\n", err);
    return err;
}

/*
 * general stop thread function
 */ 
int
__amp_stop_thread (amp_thread_t *threadp, amp_sem_t *wakeup_sem)
{
    amp_s32_t err = 0;

    AMP_ENTER("__amp_stop_thread: enter\n");
    if (!threadp) {
        AMP_ERROR("__amp_stop_thread: no arg\n");
        err = -EINVAL;
        goto EXIT;
    }

    if (!wakeup_sem) {
        AMP_ERROR("__amp_stop_thread: no wake up sem\n");
        err = -EINVAL;
        goto EXIT;
    }

    if (!threadp->at_isup) {
        AMP_DMSG("__amp_stop_thread: at_isup is zero\n");
        goto EXIT;
    }

    threadp->at_shutdown = 1;

    amp_sem_up(wakeup_sem);
    amp_sem_down(&threadp->at_downsem);

EXIT:
    AMP_ENTER("__amp_stop_thread: leave\n");
    return err;
}

/*
 * start a srvin thread
 *
 * seqno - the sequence of this thread, counting from 0
 */ 
int 
__amp_start_srvin_thread (int seqno)
{
    amp_s32_t err = 0;
    amp_thread_t *threadp = NULL;

    AMP_ENTER("__amp_start_srvin_thread: enter\n");
    if ((seqno < 0) || (seqno>=AMP_MAX_THREAD_NUM)) {
        AMP_ERROR("__amp_start_srvin_thread: wrong seqno:%d\n", seqno);
        err = -EINVAL;
        goto EXIT;
    }

    threadp = &amp_srvin_threads[seqno];
    amp_sem_init_locked(&threadp->at_startsem);
    amp_sem_init_locked(&threadp->at_downsem);
    threadp->at_seqno = seqno;
    threadp->at_shutdown = 0;
    threadp->at_isup = 0;

    err = __amp_create_thread(__amp_serve_in_thread, threadp);
    amp_lock(&amp_threads_lock);
    if (!err && (seqno >= amp_srvin_thread_num)) 
        amp_srvin_thread_num =  seqno + 1;
    amp_unlock(&amp_threads_lock);

EXIT:
    AMP_LEAVE("__amp_start_srvin_thread: leave\n");
    return err;
}

/*
 * start a srvout thread
 */ 
int
__amp_start_srvout_thread (int seqno)
{
    amp_s32_t err = 0;
    amp_thread_t *threadp = NULL;
               
    AMP_ENTER("__amp_start_srvout_thread: enter\n");

    if ((seqno < 0) || (seqno>=AMP_MAX_THREAD_NUM)) {
        AMP_ERROR("__amp_start_srvout_thread: wrong seqno:%d\n", seqno);
        err = -EINVAL;
        goto EXIT;
    }
                         
    threadp = &amp_srvout_threads[seqno];
    amp_sem_init_locked(&threadp->at_startsem);
    amp_sem_init_locked(&threadp->at_downsem);
    threadp->at_seqno = seqno;
    threadp->at_shutdown = 0;
    threadp->at_isup = 0;
                                   
    err = __amp_create_thread(__amp_serve_out_thread, threadp);
    
    amp_lock(&amp_threads_lock);

    if (!err && (seqno >= amp_srvout_thread_num)) 
        amp_srvout_thread_num = seqno + 1;

    amp_unlock(&amp_threads_lock);
    
EXIT:
    AMP_LEAVE("__amp_start_srvout_thread: leave, threadp:%p\n", threadp);
    return err;
}

/*
 * start a reconn thread
 */ 
int
__amp_start_reconn_thread (int seqno)
{
    amp_s32_t err = 0;
    amp_thread_t *threadp = NULL;

    AMP_ENTER("__amp_start_reconn_thread: enter\n");

    if(amp_reconn_thread_started) {
        goto EXIT;
    }

    if ((seqno < 0) || (seqno>=AMP_MAX_THREAD_NUM)) {
        AMP_ERROR("__amp_start_reconn_thread: wrong seqno:%d\n", seqno);
        err = -EINVAL;
        goto EXIT;
    }

    threadp = &amp_reconn_threads[seqno];
    amp_sem_init_locked(&threadp->at_startsem);
    amp_sem_init_locked(&threadp->at_downsem);
    threadp->at_seqno = seqno;
    threadp->at_shutdown = 0;
    threadp->at_isup = 0;

    err = __amp_create_thread(__amp_reconn_thread, threadp);
   
    amp_lock(&amp_threads_lock);

    if (!err && (seqno >= amp_reconn_thread_num)) 
        amp_reconn_thread_num = seqno + 1;

    amp_unlock(&amp_threads_lock);

    amp_reconn_thread_started = 1;

EXIT:
    AMP_LEAVE("__amp_start_reconn_thread: leave\n");
    return err;
}

/*
 * start a netmorn thread
 */ 
amp_thread_t *
__amp_start_netmorn_thread (amp_comp_context_t *ctxt)
{
    amp_s32_t err = 0;
    amp_thread_t *threadp = NULL;

    AMP_ENTER("__amp_start_netmorn_thread: enter\n");
    
    if(!ctxt) {
        AMP_ERROR("__amp_start_netmorn_thread: no context\n");
        goto EXIT;
    }
    threadp =(amp_thread_t *)amp_alloc(sizeof(amp_thread_t));
    if (!threadp) {
        AMP_ERROR("__amp_start_netmorn_thread: alloc for thread error\n");
        goto EXIT;
    }
    memset(threadp, 0, sizeof(amp_thread_t));
    amp_sem_init_locked(&threadp->at_startsem);
    amp_sem_init_locked(&threadp->at_downsem);
    threadp->at_seqno = 1;
    threadp->at_shutdown = 0;
    threadp->at_isup = 0;
    threadp->at_provite = ctxt;

    err = __amp_create_thread(__amp_netmorn_thread, threadp);
    if (err < 0) {
        AMP_ERROR("__amp_start_netmorn_thread: create thread error, err:%d\n", err);
        amp_free(threadp, sizeof(amp_thread_t));
        goto EXIT;
    }


EXIT:
    AMP_LEAVE("__amp_start_netmorn_thread: leave\n");
    return threadp;
}


/*
 * start initial number of srvin threads
 */ 
int
__amp_start_srvin_threads (void)
{
    amp_s32_t err = 0;
    amp_u32_t i;

    AMP_ENTER("__amp_start_srvin_threads: enter\n");

    for (i=0; i<AMP_SRVIN_THR_INIT_NUM; i++) {
        err = __amp_start_srvin_thread (i);
        if (err < 0)
            goto ERROR;
    }

EXIT:
    AMP_LEAVE("__amp_start_srvin_threads: leave\n");
    return err;

ERROR:
    __amp_stop_srvin_threads();
    goto EXIT;
}

    
/*
 * start initial number of srvout threads
 */ 
int 
__amp_start_srvout_threads (void)
{
    amp_s32_t err = 0;
    amp_u32_t i;

    AMP_ENTER("__amp_start_srvout_threads: enter, totolnum:%d\n", amp_srvout_thread_num);

    for (i=0; i<AMP_SRVOUT_THR_INIT_NUM; i++) {
        err = __amp_start_srvout_thread (i);
        if (err < 0)
            goto ERROR;
    }

EXIT:
    AMP_LEAVE("__amp_start_srvout_threads: leave\n");
    return err;

ERROR:
    __amp_stop_srvout_threads();
    goto EXIT;
}

int
__amp_start_reconn_threads(void)
{
    amp_s32_t err = 0;
    
    err = __amp_start_reconn_thread(0);
    if(err != 0){
        AMP_ERROR("amp_start_reconn_threads: start reconn thread error\n");
        goto EXIT;
    }
    err = __amp_start_wakeup_thread(0);
    if(err != 0){
        AMP_ERROR("amp_start_reconn_threads: start wakeup thread error\n");
    }

EXIT:
    return err;
}

/*
 * start initial number of reconn threads
 */ 
int
__amp_start_wakeup_thread (int seqno)
{
    amp_s32_t err = 0;
    amp_thread_t*  threadp;

    AMP_ENTER("__amp_start_wakeup_thread: enter\n");
 
    if(amp_wakeup_thread_started) {
        goto EXIT;
    }

    if ((seqno < 0) || (seqno>=AMP_MAX_THREAD_NUM)) {
        AMP_ERROR("__amp_start_wakeup_thread: wrong seqno:%d\n", seqno);
        err = -EINVAL;
        goto EXIT;
    }

    threadp = &amp_wakeup_threads[seqno];
    amp_sem_init_locked(&threadp->at_startsem);
    amp_sem_init_locked(&threadp->at_downsem);
    threadp->at_seqno = seqno;
    threadp->at_shutdown = 0;
    threadp->at_isup = 0;

    err = __amp_create_thread(__amp_wakeup_reconn_thread, threadp);
    
    amp_lock(&amp_threads_lock);

    if (!err && (seqno >= amp_wakeup_thread_num)) 
        amp_wakeup_thread_num = seqno + 1;

    amp_unlock(&amp_threads_lock);

    amp_wakeup_thread_started = 1;

EXIT:                   
    AMP_LEAVE("__amp_start_wakeup_thread: leave\n");
    return err;
}

/*
 * start a listen thread.
 */
amp_thread_t* 
__amp_start_listen_thread(amp_comp_context_t *ctxt)
{
    amp_s32_t err = 0;
    amp_thread_t *threadp = NULL;

    AMP_ENTER("__amp_start_listen_thread: enter\n");

    if (!ctxt)  {
        AMP_ERROR("__amp_start_listen_thread: no context\n");
        goto EXIT;
    }


    threadp = (amp_thread_t *)amp_alloc(sizeof(amp_thread_t));
    if (!threadp) {
        AMP_ERROR("__amp_start_listen_thread: alloc threadp error\n");
        goto EXIT;
    }

    memset(threadp, 0, sizeof(amp_thread_t));
    amp_sem_init_locked(&threadp->at_startsem);
    amp_sem_init_locked(&threadp->at_downsem);
    threadp->at_seqno = ctxt->acc_this_id;
    threadp->at_shutdown = 0;
    threadp->at_isup = 0;
    threadp->at_provite = ctxt;

    err = __amp_create_thread(__amp_listen_thread, threadp);

    if (err < 0) {
        amp_free(threadp, sizeof(amp_thread_t));
        threadp = NULL;
        goto EXIT;
    }

EXIT:
    AMP_LEAVE("__amp_start_listen_thread: leave\n");
    return threadp;
    
}

int
__amp_stop_wakeup_thread(int seqno)
{
    amp_s32_t err = 0;
    amp_thread_t *threadp = NULL;

    AMP_ENTER("__amp_stop_wakeup_thread: enter\n");

    if ((seqno < 0) || (seqno>=AMP_MAX_THREAD_NUM)) {
        AMP_ERROR("__amp_stop_wakeup_thread: wrong seqno:%d\n", seqno);
        err = -EINVAL;
        goto EXIT;
    }

    threadp = &amp_wakeup_threads[seqno];

    if (!threadp->at_isup) {
        AMP_WARNING("__amp_stop_wakeup_thread: to stop a stopped thread\n");
        goto EXIT;
    }

    threadp->at_shutdown = 1;
    AMP_DMSG("__amp_stop_wakeup_thread: to stop wakeup thread:%p\n", threadp);
    amp_sem_up(&amp_reconn_sem);
    amp_sem_down(&threadp->at_downsem);
    
    if(seqno == (amp_wakeup_thread_num - 1)){
        amp_lock(&amp_threads_lock);
        amp_wakeup_thread_num--;
        amp_unlock(&amp_threads_lock);
    }
   
    memset(threadp, 0, sizeof(amp_thread_t));
    amp_wakeup_thread_started = 0;

EXIT:
    AMP_LEAVE("__amp_stop_wakeup_thread: leave\n");
    return err;
}

int
__amp_stop_reconn_thread (int seqno)
{
    amp_s32_t err = 0;
    amp_thread_t *threadp = NULL;

    AMP_ENTER("__amp_stop_reconn_thread: enter\n");

    if ((seqno < 0) || (seqno>=AMP_MAX_THREAD_NUM)) {
        AMP_ERROR("__amp_stop_reconn_thread: wrong seqno:%d\n", seqno);
        err = -EINVAL;
        goto EXIT;
    }

    threadp = &amp_reconn_threads[seqno];

    if (!threadp->at_isup) {
        AMP_WARNING("__amp_stop_reconn_thread: to stop a stopped thread\n");
        goto EXIT;
    }

    threadp->at_shutdown = 1;
    AMP_DMSG("__amp_stop_reconn_thread: to stop reconn thread:%p\n", threadp);
    amp_sem_up(&amp_reconn_sem);
    amp_sem_down(&threadp->at_downsem);
    
    if(seqno == (amp_reconn_thread_num - 1)){
        amp_lock(&amp_threads_lock);
        amp_reconn_thread_num--;
        amp_unlock(&amp_threads_lock);
    }
   
    memset(threadp, 0, sizeof(amp_thread_t));
    amp_reconn_thread_started = 0;

EXIT:
    AMP_LEAVE("__amp_stop_reconn_thread: leave\n");
    return err;
}

/*
 * stop a reconn thread.
 */ 
int
__amp_stop_netmorn_thread (amp_comp_context_t *ctxt)
{
    amp_s32_t err = 0;
    amp_thread_t *threadp = NULL;

    AMP_ENTER("__amp_stop_netmorn_thread: enter\n");

    threadp = ctxt->acc_netmorn_thread;
    if (!threadp) {
        AMP_ERROR("__amp_stop_netmorn_thread: no netmornitor thread to ctxt, type:%d,id:%d\n", \
                          ctxt->acc_this_type, ctxt->acc_this_id);
        err = -EINVAL;
        goto EXIT;
    }

    if (!threadp->at_isup) {
        AMP_ERROR("__amp_stop_netmorn_thread: to stop a stopped thread\n");
        goto EXIT;
    }
                                        

    threadp->at_shutdown = 1;
    AMP_DMSG("__amp_stop_netmorn_thread: to stop netmorn thread:%p\n", threadp);
    amp_sem_up(&amp_netmorn_sem);
    amp_sem_down(&threadp->at_downsem);

    ctxt->acc_netmorn_thread = NULL;
    amp_free(threadp, sizeof(amp_thread_t));

EXIT:
    AMP_LEAVE("__amp_stop_netmorn_thread: leave\n");
    return err;
}


/*
 * stop all srvin threads.
 */ 
int
__amp_stop_srvin_threads (void)
{
    amp_u32_t i;
    amp_thread_t  *threadp = NULL;

    AMP_DMSG("__amp_stop_srvin_threads: enter, total_num:%d\n", amp_srvin_thread_num);
    
    for(i=0; i<amp_srvin_thread_num; i++) {
        threadp = &amp_srvin_threads[i];
        if (!threadp->at_isup)
            continue;
        threadp->at_shutdown = 1;
    }

    for(i=0; i<amp_srvin_thread_num; i++) 
        amp_sem_up(&amp_process_in_sem);
    
    
    for(i=0; i<amp_srvin_thread_num; i++) {
        threadp = &amp_srvin_threads[i];
        if (!threadp->at_isup)
            continue;
        amp_sem_down(&threadp->at_downsem);
        threadp->at_isup = 0;
    }

    amp_srvin_thread_num = 0;
        
    AMP_LEAVE("__amp_stop_srvin_threads: leave\n");
    return 0;
}

/*
 * stop all srvout threads.
 */ 
int
__amp_stop_srvout_threads (void)
{
    amp_u32_t i;
    amp_thread_t  *threadp = NULL;

    AMP_DMSG("__amp_stop_srvout_threads: enter\n");

    for(i=0; i<amp_srvout_thread_num; i++) {
        threadp = &amp_srvout_threads[i];
        threadp->at_shutdown = 1;
    }

    for(i=0; i<amp_srvout_thread_num; i++) {
        threadp = &amp_srvout_threads[i];
        if (!threadp->at_isup)
            continue;
        AMP_DMSG("__amp_stop_srvout_threads: stop thread:%p\n", threadp);
        amp_sem_up(&amp_process_out_sem);
    }
        
    for(i=0; i<amp_srvout_thread_num; i++) {
        threadp = &amp_srvout_threads[i];
        if (!threadp->at_isup)
            continue;
        amp_sem_down(&threadp->at_downsem);
        threadp->at_isup = 0;
    }

    amp_srvout_thread_num = 0;

    AMP_LEAVE("__amp_stop_srvout_threads: leave\n");
    return 0;
}

int
__amp_stop_reconn_threads(void)
{
    __amp_stop_wakeup_thread(0);
    __amp_stop_reconn_thread(0);

    return 0;
}

/*
 * stop the listen thread belongs to  specific component context
 */
int
__amp_stop_listen_thread(amp_comp_context_t *ctxt)
{
    amp_thread_t  *threadp = NULL;
    AMP_ENTER("__amp_stop_listen_thread: enter\n");
    amp_s32_t  notifyid = 1;

    threadp = ctxt->acc_listen_thread;
    if (!threadp->at_isup) {
        AMP_DMSG("__amp_stop_listen_thread: to stop a stopped thread\n");
        goto EXIT;
    }
    pthread_mutex_lock(&ctxt->acc_lock);
    threadp->at_shutdown = 1;
    pthread_mutex_unlock(&ctxt->acc_lock);
    
    write(ctxt->acc_notifyfd, (char *)&notifyid, sizeof(amp_s32_t));
    amp_sem_down(&threadp->at_downsem);
    ctxt->acc_listen_thread = NULL;
    
EXIT:
    AMP_LEAVE("__amp_stop_listen_thread: leave\n");
    return 0;
}


/*
 * the callback of serving outcoming request threads.
 */
void*
__amp_serve_out_thread (void *argv)
{
    amp_thread_t *threadp = NULL;
    amp_u32_t   seqno;
    amp_request_t  *req;
    amp_connection_t *conn; 
    amp_u32_t   conn_type;  /*tcp or udp*/
    amp_u32_t   req_type;    /*msg or data*/
    struct timeval thistime;
    amp_s32_t   err = 0;
    amp_u32_t   flags = 0;
    amp_u32_t   need_ack = 0;
    struct sockaddr_in  sout_addr;
    amp_u32_t  sendsize = 0;


    AMP_ENTER("__amp_serv_out_thread: enter, %ld\n", pthread_self());

    threadp = (amp_thread_t *)argv;
    seqno = threadp->at_seqno;

    threadp->at_isup = 1;
    threadp->at_thread_id = pthread_self();

    amp_sem_up(&threadp->at_startsem);
    AMP_DMSG("__amp_serve_out_thread: thread:%p, up\n", threadp);

    /*
     * ok, do the main work.
     */

AGAIN:
    AMP_ENTER("__amp_serve_out_thread: before sem\n");

    amp_sem_down(&amp_process_out_sem);
    if (threadp->at_shutdown) {
        AMP_DMSG("__amp_serve_out_thread: tell us to down\n"); 
        goto EXIT;
    }
    AMP_ENTER("__amp_serve_out_thread: get sem, threadp:%p\n", threadp);

    amp_lock(&amp_sending_list_lock);
    if (list_empty(&amp_sending_list)) {
        amp_unlock(&amp_sending_list_lock);
        goto AGAIN;
    }
    req = list_entry(amp_sending_list.next, amp_request_t, req_list);
    amp_lock(&req->req_lock);
    list_del_init(&req->req_list);
    req->req_error = 0;
    //req->req_send_state = AMP_REQ_SEND_INIT;
    amp_unlock(&req->req_lock);
    amp_unlock(&amp_sending_list_lock);
    sendsize = 0;

    amp_gettimeofday(&thistime);

    if (!req->req_msg && !req->req_reply) {
        AMP_ERROR("__amp_serve_out_thread: nothing need to be sent \n");
        req->req_error = -EINVAL;
        goto ERROR;
    }
    
    AMP_ENTER("__amp_serve_out_thread: to send req:%p\n", req); 
    
    req_type = req->req_type;

    if ((req_type != (AMP_REQUEST | AMP_MSG))  &&
            (req_type != (AMP_REQUEST | AMP_DATA)) &&
            (req_type != (AMP_REPLY | AMP_MSG)) &&
            (req_type != (AMP_REPLY | AMP_DATA))) {
        AMP_ERROR("__amp_serve_out_thread: wrong msg type: 0x%x\n", req_type);
        req->req_error = -EINVAL;
        goto ERROR;
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
                    AMP_WARNING("__amp_serve_out_thread: no valid conn to peer (type:%d, id:%d, err:%d)\n", \
                                                req->req_remote_type, \
                                                req->req_remote_id, \
                                                err);
                
                    if (req->req_resent && (req->req_type & AMP_REQUEST)) {
                        __amp_add_resend_req(req);
                        goto AGAIN;
                    }
                case 1:
                    AMP_WARNING("__amp_serve_out: no conn to peer(type:%d, id:%d, err:%d)\n", \
                                                req->req_remote_type, 
                                                req->req_remote_id, \
                                                err);
                default:
                    AMP_WARNING("__amp_serve_out_thread:amp_select_conn goto error\n");
                    req->req_error = -ENOTCONN;
                    err = req->req_error;
                    goto ERROR;
            }
        }
    }
       /*
        * the senders and receivers will all change this state
        */
    
    switch(conn->ac_state) {
        case AMP_CONN_BAD:
        case AMP_CONN_NOTINIT:
        case AMP_CONN_CLOSE:
        case AMP_CONN_RECOVER:
            amp_unlock(&conn->ac_lock);
            AMP_WARNING("__amp_serve_out_thread: conn:%p is not valid currently\n", conn);
            goto SELECT_CONN;
        default:
            break;
    }

    /*
     * we get a good connection, so doing the following work.
     */
    conn_type = conn->ac_type;       
    if ((!AMP_HAS_TYPE(conn_type)) || 
           (!AMP_OP(conn_type, proto_sendmsg)) || 
           (!AMP_OP(conn_type, proto_senddata)))  {
        AMP_WARNING("__amp_serve_out_thread: conn:%p, has no operations\n", conn);
        req->req_error = -ENOSYS;
        amp_unlock(&conn->ac_lock);
        err = req->req_error;
        goto ERROR;
    }   
    
    
    //conn->ac_weight += sendsize;
    amp_unlock(&conn->ac_lock); 
    amp_sem_down(&conn->ac_sendsem);

    AMP_DMSG("__amp_serve_out_thread: after down sem, req:%p, get conn:%p\n", req, conn);
    if (conn->ac_state != AMP_CONN_OK) {
        AMP_WARNING("__amp_serve_out_thread: before send, state of conn:%p is invalid:%d\n", \
                            conn, conn->ac_state);
        amp_lock(&conn->ac_lock);
        conn->ac_weight -= sendsize;
        amp_unlock(&conn->ac_lock);
        amp_sem_up(&conn->ac_sendsem);
        goto SELECT_CONN;

    }
    
    /*stage1: send msg*/
    amp_lock(&req->req_lock);
    req->req_conn = conn;
    //req->req_send_state = AMP_REQ_SEND_START;
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

        AMP_DMSG("__amp_serve_out_thread: conn:%p, send request\n", conn);
        err = AMP_OP(conn_type, proto_sendmsg)(&conn->ac_sock, \
                                                       &sout_addr, \
                                                       sizeof(sout_addr), \
                                                       req->req_msglen, \
                                                       req->req_msg, \
                                                       flags);
    } else {
        AMP_DMSG("__amp_serve_out_thread: conn:%p, send reply\n", conn);
        
        if (conn->ac_type == AMP_CONN_TYPE_UDP)
            sout_addr = req->req_reply->amh_addr;
        
        err = AMP_OP(conn_type, proto_sendmsg)(&conn->ac_sock, \
                                                       &sout_addr, \
                                                       sizeof(sout_addr), \
                                                       req->req_replylen, \
                                                       req->req_reply, \
                                                       flags);
    }
    
    if (err < 0) {
        AMP_ERROR("__amp_serve_out_thread: sendmsg error, conn:%p, req:%p, err:%d\n", conn, req, err);
        req->req_conn = NULL;
        amp_unlock(&req->req_lock);
        goto SEND_ERROR;
    }

    if (req_type & AMP_DATA)  {
        /*stage2: send data*/
        AMP_DMSG("__amp_serv_out_thread: conn:%p, send data\n", conn);
        req->req_stage = AMP_REQ_STAGE_DATA;
        err = AMP_OP(conn_type, proto_senddata)(&conn->ac_sock,
                                                        &sout_addr, \
                                                        sizeof(sout_addr), \
                                                        req->req_niov,
                                                        req->req_iov,
                                                        0);
        if (err < 0) {
            AMP_ERROR("__amp_serve_out_thread: senddata error, conn:%p, req:%p, err:%d\n",\
                                  conn, req, err);
            req->req_conn = NULL;
            amp_unlock(&req->req_lock);
            goto SEND_ERROR;
        }
    }

    AMP_DMSG("__amp_serve_out_thread: finished send, conn:%p\n", conn);
    //req->req_send_state = AMP_REQ_SEND_END;
    amp_unlock(&req->req_lock);

    if (need_ack) { /*waiting for ack*/
        AMP_LEAVE("__amp_serve_out_thread: req:%p, need ack\n", req);
        amp_lock(&amp_waiting_reply_list_lock);
        amp_lock(&req->req_lock);
        if(!__amp_within_waiting_reply_list(req))
            if(list_empty(&req->req_list)){
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
        goto AGAIN;
    /*
     * do not need ack
     */

    if (req->req_need_free) {
        if (req_type & AMP_REQUEST)
            amp_free(req->req_msg, req->req_msglen);
        else
            amp_free(req->req_reply, req->req_replylen);

    } else {
        amp_sem_up(&req->req_waitsem);  
    }

    goto AGAIN;
     

SEND_ERROR:
    AMP_DMSG("__amp_serve_out_thread: send error through conn:%p, err:%d\n", conn, err);

    amp_lock(&conn->ac_lock);
    AMP_DMSG("__amp_serve_out_thread: before check\n");
    
    if (conn->ac_type == AMP_CONN_TYPE_TCP)  {
        conn->ac_datard_count = 0;
        conn->ac_sched = 0;     
    }
    conn->ac_weight -= sendsize;
    amp_unlock(&conn->ac_lock);

    AMP_OP(conn_type, proto_disconnect)((void *)&conn->ac_sock);//modified by weizheng 2013-12-20
    
    amp_lock(&amp_reconn_conn_list_lock);
    amp_lock(&conn->ac_lock);
    if (conn->ac_need_reconn) {//by weizheng 2013-12-10 if the quota right 
        /*
         * we must add the bad connection to the reconnect list.
         */

        if (conn->ac_state != AMP_CONN_RECOVER) {
            AMP_ERROR("__amp_serve_out_thread: set conn:%p to recover\n", conn);
            conn->ac_state = AMP_CONN_RECOVER;  
            if (list_empty(&conn->ac_reconn_list)){
                list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
                amp_sem_up(&amp_reconn_sem);
            }
        } else 
            AMP_ERROR("__amp_serve_out_thread: someone else has set conn:%p to recover\n", conn);
            
    } else if (conn->ac_type == AMP_CONN_TYPE_TCP)   { 
        /*
         * Maybe it's in server side or it's realy need to be released, so we free it.
         */
        if (conn->ac_state != AMP_CONN_CLOSE) {
            AMP_ERROR("__amp_serve_out_thread: set conn:%p to close\n", conn);
            conn->ac_state = AMP_CONN_CLOSE;
            if (list_empty(&conn->ac_reconn_list)){
                list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
                amp_sem_up(&amp_reconn_sem);
            }
        } else 
            AMP_ERROR("__amp_serve_out_thread: someone else has set conn:%p to close\n", conn);
    }
    amp_unlock(&conn->ac_lock);
    amp_unlock(&amp_reconn_conn_list_lock);
    
    amp_sem_up(&conn->ac_sendsem);
    goto SELECT_CONN;
                                
ERROR:
    AMP_ERROR("__amp_serve_out_thread: at ERROR, conn:%p, req:%p\n", conn, req);
    amp_sem_up(&req->req_waitsem);
    goto AGAIN;
    
EXIT:

    AMP_LEAVE("__amp_serve_out_thread: leave: %ld\n", pthread_self());
    amp_sem_up(&threadp->at_downsem);
    return 0;
}

/*
 * the callback of serving incoming msg requests
 */
void*
__amp_serve_in_thread (void *argv)
{
    amp_thread_t *threadp = NULL;
    amp_u32_t    seqno;
    amp_request_t  *req = NULL;
    amp_connection_t *conn; 
    amp_u32_t    conn_type;  /*tcp or udp*/
    amp_u32_t    req_type;    /*msg or data*/
    amp_s32_t    err = 0;
    amp_s8_t     *recvmsg = NULL;
    amp_message_t  *msgp = NULL;
    amp_u32_t niov;
    amp_kiov_t  *iov = NULL;
    
    AMP_ENTER("__amp_serv_in_thread: enter, %ld\n", pthread_self());

    threadp = (amp_thread_t *)argv;
    seqno = threadp->at_seqno;

    threadp->at_isup = 1;
    amp_sem_up(&threadp->at_startsem);

AGAIN:
    AMP_ENTER("__amp_serve_in_thread: begin to down semaphore\n");
    amp_sem_down(&amp_process_in_sem);
    AMP_ENTER("__amp_serve_in_thread: after down semaphore\n");

    if (threadp->at_shutdown) {
        AMP_DMSG("__amp_serve_in_thread: to exit, threadp:%p\n", threadp);
        goto EXIT;
    }
    /*
     * the refcont of conn in dataready list is set to be more
     * than one
     */
    amp_lock(&amp_dataready_conn_list_lock);
    if (list_empty(&amp_dataready_conn_list)) {
        amp_unlock(&amp_dataready_conn_list_lock);
        goto AGAIN;
    }
    conn = list_entry(amp_dataready_conn_list.next, amp_connection_t, ac_dataready_list);
    amp_lock(&conn->ac_lock);

    if(!list_empty(&conn->ac_dataready_list)){
        list_del_init(&conn->ac_dataready_list);
    }
    else{
        amp_unlock(&conn->ac_lock);
        amp_unlock(&amp_dataready_conn_list_lock);
        goto AGAIN;
    }
    conn_type = conn->ac_type;
    amp_unlock(&conn->ac_lock);
    amp_unlock(&amp_dataready_conn_list_lock);

    /*
     * firstly get a request.
     */
    amp_lock(&conn->ac_lock);
    __amp_alloc_request(&req);//by weizheng 2013-11-18, alloc for request, refcount =1
    req->req_remote_id = conn->ac_remote_id;
    req->req_remote_type = conn->ac_remote_comptype;

    /*
     * receive the header
     */
    AMP_DMSG("__amp_serve_in_thread: before recv msg header\n");
    amp_sem_down(&conn->ac_recvsem);
    msgp = NULL;
    err = __amp_recv_msg(conn, &msgp);
    AMP_DMSG("__amp_serve_in_thread: after recv msg header, msgp:%p\n", msgp);
    if (err < 0) {
        amp_unlock(&conn->ac_lock);
        amp_sem_up(&conn->ac_recvsem);
        AMP_WARNING("__amp_serve_in_thread: get msg header error, err:%d, conn:%p\n", err, conn);
        AMP_WARNING("__amp_serve_in_thread: remote_id:%d, remote_comptype:%d, ac_sock:%d\n", \
                             conn->ac_remote_id,conn->ac_remote_comptype, conn->ac_sock);
        if (msgp)
            amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
        __amp_free_request(req);

        /*
         * this connection is error, need reconnection.
         */
        AMP_OP(conn_type, proto_disconnect)((void *)&conn->ac_sock);

        amp_lock(&amp_reconn_conn_list_lock);
        amp_lock(&conn->ac_lock);
        if (conn->ac_state == AMP_CONN_OK) {
            AMP_WARNING("__amp_serve_in_thread: resent the conn:%p\n", conn);   
            if (conn->ac_need_reconn) {
                conn->ac_state = AMP_CONN_RECOVER;
                AMP_ERROR("__amp_serve_in_thread: need recover\n");
            } else {
                conn->ac_state = AMP_CONN_CLOSE;            
                AMP_ERROR("__amp_serve_in_thread: close it\n");
            }
            if (list_empty(&conn->ac_reconn_list)) {
                AMP_ERROR("__amp_serve_in_thread: add conn:%p to reconn list\n", conn);
                list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
                amp_sem_up(&amp_reconn_sem);//modified by weizheng 2013-11-18, not sure
            } else
                AMP_ERROR("__amp_serve_in_thread: conn:%p has added to reconn list\n", conn);
        }
        amp_unlock(&conn->ac_lock);
        amp_unlock(&amp_reconn_conn_list_lock);

        goto AGAIN;
    } 


    AMP_DMSG("__amp_serve_in_thread: judge amh_magic\n");
    if (msgp->amh_magic != AMP_REQ_MAGIC)  {
        AMP_ERROR("__amp_serve_in_thread: wrong request header, conn:%p,type:0x%x,magic:0x%x,length: %d\n", \
                        conn, 
                        msgp->amh_type,
                        msgp->amh_magic, 
                        msgp->amh_size);
        amp_unlock(&conn->ac_lock);
 
        amp_lock(&amp_reconn_conn_list_lock);
        amp_lock(&conn->ac_lock);
        if (conn->ac_state == AMP_CONN_OK) {
            AMP_ERROR("__amp_serve_in_thread: reset the conn:%p\n", conn);
            AMP_OP(conn_type, proto_disconnect)((void *)&conn->ac_sock);
            if (conn->ac_need_reconn) {
                conn->ac_state = AMP_CONN_RECOVER;
                AMP_ERROR("__amp_serve_in_thread: conn:%p, need recover\n", conn);
            } else {
                conn->ac_state = AMP_CONN_CLOSE;            
                AMP_ERROR("__amp_serve_in_thread: conn:%p, close it\n", conn);
            }
            if (list_empty(&conn->ac_reconn_list))
                list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
        }
        amp_unlock(&conn->ac_lock);
        amp_unlock(&amp_reconn_conn_list_lock);

        amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
        __amp_free_request(req);
        amp_sem_up(&conn->ac_recvsem);//modified by weizheng 2013-11-18
        goto AGAIN;
    }

    req_type = msgp->amh_type;
     
    AMP_DMSG("__amp_serve_in_thread: judge req_type:%d\n", req_type);

    /*
     * verify the request
     */
    
     AMP_DMSG("__amp_serve_in_thread: judge what kind of msg\n");
     if ((req_type != (AMP_REQUEST | AMP_MSG))  &&
             (req_type != (AMP_REQUEST | AMP_DATA)) &&
             (req_type != (AMP_REPLY | AMP_MSG)) &&
             (req_type != (AMP_REPLY | AMP_DATA))){
        AMP_ERROR("__amp_serve_in_thread: get wrong request header, conn:%p, type:%d\n", conn, req_type);
        amp_unlock(&conn->ac_lock);

        amp_lock(&amp_reconn_conn_list_lock);
        amp_lock(&conn->ac_lock);
        if (conn->ac_state == AMP_CONN_OK) {
            AMP_ERROR("__amp_serve_in_thread: reset the conn:%p\n", conn);
            AMP_OP(conn_type, proto_disconnect)((void *)&conn->ac_sock);
            if(conn->ac_need_reconn) {
                conn->ac_state = AMP_CONN_RECOVER;
                AMP_ERROR("__amp_serve_in_thread: conn:%p, need recover\n", conn);
            } else {
                conn->ac_state = AMP_CONN_CLOSE;    
                AMP_ERROR("__amp_serve_in_thread: conn:%p, close it\n", conn);
            }
            
            if (list_empty(&conn->ac_reconn_list))
                list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
        }
        amp_unlock(&conn->ac_lock);
        amp_unlock(&amp_reconn_conn_list_lock);

        __amp_free_request(req);
        amp_sem_up(&conn->ac_recvsem);//modified by weizheng 2013-11-18
        amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
        goto AGAIN;
    }


    AMP_DMSG("__amp_serve_in_thread: is it a reply\n");
    if (req_type & AMP_REPLY)   {
        /*
         * get a reply, get the request
         */
        amp_request_t  *tmpreqp = NULL;
        
        AMP_DMSG("__amp_serve_in_thread: it's a reply\n");
        tmpreqp = (amp_request_t *)((unsigned long) msgp->amh_sender_handle);
        AMP_DMSG("__amp_serve_in_thread: original req:%p\n", tmpreqp);
        if (msgp->amh_xid != conn->ac_this_id || 
                msgp->amh_pid != conn->ac_this_type || 
                !__amp_reqheader_equal(tmpreqp->req_msg, msgp)) {
            AMP_ERROR("__amp_serve_in_thread: conn:%p, local_tmp_req:%p, receive_req:%p, two header is not equal\n", \
                                  conn, req, tmpreqp);
            __amp_free_request(req);

            if(conn_type == AMP_CONN_TYPE_TCP) 
                amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
            else
                amp_free(msgp, AMP_MAX_MSG_SIZE);
            amp_unlock(&conn->ac_lock);
            goto ADD_BACK;
            
        } else {
            __amp_free_request(req);
            req = tmpreqp; //by weizheng 2013-11-18, switch to the orignal req, now req->req_refcount=2
            amp_lock(&amp_waiting_reply_list_lock);
            amp_lock(&req->req_lock);
            req->req_conn = conn;
            //req->req_send_state = AMP_REQ_RECV;
            if(__amp_within_waiting_reply_list(req))
                list_del_init(&req->req_list);//delete from amp_wating_reply_list
            amp_unlock(&req->req_lock);
            amp_unlock(&amp_waiting_reply_list_lock);

        }       
    }
    /*
     * receive the data, if needed.
     */
    if (req_type & AMP_DATA)  {
        AMP_DMSG("__amp_serve_in_thread: has data\n");
        if ((req_type & AMP_REPLY) && req->req_iov) { /*for we have provided for it.*/
            niov = req->req_niov;
            iov = req->req_iov;
        } else {
            /*
             * if we have provided the alloc function, then calling it, else
             * use the default one.
             */
            recvmsg = (amp_s8_t *)((amp_s8_t *)msgp + AMP_MESSAGE_HEADER_LEN);
            err = conn->ac_allocpage_cb(recvmsg, &niov, &iov);
            if (err < 0) {
                amp_unlock(&conn->ac_lock);

                /*
                 * we can be sure, no memory now, in order to lest the following requests
                 * continue bother us, reset it, so that the remote peer will send it again.
                 */
                AMP_ERROR("__amp_serve_in_thread: conn:%p, alloc page error, err:%d\n", conn, err);
                amp_lock(&amp_reconn_conn_list_lock);
                amp_lock(&conn->ac_lock);
                if (conn->ac_state == AMP_CONN_OK) {
                    AMP_ERROR("__amp_serve_in_thread: alloc page error, resent conn:%p\n", conn);
                    AMP_OP(conn_type, proto_disconnect)((void *)&conn->ac_sock);
                    if(conn->ac_need_reconn) {
                        conn->ac_state = AMP_CONN_RECOVER;
                    } else
                        conn->ac_state = AMP_CONN_CLOSE;            
                    if (list_empty(&conn->ac_reconn_list))
                        list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
                }
                amp_unlock(&conn->ac_lock);
                amp_unlock(&amp_reconn_conn_list_lock);

                amp_lock(&req->req_lock);
                req->req_conn = NULL;
                amp_unlock(&req->req_lock);

                amp_sem_up(&conn->ac_recvsem);
                if ((req_type & AMP_REPLY) && req->req_need_ack) {
                    req->req_error = -ENOMEM;
                    amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
                    amp_sem_up(&req->req_waitsem);//modifid by weizheng 2013-12-23
                } else if((req_type & AMP_REQUEST) && req->req_resent) {
                    __amp_add_resend_req(req);
                }
                goto AGAIN;
            }
        }
        /*ok, the page is prepared, receive the data now*/
        err = AMP_OP(conn_type, proto_recvdata)(&conn->ac_sock,
                                                        &conn->ac_remote_addr,
                                                        sizeof(conn->ac_remote_addr),
                                                        niov,
                                                        iov,
                                                        0);
    
        if (err < 0) {
            amp_unlock(&conn->ac_lock);

            AMP_ERROR("__amp_serve_in_thread: recv data error, conn:%p, err:%d\n", \
                                  conn, err);
            AMP_DMSG("__amp_serve_in_thread: it's tcp, state:%d\n", conn->ac_state);
            if (iov != req->req_iov)  /*free all the alloced space*/
            {
                conn->ac_freepage_cb(niov, iov);
            }

            amp_lock(&req->req_lock);
            req->req_conn = NULL;
            amp_unlock(&req->req_lock);

            amp_lock(&amp_reconn_conn_list_lock);
            amp_lock(&conn->ac_lock);
            if (conn->ac_state == AMP_CONN_OK) {
                AMP_ERROR("__amp_serve_in_thread: reset conn:%p\n", conn);
                AMP_OP(conn_type, proto_disconnect)((void *)&conn->ac_sock);
                if (conn->ac_need_reconn) {
                    AMP_ERROR("__amp_serve_in_thread: set conn:%p recover\n", conn);
                    conn->ac_state = AMP_CONN_RECOVER;
                } else {
                    AMP_ERROR("__amp_serve_in_thread: close conn:%p\n", conn);
                    conn->ac_state = AMP_CONN_CLOSE;            
                }
                if (list_empty(&conn->ac_reconn_list)){
                    list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
                    amp_sem_up(&amp_reconn_sem);
                }
            } else 
                AMP_ERROR("__amp_serve_in_thread: conn:%p, state:%d\n",
                                          conn, conn->ac_state);
            amp_unlock(&conn->ac_lock);
            amp_unlock(&amp_reconn_conn_list_lock);
            
            if (req_type & AMP_REPLY) {
                amp_lock(&req->req_lock);
                req->req_error = -EPROTO;
                amp_unlock(&req->req_lock);
                amp_sem_up(&req->req_waitsem); 
            }                       

            if(conn_type == AMP_CONN_TYPE_TCP) 
                amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
            else
                amp_free(msgp, AMP_MAX_MSG_SIZE);
            amp_sem_up(&conn->ac_recvsem);
            
            if (conn_type == AMP_CONN_TYPE_TCP) {
                goto AGAIN;
            }
        }

        if (req->req_iov != iov) {
            req->req_niov = niov;
            req->req_iov = iov;
        }       

    }
    if (req_type & AMP_REQUEST) {
        AMP_DMSG("__amp_serve_in_thread: it's a request, queue it\n");
        req->req_msg = msgp;
        req->req_type = req_type;
        req->req_conn = conn;
        req->req_msglen = msgp->amh_size + sizeof(amp_message_t);
        AMP_DMSG("__amp_serve_in_thread: before queue request, req:%p\n",
              req);
        AMP_DMSG("__amp_serve_in_thread: conn:%p, ac_queue_cb:%p\n", conn, conn->ac_queue_cb);
        if (!conn->ac_queue_cb) 
            AMP_ERROR("__amp_serve_in_thread: no queue callback for conn:%p\n", conn);
        else
            conn->ac_queue_cb(req);
    } else if (req_type & AMP_REPLY) {

        if (req->req_need_ack) {            
            AMP_DMSG("__amp_serve_in_thread: need wait it\n");
            req->req_reply = msgp;
            req->req_replylen = msgp->amh_size + sizeof(amp_message_t);
            if(__amp_reqheader_equal(req->req_msg, msgp)){
                struct timeval  tv_now;
                amp_gettimeofday(&tv_now);
                req->req_msg->amh_send_ts.sec = tv_now.tv_sec;
                req->req_msg->amh_send_ts.usec = tv_now.tv_usec;
                amp_sem_up(&req->req_waitsem);
            }
        } else {
            AMP_DMSG("__amp_serve_in_thread: needn't ack, free req_msg\n");     
            if(conn_type == AMP_CONN_TYPE_TCP) 
                amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
            else
                amp_free(msgp, AMP_MAX_MSG_SIZE);           
        } 
    }
    amp_unlock(&conn->ac_lock);
    
ADD_BACK:
    AMP_DMSG("__amp_serve_in_thread: add back\n");
    amp_sem_up(&conn->ac_recvsem);
    __amp_add_to_listen_fdset(conn);
    goto AGAIN;

EXIT:
    AMP_DMSG("__amp_serve_in_thread: leave: %ld\n",    pthread_self());
    amp_sem_up(&threadp->at_downsem);
    return 0;


}

/*
 * the callback of serving reconnection service.
 * 
 * this thread need lock the conn and sendsem
 * to ensure that we only allow one reconnection thread.
 */
void * __amp_wakeup_reconn_thread(void *argv)
{
    amp_thread_t *threadp = NULL;
    amp_s32_t remain_seconds;
    remain_seconds = AMP_CONN_RECONN_INTERVAL;

    threadp = (amp_thread_t *)argv;
    threadp->at_isup= 1;
    amp_sem_up(&threadp->at_startsem);
AGAIN:
    if (threadp->at_shutdown)
        goto EXIT;
    remain_seconds = sleep(remain_seconds);
    if (!remain_seconds) {
        remain_seconds = AMP_CONN_RECONN_INTERVAL;
        amp_sem_up(&amp_reconn_sem);
        amp_sem_up(&amp_netmorn_sem);
    }
    else if (remain_seconds < 0) {
        AMP_ERROR("__amp_wakeup_recon_thread: sleep returned:%d\n", remain_seconds);
        remain_seconds = AMP_CONN_RECONN_INTERVAL;
    }

    goto AGAIN;
EXIT:
    amp_sem_up(&threadp->at_downsem);
    amp_sem_up(&amp_reconn_finalize_sem);
    return NULL;
}


void* __amp_reconn_thread(void *argv)
{
    amp_thread_t *threadp = NULL;
    amp_comp_context_t *ctxt = NULL;
    amp_connection_t *conn; 
    amp_connection_t *tmpconn = NULL;
    amp_request_t *req = NULL;
    amp_comp_conns_t *cmp_conns = NULL;
    conn_queue_t   *cnq = NULL;
    struct list_head  *head = NULL;
    struct list_head  *pos = NULL;
    struct list_head  *nxt = NULL;

    amp_s32_t err = 0;
    amp_s32_t type;
    amp_s32_t id;
    amp_s32_t remain_times;
    amp_s32_t i;
    amp_u32_t seqno;
    amp_u32_t has_valid_conn;
    amp_u32_t no_conns;
    amp_u32_t force_notify;
    amp_u64_t last_conn_time = 0;
    sigset_t  thread_sigs;

    
    AMP_ENTER("__amp_reconn_thread: enter, %ld\n", pthread_self());

    threadp = (amp_thread_t *)argv;
    seqno = threadp->at_seqno;

    sigemptyset(&thread_sigs);
    sigaddset(&thread_sigs, SIGTERM);
    pthread_sigmask(SIG_UNBLOCK, &thread_sigs, NULL);

    threadp->at_isup= 1;
    amp_sem_up(&threadp->at_startsem);

    /*
     * doing main work
     */
DOWN_SEM:
    amp_sem_down(&amp_reconn_sem);
    amp_sem_down(&amp_reconn_finalize_sem);
    AMP_DMSG("__amp_reconn_thread: wake up\n");
    if (threadp->at_shutdown) 
        goto EXIT;
    
    while(1){
        
        amp_lock(&amp_reconn_conn_list_lock);
        if(list_empty(&amp_reconn_conn_list)){
            amp_unlock(&amp_reconn_conn_list_lock);
            break;
        }
        conn = list_entry(amp_reconn_conn_list.next, amp_connection_t, ac_reconn_list );
        amp_lock(&conn->ac_lock);
        if(!list_empty(&conn->ac_reconn_list)){
            list_del_init(&conn->ac_reconn_list);
        }else{
            amp_unlock(&conn->ac_lock);
            amp_unlock(&amp_reconn_conn_list_lock);
            continue;
        }
        amp_unlock(&conn->ac_lock);
        amp_unlock(&amp_reconn_conn_list_lock);
      
        AMP_ERROR("__amp_reconn_thread: to handle conn:%p\n", conn);
        ctxt = conn->ac_ctxt;
        /*
         * insert by weizheng 2014-1-2 remove the sock form listen set in time
         */

        if (conn->ac_remote_port == 0) {
            AMP_ERROR("__amp_reconn_thread: the remote port of conn: %p is zero\n", conn);
            amp_lock(&conn->ac_lock);
            conn->ac_state = AMP_CONN_BAD;
            amp_unlock(&conn->ac_lock);
            __amp_dequeue_conn(conn, ctxt);
            __amp_free_conn(conn);//canot delete
            continue;
        }
        
        
        if ((conn->ac_state == AMP_CONN_CLOSE) && !conn->ac_need_reconn) {
            /*
             * if we're server, then we just free it.
             */
            AMP_ERROR("__amp_reconn_thread: close conn:%p, before dequeue conn, refcount:%d\n", \
                                  conn, conn->ac_refcont);
            amp_lock(&conn->ac_lock);
            conn->ac_state = AMP_CONN_BAD;
            conn->ac_weight = 0;
            amp_unlock(&conn->ac_lock);
            __amp_dequeue_conn(conn, ctxt);
            __amp_free_conn(conn);//cannot delete
            continue;
        }

        AMP_ERROR("__amp_reconn_thread: need reconnect for conn:%p\n", conn);
        
        err = __amp_connect_server(conn);
        if (err < 0) {
            AMP_ERROR("__amp_reconn_thread: conn:%p, remaintimes:%d, error, err:%d\n", \
                                  conn, conn->ac_remain_times, err);
            has_valid_conn = 0;
            force_notify = 0;
            no_conns = 1;
            
            if (conn->ac_remain_times <= 0) { 
                conn->ac_remain_times = AMP_CONN_RECONN_MAXTIMES; 
                force_notify = 1;
                /*AMP_ERROR("__amp_reconn_thread: conn:%p cannot connect server, closed and free !!!\n",conn);
                amp_lock(&conn->ac_lock);
                conn->ac_state = AMP_CONN_BAD;
                amp_unlock(&conn->ac_lock); 
                __amp_dequeue_conn(conn, ctxt);
                __amp_free_conn(conn);//cannot delete
                continue;*/
            }

            amp_lock(&amp_reconn_conn_list_lock);
            amp_lock(&conn->ac_lock);
            
            conn->ac_remain_times --;
            if (list_empty(&conn->ac_reconn_list))
                list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
            
            amp_unlock(&conn->ac_lock);
            amp_unlock(&amp_reconn_conn_list_lock);

            type = conn->ac_remote_comptype;
            id = conn->ac_remote_id;
            remain_times = conn->ac_remain_times;


            if (ctxt && (ctxt->acc_conns) && (remain_times >= (AMP_CONN_RECONN_MAXTIMES - 1))) {
                cmp_conns = &(ctxt->acc_conns[type]);
                if (!cmp_conns->acc_remote_conns)
                    continue;

                pthread_mutex_lock(&(cmp_conns->acc_remote_conns[id].queue_lock));
                cnq = &(cmp_conns->acc_remote_conns[id]);
                head = &(cmp_conns->acc_remote_conns[id].queue);    
    
                if (list_empty(head)) {
                    AMP_ERROR("__amp_reconn_thread: no connection corresponding to type:%d, id:%d\n", type, id);
                    pthread_mutex_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));
                    continue;
                }

                if (cnq->active_conn_num < 0) {
                    AMP_ERROR("__amp_reconn_thread: type:%d, id:%d, active_conn_num:%d ,wrong\n", \
                                                  type, id, cnq->active_conn_num);
                    pthread_mutex_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));
                    continue;
                }

                for(i=0; i<=cnq->active_conn_num; i++) {
                    tmpconn = cnq->conns[i];
                    if (tmpconn && (tmpconn != conn)) {
                        if(tmpconn->ac_state == AMP_CONN_OK){
                            has_valid_conn ++;
                            force_notify = 1;
                        }
                    }
                    no_conns = 0;
                }

                pthread_mutex_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));
                AMP_WARNING("__amp_reconn_thread: type:%d,id:%d,has_vaid_conn:%d\n", \
                                          type, id, has_valid_conn);
                
                __amp_remove_resend_reqs(conn, force_notify, no_conns);
                __amp_remove_waiting_reply_reqs(conn, force_notify, no_conns);
                
            }
            sleep(1);
            continue;       
        }

        amp_lock(&conn->ac_lock);
        conn->ac_state = AMP_CONN_OK;
        last_conn_time = conn->ac_last_reconn;
        conn->ac_last_reconn = time(NULL);
        last_conn_time = conn->ac_last_reconn - last_conn_time;
        conn->ac_remain_times = AMP_CONN_RECONN_MAXTIMES;
        amp_unlock(&conn->ac_lock);
        AMP_WARNING("amp_reconn_thread: reconn success, connection lost during %lld seconds\n",last_conn_time);
        
        amp_lock(&amp_waiting_reply_list_lock);
        list_for_each_safe(pos, nxt, &amp_waiting_reply_list) 
        {
            req = list_entry(pos, amp_request_t, req_list);
            if((req->req_remote_type == conn->ac_remote_comptype) && 
                (req->req_remote_id == conn->ac_remote_id) &&
                (req->req_conn == NULL ||  req->req_conn == conn))
            {

                amp_lock(&req->req_lock);
                list_del_init(&req->req_list);
                amp_unlock(&req->req_lock);
                __amp_add_resend_req(req);
            }
        }
        amp_unlock(&amp_waiting_reply_list_lock);

        /*
         * revoke all need resend requests
         */
        __amp_revoke_resend_reqs (conn);
        __amp_add_to_listen_fdset(conn);

        if (threadp->at_shutdown) 
            goto EXIT;
    }

    if (threadp->at_shutdown)
        goto EXIT;
    amp_sem_up(&amp_reconn_finalize_sem);
    goto DOWN_SEM;      


EXIT:
    AMP_LEAVE("__amp_reconn_thread: leave: %ld\n", pthread_self());
    amp_sem_up(&threadp->at_downsem);
    return 0;
}

/*
 * the callback of network mornitor thread.
 * 
 */
struct {
    struct cmsghdr cm;
    struct in_pktinfo ipi;
} scmsg = {{sizeof(struct cmsghdr) + sizeof(struct in_pktinfo), SOL_IP, IP_PKTINFO}, {0, }};
#ifndef ICMP_FILTER
#define ICMP_FILTER  1
struct icmp_filter {
    amp_u32_t   data;
};
#endif

void __amp_install_filter(amp_s32_t icmp_sock, amp_s32_t ident)
{
    static struct sock_filter insns[] = {
            BPF_STMT(BPF_LDX|BPF_B|BPF_MSH, 0), 
            BPF_STMT(BPF_LD|BPF_H|BPF_IND, 4), 
            BPF_JUMP(BPF_JMP|BPF_JEQ|BPF_K, 0xAAAA, 0, 1), 
            BPF_STMT(BPF_RET|BPF_K, ~0U),
            BPF_STMT(BPF_LD|BPF_B|BPF_IND, 0), 
            BPF_JUMP(BPF_JMP|BPF_JEQ|BPF_K, ICMP_ECHOREPLY, 1, 0), 
            BPF_STMT(BPF_RET|BPF_K, 0xFFFFFFF), 
            BPF_STMT(BPF_RET|BPF_K, 0) 
        };
    static struct sock_fprog filter = {
        sizeof insns / sizeof(insns[0]),
        insns
    };

    /* Patch bpflet for current identifier. */
    insns[2] = (struct sock_filter)BPF_JUMP(BPF_JMP|BPF_JEQ|BPF_K, htons(ident), 0, 1);

    if (setsockopt(icmp_sock, SOL_SOCKET, SO_ATTACH_FILTER, &filter, sizeof(filter)))
        AMP_ERROR("install_filter: failed to install socket filter\n");
}


void *
__amp_netmorn_thread (void *argv)
{
    amp_thread_t *threadp = NULL;
    amp_connection_t   *conn = NULL;
    amp_comp_context_t *ctxt = NULL;
    amp_s32_t        err = 0;
    amp_u32_t     seqno;
    struct list_head *head = NULL;
    amp_s8_t         *sndbufp = NULL;
    amp_s8_t         *rcvbufp = NULL;
    conn_queue_t     *cnq = NULL;
    amp_u32_t        type;
    amp_u32_t        id;
    amp_u32_t       *raddr = NULL;
    amp_u32_t       *failaddr = NULL;
    amp_u32_t        raddr_num;
    amp_u32_t        failaddr_num;
    amp_u32_t        size;
    amp_s32_t        i, j;
    amp_s32_t        icmp_sock = -1;
    struct icmphdr   *icp = NULL;
    struct sockaddr_in whereto;
    struct sockaddr_in target;
    struct iovec      iov;
    struct msghdr     msg;
    struct timeval    tv;
    struct ip        *ip;
    amp_s32_t         ping_try_times;
    amp_s32_t         iphdrlen;
    amp_u32_t         datalen = 64;
    amp_s32_t         hold;
    amp_u16_t         ident = 0;
    amp_u16_t         seq = 0;
    struct icmp_filter  filt;
    amp_u32_t         sleep_time;
    
    
    AMP_ENTER("__amp_netmorn_thread: enter, %ld\n", pthread_self());

    threadp = (amp_thread_t *)argv;
    seqno = threadp->at_seqno;
    ctxt = (amp_comp_context_t *)(threadp->at_provite);

    threadp->at_isup= 1;

    threadp->at_thread_id = pthread_self();

    amp_sem_up(&threadp->at_startsem);

    /*
     * doing main work
     */
    size = sizeof(amp_u32_t) * AMP_CONN_MAXIP_PER_NODE;
    raddr = (amp_u32_t *)malloc(size);
    if (!raddr) {
        AMP_ERROR("__amp_netmorn_thread: alloc for raddr error\n");
        goto EXIT;
    }
    memset(raddr, 0, size);
    failaddr = (amp_u32_t *)malloc(size);
    if (!failaddr) {
        AMP_ERROR("__amp_netmorn_thread: alloc for fail addr error\n");
        goto EXIT;
    }
    memset(failaddr, 0, size);
    raddr_num = 0;
    failaddr_num = 0;

    ident = ctxt->acc_this_type << 8;
    ident += ctxt->acc_this_id;
    
    AMP_DMSG("__amp_netmorn_thread: this type:%d, this id:%d, ident:%d\n",\
                 ctxt->acc_this_type, ctxt->acc_this_id, ident);
    
    sndbufp = (amp_s8_t *)malloc(4096);
    if (!sndbufp) {
        AMP_ERROR("__amp_netmorn_thread: alloc for buffer error\n");
        goto EXIT;
    }
    
    rcvbufp = (amp_s8_t *)malloc(4096);
    if (!rcvbufp) {
        AMP_ERROR("__amp_netmorn_thread: alloc for buffer error\n");
        goto EXIT;
    }

DOWN_SEM:
    amp_sem_down(&amp_netmorn_sem);
    AMP_DMSG("__amp_netmorn_thread: wake up\n");

    for (type=0; type<AMP_MAX_COMP_TYPE; type++) {
        if (!ctxt->acc_conns[type].acc_remote_conns) {
            AMP_DMSG("__amp_netmorn_thread: no acc_remote_conns for type:%d\n", type);
            continue;
        }
        if (!ctxt->acc_conns[type].acc_alloced_num) {
            AMP_DMSG("__amp_netmorn_thread: acc_alloced_num is zero for type:%d\n", type);
            continue;
        }
        
        for (id=1; id<ctxt->acc_conns[type].acc_alloced_num; id++) {
            head = &(ctxt->acc_conns[type].acc_remote_conns[id].queue);
            if(list_empty(head)) {
                continue;
            }
            AMP_DMSG("__amp_netmorn_thread: type:%d, id:%d\n", type, id);

            pthread_mutex_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
            cnq = &(ctxt->acc_conns[type].acc_remote_conns[id]);
            raddr_num = 0;
            for (i=0; i<=cnq->active_conn_num; i++) {
                conn = cnq->conns[i];
                if (!conn)
                    continue;

                AMP_DMSG("__amp_netmorn_thread: conn:%p\n", conn);
                if (conn->ac_state == AMP_CONN_OK) {
                    for (j=0; j<raddr_num; j++) {
                        if (raddr[j] == conn->ac_remote_ipaddr) 
                            break;
                    }
                    if (j>=raddr_num) {
                        AMP_DMSG("__amp_netmorn_thread: ipaddr:%d\n", \
                                                         conn->ac_remote_ipaddr);

                        raddr[raddr_num] = conn->ac_remote_ipaddr;
                        raddr_num ++;
                        if (raddr_num > AMP_CONN_MAXIP_PER_NODE) {
                            AMP_ERROR("__amp_netmorn_thread: radd_num:%d, too large\n", \
                                                                  raddr_num);
                            break;
                        }
                    }
                }
            }
            pthread_mutex_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
            
            /*do icmp*/
            failaddr_num = 0;
            for (i=0; i<raddr_num; i++) {
                ping_try_times =1;
                icmp_sock = socket (AF_INET, SOCK_RAW, IPPROTO_ICMP);
                if (icmp_sock < 0) {
                    AMP_ERROR("__amp_netmorn_thread: create icmp sock error, err:%d\n", errno);
                    goto EXIT;
                } 
                hold = 65536;
                err = setsockopt(icmp_sock, SOL_SOCKET, SO_SNDBUF, (amp_s8_t *)&hold, sizeof(hold));
                if (err) {
                    AMP_ERROR("__amp_netmorn_thread: set send buffer error, err:%d\n", errno);
                    goto EXIT;
                }
                err = setsockopt(icmp_sock, SOL_SOCKET, SO_RCVBUF, (amp_s8_t *)&hold, sizeof(hold));
                if (err) {
                    AMP_ERROR("__amp_netmorn_thread: set recv buffer error, err:%d\n", errno);
                    goto EXIT;
                }
    
                tv.tv_usec = 0;
                tv.tv_sec = AMP_ETHER_SNDTIMEO;
                err = setsockopt(icmp_sock, SOL_SOCKET, SO_SNDTIMEO, (amp_s8_t *)&tv, sizeof(tv));
                if (err) {
                    AMP_ERROR("__amp_netmorn_thread: set send timeout error, err:%d\n", errno);
                    goto EXIT;
                }

                tv.tv_sec = AMP_ETHER_RCVTIMEO;
                err = setsockopt(icmp_sock, SOL_SOCKET, SO_RCVTIMEO, (amp_s8_t *)&tv, sizeof(tv));
                if (err) {
                    AMP_ERROR("__amp_netmorn_thread: set recv timeout error, err:%d\n", errno);
                    goto EXIT;
                }
                filt.data = ~((1<<ICMP_SOURCE_QUENCH)|
                                              (1<<ICMP_TIME_EXCEEDED)|
                                              (1<<ICMP_PARAMETERPROB)|
                                              (1<<ICMP_REDIRECT)|
                                              (1<<ICMP_ECHOREPLY));
    
                err = setsockopt(icmp_sock, SOL_RAW, ICMP_FILTER, (char*)&filt, sizeof(filt));
                if (err) {
                    AMP_ERROR("__amp_netmorn_thread: set icmp filter error, err:%d\n", errno);
                    goto EXIT;
                }
                __amp_install_filter(icmp_sock, ident);

                gettimeofday(&tv, NULL);
                memset(&whereto, 0, sizeof(whereto));
                whereto.sin_family = AF_INET;
                whereto.sin_addr.s_addr = htonl(raddr[i]);
                memset(sndbufp, 0, 4096);
                icp = (struct icmphdr *)sndbufp;
                icp->type = ICMP_ECHO;
                icp->code = 0;
                icp->checksum = 0;
                seq ++;
                icp->un.echo.id = ident;
                icp->un.echo.sequence = seq;
                datalen = 64;
                icp->checksum = __amp_nm_cksum((amp_u16_t *)icp, datalen);
                iov.iov_len = datalen;
                iov.iov_base = sndbufp;
                msg.msg_name = &whereto;
                msg.msg_namelen = sizeof(whereto);
                msg.msg_iov = &iov;
                msg.msg_iovlen = 1;
                msg.msg_control = &scmsg;
                msg.msg_controllen = sizeof(scmsg);
                msg.msg_flags = 0;
                AMP_DMSG("__amp_netmorn_thread: raddr:%d, id:%d, seq:%d\n", raddr[i], ident, seq);

                sleep_time = 0;
SEND_AGAIN:
                if (threadp->at_shutdown) {
                    AMP_ERROR("__amp_netmorn_thread: before sendmsg, tell us to down\n");
                    goto EXIT;
                }
                
                err = sendmsg(icmp_sock, &msg, 0);
                if (err < 0) {
                    if (errno == EAGAIN || errno == EINTR){
                        sleep(2);
                        sleep_time++;
                        if(sleep_time < AMP_CONN_RETRY_TIMES)
                            goto SEND_AGAIN;
                    }
                    AMP_ERROR("__amp_netmorn_thread: contact with node, type:%d,id:%d, error:%d\n", \
                                                  type, id, errno);
                    failaddr[failaddr_num] = raddr[i];
                    failaddr_num ++;
                    close(icmp_sock);
                    icmp_sock = -1;
                    continue;
                }
                
                /*to receive ack from remote icmp*/
                AMP_DMSG("__amp_netmorn_thread: recv icmp msg\n");


                sleep_time = 0;
RECV_AGAIN:
                if (threadp->at_shutdown) {
                    AMP_ERROR("__amp_netmorn_thread: before recvmsg, tell us to down\n");
                    goto EXIT;
                }

                memset(rcvbufp, 0, 4096);
                iov.iov_base = rcvbufp;
                iov.iov_len = 4096;
                msg.msg_name = (void *)&target;
                msg.msg_namelen = sizeof(target);
                msg.msg_iov = &iov;
                msg.msg_iovlen = 1;
                msg.msg_flags = MSG_DONTWAIT;
                msg.msg_control = NULL;
                msg.msg_controllen = 0;
                err = recvmsg(icmp_sock, &msg, MSG_DONTWAIT);
                if (err < 0) {
                    if (errno==EAGAIN || errno==EINTR) {
                        sleep_time++;
                        sleep(2);
                        if (sleep_time < AMP_CONN_RETRY_TIMES)
                            goto RECV_AGAIN;
                    }

                    if(ping_try_times++ < 5){
                        printf("__amp_netmorn_thread:ping try again, errno: %d...\n", errno);
                        sleep(1);
                        goto SEND_AGAIN;
                    }

//REMOTE_FAILED:
                    AMP_ERROR("__amp_netmorn_thread: recv from node type:%d,id:%d error:%d\n", \
                                                  type, id, err);
                    failaddr[failaddr_num] = raddr[i];
                    failaddr_num ++;
                    close(icmp_sock);
                    icmp_sock = -1;
                    continue;
                }
                AMP_DMSG("__amp_netmorn_thread: recv err:%d\n", err);

                ip = (struct ip *)rcvbufp;
                iphdrlen = ip->ip_hl << 2;
                icp = (struct icmphdr *)(rcvbufp + iphdrlen);
                if ((err - iphdrlen) < sizeof(struct icmphdr)) {
                    AMP_ERROR("__amp_netmorn_thread: icmp packet is too small, type:%d,id:%d\n", \
                                                  type, id);
                    failaddr[failaddr_num] = raddr[i];
                    failaddr_num ++;
                    close(icmp_sock);
                    icmp_sock = -1;
                    continue;
                }
                if (icp->type == ICMP_ECHO){
                    AMP_DMSG("__amp_netmorn_thread: get echo, id:%d, seq:%d\n", \
                                                 icp->un.echo.id, icp->un.echo.sequence);
                    goto RECV_AGAIN;

                }
                if ((icp->type == ICMP_ECHOREPLY) && (icp->un.echo.id == ident)) {
                    if (icp->un.echo.sequence == seq) {
                        AMP_DMSG("__amp_netmorn_thread: get reponse from peer, type:%d, id:%d, seq:%d\n", 
                                                         type, icp->un.echo.id, icp->un.echo.sequence);
                    } else {
                        AMP_ERROR("__amp_netmorn_thread: remote node, type:%d,id:%d, unreachable\n", type, id); 
                        AMP_ERROR("__amp_netmorn_thread: icp type:%d, id:%d, seq:%d\n", \
                                                          icp->type, icp->un.echo.id, icp->un.echo.sequence);
                        AMP_ERROR("__amp_netmorn_thread: but ident:%d, seq:%d\n", ident, seq);
                        failaddr[failaddr_num] = raddr[i];
                        failaddr_num ++;
                    }
                    close(icmp_sock);
                    icmp_sock = -1;
                } else {
                    AMP_ERROR("__amp_netmorn_thread: get reponse, type:%d, id:%d, seq:%d\n", \
                                                  type, icp->un.echo.id, icp->un.echo.sequence);
                    AMP_ERROR("__amp_netmorn_thread: we want:id:%d, seq:%d\n", ident, seq);
                    goto RECV_AGAIN;
                }

            }
            AMP_DMSG("__amp_netmorn_thread: type:%d, id:%d, failaddr_num:%d\n", type, id, failaddr_num);

            if (failaddr_num) {
                pthread_mutex_lock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
                /*set some connection to be reconn*/
                for (i=0; i<=cnq->active_conn_num; i++) {
                    conn = cnq->conns[i];
                    if (!conn)
                        continue;
                    
                    if (conn->ac_state != AMP_CONN_OK) {
                        continue;
                    }
                    for (j=0; j<failaddr_num; j++) {
                        if (conn->ac_remote_ipaddr == failaddr[j]) {
                            AMP_ERROR("__amp_netmorn_thread: failed conn:%p\n", conn);
                            AMP_OP(conn->ac_type, proto_disconnect)(&conn->ac_sock);
                            amp_lock(&amp_reconn_conn_list_lock);
                            amp_lock(&conn->ac_lock);

                            if (conn->ac_need_reconn)
                                conn->ac_state = AMP_CONN_RECOVER;
                            else
                                conn->ac_state = AMP_CONN_CLOSE;

                            conn->ac_datard_count = 0;
                            conn->ac_sched = 0;

                            if (list_empty(&conn->ac_reconn_list)) {
                                AMP_ERROR("__amp_netmorn_thread: add conn:%p to reconn list\n", conn);
                                list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
                            }
                            amp_unlock(&conn->ac_lock);
                            amp_unlock(&amp_reconn_conn_list_lock);
                        }
                    }
                }

                pthread_mutex_unlock(&(ctxt->acc_conns[type].acc_remote_conns[id].queue_lock));
                amp_sem_up(&amp_reconn_sem);//process the invalid conn in time
            }
        }
    }

    /*
     * sleep for some time
     */
    if (threadp->at_shutdown)
        goto EXIT;

    goto DOWN_SEM;      

EXIT:
    
    if (icmp_sock > 0) 
        close(icmp_sock);

    if (raddr)
        free(raddr);
    if (failaddr)
        free(failaddr);
    if (sndbufp)
        free(sndbufp);
    if (rcvbufp)
        free(rcvbufp);

    AMP_LEAVE("__amp_netmorn_thread: leave: %ld\n", pthread_self());
    amp_sem_up(&threadp->at_downsem);

    return NULL;
}


// by Chen Zhuan at 2009-02-05

#ifdef __AMP_LISTEN_EPOLL
//#define EPOLL_EVENT_SIZE 512 
#define EPOLL_EVENT_SIZE AMP_CONN_ADD_INCR
#endif


/*
 * thread for listen 
 */
void* __amp_listen_thread (void *argv)
{
    amp_thread_t  *threadp = NULL;
    amp_u32_t  seqno;
    amp_connection_t  *conn;
    amp_connection_t  *new_connp = NULL;
    amp_u32_t  conn_type;  /*tcp or udp*/
    amp_s32_t  err = 0;
    amp_comp_context_t  *ctxt = NULL;
    amp_s32_t  listen_sockfd;
    amp_u32_t  i;
    
// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------
#ifdef __AMP_LISTEN_SELECT

    fd_set  cur_fdset;
    amp_u32_t  maxfd;

#endif
#ifdef __AMP_LISTEN_POLL

    amp_u32_t  maxfd;

#endif
#ifdef __AMP_LISTEN_EPOLL

    amp_s32_t  fd = 0;
    amp_s32_t  nfds = 0;
    struct epoll_event  /*ev,*/ events[EPOLL_EVENT_SIZE];

#endif
// -----------------------------------------------------------------

    AMP_ENTER("__amp_listen_thread: enter, %ld\n", pthread_self());
    threadp = (amp_thread_t *)argv;
    seqno = threadp->at_seqno;

    threadp->at_isup= 1;

    threadp->at_thread_id = pthread_self();
    amp_sem_up(&threadp->at_startsem);

    ctxt = (amp_comp_context_t *) (threadp->at_provite);
    if (!ctxt) {
        AMP_ERROR("__amp_listen_thread: no ctxt\n");
        goto EXIT;
    }

    conn_type = AMP_CONN_TYPE_TCP;

    while (1)
    {
        pthread_mutex_lock(&ctxt->acc_lock);
        conn = ctxt->acc_listen_conn;
        listen_sockfd = amp_listen_sockfd;
// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------
#ifdef __AMP_LISTEN_SELECT
        cur_fdset = ctxt->acc_readfds;
        maxfd = ctxt->acc_maxfd;
#endif
#ifdef __AMP_LISTEN_POLL
        maxfd = ctxt->acc_maxfd;
#endif
// -----------------------------------------------------------------
        pthread_mutex_unlock(&ctxt->acc_lock);

// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------
#ifdef __AMP_LISTEN_SELECT
        err = select( ctxt->acc_maxfd+1, &cur_fdset, NULL, NULL, NULL );
#endif
#ifdef __AMP_LISTEN_POLL
        err = poll( ctxt->acc_poll_list, ctxt->acc_maxfd+1, -1 );
        // amp_fd_print( ctxt->acc_poll_list, ctxt->acc_poll_size );
        // AMP_ERROR( "ctxt->acc_poll_size=%d, poll return %d\n", ctxt->acc_poll_size, err );
#endif
#ifdef __AMP_LISTEN_EPOLL
        nfds = epoll_wait( ctxt->acc_epfd, events, EPOLL_EVENT_SIZE, -1 );
        err = nfds;
#endif
// -----------------------------------------------------------------

        if (err < 0) {
            AMP_WARNING("__amp_listen_thread: select returned:%d\n", err);
            continue;
        }

// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------
#ifdef __AMP_LISTEN_SELECT
        if( FD_ISSET( ctxt->acc_srvfd, &cur_fdset ) ) {
            amp_s32_t  notifyid;
            read(ctxt->acc_srvfd, &notifyid, sizeof(amp_s32_t));
            if (threadp->at_shutdown)
                goto EXIT;
            //FD_CLR(ctxt->acc_srvfd, &cur_fdset);
        }
#endif
#ifdef __AMP_LISTEN_POLL
        if( amp_poll_fd_isset( ctxt->acc_srvfd, ctxt->acc_poll_list ) ) {
            amp_s32_t  notifyid;
            read(ctxt->acc_srvfd, &notifyid, sizeof(amp_s32_t));
            if (threadp->at_shutdown)
                goto EXIT;
            //amp_fd_clr( ctxt->acc_srvfd, ctxt->acc_poll_list, ctxt->acc_poll_size );
        }
#endif
#ifdef __AMP_LISTEN_EPOLL
        if( amp_epoll_fd_isset( ctxt->acc_srvfd, events, nfds ) ) {
            amp_s32_t  notifyid;
            read( ctxt->acc_srvfd, &notifyid, sizeof(amp_s32_t) );
            if( threadp->at_shutdown )
                goto EXIT;
            /*for( i=0; i<nfds; i++ ) {
                if( events[i].data.fd == ctxt->acc_srvfd ) {
                    events[i].data.fd = -1;
                }
            }*/
        }
#endif
// -----------------------------------------------------------------


// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------
#ifdef __AMP_LISTEN_SELECT
        if((listen_sockfd >= 0) && (FD_ISSET(listen_sockfd, &cur_fdset)))
#endif
#ifdef __AMP_LISTEN_POLL
        if((listen_sockfd >= 0) && (amp_poll_fd_isset( listen_sockfd, ctxt->acc_poll_list )))
#endif
#ifdef __AMP_LISTEN_EPOLL
        if((listen_sockfd >= 0) && (amp_epoll_fd_isset( listen_sockfd, events, nfds )))
#endif
// -----------------------------------------------------------------

        {
ALLOC_AGAIN:
            new_connp = NULL;
            err = __amp_alloc_conn(&new_connp);
            if (err)  {
                AMP_ERROR("__amp_listen_thread: alloc new connection error, err:%d\n", err);
                goto ALLOC_AGAIN;
            }

            new_connp->ac_type = AMP_CONN_TYPE_TCP;
            new_connp->ac_need_reconn = 1;//by weizheng 2013-11-19,reconn
            new_connp->ac_ctxt = ctxt;
            new_connp->ac_this_id = ctxt->acc_this_id;
            new_connp->ac_this_type = ctxt->acc_this_type;
            new_connp->ac_allocpage_cb = conn->ac_allocpage_cb;
            new_connp->ac_freepage_cb = conn->ac_freepage_cb;
            new_connp->ac_queue_cb = conn->ac_queue_cb;
            err = __amp_accept_connection (&listen_sockfd, new_connp);

            if (err < 0) {
                AMP_ERROR("__amp_listen_thread: accept a new connection error, err:%d\n", err);
                __amp_free_conn(new_connp);
                goto LISTEN_DATA;
            }
            __amp_dequeue_invalid_conn(new_connp,ctxt);//insert by weizheng 2014-01-19, before accept the new conn, dequeue the invalid conn
            err = __amp_enqueue_conn(new_connp, ctxt);
            if (err < 0)  {
                AMP_ERROR("__amp_listen_thread: enqueue the connection error, err = %d\n", err);
                AMP_OP(conn_type, proto_disconnect)(&new_connp->ac_sock);
                memset(new_connp, 0, sizeof(amp_connection_t));
                __amp_dequeue_conn(new_connp,ctxt);
                __amp_free_conn(new_connp);
                goto LISTEN_DATA;
            }
            __amp_add_to_listen_fdset(new_connp);
            __amp_revoke_resend_reqs (new_connp);
            AMP_DMSG("__amp_listen_thread: finish accept conn:%p, remote_id:%d, remote_type:%d, sock:%d,refcount:%d\n", \
                                  new_connp, \
                                  new_connp->ac_remote_id, \
                                  new_connp->ac_remote_comptype,\
                                  new_connp->ac_sock, \
                                  new_connp->ac_refcont);
        }
LISTEN_DATA:
// by Chen Zhuan at 2009-02-05
// -----------------------------------------------------------------
#ifdef __AMP_LISTEN_SELECT
        for( i=0; i<(maxfd+1); i++ ) {
            amp_connection_t *tmpconn = NULL;           
            if (i == listen_sockfd)
                continue;
            if (i == ctxt->acc_srvfd)
                continue;
            if(!FD_ISSET(i, &cur_fdset)) 
                continue;
            
            pthread_mutex_lock(&ctxt->acc_lock);
            FD_CLR(i, &ctxt->acc_readfds);
            pthread_mutex_unlock(&ctxt->acc_lock);

            tmpconn = ctxt->acc_conn_table[i];
            if (!tmpconn) {
                AMP_ERROR("__amp_listen_thread: no connection for fd:%d\n", i);
                continue;
            }
            
            amp_lock(&amp_dataready_conn_list_lock);
            amp_lock(&tmpconn->ac_lock);
            if(list_empty(&tmpconn->ac_dataready_list))
                list_add_tail(&tmpconn->ac_dataready_list, &amp_dataready_conn_list);
            amp_unlock(&tmpconn->ac_lock);
            amp_unlock(&amp_dataready_conn_list_lock);
            amp_sem_up(&amp_process_in_sem);
        }
#endif
#ifdef __AMP_LISTEN_POLL
        for( i=0; i<(maxfd+1); i++ ) {
            amp_connection_t *tmpconn = NULL;
            if( i == listen_sockfd )
                continue;
            if( i == ctxt->acc_srvfd )
                continue;
            if( !amp_poll_fd_isset( i, ctxt->acc_poll_list ) )
                continue;
            
            pthread_mutex_lock(&ctxt->acc_lock);
            amp_poll_fd_clr( i, ctxt->acc_poll_list );
            pthread_mutex_unlock(&ctxt->acc_lock);

            tmpconn = ctxt->acc_conn_table[i];
            if(!tmpconn){
                AMP_ERROR("__amp_listen_thread: no connection for fd:%d\n", i);
                continue;
            }
            
            amp_lock(&amp_dataready_conn_list_lock);
            amp_lock(&tmpconn->ac_lock);
            if(list_empty(&tmpconn->ac_dataready_list))
                list_add_tail(&tmpconn->ac_dataready_list, &amp_dataready_conn_list);
            amp_unlock(&tmpconn->ac_lock);
            amp_unlock(&amp_dataready_conn_list_lock);
            amp_sem_up(&amp_process_in_sem);
        }
#endif
#ifdef __AMP_LISTEN_EPOLL
        for( i=0; i<nfds; i++ ) {
            fd = events[i].data.fd;
            amp_connection_t *tmpconn = NULL;
            if( fd < 0 )
                continue;
            if( fd == listen_sockfd )
                continue;
            if( fd == ctxt->acc_srvfd )
                continue;

            pthread_mutex_lock(&ctxt->acc_lock);
            amp_epoll_fd_clear( fd, ctxt->acc_epfd );
            pthread_mutex_unlock(&ctxt->acc_lock);

            tmpconn = ctxt->acc_conn_table[fd];
            if (!tmpconn) {
                AMP_ERROR( "__amp_listen_thread: no connection for fd:%d, i=%d\n", fd, i );
                continue;
            }
            
            amp_lock(&amp_dataready_conn_list_lock);
            amp_lock(&tmpconn->ac_lock);
            if(list_empty(&tmpconn->ac_dataready_list))
                list_add_tail(&tmpconn->ac_dataready_list, &amp_dataready_conn_list);
            amp_unlock(&tmpconn->ac_lock);
            amp_unlock(&amp_dataready_conn_list_lock);
            amp_sem_up(&amp_process_in_sem);
        }
#endif
// -----------------------------------------------------------------

    }
    
EXIT:
    AMP_LEAVE("__amp_listen_thread: leave: %ld\n", pthread_self());
    amp_sem_up(&threadp->at_downsem);
    return NULL;
}


/* 
 * thread fundation initialization
 */ 
int
__amp_threads_init ()
{
    amp_s32_t err = 0;

    AMP_ENTER("__amp_threads_init: enter\n");

    amp_sem_init_locked(&amp_process_in_sem);
    amp_sem_init_locked(&amp_process_out_sem);
    amp_sem_init_locked(&amp_reconn_sem);
    amp_sem_init_locked(&amp_netmorn_sem);
    amp_sem_init(&amp_reconn_finalize_sem);
    
    
    amp_srvin_thread_num = 0;
    amp_srvout_thread_num = 0;
    amp_reconn_thread_num = 0;
    amp_wakeup_thread_num = 0;
    amp_listen_sockfd = -1;

    amp_lock_init(&amp_threads_lock);
    
    amp_srvin_threads = 
        (amp_thread_t *)amp_alloc(sizeof(amp_thread_t) * AMP_MAX_THREAD_NUM);
    if (!amp_srvin_threads) {
        AMP_ERROR("__amp_threads_init: malloc for srvin threads error\n");
        err = -ENOMEM;
        goto EXIT;
    }

    amp_srvout_threads = 
        (amp_thread_t *)amp_alloc(sizeof(amp_thread_t) * AMP_MAX_THREAD_NUM);
    if (!amp_srvin_threads) {
        AMP_ERROR("__amp_threads_init: malloc for srvout threads error\n");
        err = -ENOMEM;
        amp_free(amp_srvin_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
        goto EXIT;
    }
    
    amp_reconn_threads = 
        (amp_thread_t *)amp_alloc(sizeof(amp_thread_t) * AMP_MAX_THREAD_NUM);
    if (!amp_srvin_threads) {
        AMP_ERROR("__amp_threads_init: malloc for reconn threads error\n");
        err = -ENOMEM;
        amp_free(amp_srvin_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
        amp_free(amp_srvout_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
        goto EXIT;
    }

    amp_wakeup_threads = 
        (amp_thread_t *)amp_alloc(sizeof(amp_thread_t) * AMP_MAX_THREAD_NUM);
    if (!amp_srvin_threads) {
        AMP_ERROR("__amp_threads_init: malloc for wakeup threads error\n");
        err = -ENOMEM;
        amp_free(amp_srvin_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
        amp_free(amp_srvout_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
        amp_free(amp_reconn_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
        goto EXIT;
    }

    memset(amp_srvin_threads, 0, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    memset(amp_srvout_threads, 0, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    memset(amp_reconn_threads, 0, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    memset(amp_wakeup_threads, 0, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    /*
     * startup all the threads;
     */ 
    err = __amp_start_srvin_threads ();
    if (err < 0) 
        goto ERROR;

    err = __amp_start_srvout_threads();
    if (err < 0) {
        __amp_stop_srvin_threads();
        goto ERROR;
    }
  
    err = __amp_start_reconn_threads();
    if(err < 0){
        __amp_stop_srvin_threads();
        __amp_stop_srvout_threads();
        goto ERROR;
    }

EXIT:
    AMP_LEAVE("__amp_threads_init: leave\n");
    return err;

ERROR:
    amp_free(amp_srvin_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    amp_free(amp_srvout_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    amp_free(amp_reconn_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    amp_free(amp_wakeup_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    amp_srvin_threads = NULL;
    amp_srvout_threads = NULL;
    amp_reconn_threads = NULL;
    amp_wakeup_threads = NULL;
    goto EXIT;
}

/*
 * threads finalize
 */ 
int
__amp_threads_finalize ()
{
    AMP_ENTER("__amp_threads_finalize: enter\n");

    /*
     * stop all threads
     */ 
    __amp_stop_srvin_threads();
    __amp_stop_srvout_threads();
    __amp_stop_reconn_threads();
    /*
     * free all resources
     */ 
    amp_free(amp_srvin_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    amp_free(amp_srvout_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    amp_free(amp_wakeup_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    amp_free(amp_reconn_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
    amp_srvin_threads = NULL;
    amp_srvout_threads = NULL;
    amp_reconn_threads = NULL;
    amp_wakeup_threads = NULL;

    AMP_LEAVE("__amp_threads_finalize: leave\n");
    return 0;
}

/*end of file*/

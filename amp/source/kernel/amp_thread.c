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
amp_sem_t amp_bh_sem;
amp_sem_t amp_reconn_finalize_sem;

amp_u32_t amp_srvin_thread_num;
amp_u32_t amp_srvout_thread_num;
amp_u32_t amp_reconn_thread_num;
amp_u32_t amp_bh_thread_num;

amp_thread_t  *amp_srvin_threads = NULL;
amp_thread_t  *amp_srvout_threads = NULL;
amp_thread_t  *amp_reconn_threads = NULL;
amp_thread_t  *amp_bh_threads = NULL;

amp_lock_t    amp_threads_lock;


/*
 * create kernel daemon
 */

 void
 __amp_kdaemonize (char *str)
 {
 #ifdef __KERNEL__
 
 #if (LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,63))
 	daemonize(str);
 #else
 	daemonize();
 	snprintf(current->comm, sizeof(current->comm), str);
#endif

#endif

 }
 

/* 
 * block signals
 */
void
__amp_blockallsigs ()
{
#ifdef __KERNEL__
	unsigned long flags;
	AMP_ENTER("__amp_blockallsigs: enter\n");

	AMP_SIGNAL_MASK_LOCK(current, flags);
	sigfillset(&current->blocked);
	AMP_RECALC_SIGPENDING;
	AMP_SIGNAL_MASK_UNLOCK(current,flags);
#else

    sigset_t  thread_sigs;

    sigemptyset (&thread_sigs);
    sigaddset (&thread_sigs, SIGALRM);
	sigaddset (&thread_sigs, SIGTERM);
	sigaddset (&thread_sigs, SIGHUP);
	sigaddset (&thread_sigs, SIGINT);
	sigaddset (&thread_sigs, SIGQUIT);

	pthread_sigmask (SIG_BLOCK, &thread_sigs, NULL);

#endif
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
	amp_message_t  header;
	amp_u32_t conn_type;
	amp_s32_t err = 0;
	amp_u32_t  size;
	amp_s8_t *bufp = NULL;

	AMP_DMSG("__amp_recv_msg: enter, conn:%p\n", conn);
	if (!conn->ac_sock) {
		AMP_ERROR("__amp_recv_msg: no sock for conn:%p\n", conn);
		err = -EINVAL;
		goto EXIT;
	}

	AMP_DMSG("__amp_recv_msg: before recv msg\n");
	
	AMP_DMSG("__amp_recv_msg: sock state:%d\n", conn->ac_sock->sk->sk_state);

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
			 err = AMP_OP(conn_type, proto_recvmsg)(conn->ac_sock, 
			                   			     &msgp->amh_addr,
			                                             sizeof(msgp->amh_addr),
			                                             AMP_MAX_MSG_SIZE,
			                                             msgp,
			                                             MSG_DONTWAIT);
			
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
			AMP_DMSG("__amp_recv_msg: recv header, sock:%p, sock->ops:%p\n",\
				     conn->ac_sock, conn->ac_sock->ops);
			err = AMP_OP(conn_type, proto_recvmsg)(conn->ac_sock, 
			                       NULL,
			                       0,
			                       sizeof(header),
			                       &header,
			                       MSG_DONTWAIT);
			if (err < 0) {
				if(err != -EAGAIN && err != -EINTR)
					AMP_ERROR("__amp_recv_msg: recv header error, err:%d,conn:%p,remote type:%d, id:%d\n", \
                                                   err, conn, conn->ac_remote_comptype, conn->ac_remote_id);
				goto EXIT;
			}

			size = header.amh_size;
			AMP_DMSG("__amp_recv_msg: header.amh_size:%d\n", size);

			if (size == 0) 
				AMP_WARNING("__amp_recv_msg: the size of payload is zero,conn:%p, remote type:%d, remote id:%d\n", \
                                             conn, conn->ac_remote_comptype, conn->ac_remote_id);
			
			msgp = (amp_message_t *)amp_alloc(size + sizeof(header));
			if (!msgp) {
				AMP_ERROR("__amp_recv_msg: no memory\n");
				err = -ENOMEM;
				goto EXIT;
			}
			memset(msgp, 0, size + sizeof(header));
			
			if (size)   {	
				bufp = (amp_s8_t *)msgp + sizeof(header);

				/*
				 * receive the remain message
				 */
RECV_BODY:
				AMP_DMSG("__amp_recv_msg: before recv msg, msgp:%p, bufp:%p\n", \
                                msgp, bufp);
				err = AMP_OP(conn_type, proto_recvmsg)(conn->ac_sock, 
							  NULL,
							  0,
							  size,
							  bufp,
							  MSG_WAITALL);
				if (err < 0) {
					
					if (err == -EAGAIN || err == -EINTR) {
						AMP_DMSG("__amp_recv_msg: we need again\n");
						goto RECV_BODY;
					}

					AMP_ERROR("__amp_recv_msg: receive msg error, err:%d\n", \
                                                   err);

					amp_free(msgp, size + sizeof(header));
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

			err = 0;
				
			break;
		default:
			AMP_ERROR("__amp_recv_msg: wrong conn type: %d\n", conn_type);
			break;
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
__amp_create_thread (int (*thrfunc)(void *), amp_thread_t *thread)
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

#ifdef __KERNEL__
	err = kernel_thread(thrfunc, (void*)thread, 0);
	if (err < 0) {
		AMP_ERROR("__amp_create_thread: create thread error, err:%d\n", err);
		goto EXIT;
	}
	AMP_DMSG("__amp_create_thread: create thread successfully\n");
#else
	err = pthread_create(&thread->thread_id, NULL, thrfunc, thread);
	if (err) {
		AMP_ERROR("__amp_create_thread: create thread error, err:%d\n", err);
		goto EXIT;
	}
#endif

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
	mb();
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
	AMP_LEAVE("__amp_start_srvout_thread: leave\n");
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

    if (amp_reconn_thread_started)
   	goto EXIT;
			   
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

#ifdef __KERNEL__
    init_waitqueue_head(&threadp->at_waitq);
#endif
								  
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
 * start a bh thread
 */ 
int
__amp_start_bh_thread (int seqno)
{
	amp_s32_t err = 0;
	amp_thread_t *threadp = NULL;
			   
	AMP_ENTER("__amp_start_bh_thread: enter\n");

	if ((seqno < 0) || (seqno>=AMP_MAX_THREAD_NUM)) {
		AMP_ERROR("__amp_start_bh_thread: wrong seqno:%d\n", seqno);
		err = -EINVAL;
		goto EXIT;
	}
						 
	threadp = &amp_bh_threads[seqno];
	amp_sem_init_locked(&threadp->at_startsem);
	amp_sem_init_locked(&threadp->at_downsem);
	threadp->at_seqno = seqno;
	threadp->at_shutdown = 0;
	threadp->at_isup = 0;
								   
	err = __amp_create_thread(__amp_bh_thread, threadp);
	
	amp_lock(&amp_threads_lock);

	if (!err && (seqno >= amp_bh_thread_num)) 
		amp_bh_thread_num = seqno + 1;
   
	amp_unlock(&amp_threads_lock);
	

EXIT:
	AMP_LEAVE("__amp_start_bh_thread: leave\n");
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

#ifdef __KERNEL__
    init_waitqueue_head(&threadp->at_waitq);
#endif
								  
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

/*
 * start initial number of reconn threads
 */ 
int
__amp_start_reconn_threads (void)
{
    amp_s32_t err = 0;
		  
    AMP_ENTER("__amp_start_reconn_threads: enter\n");

   
			   
    err = __amp_start_reconn_thread (0);
    				    
    AMP_LEAVE("__amp_start_reconn_threads: leave\n");
    return err;
}

/*
 * start initial number of srvout threads
 */ 
int 
__amp_start_bh_threads (void)
{
	amp_s32_t err = 0;
	amp_u32_t i;
		  
	AMP_ENTER("__amp_start_bh_threads: enter, totolnum:%d\n", amp_bh_thread_num);
			   
    	for (i=0; i<AMP_BH_THR_NUM; i++) {
		err = __amp_start_bh_thread (i);
		if (err < 0)
			goto ERROR;
	}
				    
EXIT:
    	AMP_LEAVE("__amp_start_bh_threads: leave\n");
    	return err;
					  
ERROR:
    	__amp_stop_bh_threads();
								   
	goto EXIT;
}



/*
 * start a listen thread.
 */
amp_thread_t* 
__amp_start_listen_thread(amp_connection_t *parent_conn)
{
	amp_s32_t err = 0;
	amp_thread_t *threadp = NULL;

	AMP_ENTER("__amp_start_listen_thread: enter\n");

	if (!parent_conn)  {
		AMP_ERROR("__amp_start_listen_thread: no parent connection\n");
		goto EXIT;
	}

	if (!parent_conn->ac_ctxt) {
		AMP_ERROR("__amp_start_listen_thread: no context in parent connection\n");
		goto EXIT;
	}

	if (parent_conn->ac_type != AMP_CONN_TYPE_TCP) {
		AMP_ERROR("__amp_start_listen_thread: parent conn is not of type tcp\n");
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
	threadp->at_seqno = parent_conn->ac_ctxt->acc_this_id;
	threadp->at_shutdown = 0;
	threadp->at_isup = 0;
	threadp->at_provite = parent_conn;

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


/*
 * stop a srvin thread
 */ 
/*
int
__amp_stop_srvin_thread (int seqno)
{
	amp_s32_t err = 0;
	amp_thread_t *threadp = NULL;

	AMP_ENTER("__amp_stop_srvin_thread: enter, seqno:%d\n", seqno);

	if ((seqno < 0) || (seqno>amp_srvin_thread_num)) {
		AMP_ERROR("__amp_stop_srvin_thread: wrong seqno:%d\n", seqno);
		err = -EINVAL;
		goto EXIT;
	}

	threadp = &amp_srvin_threads[seqno];
	if (!threadp->at_isup) {
		AMP_WARNING("__amp_stop_srvin_thread: to stop a stopped thread\n");
		goto EXIT;
	}

	err = __amp_stop_thread(threadp, &amp_process_in_sem);

	if (err < 0)
		goto EXIT;

	if (seqno == (amp_srvin_thread_num - 1)) {
		amp_lock(&amp_threads_lock);
		amp_srvin_thread_num --;
		amp_unlock(&amp_threads_lock);
	}
	memset(threadp, 0, sizeof(amp_thread_t));

EXIT:
	AMP_LEAVE("__amp_stop_srvin_thread: leave\n");
	return err;
}
*/
/*
 * stop a srvout thread
 */ 
/*
int
__amp_stop_srvout_thread (int seqno)
{
    amp_s32_t err = 0;
    amp_thread_t *threadp = NULL;
		    
    AMP_ENTER("__amp_stop_srvout_thread: enter, seqno:%d\n", seqno);
						 
    if ((seqno < 0) || (seqno >= amp_srvout_thread_num)) {
	AMP_ERROR("__amp_stop_srvout_thread: wrong seqno:%d\n", seqno);
	err = -EINVAL;
	goto EXIT;
    }
							  
    threadp = &amp_srvout_threads[seqno];
    if (!threadp->at_isup) {
	AMP_WARNING("__amp_stop_srvout_thread: to stop a stopped thread\n");
        goto EXIT;
    }
									    
    err = __amp_stop_thread(threadp, &amp_process_out_sem);
    if (err < 0)
	goto EXIT;
	

    if (seqno == (amp_srvout_thread_num - 1)) {
        amp_lock(&amp_threads_lock);
        amp_srvout_thread_num --;
        amp_unlock(&amp_threads_lock);
    }
    memset(threadp, 0, sizeof(amp_thread_t));
	    
EXIT:
    AMP_LEAVE("__amp_stop_srvout_thread: leave\n");
    return err;
}
*/
/*
 * stop a reconn thread.
 */ 
int
__amp_stop_reconn_thread (int seqno)
{
	amp_s32_t err = 0;
    amp_thread_t *threadp = NULL;
		    
    AMP_ENTER("__amp_stop_reconn_thread: enter\n");
						 
    if ((seqno < 0) || (seqno>=amp_reconn_thread_num)) {
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
    mb();
    if (waitqueue_active(&threadp->at_waitq))
		 wake_up(&threadp->at_waitq);
    amp_sem_down(&threadp->at_downsem);
	
    if (seqno == (amp_reconn_thread_num - 1)) {
        amp_lock(&amp_threads_lock);
        amp_reconn_thread_num --;
        amp_unlock(&amp_threads_lock);
    }
    memset(threadp, 0, sizeof(amp_thread_t));
    amp_reconn_thread_started = 0;
	    
EXIT:
    AMP_LEAVE("__amp_stop_reconn_thread: leave\n");
    return err;
}

/*
 * stop a netmorn thread.
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
	err = -EINVAL;
        goto EXIT;
    }
									    
    
    threadp->at_shutdown = 1;
    mb();
    if (waitqueue_active(&threadp->at_waitq))
	 wake_up(&threadp->at_waitq);

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

	AMP_ENTER("__amp_stop_srvin_threads: enter, total_num:%d\n", \
			   amp_srvin_thread_num);

	for(i=0; i<amp_srvin_thread_num; i++) {
                threadp = &amp_srvin_threads[i];
                threadp->at_shutdown = 1;
	}

	for(i=0; i<amp_srvin_thread_num; i++) {
		threadp = &amp_srvin_threads[i];
		if (!threadp->at_isup)
			continue;
		mb ();
		amp_sem_up(&amp_process_in_sem);
	}
	
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

	AMP_ENTER("__amp_stop_srvout_threads: enter\n");
	
	for(i=0; i<amp_srvout_thread_num; i++) {
                threadp = &amp_srvout_threads[i];
                threadp->at_shutdown = 1;
	}


	for(i=0; i<amp_srvout_thread_num; i++) {
		threadp = &amp_srvout_threads[i];
		if (!threadp->at_isup)
			continue;
		mb();
		amp_sem_up(&amp_process_out_sem);
	}
		
	for(i=0; i<amp_srvout_thread_num; i++) {
		threadp = &amp_srvout_threads[i];
		if (!threadp->at_isup)
			continue;
		down(&threadp->at_downsem);
		threadp->at_isup = 0;
	}

	amp_srvout_thread_num = 0;

	AMP_LEAVE("__amp_stop_srvout_threads: leave\n");
	return 0;
}

/*
 * stop all reconn threads.
 */ 
int
__amp_stop_reconn_threads (void)
{

	AMP_ENTER("__amp_stop_reconn_threads: enter\n");

	/*we only need one reconnection thread*/
	__amp_stop_reconn_thread (0);

	AMP_LEAVE("__amp_stop_reconn_threads: leave\n");
	return 0;
}


/*
 * stop all bh threads.
 */ 
int
__amp_stop_bh_threads (void)
{
	amp_u32_t i;
	amp_thread_t  *threadp = NULL;

	AMP_ENTER("__amp_stop_bh_threads: enter, total_num:%d\n", \
			   amp_bh_thread_num);

	for(i=0; i<amp_bh_thread_num; i++) {
                threadp = &amp_bh_threads[i];
                threadp->at_shutdown = 1;
	}

	for(i=0; i<amp_bh_thread_num; i++) {
		threadp = &amp_bh_threads[i];
		if (!threadp->at_isup)
			continue;
		mb ();
		amp_sem_up(&amp_bh_sem);
	}
	
	for(i=0; i<amp_bh_thread_num; i++) {
		threadp = &amp_bh_threads[i];
		if (!threadp->at_isup)
			continue;
		amp_sem_down(&threadp->at_downsem);
		threadp->at_isup = 0;
	}

	amp_bh_thread_num = 0;
		
	AMP_LEAVE("__amp_stop_bh_threads: leave\n");
	return 0;
}
/*
 * stop the listen thread belongs to  specific component context
 */
int
__amp_stop_listen_thread(amp_comp_context_t *ctxt)
{
	amp_s32_t   err = 0;
	amp_thread_t  *threadp = NULL;
	AMP_ENTER("__amp_stop_listen_thread: enter\n");

	threadp = ctxt->acc_listen_thread;
	if (!threadp->at_isup) {
		AMP_WARNING("__amp_stop_srvout_thread: to stop a stopped thread\n");
        goto EXIT;
    }
									    
    err = __amp_stop_thread(threadp, &ctxt->acc_listen_conn->ac_listen_sem);
    if (err < 0)
		goto EXIT;

    ctxt->acc_listen_thread = NULL;
	
EXIT:
	AMP_LEAVE("__amp_stop_listen_thread: leave\n");
	return 0;
}


/*
 * the callback of serving outcoming request threads.
 */
int
__amp_serve_out_thread (void *argv)
{
	amp_thread_t *threadp = NULL;
	amp_u32_t seqno;
	char str[16];
	amp_request_t  *req;
	amp_connection_t *conn;	
	amp_u32_t        conn_type;  /*tcp or udp*/
	amp_u32_t        req_type;    /*msg or data*/
        struct timeval thistime;
	amp_s32_t   err = 0;
	amp_u32_t  flags = 0;
	amp_u32_t  need_ack = 0;
	struct sockaddr_in  sout_addr;
	amp_u32_t  sendsize = 0;

	   
#ifdef __KERNEL__
        AMP_ENTER("__amp_serv_out_thread: enter, %d\n", current->pid);
#else
        AMP_ENTER("__amp_serv_out_thread: enter, %d\n", pthread_self());
#endif

	threadp = (amp_thread_t *)argv;
        seqno = threadp->at_seqno;

	sprintf(str, "srvout_%d", seqno);
	__amp_kdaemonize (str);
	__amp_blockallsigs ();

        threadp->at_isup = 1;
	if (!(current->flags & PF_MEMALLOC)) {
		AMP_ERROR("[%d]__amp_serv_out_thread: set PF_MEMALOC\n", current->pid);
		current->flags |= PF_MEMALLOC;
	}
#ifdef __KERNEL__
	threadp->at_task = current;
#else
	threadp->at_thread_id = pthread_self();
#endif

	amp_sem_up(&threadp->at_startsem);

	/*
	 * ok, do the main work.
	 */

AGAIN:
	AMP_DMSG("__amp_serve_out_thread: before sem\n");
	err = down_interruptible(&amp_process_out_sem);
	if (err < 0) {
		AMP_ERROR("__amp_serve_out_thread: down sem error, err:%d\n", err);
		goto EXIT;
	}
	if (threadp->at_shutdown) {
		AMP_DMSG("__amp_serve_out_thread: tell us to down\n"); 
		goto EXIT;
	}
	AMP_DMSG("__amp_serve_out_thread: get sem\n");

	amp_lock(&amp_sending_list_lock);
	if (list_empty(&amp_sending_list)) {
		amp_unlock( &amp_sending_list_lock);
		goto AGAIN;
	}
	req = list_entry(amp_sending_list.next, amp_request_t, req_list);
	list_del_init(&req->req_list);
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
                        sendsize = sendsize + (4096 << req->req_niov);
        } else {
                sendsize = req->req_replylen;
                if (req_type & AMP_DATA)
                        sendsize = sendsize + (4096 << req->req_niov);
        }

SELECT_CONN:
	
	err = __amp_select_conn(req->req_remote_type, 
		                req->req_remote_id, 
		                req->req_ctxt, 
		                &conn);
	if (err) {
		switch(err)  {
			case 2:
				AMP_WARNING("__amp_serve_out_thread: no valid conn to peer (type:%d, id:%d, err:%d)\n", \
					     req->req_remote_type, \
				             req->req_remote_id, \
				             err);
				
				if (req->req_resent) {
					__amp_add_resend_req(req);
					goto AGAIN;
				}
			case 1:
				AMP_WARNING("__amp_serve_out: no conn to peer(type:%d, id:%d, err:%d)\n", \
					     req->req_remote_type, \
					     req->req_remote_id, \
					     err);
			default:				
				req->req_error = -ENOTCONN;
				goto ERROR;

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
		AMP_ERROR("__amp_serve_out_thread: conn:%p(type:%d, id:%d), has no operations\n", \
                           conn, conn->ac_remote_comptype, conn->ac_remote_id);
	   	req->req_error = -ENOSYS;
		amp_unlock(&conn->ac_lock);
		goto ERROR;
       }	
	
	
       atomic_inc(&conn->ac_refcont);	      
       conn->ac_weight += sendsize;
       amp_unlock(&conn->ac_lock); 

       amp_sem_down(&conn->ac_sendsem);
       AMP_DMSG("__amp_serve_out_thread: after down sem, req:%p, get conn:%p\n", \
	       req, conn);

       if (conn->ac_state != AMP_CONN_OK) {
		AMP_ERROR("__amp_serve_out_thread: before send, state of conn:%p(type:%d,id:%d) is invalid:%d\n", \
                             conn, conn->ac_remote_comptype, conn->ac_remote_id, conn->ac_state);
		conn->ac_weight -= sendsize;
		amp_sem_up(&conn->ac_sendsem);
		__amp_free_conn(conn);
		goto SELECT_CONN;

       }
	
	/*stage1: send msg*/
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
		AMP_ENTER("__amp_serv_out_thread: add req:%p to waiting reply list\n", req);
		amp_lock(&amp_waiting_reply_list_lock);
		list_add_tail(&req->req_list, &amp_waiting_reply_list);
		amp_unlock(&amp_waiting_reply_list_lock);
	}
	*/

	if (req_type & AMP_REQUEST) {
		AMP_DMSG("__amp_serve_out_thread: conn:%p, send request\n", conn);
		if (req->req_msg->amh_magic != AMP_REQ_MAGIC) {
			AMP_ERROR("__amp_serve_out_thread: wrong magic for request\n");
		}

		if (req->req_msg->amh_size == 0) {
			AMP_ERROR("__amp_serve_out_thread: payload is zero\n");

		}
		err = AMP_OP(conn_type, proto_sendmsg)(conn->ac_sock, \
	                                           &sout_addr, \
	                                           sizeof(sout_addr), \
	                                           req->req_msglen, \
	                                           req->req_msg, \
	                                           flags);
	} else {
		AMP_DMSG("__amp_serve_out_thread: conn:%p, send reply\n", conn);
		
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
		AMP_ERROR("__amp_serve_out_thread: sendmsg error, conn:%p, req:%p, err:%d\n", \
                           conn, req, err);
		amp_unlock(&req->req_lock);
		/*
		if (need_ack) {
			AMP_DMSG("__amp_serve_out_thread: send msg error, err:%d\n", err);
			amp_lock(&amp_waiting_reply_list_lock);
			list_del_init(&req->req_list);
			amp_unlock(&amp_waiting_reply_list_lock);
		}
		*/
		goto SEND_ERROR;
	}

	if (req_type & AMP_DATA)  {
		/*stage2: send data*/
		AMP_DMSG("__amp_serv_out_thread: conn:%p, send data\n", conn);
		req->req_stage = AMP_REQ_STAGE_DATA;
		err = AMP_OP(conn_type, proto_senddata)(conn->ac_sock,
		                                        &sout_addr, \
	                                                sizeof(sout_addr), \
	                                                req->req_niov,
	                                                req->req_iov,
	                                                0);
		if (err < 0) {
			AMP_ERROR("__amp_serve_out_thread: senddata error, conn:%p, req:%p, err:%d\n",\
                                   conn, req, err);
			amp_unlock(&req->req_lock);
			/*
			if (need_ack) {
				AMP_ERROR("__amp_serv_out_thread: send data error, err:%d\n", err);
				amp_lock(&amp_waiting_reply_list_lock);
				list_del_init(&req->req_list);
				amp_unlock(&amp_waiting_reply_list_lock);
			}
			*/
			goto SEND_ERROR;
		}
	}

	conn->ac_weight -= sendsize;
	amp_sem_up(&conn->ac_sendsem);
	AMP_DMSG("__amp_serve_out_thread: finished send,conn:%p\n", conn);
	__amp_free_conn(conn);
	
	if (need_ack) { /*waiting for ack*/
		AMP_DMSG("__amp_serve_out_thread: req:%p, need ack\n", req);
		amp_lock(&amp_waiting_reply_list_lock);
		list_add_tail(&req->req_list, &amp_waiting_reply_list);
		amp_unlock(&amp_waiting_reply_list_lock);

		amp_unlock(&req->req_lock);
		goto AGAIN;
	}

	amp_unlock(&req->req_lock);

	/*
	 * do not need ack
	 */
	if (req->req_need_free) {
		AMP_ERROR("__amp_serve_out_thread: need free request:%p\n", \
                           req);
		if ((req_type & AMP_REQUEST) && req->req_msg)
			amp_free(req->req_msg, req->req_msglen);
		else if (req->req_reply)
			amp_free(req->req_reply, req->req_replylen);
		__amp_free_request(req);
	} else
		amp_sem_up(&req->req_waitsem);	

	__amp_free_request(req);

	AMP_DMSG("__amp_serve_out_thread: after sem up\n");
	//yield();
	cond_resched();
	goto AGAIN;
	 

SEND_ERROR:
	printk("__amp_serve_out_thread: send error through conn:%p, err:%d\n", 
                  conn, err);

	conn->ac_weight -= sendsize;
	amp_sem_up(&conn->ac_sendsem);
	amp_lock(&conn->ac_lock);
	AMP_DMSG("__amp_serve_out_thread: before check\n");
	
	if (conn->ac_type == AMP_CONN_TYPE_TCP)  {
		conn->ac_datard_count = 0;
		conn->ac_sched = 0;		
	}

	if (conn->ac_need_reconn) {
		/*
		 * we must add the bad connection to the reconnect list.
		 */
		if (conn->ac_state != AMP_CONN_RECOVER) {
			AMP_ERROR("__amp_serve_out_thread: set conn:%p to recover\n", conn);
			AMP_ERROR("__amp_serve_out_thread: remoteid:%d, remotetype:%d\n", conn->ac_remote_id, conn->ac_remote_comptype);
			/*
                        AMP_OP(conn_type, proto_disconnect)((void *)&conn->ac_sock);
			conn->ac_sock = NULL;
			*/
			conn->ac_state = AMP_CONN_RECOVER;	
			amp_lock(&amp_reconn_conn_list_lock);
			if (list_empty(&conn->ac_reconn_list)) {
				AMP_DMSG("__amp_serve_out_thread: add conn:%p to reconn_list\n", conn);
				list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
			}
			amp_unlock(&amp_reconn_conn_list_lock);
		} else 
			AMP_ERROR("__amp_serve_out_thread: someone else has set conn:%p to recover\n", conn);
			
	} else if (conn->ac_type == AMP_CONN_TYPE_TCP)   { 
	        /*
	         * Maybe it's in server side or it's realy need to be released, so we free it.
	         */
		if (conn->ac_state != AMP_CONN_CLOSE) {
			AMP_DMSG("[%d]__amp_serve_out_thread: set conn:%p to close\n", \
                                current->pid, conn);
			conn->ac_state = AMP_CONN_CLOSE;
			amp_lock(&amp_reconn_conn_list_lock);
			if (list_empty(&conn->ac_reconn_list)) {
				AMP_DMSG("__amp_serve_out_thread: add conn:%p to reconn_list\n", conn);
				list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
			}
			amp_unlock(&amp_reconn_conn_list_lock);
		} else 
			AMP_ERROR("__amp_serve_out_thread: someone else has set conn:%p to close\n", conn);
	}
		
	atomic_dec(&conn->ac_refcont);
	amp_unlock(&conn->ac_lock);	
	goto SELECT_CONN;
								
ERROR:
	AMP_ERROR("__amp_serve_out_thread: at ERROR, conn:%p, req:%p\n", \
                   conn, req);
	__amp_free_request(req);
	amp_sem_up(&req->req_waitsem);

	goto AGAIN;
	
EXIT:

#ifdef __KERNEL__
	AMP_LEAVE("__amp_serve_out_thread: leave: %d\n", current->pid);
#else
	AMP_LEAVE("__amp_serve_out_thread: leave: %d\n", pthread_self());
#endif

	amp_sem_up(&threadp->at_downsem);
	return 0;
}

/*
 * the callback of serving incoming msg requests
 */
int 
__amp_serve_in_thread (void *argv)
{
	amp_thread_t *threadp = NULL;
	amp_u32_t seqno;
	char str[16];
	amp_request_t  *req = NULL;
	amp_connection_t *conn;	
	amp_u32_t  conn_type;  /*tcp or udp*/
	amp_u32_t  req_type;    /*msg or data*/
	amp_s32_t   err = 0;
	amp_u32_t   discard = 0;
	amp_u32_t   hasdata = 0;
	amp_s8_t     *discardmsg = NULL;
	amp_s8_t     *recvmsg = NULL;
	amp_message_t  *msgp = NULL;
	amp_u32_t niov;
	amp_kiov_t  *iov = NULL;
	
	   
#ifdef __KERNEL__
        AMP_ENTER("__amp_serv_in_thread: enter, %d\n",	current->pid);
#else
	AMP_ENTER("__amp_serv_in_thread: enter, %d\n",	pthread_self());
#endif

	threadp = (amp_thread_t *)argv;
        seqno = threadp->at_seqno;

	sprintf(str, "srvin_%d", seqno);
	__amp_kdaemonize (str);
	__amp_blockallsigs ();

        threadp->at_isup = 1;
	/*
	if (!(current->flags & PF_MEMALLOC)) {
		AMP_ERROR("[%d]__amp_serve_in_thread: set PF_MEMALLOC\n", current->pid);
		current->flags |= PF_MEMALLOC;
	}
	*/
		
#ifdef __KERNEL__
	threadp->at_task = current;
#else
	threadp->at_thread_id = pthread_self();
#endif

	amp_sem_up(&threadp->at_startsem);
	discardmsg = vmalloc(AMP_MAX_MSG_SIZE);
	if (!discardmsg)  {
		AMP_ERROR("__amp_serve_in_thread: vmalloc for tmp msg error\n");
		goto EXIT;
	}

AGAIN:
	AMP_DMSG("__amp_serve_in_thread: begin to down semaphore\n");
	err = down_interruptible(&amp_process_in_sem);
	if (err < 0) {
		AMP_ERROR("__amp_serve_in_thread: down sem return error, err:%d\n", err);
		goto EXIT;
	}
	AMP_DMSG("__amp_serve_in_thread: after down semaphore\n");

	if (err < 0)
		goto EXIT;

	if (threadp->at_shutdown) 
		goto EXIT;

	amp_lock(&amp_dataready_conn_list_lock);
	if (list_empty(&amp_dataready_conn_list)) {
		amp_unlock( &amp_dataready_conn_list_lock);
		goto AGAIN;
	}
	/*
         * the refcont of conn in dataready list is set to be more
         * than one
         */
	conn = list_entry(amp_dataready_conn_list.next, \
                          amp_connection_t, \
                          ac_dataready_list);
	list_del_init(&conn->ac_dataready_list);
	amp_unlock(&amp_dataready_conn_list_lock);

	discard = 0;
	hasdata = 0;

	conn_type = conn->ac_type;
	
	/*
	 * firstly get a request.
	 */
	__amp_alloc_request(&req);
	req->req_remote_id = conn->ac_remote_id;
	req->req_remote_type = conn->ac_remote_comptype;

	/*
	 * receive the header
	 */
	amp_sem_down(&conn->ac_recvsem);
	AMP_DMSG("__amp_serve_in_thread: after down sem, conn:%p\n", conn);
	msgp = NULL;
	err = __amp_recv_msg(conn, &msgp);
	AMP_DMSG("__amp_serve_in_thread: after recv msg header, msgp:%p\n", msgp);
	
	if (err < 0) {
		amp_sem_up(&conn->ac_recvsem);
		AMP_DMSG("__amp_serve_in_thread: after sem up\n");
		AMP_DMSG("__amp_serve_in_thread: get msg header error, err:%d, conn:%p\n", \
			err, conn);
		amp_lock(&conn->ac_lock);
		if (!conn->ac_sock) {
			AMP_ERROR("__amp_serve_in_thread: conn:%p,remote type:%d, id:%d no socket\n", \
                                   conn, conn->ac_remote_comptype, conn->ac_remote_id);
			conn->ac_datard_count = 0;
			conn->ac_sched = 0;
			amp_unlock(&conn->ac_lock);
			__amp_free_conn(conn);
			__amp_free_request(req);
			goto AGAIN;
		}
		
		if ((conn_type == AMP_CONN_TYPE_TCP) &&  \
                      (conn->ac_sock->sk->sk_state == TCP_CLOSE_WAIT)) {
			AMP_ERROR("__amp_serve_in_thread: conn:%p is closed, reconn will release it\n", conn);
			conn->ac_datard_count = 0;
			conn->ac_sched = 0;
			amp_unlock(&conn->ac_lock);
			__amp_free_conn(conn);
			__amp_free_request(req);
			goto AGAIN;
		}
		if (conn->ac_sock && conn->ac_sock->sk)
			AMP_DMSG("__amp_serve_in_thread: sock state:%d\n", \
				  conn->ac_sock->sk->sk_state);
		
		if (err == -EAGAIN) { /*no message remain */
			AMP_DMSG("__amp_serve_in_thread: get EAGAIN from conn:%p\n", conn);
			if (conn->ac_datard_count) {
				AMP_DMSG("__amp_serve_in_thread: datard_count:%d, not zero\n", \
                                           conn->ac_datard_count);
				if (conn->ac_datard_count > 1)
					conn->ac_datard_count = 1;
				else
					conn->ac_datard_count--;
				amp_unlock(&conn->ac_lock);
				amp_lock(&amp_dataready_conn_list_lock);
				list_add_tail(&conn->ac_dataready_list, &amp_dataready_conn_list);
				amp_unlock(&amp_dataready_conn_list_lock);
				amp_sem_up(&amp_process_in_sem);
			} else {
				conn->ac_datard_count = 0;
				conn->ac_sched = 0;
				amp_unlock(&conn->ac_lock);
				__amp_free_conn(conn);
			}
			__amp_free_request(req);
			goto AGAIN;			
		}	

		/*
		 * this connection is error, need reconnection.
		 */
		if (conn_type == AMP_CONN_TYPE_TCP) { /*we must reset the network*/
			AMP_ERROR("__amp_serve_in_thread: conn:%p, ac_sock:%p\n", conn, conn->ac_sock);
			conn->ac_datard_count = 0;
			conn->ac_sched = 0;
			if (conn->ac_state == AMP_CONN_OK) {
				AMP_ERROR("__amp_serve_in_thread: resent the conn:%p\n", conn);	
				if (conn->ac_need_reconn) {
					AMP_OP(conn_type, proto_disconnect)((void *)conn->ac_sock);
					conn->ac_sock = NULL;
					conn->ac_state = AMP_CONN_RECOVER;
					AMP_ERROR("__amp_serve_in_thread: need recover\n");
				} else {
					conn->ac_state = AMP_CONN_CLOSE;			
					AMP_DMSG("[%d]__amp_serve_in_thread: close it\n", \
                                                current->pid);
				}
				amp_lock(&amp_reconn_conn_list_lock);
				if (list_empty(&conn->ac_reconn_list)) {
					AMP_ERROR("__amp_serve_in_thread: recvmsg error:%d, add conn:%p to reconn list\n", err, conn);
					list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
				} else
					AMP_ERROR("__amp_serve_in_thread: conn:%p has added to reconn list\n", conn);
				amp_unlock(&amp_reconn_conn_list_lock);
			}
			amp_unlock(&conn->ac_lock);
			__amp_free_conn(conn);	
			if (msgp)
			       amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
			__amp_free_request(req);
			goto AGAIN;
		} 

		amp_unlock(&conn->ac_lock);	
		__amp_free_request(req);
		
		goto ADD_BACK;	
		
	}
	AMP_DMSG("[%d]__amp_serve_in_thread: recv from conn:%p\n", current->pid, conn);
	

	AMP_DMSG("__amp_serve_in_thread: judge amh_magic\n");
	if (msgp->amh_magic != AMP_REQ_MAGIC)  {
	     printk("[%d]__amp_serve_in_thread: wrong request header, conn:%p,type:0x%x,magic:0x%x,length: %d\n", \
	                current->pid, conn, 
                        msgp->amh_type,
			msgp->amh_magic, 
			msgp->amh_size);
		amp_sem_up(&conn->ac_recvsem);
		 
		amp_lock(&conn->ac_lock);
		if (conn_type == AMP_CONN_TYPE_TCP)  {
			AMP_ERROR("__amp_serve_in_thread: conn:%p, state:%d\n", conn, conn->ac_state);
			if (conn->ac_state == AMP_CONN_OK) {
				AMP_ERROR("__amp_serve_in_thread: reset the conn:%p\n", conn);
				if (conn->ac_need_reconn) {
					conn->ac_state = AMP_CONN_RECOVER;
					AMP_OP(conn_type, proto_disconnect)((void*)&conn->ac_sock);
					conn->ac_sock = NULL;
					AMP_ERROR("__amp_serve_in_thread: conn:%p, need recover\n", conn);
				} else {
					conn->ac_state = AMP_CONN_CLOSE;			
					printk("[%d]__amp_serve_in_thread: conn:%p, close it\n", \
					        current->pid, conn);
				}
				amp_lock(&amp_reconn_conn_list_lock);
				if (list_empty(&conn->ac_reconn_list)) {
					AMP_ERROR("__amp_serve_in_thread: wrong magic, add conn:%p to reconn list\n", conn);
					list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
				}
				amp_unlock(&amp_reconn_conn_list_lock);
			}
			conn->ac_datard_count = 0;
			conn->ac_sched = 0;
			amp_unlock(&conn->ac_lock);
			__amp_free_conn(conn);
			amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
			__amp_free_request(req);
			goto AGAIN;
		}

		/*
		 * for upd, we just discard this request, and add it back so that the srvin thread
		 * will get the other request later.
		 */
		amp_unlock(&conn->ac_lock);
		amp_free(msgp, AMP_MAX_MSG_SIZE); 	
		__amp_free_request(req);
		goto ADD_BACK;
	}
	

	req_type = msgp->amh_type;
	 
	AMP_DMSG("__amp_serve_in_thread: judge req_type:%d\n", req_type);
	/*
	 * maybe it's a hello message by udp
	 */
	if (req_type == AMP_HELLO) {
		/*
		 * just send back an ack.
		 */
		amp_message_t  msghd;
		struct sockaddr_in *addrp = NULL;
		amp_u32_t addr_len = 0;

		AMP_DMSG("__amp_serve_in_thread: it's a hello msg\n");

		amp_sem_up(&conn->ac_recvsem);
		if (conn_type != AMP_CONN_TYPE_UDP) {
			printk("__amp_serve_in_thread: get a hello-world from non-udp protocol\n");
			amp_lock(&conn->ac_lock);

			conn->ac_datard_count = 0;
			conn->ac_sched = 0;
			if (conn->ac_state == AMP_CONN_OK) {
				if (conn->ac_need_reconn) {
					conn->ac_state = AMP_CONN_RECOVER;
					AMP_OP(conn_type, proto_disconnect)((void*)&conn->ac_sock);
					conn->ac_sock = NULL;
				} else
					conn->ac_state = AMP_CONN_CLOSE;			
				amp_lock(&amp_reconn_conn_list_lock);
				if (list_empty(&conn->ac_reconn_list))
					list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
				amp_unlock(&amp_reconn_conn_list_lock);
			}
			amp_unlock(&conn->ac_lock);
			__amp_free_conn(conn);
			amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
			__amp_free_request(req);
			goto AGAIN;
		}
		
		addrp = &(msghd.amh_addr);
		addr_len = sizeof(struct sockaddr_in);		
	
		memset(&msghd, 0, sizeof(msghd));
		msghd.amh_magic = AMP_REQ_MAGIC;
		msghd.amh_type = AMP_HELLO_ACK;
		err = AMP_OP(conn_type, proto_sendmsg)(conn->ac_sock, 
			                                   addrp,
			                                   addr_len,
							   sizeof(msghd),
			                                   &msghd,
							   0);
		
		if (conn_type == AMP_CONN_TYPE_UDP) 
			amp_free(msgp, AMP_MAX_MSG_SIZE);
		else
			amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
		__amp_free_request(req);
		goto ADD_BACK;
	}

	/*
	 * verify the request
	 */
	
	 AMP_DMSG("__amp_serve_in_thread: judge what kind of msg\n");
	 if ((req_type != (AMP_REQUEST | AMP_MSG))  &&
		(req_type != (AMP_REQUEST | AMP_DATA)) &&
		(req_type != (AMP_REPLY | AMP_MSG)) &&
		(req_type != (AMP_REPLY | AMP_DATA))){
	 	printk("[%d]__amp_serve_in_thread: get wrong request header, conn:%p, type:%d\n", \
		        current->pid, conn, req_type);
		amp_sem_up(&conn->ac_recvsem);
		if (conn_type == AMP_CONN_TYPE_TCP)  {
			amp_lock(&conn->ac_lock);
			conn->ac_datard_count = 0;
			conn->ac_sched = 0;
			if (conn->ac_state == AMP_CONN_OK) {
				AMP_ERROR("__amp_serve_in_thread: reset the conn:%p\n", conn);
				if(conn->ac_need_reconn) {
					AMP_OP(conn_type, proto_disconnect)((void*)&conn->ac_sock);
					conn->ac_sock = NULL;
					conn->ac_state = AMP_CONN_RECOVER;
					AMP_ERROR("__amp_serve_in_thread: conn:%p, need recover\n", conn);
				} else {
					conn->ac_state = AMP_CONN_CLOSE;	
					printk("[%d]__amp_serve_in_thread: conn:%p, close it\n", \
						current->pid, conn);
				}
			
				amp_lock(&amp_reconn_conn_list_lock);
				if (list_empty(&conn->ac_reconn_list)) {
					AMP_ERROR("__amp_serve_in_thread: wrong req_type, add conn:%p to reconn list\n", conn);
					list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
				}
				amp_unlock(&amp_reconn_conn_list_lock);
			}
			amp_unlock(&conn->ac_lock);
			__amp_free_conn(conn);
			amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
			__amp_free_request(req);
			
			goto AGAIN;
		}

		/*
		 * for upd, we just discard this request, and add it back so that the srvin thread
		 * will get the other request later.
		 */		
		amp_free(msgp, AMP_MAX_MSG_SIZE);
		__amp_free_request(req);
				
		goto ADD_BACK;
	 }
	 
	//amp_unlock(&conn->ac_lock);

	AMP_DMSG("__amp_serve_in_thread: is it a reply\n");
	if (req_type & AMP_REPLY)   {
		/*
		 * get a reply, get the request
		 */
		amp_request_t  *tmpreqp = NULL;
		
		AMP_DMSG("__amp_serve_in_thread: it's a reply\n");
		tmpreqp = (amp_request_t *)((unsigned long) msgp->amh_sender_handle);

		AMP_DMSG("__amp_serve_in_thread: original req:%p\n", tmpreqp);
		amp_lock(&amp_free_request_lock);
		if (!__amp_reqheader_equal(tmpreqp->req_msg, msgp)) {
			AMP_ERROR("__amp_serve_in_thread: conn:%p, req:%p, two header is not equal\n", \
                                   conn, tmpreqp);
			amp_unlock(&amp_free_request_lock);
			__amp_free_request(req);
			if(conn_type == AMP_CONN_TYPE_TCP) 
			  	amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
			else
			  	amp_free(msgp, AMP_MAX_MSG_SIZE);

			amp_sem_up(&conn->ac_recvsem);
			goto ADD_BACK;
			
		} else {
			amp_unlock(&amp_free_request_lock);				
			__amp_free_request(req);
			req = tmpreqp; 
			amp_lock(&req->req_lock);
			/* 
                         * remove from wait for reply list.
                         */
			AMP_DMSG("__amp_serve_in_thread: remove req:%p from waiting reply list\n", req);
			amp_lock(&amp_waiting_reply_list_lock);
		        list_del_init(&req->req_list);
		        amp_unlock(&amp_waiting_reply_list_lock);
			amp_unlock(&req->req_lock);
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
				/*
				 * we can be sure, no memory now, in order to lest the following requests
				 * continue bother us, reset it, so that the remote peer will send it again.
				 */
			      printk("__amp_serve_in_thread: conn:%p, alloc page error, err:%d\n", conn, err);
			      amp_sem_up(&conn->ac_recvsem);
			      amp_lock(&conn->ac_lock);
			      if (conn_type == AMP_CONN_TYPE_TCP) { /*we must reset the network*/
			             
			              conn->ac_datard_count = 0;
			              conn->ac_sched = 0;	
				      if (conn->ac_state == AMP_CONN_OK) {
						printk("[%d]__amp_serve_in_thread: alloc page error, resent conn:%p\n", \
							current->pid, conn);
						if(conn->ac_need_reconn) {
				      			AMP_OP(conn_type, proto_disconnect)((void *)&conn->ac_sock);
							conn->ac_sock = NULL;
							conn->ac_state = AMP_CONN_RECOVER;
				      		} else {
							conn->ac_state = AMP_CONN_CLOSE;
							printk("[%d]__amp_serve_in_thread: close it\n", current->pid);
						}			
				      		amp_lock(&amp_reconn_conn_list_lock);
				      		if (list_empty(&conn->ac_reconn_list)) {
							AMP_ERROR("__amp_serve_in_thread: allocpage error, add conn:%p to reconn list\n", conn);
				      			list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
						}
				      		amp_unlock(&amp_reconn_conn_list_lock);
				      }
				      amp_unlock(&conn->ac_lock);	
				      __amp_free_conn(conn);
				      __amp_free_request(req);
				      amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
				      goto AGAIN;
		             } 

			      /*
			       * come to here only when we use the udp protocol.
			       */
			      amp_unlock(&conn->ac_lock);
				  
			      amp_free(msgp, AMP_MAX_MSG_SIZE);
			      if (req_type & AMP_REPLY)  {
				  	amp_lock(&req->req_lock);
					if(req->req_need_ack) {
						req->req_error = -ENOMEM;
						amp_sem_up(&req->req_waitsem);
					}
					amp_unlock(&req->req_lock);
			      	}

				__amp_free_request(req);				  
			      	
			      goto ADD_BACK;
			}
		}

		/*ok, the page is prepared, receive the data now*/
		AMP_DMSG("__amp_serve_in_thread: before recv data\n");
		err = AMP_OP(conn_type, proto_recvdata)(conn->ac_sock,
				                        &conn->ac_remote_addr,
		                                        sizeof(conn->ac_remote_addr),
		                                        niov,
		                                        iov,
		                                        0);
		if (err < 0) {
			AMP_ERROR("__amp_serve_in_thread: recv data error, conn:%p, err:%d\n", \
                                  conn, err);
			amp_sem_up(&conn->ac_recvsem);
			amp_lock(&conn->ac_lock);
			if (conn_type == AMP_CONN_TYPE_TCP) { /*we must reset the network*/
				AMP_DMSG("__amp_serve_in_thread: it's tcp, state:%d\n", conn->ac_state);
				AMP_DMSG("__amp_serve_in_thread: sock state:%d\n", conn->ac_sock->sk->sk_state);
				conn->ac_datard_count = 0;
				conn->ac_sched = 0;		
				if (conn->ac_state == AMP_CONN_OK) {
					AMP_ERROR("__amp_serve_in_thread: reset conn:%p\n", conn);
					if (conn->ac_need_reconn) {
						AMP_ERROR("__amp_serve_in_thread: set conn:%p recover\n", conn);
						AMP_OP(conn_type, proto_disconnect)((void *)&conn->ac_sock);
						conn->ac_sock = NULL;
						conn->ac_state = AMP_CONN_RECOVER;
					} else {
						printk("[%d]__amp_serve_in_thread: close conn:%p\n", current->pid, conn);
						conn->ac_state = AMP_CONN_CLOSE;			
					}
					amp_lock(&amp_reconn_conn_list_lock);
					if (list_empty(&conn->ac_reconn_list)) {
						AMP_ERROR("__amp_serve_in_thread: recvdata error, add conn:%p to reconn_list\n", conn);
						list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
					}
					amp_unlock(&amp_reconn_conn_list_lock);
				} else 
					AMP_ERROR("__amp_serve_in_thread: conn:%p, state:%d\n",
                                                  conn, conn->ac_state);
			}
			amp_unlock(&conn->ac_lock);
			
			if (req_type & AMP_REPLY) {
				amp_lock(&req->req_lock);
				req->req_error = -EPROTO;
				amp_sem_up(&req->req_waitsem);	
				amp_unlock(&req->req_lock);
			}						
			if (iov != req->req_iov)  /*free all the alloced space*/
				conn->ac_freepage_cb(niov, iov);
			
			__amp_free_request(req);

			if(conn_type == AMP_CONN_TYPE_TCP) 
				amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
			else
				amp_free(msgp, AMP_MAX_MSG_SIZE);
			
			if (conn_type == AMP_CONN_TYPE_TCP) {
				__amp_free_conn(conn);
				goto AGAIN;
			} else
				goto ADD_BACK;
			
		}

		if (req->req_iov != iov) {
			req->req_niov = niov;
			req->req_iov = iov;
		}

	 AMP_DMSG("__amp_serve_in_thread: finish recv data\n");		

	 }
	amp_sem_up(&conn->ac_recvsem);
	AMP_DMSG("__amp_serve_in_thread: after sem up\n");
	
	 
	if (req_type & AMP_REQUEST) {
		AMP_DMSG("__amp_serve_in_thread: it's a request, queue it\n");
		req->req_msg = msgp;
		req->req_type = req_type;
		req->req_msglen = msgp->amh_size + sizeof(amp_message_t);
		if (!conn->ac_queue_cb)  {
			AMP_ERROR("__amp_serve_in_thread: No queue request callback for conn:%p\n",\
                                   conn);
			AMP_ERROR("__amp_serve_in_thread: remote_type:%d, remote_id:%d\n", \
                                   conn->ac_remote_comptype, conn->ac_remote_id);
			amp_free(msgp, req->req_msglen);
			__amp_free_request(req);
		} else
			conn->ac_queue_cb(req);
	} else if (req_type & AMP_REPLY) {	
		AMP_DMSG("__amp_serve_in_thread: it's a reply\n");
		if (req->req_need_ack) {			
			AMP_DMSG("__amp_serve_in_thread: need wait it\n");
			req->req_reply = msgp;
			req->req_replylen = msgp->amh_size + sizeof(amp_message_t);
			__amp_free_request( req);
			amp_sem_up(&req->req_waitsem);
			
		} else {
			AMP_DMSG("__amp_serve_in_thread: needn't ack, free req_msg\n");		
			if(conn_type == AMP_CONN_TYPE_TCP) 
				amp_free(msgp, (msgp->amh_size + sizeof(amp_message_t)));
			else
				amp_free(msgp, AMP_MAX_MSG_SIZE);			
			__amp_free_request(req); /*we added it */
		}				
	}
	
ADD_BACK:
	AMP_DMSG("[%d]__amp_serve_in_thread: finished recv from conn:%p\n", current->pid, conn);
	amp_lock(&conn->ac_lock);
//	conn->ac_datard_count --;
	if (conn->ac_datard_count) {
		AMP_DMSG("__amp_serve_in_thread: add conn:%p back to ready list\n", conn);
		amp_unlock(&conn->ac_lock);
		amp_lock(&amp_dataready_conn_list_lock);
		list_add_tail(&conn->ac_dataready_list, &amp_dataready_conn_list);
		amp_unlock(&amp_dataready_conn_list_lock);
		amp_sem_up(&amp_process_in_sem);
		/*Here, we add the conn back, so we needn't free the conn*/
	} else {
		/*
		 * no data come in
		 */
		AMP_ENTER("__amp_serve_in_thread: conn:%p, no data come in, free it\n", conn);
		conn->ac_sched = 0;
		amp_unlock(&conn->ac_lock);
		__amp_free_conn(conn);
	}
	//yield();
	cond_resched();
	goto AGAIN;

EXIT:

#ifdef __KERNEL__
	AMP_LEAVE("__amp_serve_in_thread: leave: %d\n",	current->pid);
#else
	AMP_LEAVE("__amp_serve_in_thread: leave: %d\n",	pthread_self());
#endif

	amp_sem_up(&threadp->at_downsem);
	return 0;


}

/*
 * the callback of serving reconnection service.
 * 
 * this thread need lock the conn and sendsem
 * to ensure that we only allow one reconnection thread.
 */
int
__amp_reconn_thread (void *argv)
{
	amp_thread_t *threadp = NULL;
	amp_u32_t seqno;
	char str[16];
	amp_connection_t *conn = NULL;	
	amp_connection_t *tmpconn = NULL;
	amp_request_t   *req = NULL;
	amp_comp_conns_t *cmp_conns = NULL;
	conn_queue_t  *cnq = NULL;
	amp_comp_context_t *ctxt = NULL;
	wait_queue_t  __wait;
	amp_u32_t        conn_type;  /*tcp or udp*/
        amp_s32_t   err = 0;
	struct list_head  list;
	struct list_head  *head = NULL;
	struct list_head  *pos = NULL;
	struct list_head  *nxt = NULL;
	struct timeval tv;
	amp_s32_t   type;
	amp_s32_t   id;
	amp_s32_t   remain_times;
	amp_s32_t   i;
	amp_u32_t   has_valid_conn;
	

	   
#ifdef __KERNEL__
        AMP_ENTER("__amp_reconn_thread: enter, %d\n", current->pid);
#else
        AMP_ENTER("__amp_reconn_thread: enter, %d\n", pthread_self());
#endif

	threadp = (amp_thread_t *)argv;
        seqno = threadp->at_seqno;

	sprintf(str, "reconn_%d", seqno);
	__amp_kdaemonize (str);
	__amp_blockallsigs ();

        threadp->at_isup= 1;
	   
#ifdef __KERNEL__
	threadp->at_task = current;
#else
	threadp->at_thread_id = pthread_self();
#endif

	amp_sem_up(&threadp->at_startsem);

	/*
	 * doing main work
	 */
WAKE_UP:

	//AMP_DMSG("__amp_reconn_thread: wake up\n");
	amp_sem_down(&amp_reconn_finalize_sem);
	
	amp_lock(&amp_reconn_conn_list_lock);
	if (list_empty(&amp_reconn_conn_list)) {
		amp_unlock(&amp_reconn_conn_list_lock);
		goto TO_SLEEP;
	}

	/*
	 * firstly get down the whole list.
	 */
	list.next = amp_reconn_conn_list.next;
	list.prev = amp_reconn_conn_list.prev;
	amp_reconn_conn_list.prev->next = &list;
	amp_reconn_conn_list.next->prev = &list;
	INIT_LIST_HEAD(&amp_reconn_conn_list);
	
	amp_unlock(&amp_reconn_conn_list_lock);

	while(!list_empty(&list)) {
		/*we lock it so that the finalize process can be synced with this one*/
		amp_lock(&amp_reconn_conn_list_lock);
		conn = list_entry(list.next, amp_connection_t, ac_reconn_list );
		list_del_init(&conn->ac_reconn_list);
		amp_unlock(&amp_reconn_conn_list_lock);

		amp_lock(&conn->ac_lock);

		AMP_DMSG("__amp_reconn_thread: to handle conn:%p\n", conn);
		if (conn->ac_remote_port == 0) {
			AMP_ERROR("__amp_reconn_thread: the remote port of conn: %p is zero\n", conn);
			conn->ac_state = AMP_CONN_BAD;
			amp_unlock(&conn->ac_lock);
			__amp_dequeue_conn(conn, conn->ac_ctxt);
			__amp_free_conn(conn);			
			continue;
		}

		if ((conn->ac_state == AMP_CONN_CLOSE) && !conn->ac_need_reconn) {
			/*
			 * if we're server, then we just free it.
			 */
			AMP_ERROR("__amp_reconn_thread: before dequeue conn, refcount:%d\n", \
                                  atomic_read(&conn->ac_refcont));
			conn->ac_state = AMP_CONN_BAD;
			amp_unlock(&conn->ac_lock); /*we should unlock before sem down*/

			if (conn->ac_sock->sk) {
				conn->ac_sock->sk->sk_shutdown = SEND_SHUTDOWN | RCV_SHUTDOWN;
				if (conn->ac_sock->sk->sk_sleep && 
                                    waitqueue_active(conn->ac_sock->sk->sk_sleep))
					wake_up_interruptible(conn->ac_sock->sk->sk_sleep);
			}
			amp_sem_down(&conn->ac_recvsem);
			amp_sem_down(&conn->ac_sendsem);	

			__amp_dequeue_conn(conn, conn->ac_ctxt);

			amp_lock(&conn->ac_lock);
			if (!list_empty(&conn->ac_protobh_list)) {
				AMP_ERROR("__amp_reconn_thread: to free conn:%p, protobh_list not empty\n", conn);

			}
			if (conn->ac_sock)
				AMP_OP(conn->ac_type, proto_disconnect)(conn->ac_sock);
			else
				AMP_ERROR("__amp_reconn_thread: close conn:%p, no sock\n", conn);
			conn->ac_sock = NULL;
			conn->ac_weight = 0;
			amp_sem_up(&conn->ac_recvsem);
			amp_sem_up(&conn->ac_sendsem);
			amp_unlock(&conn->ac_lock);
			__amp_free_conn(conn);
			continue;
		}

		AMP_DMSG("__amp_reconn_thread: need reconnect for conn:%p, remote type:%d, id:%d\n", \
                             conn, conn->ac_remote_comptype, conn->ac_remote_id);

		conn_type = conn->ac_type;
		amp_unlock(&conn->ac_lock); /*connect and sem process will be rescheduled*/

		if (conn->ac_sock) {
			AMP_ERROR("__amp_reconn_thread: disconnect sock\n");
			if (conn->ac_sock->sk) {
				conn->ac_sock->sk->sk_shutdown = SEND_SHUTDOWN | RCV_SHUTDOWN;
				if (conn->ac_sock->sk->sk_sleep && 
                                    waitqueue_active(conn->ac_sock->sk->sk_sleep))
					wake_up_interruptible(conn->ac_sock->sk->sk_sleep);
			}

			amp_sem_down(&conn->ac_sendsem);
			amp_sem_down(&conn->ac_recvsem);
			AMP_OP(conn->ac_type, proto_disconnect)(conn->ac_sock);
			conn->ac_sock = NULL;
			amp_sem_up(&conn->ac_sendsem);
			amp_sem_up(&conn->ac_recvsem);
		}

		err = __amp_connect_server(conn, conn->ac_this_type, conn->ac_this_id);
		amp_lock(&conn->ac_lock);
		if (err < 0) {
			AMP_ERROR("__amp_reconn_thread: conn:%p, remaintimes:%lld, error, err:%d\n", \
                                   conn, conn->ac_remain_times, err);
			conn->ac_remain_times --;
			if (conn->ac_remain_times <= 0) {			
				conn->ac_state = AMP_CONN_BAD;	
				amp_unlock(&conn->ac_lock);
				__amp_dequeue_conn(conn, conn->ac_ctxt);
				__amp_free_conn(conn);
				continue;
			}
			conn->ac_last_reconn = jiffies;
			amp_lock(&amp_reconn_conn_list_lock);
			if (list_empty(&conn->ac_reconn_list)) {
				AMP_ERROR("__amp_reconn_thread: connect server error, add conn:%p to reconn_conn_list\n", conn);
				list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
			}
			amp_unlock(&amp_reconn_conn_list_lock);
			type = conn->ac_remote_comptype;
			id = conn->ac_remote_id;
			remain_times = conn->ac_remain_times;
			ctxt = conn->ac_ctxt;
			amp_unlock(&conn->ac_lock);

			if (ctxt && 
                           (ctxt->acc_conns) && 
                           (remain_times >= (AMP_CONN_RECONN_MAXTIMES - 1))) {
				cmp_conns = &(ctxt->acc_conns[type]);
				if (!cmp_conns->acc_remote_conns)
					continue;

				read_lock(&(cmp_conns->acc_remote_conns[id].queue_lock));
				cnq = &(cmp_conns->acc_remote_conns[id]);
				head = &(cmp_conns->acc_remote_conns[id].queue);	
	
				if (list_empty(head)) {
					AMP_ERROR("__amp_reconn_thread: no connection corresponding to type:%d, id:%d\n",
			              		   type, id);
					read_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));
					continue;
				}

				if (cnq->active_conn_num < 0) {
					AMP_ERROR("__amp_reconn_thread: type:%d, id:%d, active_conn_num:%d ,wrong\n", \
			   			   type, id, cnq->active_conn_num);
					read_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));
					continue;
				}
				has_valid_conn = 0;

				for(i=0; i<cnq->active_conn_num; i++) {
					tmpconn = cnq->conns[i];
					if (tmpconn && (tmpconn != conn)) {
						amp_lock(&tmpconn->ac_lock);
						if(tmpconn->ac_state == AMP_CONN_OK) 
							has_valid_conn ++;
						amp_unlock(&tmpconn->ac_lock);
					}
				}
				read_unlock(&(cmp_conns->acc_remote_conns[id].queue_lock));
			 	AMP_DMSG("__amp_reconn_thread: type:%d,id:%d,has_vaid_conn:%d\n", \
                                          type, id, has_valid_conn);
				if (!has_valid_conn) {
					amp_lock(&amp_waiting_reply_list_lock);
					list_for_each_safe(pos, nxt, &amp_waiting_reply_list) {
						req = list_entry(pos, amp_request_t, req_list);
						if ((req->req_remote_id == id) && 
                     				    (req->req_remote_type == type)) {
							AMP_DMSG("__amp_reconn_thread: find a request:%p\n", req);
							if(!req->req_resent){
								AMP_ERROR("__amp_reconn_thread: wake up request: %p\n", req);
								list_del_init(&req->req_list);
								req->req_error = -ENETUNREACH;
								__amp_free_request(req);

								amp_sem_up(&req->req_waitsem);
                          				}
						}
					}
					amp_unlock(&amp_waiting_reply_list_lock);
				}
			}

			continue;		
		}

		/*
	 	* reconnec successfully.
		 */
		conn->ac_state = AMP_CONN_OK;
		conn->ac_remain_times = AMP_CONN_RECONN_MAXTIMES;

		/*
		 * revoke all need resend requests
		 */
		__amp_revoke_resend_reqs (conn);

		amp_unlock(&conn->ac_lock);

		if (threadp->at_shutdown) {
			amp_sem_up(&amp_reconn_finalize_sem);
			goto EXIT;
		}

	}

	/*
	 * sleep for some time
	 */
TO_SLEEP:
	//AMP_DMSG("__amp_reconn_thread: to sleep for %d HZ\n", AMP_CONN_RECONN_INTERVAL);
	amp_sem_up(&amp_reconn_finalize_sem);
	init_waitqueue_entry(&__wait, current);
	add_wait_queue(&threadp->at_waitq, &__wait);
	set_current_state(TASK_INTERRUPTIBLE);
	if (threadp->at_shutdown) {
		remove_wait_queue(&threadp->at_waitq, &__wait);
		current->state = TASK_RUNNING;
		goto EXIT;
	}
	tv.tv_sec = AMP_CONN_RECONN_INTERVAL;
	tv.tv_usec = 0;
	__set_current_state(TASK_UNINTERRUPTIBLE);
	schedule_timeout(timeval_to_jiffies(&tv));
	remove_wait_queue(&threadp->at_waitq, &__wait);
	current->state = TASK_RUNNING;

	if (threadp->at_shutdown)
		goto EXIT;

	goto WAKE_UP;		


EXIT:
#ifdef __KERNEL__
	AMP_LEAVE("__amp_reconn_thread: leave: %d\n", current->pid);
#else
	AMP_LEAVE("__amp_reconn_thread: leave: %d\n", pthread_self());
#endif
	amp_sem_up(&threadp->at_downsem);
	return 0;
}

/*
 * thread for listen 
 */
 int __amp_listen_thread (void *argv)
{
	amp_thread_t *threadp = NULL;
	amp_u32_t seqno;
	char str[16];
	amp_connection_t *conn;
	amp_connection_t *new_connp = NULL;
	amp_u32_t        conn_type;  /*tcp or udp*/
        amp_s32_t   err = 0;
	amp_comp_context_t *ctxt = NULL;
	struct socket *parentsock = NULL;
	

	   
#ifdef __KERNEL__
        AMP_ENTER("__amp_listen_thread: enter, %d\n", current->pid);
#else
        AMP_ENTER("__amp_listen_thread: enter, %d\n", pthread_self());
#endif

	threadp = (amp_thread_t *)argv;
        seqno = threadp->at_seqno;

	sprintf(str, "listen_%d", seqno);
	__amp_kdaemonize (str);
	__amp_blockallsigs ();

        threadp->at_isup= 1;
	   
#ifdef __KERNEL__
	threadp->at_task = current;
#else
	threadp->at_thread_id = pthread_self();
#endif

	amp_sem_up(&threadp->at_startsem);

	conn = (amp_connection_t *) (threadp->at_provite);
	if (!conn) {
		AMP_ERROR("__amp_listen_thread: no connection\n");
		goto EXIT;
	}

	if (conn->ac_type != AMP_CONN_TYPE_TCP) {
		AMP_ERROR("__amp_listen_thread: not a tcp connection\n");
		goto EXIT;
	}

	conn_type = conn->ac_type;

	if (!conn->ac_ctxt) {
		AMP_ERROR("__amp_listen_thread: no context in connection\n");
		goto EXIT;
	}

	ctxt = conn->ac_ctxt;
	parentsock = conn->ac_sock;
	if (!parentsock)  {
		AMP_ERROR("__amp_listen_thread: no parent socket\n");
		goto EXIT;

	}


AGAIN:

	new_connp = NULL;
	err = __amp_alloc_conn(&new_connp);
	if (err)  {
		AMP_ERROR("__amp_listen_thread: alloc new connection error, err:%d\n", err);
		__set_current_state(TASK_UNINTERRUPTIBLE);
		schedule_timeout(300);
		goto AGAIN;

	}	

GET_SEM:

	AMP_DMSG("%s: before down listen sem\n", str);
	err = down_interruptible(&conn->ac_listen_sem);

	AMP_DMSG("%s: get a semaphore\n", str);
	if (err < 0) {
		printk("%s: down sem error, err:%d\n", str, err);
		goto EXIT;
	}
	if (threadp->at_shutdown) 
		goto EXIT;

	new_connp->ac_type = AMP_CONN_TYPE_TCP;
	new_connp->ac_need_reconn = 0;
	new_connp->ac_ctxt = ctxt;
	new_connp->ac_this_id = ctxt->acc_this_id;
	new_connp->ac_this_type = ctxt->acc_this_type;
	new_connp->ac_allocpage_cb = conn->ac_allocpage_cb;
	new_connp->ac_freepage_cb = conn->ac_freepage_cb;
	new_connp->ac_queue_cb = conn->ac_queue_cb;
	/*
	amp_lock(&new_connp->ac_lock);  
	*/
	
	err = __amp_accept_connection (parentsock, new_connp);

	if (err < 0) {
		AMP_ERROR("__amp_listen_thread: accept a new connection error, err:%d\n", err);
		/*
		amp_unlock(&new_connp->ac_lock);
		*/
		goto GET_SEM;
	}
	


	/*
	err = __amp_enqueue_conn(new_connp, ctxt);
	if (err < 0)  {
		AMP_ERROR("__amp_listen_thread: enqueue the connection error, err = %d\n", err);
		AMP_OP(conn_type, proto_disconnect)(new_connp->ac_sock);
		new_connp->ac_sock = NULL;
		amp_unlock(&new_connp->ac_lock);
		memset(new_connp, 0, sizeof(amp_connection_t));
		goto GET_SEM;
	}
	*/

	/*
	amp_unlock(&new_connp->ac_lock);
	*/
	__amp_revoke_resend_reqs (new_connp);
	AMP_ERROR("__amp_listen_thread: finish accept conn:%p\n", new_connp);

	goto AGAIN;
	
EXIT:
#ifdef __KERNEL__
	AMP_LEAVE("__amp_listen_thread: leave: %d\n", current->pid);
#else
	AMP_LEAVE("__amp_listen_thread: leave: %d\n", pthread_self());
#endif
	if (new_connp)
		__amp_free_conn(new_connp);
	amp_sem_up(&threadp->at_downsem);
	return 0;
}

/*
 * the callback of bh threads.
 */
int
__amp_bh_thread (void *argv)
{
	amp_thread_t *threadp = NULL;
	amp_u32_t seqno;
	char str[16];
	amp_connection_t *conn;	
	amp_u32_t        state_type;  /*tcp or udp*/
	amp_s32_t   err = 0;
	amp_u32_t   i;

	   
#ifdef __KERNEL__
        AMP_ENTER("__amp_bh_thread: enter, %d\n", current->pid);
#else
        AMP_ENTER("__amp_bh_thread: enter, %d\n", pthread_self());
#endif

	threadp = (amp_thread_t *)argv;
        seqno = threadp->at_seqno;

	sprintf(str, "bhthr_%d", seqno);
	__amp_kdaemonize (str);
	__amp_blockallsigs ();

        threadp->at_isup = 1;
#ifdef __KERNEL__
	threadp->at_task = current;
#else
	threadp->at_thread_id = pthread_self();
#endif

	amp_sem_up(&threadp->at_startsem);

	/*
	 * ok, do the main work.
	 */

AGAIN:
	AMP_DMSG("__amp_bh_thread: before sem\n");
	err = down_interruptible(&amp_bh_sem);
	if (err < 0) {
		AMP_ERROR("__amp_bh_thread: down sem error, err:%d\n", err);
		goto EXIT;
	}
	if (threadp->at_shutdown) {
		AMP_WARNING("__amp_bh_thread: tell us to down\n"); 
		goto EXIT;
	}
	AMP_DMSG("__amp_bh_thread: get sem\n");
	
	spin_lock_bh(&amp_bh_req_list_lock);
	if (list_empty(&amp_bh_req_list)) {
		spin_unlock_bh(&amp_bh_req_list_lock);
		goto AGAIN;
	}
	conn = list_entry(amp_bh_req_list.next, amp_connection_t, ac_protobh_list);
	list_del_init(&conn->ac_protobh_list);
	spin_unlock_bh(&amp_bh_req_list_lock);

	state_type = conn->ac_bhstate;
	if (state_type != AMP_CONN_BH_DATAREADY &&
	    state_type != AMP_CONN_BH_STATECHANGE) {
		for (i=0; i<10; i++) 
			printk("__amp_bh_thread: wrong bhstate:%d, conn:%p\n",
				state_type, conn);
		panic("__amp_bh_thread: wrong bhstate:%d, conn:%p\n", state_type, conn);
		goto AGAIN;
	} else
		amp_lock(&conn->ac_lock);

	if (conn->ac_state != AMP_CONN_OK) {
		AMP_ERROR("__amp_bh_thread: conn:%p not ok now, state:%d, state_type:%d\n", \
                           conn, conn->ac_state, state_type);
		amp_unlock(&conn->ac_lock);
		goto AGAIN;
	}

	AMP_DMSG("__amp_bh_thread: get conn:%p\n", conn);
	if (state_type == AMP_CONN_BH_DATAREADY) {
		AMP_DMSG("__amp_bh_thread: it's a dataready request\n");
		if (conn->ac_bhstate != AMP_CONN_BH_DATAREADY) {
			AMP_ERROR("__amp_bh_thread: the bh state of conn:%p is changed, not dataready now\n", conn);
			amp_unlock(&conn->ac_lock);
			goto AGAIN;
		}
		if (!conn->ac_sock)  {
			printk("__amp_bh_thread: no sock\n");
			amp_unlock(&conn->ac_lock);
			goto AGAIN;
		}
		
		if (!conn->ac_sock->sk) {
			printk("__amp_bh_thread: no sk in socket\n");
			amp_unlock(&conn->ac_lock);
			goto AGAIN;
		}

		if (conn->ac_sock->sk->sk_state == TCP_CLOSE_WAIT) {
			AMP_DMSG("__amp_bh_thread: it's TCP_CLOSE_WAIT state\n");
			amp_unlock(&conn->ac_lock);
			goto AGAIN;
		}
		if (conn->ac_sock->sk->sk_state == TCP_CLOSE) {
			AMP_DMSG("__amp_bh_thread: it's TCP_CLOSE state\n");
			amp_unlock(&conn->ac_lock);
			goto AGAIN;
		}
	
		if (conn->ac_state != AMP_CONN_OK) {
			AMP_DMSG("__amp_bh_thread: the state of conn:%p is not ok, state:%d\n", \
				  conn, conn->ac_state);
			AMP_DMSG("__amp_bh_thread: sock of this conn:%p\n", conn->ac_sock);
		}

		conn->ac_datard_count ++;

		if (!conn->ac_sched) {
			AMP_DMSG("__amp_bh_thread: conn:%p, not sched, schedule it\n", conn);
			conn->ac_sched = 1;
			atomic_inc(&conn->ac_refcont);
		        amp_unlock(&conn->ac_lock);
			amp_lock(&amp_dataready_conn_list_lock);
			if (list_empty(&conn->ac_dataready_list)) {
				AMP_DMSG("[%d]__amp_bh_thread: schedule conn:%p\n", current->pid, conn);
				list_add_tail(&conn->ac_dataready_list, &amp_dataready_conn_list);
			} else
				printk("__amp_bh_thread: conn:%p, not scheded, but dataready_list is not empty\n", conn);
			amp_unlock(&amp_dataready_conn_list_lock);
			up(&amp_process_in_sem);
		} else {
			amp_unlock(&conn->ac_lock);
			AMP_DMSG("__amp_bh_thread: conn:%p, already scheduled, ac_datard_count:%d\n", \
                        	     conn, conn->ac_datard_count);
			//up(&amp_process_in_sem);
		}


	} else if (state_type == AMP_CONN_BH_STATECHANGE) {
		AMP_DMSG("__amp_bh_thread: it's state change request\n");
		if (conn->ac_state != AMP_CONN_OK) {
			AMP_DMSG("__amp_bh_thread: conn:%p not ok\n", conn);
			amp_unlock(&conn->ac_lock);
			goto AGAIN;
		}
		if (conn->ac_need_reconn)
			conn->ac_state = AMP_CONN_RECOVER;
		else {
			AMP_DMSG("__amp_bh_thread: close conn:%p\n", conn);
			conn->ac_state = AMP_CONN_CLOSE;
		}

		amp_unlock(&conn->ac_lock);
	
		amp_lock(&amp_reconn_conn_list_lock);
		if (list_empty(&conn->ac_reconn_list)) {
			AMP_DMSG("__amp_bh_thread: add conn:%p to reconn list\n", conn);
			list_add_tail(&conn->ac_reconn_list, &amp_reconn_conn_list);
		}
		amp_unlock(&amp_reconn_conn_list_lock);
	} else  {
		for(i=0; i<100; i++)
			printk("__amp_bh_thread: wrong bhstate:%d of conn:%p\n", state_type, conn);
		amp_unlock(&conn->ac_lock);	
		spin_lock(&amp_dataready_conn_list_lock);
	}
	cond_resched();
	
	goto AGAIN;

EXIT:
#ifdef __KERNEL__
	AMP_LEAVE("__amp_bh_thread: leave: %d\n", current->pid);
#else
	AMP_LEAVE("__amp_bh_thread: leave: %d\n", pthread_self());
#endif

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
} scmsg = {{sizeof(struct cmsghdr) + sizeof(struct in_pktinfo), SOL_IP, IP_PKTINFO}, 
	  {0, }};

void __amp_install_filter(struct socket *icmp_sock, amp_s32_t ident)
{
	amp_s32_t err;

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

	err = sock_setsockopt(icmp_sock, SOL_SOCKET, 
                              SO_ATTACH_FILTER, 
                              (char *)&filter, sizeof(filter));
	if (err < 0)
		AMP_ERROR("__amp_install_filter: failed to install socket filter, err:%d\n", err);
}



int
__amp_netmorn_thread (void *argv)
{
	amp_thread_t *threadp = NULL;
	char str[16];
	amp_connection_t   *conn = NULL;
	amp_request_t      *req = NULL;
	amp_comp_context_t *ctxt = NULL;	
	wait_queue_t     __wait;
        amp_s32_t        err = 0;
	amp_u32_t     seqno;
	struct list_head *head = NULL;
	struct list_head *pos = NULL;
	struct list_head *nxt = NULL;
	amp_s8_t         *sndbufp = NULL;
	amp_s8_t         *rcvbufp = NULL;
	conn_queue_t     *cnq = NULL;
	amp_u32_t        type;
	amp_u32_t        id;
	amp_u32_t       *raddr = NULL;
	amp_u32_t       *failaddr = NULL;
	amp_u32_t        raddr_num;
	amp_u32_t        failaddr_num;
	amp_u32_t        has_valid_conn;
	amp_u32_t        size;
	amp_s32_t        i, j;
	struct socket    *icmp_sock = NULL;
	struct icmphdr   *icp = NULL;
	struct sockaddr_in whereto;
	struct sockaddr_in target;
	struct iovec      iov;
	struct msghdr     msg;
	struct timeval    tv;
	struct iphdr     *ip;
	unsigned long     tv_jiffies;
	amp_s32_t         iphdrlen;
	amp_u32_t         datalen = 64;
	amp_s32_t         hold;
	amp_u16_t         ident = 0;
	amp_u16_t         seq = 0;
	mm_segment_t      old_fs;
	struct icmp_filter  filt;
	amp_u32_t         sleep_time;
	
	

	   
#ifdef __KERNEL__
        AMP_ENTER("__amp_netmorn_thread: enter, %d\n", current->pid);
#else
        AMP_ENTER("__amp_netmorn_thread: enter, %d\n", pthread_self());
#endif

	threadp = (amp_thread_t *)argv;
        seqno = threadp->at_seqno;
	ctxt = (amp_comp_context_t *)(threadp->at_provite);

	sprintf(str, "nm_%d_%d", ctxt->acc_this_type, ctxt->acc_this_id);
	__amp_kdaemonize (str);
	__amp_blockallsigs ();

        threadp->at_isup= 1;
	   
#ifdef __KERNEL__
	threadp->at_task = current;
#else
	threadp->at_thread_id = pthread_self();
#endif

	amp_sem_up(&threadp->at_startsem);

	old_fs = get_fs();
	set_fs(KERNEL_DS);

	/*
	 * doing main work
	 */
	size = sizeof(amp_u32_t) * AMP_CONN_MAXIP_PER_NODE;
	raddr = (amp_u32_t *)kmalloc(size, GFP_KERNEL);
	if (!raddr) {
		AMP_ERROR("__amp_netmorn_thread: alloc for raddr error\n");
		goto EXIT;
	}
	memset(raddr, 0, size);
	failaddr = (amp_u32_t *)kmalloc(size, GFP_KERNEL);
	if (!failaddr) {
		AMP_ERROR("__amp_netmorn_thread: alloc for fail addr error\n");
		goto EXIT;
	}
	memset(failaddr, 0, size);
	raddr_num = 0;
	failaddr_num = 0;

	ident = ctxt->acc_this_type << 8;
	ident += ctxt->acc_this_id;

	AMP_ERROR("__amp_netmorn_thread: this type:%d, this id:%d, ident:%d\n",\
                   ctxt->acc_this_type, ctxt->acc_this_id, ident);
	
	sndbufp = (amp_s8_t *)kmalloc(4096, GFP_KERNEL);
	if (!sndbufp) {
		AMP_ERROR("__amp_netmorn_thread: alloc for buffer error\n");
		goto EXIT;
	}
	
	rcvbufp = (amp_s8_t *)kmalloc(4096, GFP_KERNEL);
	if (!rcvbufp) {
		AMP_ERROR("__amp_netmorn_thread: alloc for buffer error\n");
		goto EXIT;
	}

	/*	
	err = sock_create_kern(PF_INET, SOCK_RAW, IPPROTO_ICMP, &icmp_sock);
	if (err < 0) {
		AMP_ERROR("__amp_netmorn_thread: create icmp sock error, err:%d\n", err);
		goto EXIT;
	} 
	hold = 65536;
	err = sock_setsockopt(icmp_sock, 
                              SOL_SOCKET, 
                              SO_SNDBUF, (amp_s8_t *)&hold, 
			      sizeof(hold));
	if (err) {
		AMP_ERROR("__amp_netmorn_thread: set send buffer error, err:%d\n", err);
		goto EXIT;
	}
	err = sock_setsockopt(icmp_sock, 
			      SOL_SOCKET, 
                              SO_RCVBUF,
                              (amp_s8_t *)&hold,
			      sizeof(hold));
	if (err) {
		AMP_ERROR("__amp_netmorn_thread: set recv buffer error, err:%d\n", err);
		goto EXIT;
	}
	
	tv.tv_sec = 3;
	tv.tv_usec = 0;
	err = sock_setsockopt(icmp_sock, SOL_SOCKET, 
                              SO_SNDTIMEO, (amp_s8_t *)&tv, sizeof(tv));
	if (err) {
		AMP_ERROR("__amp_netmorn_thread: set send timeout error, err:%d\n", err);
		goto EXIT;
	}

	err = sock_setsockopt(icmp_sock, SOL_SOCKET, 
                              SO_RCVTIMEO, (amp_s8_t *)&tv, sizeof(tv));
	if (err) {
		AMP_ERROR("__amp_netmorn_thread: set recv timeout error, err:%d\n", err);
		goto EXIT;
	}
	filt.data = ~((1<<ICMP_SOURCE_QUENCH)|
		      (1<<ICMP_DEST_UNREACH)|
		      (1<<ICMP_TIME_EXCEEDED)|
		      (1<<ICMP_PARAMETERPROB)|
		      (1<<ICMP_REDIRECT)|
		      (1<<ICMP_ECHOREPLY));
	
	err = sock_setsockopt(icmp_sock, SOL_RAW, ICMP_FILTER, (char*)&filt, sizeof(filt));
	if (err) {
		AMP_ERROR("__amp_netmorn_thread: set icmp filter error, err:%d\n", err);
		goto EXIT;
	}
	*/


WAKE_UP:

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
				/*AMP_DMSG("__amp_netmorn_thread: type:%d,id:%d,queue is empty\n", \
                                          type, id);*/
				continue;
                        }
			AMP_DMSG("__amp_netmorn_thread: type:%d, id:%d\n", type, id);

			read_lock(&ctxt->acc_conns[type].acc_remote_conns[id].queue_lock);
			cnq = &(ctxt->acc_conns[type].acc_remote_conns[id]);
			raddr_num = 0;
			for (i=0; i<=cnq->active_conn_num; i++) {
				conn = cnq->conns[i];
				if (!conn)
					continue;
				AMP_DMSG("__amp_netmorn_thread: conn:%p\n", \
                                          conn);
				amp_lock(&conn->ac_lock);
				/*
				if ((conn->ac_state == AMP_CONN_OK) && 
				    (conn->ac_need_reconn)) {
				*/
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
							amp_unlock(&conn->ac_lock);
							break;
						}
					}
				}
				amp_unlock(&conn->ac_lock);
			}
			read_unlock(&ctxt->acc_conns[type].acc_remote_conns[id].queue_lock);
			
			/*do icmp*/
			failaddr_num = 0;
			for (i=0; i<raddr_num; i++) {
				err = sock_create_kern(PF_INET, SOCK_RAW, IPPROTO_ICMP, &icmp_sock);
				if (err < 0) {
					AMP_ERROR("__amp_netmorn_thread: create icmp sock error, err:%d\n", err);
					goto EXIT;
				} 
				hold = 65536;
				err = sock_setsockopt(icmp_sock, 
                              			      SOL_SOCKET, 
                                                      SO_SNDBUF, (amp_s8_t *)&hold, 
			        sizeof(hold));
	                        if (err) {
					AMP_ERROR("__amp_netmorn_thread: set send buffer error, err:%d\n", err);
					goto EXIT;
				}
				err = sock_setsockopt(icmp_sock, 
			      			      SOL_SOCKET, 
                              		    	      SO_RCVBUF,
                              			      (amp_s8_t *)&hold,
			                              sizeof(hold));
				if (err) {
					AMP_ERROR("__amp_netmorn_thread: set recv buffer error, err:%d\n", err);
					goto EXIT;
				}
	
				tv.tv_sec = 3;
				tv.tv_usec = 0;
				err = sock_setsockopt(icmp_sock, SOL_SOCKET, 
                              				SO_SNDTIMEO, (amp_s8_t *)&tv, sizeof(tv));
				if (err) {
					AMP_ERROR("__amp_netmorn_thread: set send timeout error, err:%d\n", err);
					goto EXIT;
				}

				err = sock_setsockopt(icmp_sock, SOL_SOCKET, 
                              			      SO_RCVTIMEO, (amp_s8_t *)&tv, sizeof(tv));
				if (err) {
					AMP_ERROR("__amp_netmorn_thread: set recv timeout error, err:%d\n", err);
					goto EXIT;
				}

				filt.data = ~((1<<ICMP_SOURCE_QUENCH)|
		      				(1<<ICMP_TIME_EXCEEDED)|
		      				(1<<ICMP_PARAMETERPROB)|
		      				(1<<ICMP_REDIRECT)|
		      				(1<<ICMP_ECHOREPLY));

	
				err = sock_setsockopt(icmp_sock, SOL_RAW, ICMP_FILTER, (char*)&filt, sizeof(filt));
				if (err) {
					AMP_ERROR("__amp_netmorn_thread: set icmp filter error, err:%d\n", err);
					goto EXIT;
				}
				__amp_install_filter(icmp_sock, ident);

				memset(&whereto, 0, sizeof(whereto));
				whereto.sin_family = AF_INET;
				AMP_DMSG("__amp_netmorn_thread: raddr:%d, id:%d, seq:%d\n", \
                                          raddr[i], ident, seq);
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
			        msg.msg_flags =0;

SEND_AGAIN:
				if (threadp->at_shutdown) {
					AMP_ERROR("__amp_netmorn_thread: before sendmsg, tell us to down\n");
					goto EXIT;
				}
                                err = sock_sendmsg(icmp_sock, &msg, datalen);
				if (err < 0) {
					if (err == -EAGAIN || err == -EINTR)
                                        	goto SEND_AGAIN;

					AMP_ERROR("__amp_netmorn_thread: contact with node, type:%d,id:%d, error:%d\n", \
                                                   type, id, err);
					failaddr[failaddr_num] = raddr[i];
					failaddr_num ++;

					if(icmp_sock->ops && icmp_sock->ops->shutdown)
						icmp_sock->ops->shutdown(icmp_sock, SEND_SHUTDOWN|RCV_SHUTDOWN);
					sock_release(icmp_sock);
					icmp_sock = NULL;

					continue;
				}
				
				/*to receive ack from remote icmp*/

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
				err = sock_recvmsg(icmp_sock, &msg, 4096, MSG_DONTWAIT);
				if (err < 0) {
					if (err == -EAGAIN || err == -EINTR) {	
						tv.tv_sec = 3;
						tv.tv_usec = 0;
						tv_jiffies = timeval_to_jiffies(&tv);
						__set_current_state(TASK_UNINTERRUPTIBLE);
						schedule_timeout(tv_jiffies);
						sleep_time++;
						if (sleep_time >= 5) {
							AMP_ERROR("__amp_netmorn_thread: need send again, type:%d, id:%d, ipaddr:%d\n", \
                                                                  type, id, raddr[i]);
							goto REMOTE_FAILED;
						}
						goto RECV_AGAIN;
					}
REMOTE_FAILED:

					AMP_ERROR("__amp_netmorn_thread: recv from node type:%d,id:%d error:%d\n", \
                                                   type, id, err);
					failaddr[failaddr_num] = raddr[i];
					failaddr_num ++;
					if(icmp_sock->ops && icmp_sock->ops->shutdown)
						icmp_sock->ops->shutdown(icmp_sock, SEND_SHUTDOWN|RCV_SHUTDOWN);
					sock_release(icmp_sock);
					icmp_sock = NULL;

					continue;
				}
				AMP_DMSG("__amp_netmorn_thread: recv err:%d\n", err);

				ip = (struct iphdr *)rcvbufp;
				iphdrlen = ip->ihl << 2;
                                icp = (struct icmphdr *)(rcvbufp + iphdrlen);
				if ((err - iphdrlen) < sizeof(struct icmphdr)) {
					AMP_ERROR("__amp_netmorn_thread: icmp packet is too small, type:%d,id:%d\n", \
                                                   type, id);
					failaddr[failaddr_num] = raddr[i];
					failaddr_num ++;
					if(icmp_sock->ops && icmp_sock->ops->shutdown)
						icmp_sock->ops->shutdown(icmp_sock, SEND_SHUTDOWN|RCV_SHUTDOWN);
					sock_release(icmp_sock);
					icmp_sock = NULL;
	
					continue;
				}
				if (icp->type == ICMP_ECHO) {
					AMP_DMSG("__amp_netmorn_thread: get echo, icp type:%d, id:%d, seq:%d\n", \
                                                  icp->type, icp->un.echo.id, icp->un.echo.sequence);
					goto RECV_AGAIN;
					

                                }
				if ((icp->type == ICMP_ECHOREPLY) && 
                                    (icp->un.echo.id == ident)) {
					if (icp->un.echo.sequence == seq) {
						AMP_DMSG("__amp_netmorn_thread: get reponse from peer, type:%d, id:%d, seq:%d\n", \
                                                          type, icp->un.echo.id, \
                                                          icp->un.echo.sequence);
                                	} else {
						AMP_ERROR("__amp_netmorn_thread: remote node, type:%d,id:%d, unreachable\n", \
                                                  	  type, id); 
						AMP_ERROR("__amp_netmorn_thread: icp type:%d, id:%d, seq:%d\n", \
                                                           icp->type, icp->un.echo.id, icp->un.echo.sequence);
						AMP_ERROR("__amp_netmorn_thread: but ident:%d, seq:%d\n", ident, seq);
						failaddr[failaddr_num] = raddr[i];
                                        	failaddr_num ++;
					
					}
					if(icmp_sock->ops && icmp_sock->ops->shutdown)
						icmp_sock->ops->shutdown(icmp_sock, SEND_SHUTDOWN|RCV_SHUTDOWN);
					sock_release(icmp_sock);
					icmp_sock = NULL;
				} else {
					AMP_ERROR("__amp_netmorn_thread: get reponse, type:%d, id:%d, seq:%d\n", \
                                                  type, icp->un.echo.id, icp->un.echo.sequence);
					AMP_ERROR("__amp_netmorn_thread: we want:id:%d, seq:%d\n", \
                                                   ident, seq);
					goto RECV_AGAIN;
				}
			}
			AMP_DMSG("__amp_netmorn_thread: type:%d, id:%d, failaddr_num:%d\n", \
                                  type, id, failaddr_num);

			if (failaddr_num) {
				write_lock(&ctxt->acc_conns[type].acc_remote_conns[id].queue_lock);
				/*set some connection to be reconn*/
				for (i=0; i<=cnq->active_conn_num; i++) {
					conn = cnq->conns[i];
					if (!conn)
						continue;
					amp_lock(&conn->ac_lock);
					/*
					if ((conn->ac_state != AMP_CONN_OK) ||
					    (!conn->ac_need_reconn)) {
					*/
					if (conn->ac_state != AMP_CONN_OK) {
						amp_unlock(&conn->ac_lock);
						continue;
					}
					for (j=0; j<failaddr_num; j++) {
						if (conn->ac_remote_ipaddr == failaddr[j]) {
							AMP_ERROR("__amp_netmorn_thread: conn:%p\n", conn);
							if (conn->ac_need_reconn)
								conn->ac_state = AMP_CONN_RECOVER;
							else
								conn->ac_state = AMP_CONN_CLOSE;
							conn->ac_datard_count = 0;
							conn->ac_sched = 0;
							amp_lock(&amp_reconn_conn_list_lock);
							if (list_empty(&conn->ac_reconn_list)) {
								AMP_ERROR("__amp_netmorn_thread: add conn:%p to reconn list\n", conn);
								list_add_tail(&conn->ac_reconn_list, \
                                                                              &amp_reconn_conn_list);
							}
							amp_unlock(&amp_reconn_conn_list_lock);
						}
					}
					amp_unlock(&conn->ac_lock);
				}

				/*if we have no valid conn, then we should wake up non-resend request*/
				has_valid_conn = 0;
				for (i=0; i<=cnq->active_conn_num; i++) {
					conn = cnq->conns[i];
					if (!conn)
						continue;
					amp_lock(&conn->ac_lock);
					if (conn->ac_state == AMP_CONN_OK) {
						has_valid_conn ++;
						amp_unlock(&conn->ac_lock);
						break;
					}
					amp_unlock(&conn->ac_lock);
				}
				AMP_DMSG("__amp_netmorn_thread: has_valid_conn:%d\n", has_valid_conn);
				/*
                                 * wake up all waiting for reply request
                                 */
				if (!has_valid_conn) {
					amp_lock(&amp_waiting_reply_list_lock);
					list_for_each_safe(pos, nxt, &amp_waiting_reply_list) {
						req = list_entry(pos, amp_request_t, req_list);
						if ((req->req_remote_id == id) && 
                                                    (req->req_remote_type == type)) {
							AMP_DMSG("__amp_netmorn_thread: find a request:%p\n", req);
							if(!req->req_resent){
								AMP_ERROR("__amp_netmorn_thread: wake up request: %p\n", req);
								list_del_init(&req->req_list);
								req->req_error = -ENETUNREACH;
								__amp_free_request(req);
								amp_sem_up(&req->req_waitsem);
                                                        }
						}
					}
					amp_unlock(&amp_waiting_reply_list_lock);
						
				}
				write_unlock(&ctxt->acc_conns[type].acc_remote_conns[id].queue_lock);
			}
		}

	}
	

	/*
	 * sleep for some time
	 */
	AMP_DMSG("__amp_netmorn_thread: to sleep for %d HZ\n", \
                  AMP_NET_MORNITOR_INTVL);

	if (threadp->at_shutdown)
		goto EXIT;

	init_waitqueue_entry(&__wait, current);
	add_wait_queue(&threadp->at_waitq, &__wait);
	set_current_state(TASK_INTERRUPTIBLE);
	if (threadp->at_shutdown) {
		remove_wait_queue(&threadp->at_waitq, &__wait);
		current->state = TASK_RUNNING;
		goto EXIT;
	}
	tv.tv_sec = AMP_NET_MORNITOR_INTVL;
	tv.tv_usec = 0;
	__set_current_state(TASK_UNINTERRUPTIBLE);
	schedule_timeout(timeval_to_jiffies(&tv));
	remove_wait_queue(&threadp->at_waitq, &__wait);
	current->state = TASK_RUNNING;

	if (threadp->at_shutdown)
		goto EXIT;

	goto WAKE_UP;		


EXIT:
	
	set_fs(old_fs);
	if (icmp_sock) {
		if (icmp_sock->ops && icmp_sock->ops->shutdown)
			icmp_sock->ops->shutdown(icmp_sock, \
                                                 SEND_SHUTDOWN|RCV_SHUTDOWN);
		sock_release(icmp_sock);
	}

	if (raddr)
		kfree(raddr);
	if (failaddr)
		kfree(failaddr);
	if (sndbufp)
		kfree(sndbufp);
	if (rcvbufp)
		kfree(rcvbufp);

#ifdef __KERNEL__
	AMP_ERROR("__amp_netmorn_thread:str:%s leave: %d\n", str, current->pid);
#else
	AMP_LEAVE("__amp_netmorn_thread: leave: %d\n", pthread_self());
#endif
	amp_sem_up(&threadp->at_downsem);

	return 0;
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
	amp_sem_init_locked(&amp_bh_sem);
	amp_sem_init(&amp_reconn_finalize_sem);
	
	
	amp_srvin_thread_num = 0;
	amp_srvout_thread_num = 0;
	amp_reconn_thread_num = 0;
	amp_bh_thread_num = 0;

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

	amp_bh_threads = 
		(amp_thread_t *)amp_alloc(sizeof(amp_thread_t) * AMP_MAX_THREAD_NUM);
	if (!amp_bh_threads) {
                AMP_ERROR("__amp_threads_init: malloc for bh threads error\n");
                err = -ENOMEM;
                amp_free(amp_srvin_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
                amp_free(amp_srvout_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
		amp_free(amp_reconn_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
                goto EXIT;
        }
	
	memset(amp_srvin_threads, 0, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
	memset(amp_srvout_threads, 0, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
	memset(amp_reconn_threads, 0, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
	memset(amp_bh_threads, 0, sizeof(amp_thread_t) * AMP_MAX_THREAD_NUM);
		 

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

	err = __amp_start_reconn_threads ();
	if (err < 0) {
		__amp_stop_srvin_threads ();
		__amp_stop_srvout_threads ();
		goto ERROR;
	}

	err = __amp_start_bh_threads ();
	if (err < 0) {
		__amp_stop_srvin_threads ();
                __amp_stop_srvout_threads ();
		__amp_stop_reconn_threads();
		goto ERROR;
	}
	
EXIT:
	AMP_LEAVE("__amp_threads_init: leave\n");
	return err;

ERROR:
	amp_free(amp_srvin_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
	amp_free(amp_srvout_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
	amp_free(amp_reconn_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
	amp_free(amp_bh_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);

	amp_srvin_threads = NULL;
	amp_srvout_threads = NULL;
	amp_reconn_threads = NULL;
	amp_bh_threads = NULL;
	goto EXIT;

}

/*
 * threads finalize
 */ 
int
__amp_threads_finalize ()
{
	AMP_WARNING("__amp_threads_finalize: enter\n");

	/*
	 * stop all threads
	 */ 
	AMP_WARNING("__amp_threads_finalize: stop srvin threads\n");
	__amp_stop_srvin_threads();
	AMP_WARNING("__amp_threads_finalize: stop srvout threads\n");
	__amp_stop_srvout_threads();
	AMP_WARNING("__amp_threads_finalize: stop reconn threads\n");
	__amp_stop_reconn_threads();
	AMP_WARNING("__amp_threads_finalize: stop bh threads\n");
	__amp_stop_bh_threads();


	/*
	 * free all resources
	 */ 
	amp_free(amp_srvin_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
	amp_free(amp_srvout_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
	amp_free(amp_reconn_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);
	amp_free(amp_bh_threads, sizeof(amp_thread_t)* AMP_MAX_THREAD_NUM);

	amp_srvin_threads = NULL;
	amp_srvout_threads = NULL;
	amp_reconn_threads = NULL;
	amp_bh_threads = NULL;
	  
	AMP_WARNING("__amp_threads_finalize: leave\n");
	return 0;
}

/*end of file*/

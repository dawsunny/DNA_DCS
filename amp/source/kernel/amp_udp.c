/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#include <amp_udp.h>
#include <amp_thread.h>

/* some internal functions */
static inline int 
__amp_udp_general_init (struct socket *sock)
{
	amp_s32_t err = 0;
	amp_s32_t rc = 0;
	struct timeval tv;
	int option;
	struct linger linger;
	mm_segment_t oldmm = get_fs();

	AMP_ENTER("__amp_udp_general_init: enter\n");

	set_fs(KERNEL_DS);

	/*
	 * set recv and send timeout
	 */ 
	tv.tv_sec = AMP_ETHER_SNDTIMEO;
	tv.tv_usec = 0;

	rc = sock_setsockopt(sock, 
			             SOL_SOCKET, 
						 SO_SNDTIMEO, 
						 (amp_s8_t*)&tv, 
						 sizeof(tv));
	if (rc) {
		AMP_ERROR("__amp_udp_general_init: set sndtimeo error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}

	tv.tv_sec = AMP_ETHER_RCVTIMEO;

	rc = sock_setsockopt(sock, 
			             SOL_SOCKET,
						 SO_RCVTIMEO,
						 (amp_s8_t *)&tv,
						 sizeof(tv));
	if (rc) { 
		AMP_ERROR("__amp_udp_general_init: set rcvtimeout error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}

		
	
	/*
	 * set send and rcv buffer size
	 */ 
	option = AMP_ETHER_SNDBUF;
	rc = sock_setsockopt(sock, 
			             SOL_SOCKET, 
						 SO_SNDBUF, 
						 (amp_s8_t *)&option, 
						 sizeof(option));
	if (rc) {
		AMP_ERROR("__amp_udp_general_init: set sndbuf error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}

	option = AMP_ETHER_RCVBUF;
	rc = sock_setsockopt(sock, 
			             SOL_SOCKET,
						 SO_RCVBUF,
						 (amp_s8_t *)&option,
						 sizeof(option));
	if (rc) {
		AMP_ERROR("__amp_udp_general_init: set rcvbuf error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}

	/*
	 * close linger
	 */ 
	linger.l_onoff = 0;
	linger.l_linger = 0;

	rc = sock_setsockopt(sock, 
			             SOL_SOCKET, 
						 SO_LINGER,
						 (amp_s8_t *)&linger,
						 sizeof(linger));
	if (rc) {
		AMP_ERROR("__amp_udp_general_init: close linger error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}
		
EXIT:
	set_fs(oldmm);
	AMP_LEAVE("__amp_udp_general_init: leave\n");
	return err;
}

/* 
 * send data to peer by udp protocol.
 * return: 0 - normal, <0 - abnormal
 */ 
int
__amp_udp_sendmsg (void *protodata, 
				   void *addr,
				   amp_u32_t addr_len,
				   amp_u32_t len, 
				   void *bufp,
				   amp_u32_t flags)
{
	amp_s32_t err = 0;
	amp_s32_t rc = 0;
	mm_segment_t oldmm;
	struct msghdr msg;
	struct iovec fragiov;
	struct socket *sock = NULL;
	struct sockaddr_in *saddrp = NULL;
	amp_s32_t slen;

	AMP_ENTER("__amp_udp_sendmsg enter\n");

	sock = (struct socket *)protodata;

	if (!sock) {
		AMP_ERROR("__amp_udp_sendmsg: no sock\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!addr || !addr_len) {
		AMP_ERROR("__amp_udp_sendmsg: no peer address\n");
		err = -EINVAL;
		goto EXIT;
	}
	saddrp = (struct sockaddr_in *)addr;

	if (!bufp || !len) {
		AMP_ERROR("__amp_udp_sendmsg: no buffer\n");
		err = -EINVAL;
		goto EXIT;
	}

	fragiov.iov_base = bufp;
	fragiov.iov_len = len;

	slen = len;
	oldmm = get_fs();
	set_fs(KERNEL_DS);

	do {
		msg.msg_name = saddrp;
		msg.msg_namelen = addr_len;
		msg.msg_iov = &fragiov;
		msg.msg_iovlen = 1;
		msg.msg_control = NULL;
		msg.msg_flags = flags;

		rc = sock_sendmsg(sock, &msg, slen);

		if (rc < 0) {
			AMP_ERROR("__amp_udp_sendmsg: send msg error, rc:%d\n", rc);
			if (rc == -EAGAIN || rc == -EINTR)
				continue;

			err = rc;
			goto EXIT;
		}

		slen -= rc;
		fragiov.iov_base += rc;
		fragiov.iov_len -= rc;
	} while (slen);

	set_fs(oldmm);

EXIT:
	AMP_LEAVE("__amp_udp_sendmsg: leave\n");
	return err;
}

/*
 * send data to peer by udp protocol.
 */
int 
__amp_udp_senddata(void *protodata, 
		           void *addr,
				   amp_u32_t addr_len,
				   amp_u32_t niov,
				   amp_kiov_t *iov,
				   amp_u32_t flags)
{
	amp_s32_t err = 0;
	amp_s32_t rc = 0;
	struct socket *sock;
	amp_kiov_t *kiov;
	struct page *page;
	amp_u32_t slen;
	amp_u32_t iovs;
	struct iovec fragiov;
	struct msghdr msg;
	struct sockaddr_in *saddrp = NULL;
	char *bufp;
	mm_segment_t oldmm = get_fs();
	amp_u32_t this_flags = flags;


	AMP_ENTER("__amp_udp_senddata: enter\n");
	sock = (struct socket *)protodata;


	if (!sock) {
		AMP_ERROR("__amp_udp_senddata: no sock\n");
		err = EINVAL;
		goto EXIT;
	}

	if (!addr || !addr_len) {
		AMP_ERROR("__amp_udp_senddata: no address\n");
		err = -EINVAL;
		goto EXIT;
	}
	saddrp = (struct sockaddr_in *)addr;

	if(!iov || !niov) {
		AMP_ERROR("__amp_udp_senddata: no iov\n");
		err = -EINVAL;
		goto EXIT;
	}

	set_fs(KERNEL_DS);

	iovs = niov;
	kiov = iov;

	while (iovs) {
		if ((kiov->ak_len + kiov->ak_offset) > PAGE_SIZE) {
			AMP_ERROR("__amp_udp_senddata: ak_len:%d, ak_offset:%d, too larger\n",
					  kiov->ak_len, kiov->ak_offset);
			err = -EINVAL;
			goto EXIT;

		}
			
		page = kiov->ak_page;
		slen = kiov->ak_len;

		if (!page) {
			AMP_ERROR("__amp_udp_senddata: no page\n");
			err = -EINVAL;
			goto EXIT;
		}
		
		bufp = ((amp_s8_t *)kmap(page)) + kiov->ak_offset;
		slen = kiov->ak_len;

		fragiov.iov_base = bufp;
		fragiov.iov_len = slen;

		if (iovs > 1)
			this_flags = flags | MSG_MORE;
		
		while (slen) {
			msg.msg_name = saddrp; 
			msg.msg_namelen = addr_len;
			msg.msg_iov = &fragiov;
			msg.msg_iovlen = 1;
			msg.msg_control = NULL;
			msg.msg_controllen = 0;
			msg.msg_flags = this_flags;
			
			rc = sock_sendmsg(sock, &msg, slen);

			if (rc < 0) {
				AMP_ERROR("__amp_udp_senddata: send error, rc:%d\n", rc);
				if (rc == -EAGAIN || rc == -EINTR)
					continue;

				err = rc;
				goto EXIT;
			}

			fragiov.iov_base += rc;
			fragiov.iov_len -= rc;
			slen -= rc;
		}

		iovs --;
		kiov ++;
	}

EXIT:
	set_fs(oldmm);
	AMP_LEAVE("__amp_udp_senddata: leave\n");
	return err;
}

/* 
 * recvmsg
 */ 
int 
__amp_udp_recvmsg(void *protodata,
		          void *addr,
				  amp_u32_t addr_len,
				  amp_u32_t len,
				  void *bufp,
				  amp_u32_t flags)
{
	amp_s32_t err = 0;
	amp_s32_t rc = 0;
	amp_u32_t slen;
	struct socket *sock = NULL;
	struct iovec fragiov;
	struct msghdr msg;
	struct sockaddr_in *saddrp = NULL;
	mm_segment_t  oldmm = get_fs();

	AMP_ENTER("__amp_udp_recvmsg: enter\n");

	sock = (struct socket *)protodata;
	
	if (!sock) {
		AMP_ERROR("__amp_udp_recvmsg: no socket\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!addr || !addr_len) {
		AMP_ERROR("__amp_udp_recvmsg: no address\n");
		err = -EINVAL;
		goto EXIT;
	}

	saddrp = (struct sockaddr_in *)addr;


	if (!bufp || !len) {
		AMP_ERROR("__amp_udp_recvmsg: no buffer\n");
		err = -EINVAL;
		goto EXIT;
	}

	fragiov.iov_base = bufp;
	fragiov.iov_len = len;

	slen = len;

	set_fs(KERNEL_DS);

	while (slen) {
		msg.msg_name = saddrp;
		msg.msg_namelen = addr_len;
		msg.msg_iov = &fragiov;
		msg.msg_iovlen = 1;
		msg.msg_control = NULL;
		msg.msg_controllen = 0;
		msg.msg_flags = flags;

		rc = sock_recvmsg (sock, &msg, slen, flags);

		if (rc <= 0) {
			AMP_ERROR("__amp_udp_recvmsg: recv error, rc:%d\n", rc);
			err = rc;
			if (rc == 0)
				err = -EPROTO;
			goto EXIT;
		}

		fragiov.iov_base += rc;
		fragiov.iov_len -= rc;

		slen -= rc;
	}

EXIT:
	set_fs(oldmm);
	AMP_LEAVE("__amp_udp_recvmsg: leave\n");
	return err;

}


/*
 * recv data blocks
 */ 
int 
__amp_udp_recvdata (void *protodata,
		            void *addr,
					amp_u32_t addr_len,
					amp_u32_t niov,
					amp_kiov_t *iov,
					amp_u32_t flags)
{
	amp_s32_t err = 0;
	amp_s32_t rc = 0;
	amp_u32_t slen;
	amp_u32_t iovs;
	struct socket *sock = NULL;
	struct iovec fragiov;
	struct msghdr msg;
	struct sockaddr_in *saddrp = NULL;
	amp_kiov_t *kiov = NULL;
	char *bufp = NULL;
	mm_segment_t  oldmm = get_fs();

	AMP_ENTER("_amp_udp_recvdata: enter\n");

	sock = (struct socket *)protodata;

	if (!sock) {
		AMP_ERROR("__amp_udp_recvdata: no sock\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!addr || !addr_len) {
		AMP_ERROR("__amp_udp_recvdata: no address\n");
		err = -EINVAL;
		goto EXIT;
	}

	saddrp = (struct sockaddr_in *)addr;

	if (!iov || !niov) {
		AMP_ERROR("__amp_udp_recvdata: no kiov\n");
		err = -EINVAL;
		goto EXIT;
	}

	kiov = iov;
	iovs = niov;

	set_fs(KERNEL_DS);

	while (iovs) {
		if (kiov->ak_len + kiov->ak_offset > PAGE_SIZE) {
			AMP_ERROR("__amp_udp_recvdata: ak_len:%d, ak_offset:%d, too large\n",
					   kiov->ak_len, kiov->ak_offset);
			err = -EINVAL;
			goto EXIT;
		}

		bufp = ((amp_s8_t *)kmap(kiov->ak_page)) + kiov->ak_offset;

		fragiov.iov_base = bufp;
		fragiov.iov_len = kiov->ak_len;
		slen = kiov->ak_len;

		while (slen) {
			msg.msg_name = saddrp;
			msg.msg_namelen = addr_len;
			msg.msg_iov = &fragiov;
			msg.msg_iovlen = 1;
			msg.msg_control = NULL;
			msg.msg_controllen = 0;
			msg.msg_flags = flags;

			rc = sock_recvmsg (sock, &msg, slen, flags);
			if (rc <= 0) {
				AMP_ERROR("__amp_udp_recvdata: recv error, rc:%d\n", rc);
				if (rc == -EAGAIN)
					continue;

				if (rc == 0)
					err = -EPROTO;
				else
					err = rc;
				goto EXIT;
			}

			fragiov.iov_base += rc;
			fragiov.iov_len -= rc;
			slen -= rc;
		}

		iovs --;
		kiov ++;
	}


EXIT:
	set_fs(oldmm);
	AMP_LEAVE("__amp_udp_recvdata: leave\n");
	return err;
}

/*
 * doing a connect
 */ 
int 
__amp_udp_connect (void *protodata_parent,
		           void **protodata_child,
				   void *addr,
				   amp_u32_t direction)
{
	amp_s32_t err;
	struct socket *new_sock = NULL;
	struct sockaddr_in *saddrp = NULL;

	AMP_ENTER("__amp_udp_connect: enter\n");

	switch (direction) {
		case AMP_CONN_DIRECTION_LISTEN:
			AMP_DEBUG(AMP_DEBUG_UDP|AMP_DEBUG_MSG, "__amp_udp_connect: listen\n");
			if (!addr) {
				AMP_ERROR("__amp_udp_connect: no address\n");
				err = -EINVAL;
				goto EXIT;
			}
			saddrp = (struct sockaddr_in *)addr;
			err = sock_create_kern (PF_INET, SOCK_DGRAM, IPPROTO_UDP, &new_sock);
			if (err < 0) {
				AMP_ERROR("__amp_udp_connect: create socket error, err:%d\n", err);
				goto EXIT;
			}

			break;

		case AMP_CONN_DIRECTION_ACCEPT:
			AMP_DEBUG(AMP_DEBUG_UDP|AMP_DEBUG_MSG, "__amp_udp_connect: accept\n");
			AMP_ERROR("__amp_udp_connect: udp no accept\n");
			err = -EINVAL;
			goto EXIT;
			break;

		case AMP_CONN_DIRECTION_CONNECT:
			AMP_DEBUG(AMP_DEBUG_UDP|AMP_DEBUG_MSG, "__amp_udp_connect: connect\n");
			if (!addr) {
				AMP_ERROR("__amp_udp_connect: no address\n");
				err = -EINVAL;
				goto EXIT;
			}
			saddrp = (struct sockaddr_in *)addr;

			err = sock_create_kern(PF_INET, SOCK_DGRAM, IPPROTO_UDP, &new_sock);
			if (err < 0) {
				AMP_ERROR("__amp_udp_connect: create socket error, err:%d\n", err);
				goto EXIT;
			}

			/* 
			 * in udp we do not need any build connection, just create a 
			 * socket for future use
			 */ 

			break;
		default:
			AMP_ERROR("__amp_udp_connect: wrong direction:%d\n", direction);
			err = -EINVAL;
			break;
	}

	/*
	 * now doing some initialization
	 */

	err = __amp_udp_general_init(new_sock);
	if (err) {
		sock_release(new_sock);
		goto EXIT;
	}

	/*
	 * return it to connection
	 */ 
	*protodata_child = new_sock;

EXIT:
	AMP_LEAVE("__amp_udp_connection: leave\n");
	return err;
}

/*
 * disconnect a udp socket
 */ 
int 
__amp_udp_disconnect (void *protodata)
{
	amp_s32_t err = 0;
	struct socket *sock = NULL;
	amp_connection_t *conn = NULL;
	
	AMP_ENTER("__amp_udp_disconnect: enter\n");

	if (!protodata) {
		AMP_ERROR("__amp_udp_disconnect: no sock\n");
		err = -EINVAL;
		goto EXIT;
	}
	sock = (struct socket *)protodata;

	conn = (amp_connection_t *)(sock->sk->sk_user_data);
	if (!conn) {
		AMP_ERROR("__amp_udp_disconnect: no connection in socket\n");
		err = -EINVAL;
		goto EXIT;
	}

	sock->sk->sk_data_ready = conn->ac_saved_data_ready;
	sock->sk->sk_write_space = conn->ac_saved_write_space;

	
	if (sock->ops && sock->ops->shutdown)
		sock->ops->shutdown(sock, SEND_SHUTDOWN|RCV_SHUTDOWN);

	sock_release (sock);
	
EXIT:
	AMP_LEAVE("__amp_udp_disconnect: leave\n");
	return err;
}

/*
 * init socket for udp
 */
int 
__amp_udp_init (void * protodata, amp_u32_t direction)
{
	amp_s32_t  err = 0;
	struct socket *sock = NULL;
	amp_connection_t *conn = NULL;
	
	AMP_ENTER("__amp_udp_init: enter\n");

	if (!protodata) {
		AMP_ERROR("__amp_udp_init: no proto data\n");
		err = -EINVAL;
		goto EXIT;
	}

	sock = (struct socket *)protodata;
	

	conn = (amp_connection_t *)(sock->sk->sk_user_data);
	if (!conn) {
		AMP_ERROR("__amp_udp_init: no conn in socket\n");
		err = -EINVAL;
		goto EXIT;
	}

	conn->ac_saved_state_change = sock->sk->sk_state_change;
	conn->ac_saved_data_ready = sock->sk->sk_data_ready;
	conn->ac_saved_write_space = sock->sk->sk_write_space;
	

	switch (direction)  {
		case AMP_CONN_DIRECTION_ACCEPT:
			AMP_DMSG("__amp_udp_init: no accept direction for udp\n");
			break;
		case AMP_CONN_DIRECTION_CONNECT:
		case AMP_CONN_DIRECTION_LISTEN:
			sock->sk->sk_data_ready = __amp_udp_data_ready;
			sock->sk->sk_write_space = __amp_udp_write_space;
			break;
		
		default:
			AMP_ERROR("__amp_udp_init: error direction: %d\n", direction);
			err = -EINVAL;
			break;
	}

EXIT:
	AMP_LEAVE("__amp_udp_init: leave\n");
	return err;

}

/*
 * callback when data ready
 */ 
void 
__amp_udp_data_ready (struct sock *sk, int count)
{
	amp_connection_t *conn;
	unsigned long flags;

	conn = (amp_connection_t *)(sk->sk_user_data);

	if(!conn) {
		sk->sk_data_ready(sk, count);
	}

	spin_lock_irqsave(&conn->ac_lock, flags);

	if (conn->ac_sched) {
		conn->ac_sched = 1;
		spin_lock(&amp_dataready_conn_list_lock);
		list_add_tail(&conn->ac_dataready_list, &amp_dataready_conn_list);
		up(&amp_process_in_sem);
		spin_unlock(&amp_dataready_conn_list_lock);
	}
	spin_unlock_irqrestore(&conn->ac_lock, flags);

}

/*
 * callback when connect coming
 *
 * The above fs component can using a dedicated thread to accept remote acceptions
 * so that we don't need any list_data_ready callbacks.
 */ 




/*
 * callback when having free space
 *
 * maybe we do not need it.
 */ 
void
__amp_udp_write_space (struct sock *sk)
{
	if (sk->sk_sleep && waitqueue_active(sk->sk_sleep))
		wake_up_interruptible(sk->sk_sleep);
}


amp_proto_interface_t  amp_udp_proto_interface = {
	type: AMP_CONN_TYPE_UDP,
	amp_proto_sendmsg: __amp_udp_sendmsg,
	amp_proto_senddata: __amp_udp_senddata,
    amp_proto_recvmsg: __amp_udp_recvmsg,
	amp_proto_recvdata: __amp_udp_recvdata,
    amp_proto_connect: __amp_udp_connect,
	amp_proto_disconnect: __amp_udp_disconnect,
	amp_proto_init: __amp_udp_init
};
					   

/*end of file*/

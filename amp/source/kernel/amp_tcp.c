/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#include <amp_tcp.h>
#include <amp_thread.h>

/* some internal functions */
static void __amp_tcp_debug_hook(void)
{
	AMP_DMSG("__amp_tcp_debug_hook\n");

}
static inline int 
__amp_tcp_general_init (struct socket *sock)
{
	amp_s32_t err = 0;
	amp_s32_t rc = 0;
	struct timeval tv;
	int option;
	struct linger linger;
	mm_segment_t oldmm = get_fs();

	AMP_ENTER("__amp_tcp_general_init: enter\n");

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
		AMP_ERROR("__amp_tcp_general_init: set sndtimeo error, rc:%d\n", rc);
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
		AMP_ERROR("__amp_tcp_general_init: set rcvtimeout error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}

		
	
	/*
	 * close nangle
	 */ 
	option = 1;
	rc = sock->ops->setsockopt(sock, 
		                   SOL_TCP, 
				   TCP_NODELAY, 
				   (amp_s8_t *)&option, 
				   sizeof(option));
	if (rc) {
		AMP_ERROR("__amp_tcp_general_init: close nangle error, rc:%d\n", rc);
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
		AMP_ERROR("__amp_tcp_general_init: set sndbuf error, rc:%d\n", rc);
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
		AMP_ERROR("__amp_tcp_general_init: set rcvbuf error, rc:%d\n", rc);
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
		AMP_ERROR("__amp_tcp_general_init: close linger error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}

	option = -1;
	rc = sock->ops->setsockopt(sock, 
			           SOL_TCP, 
				   TCP_LINGER2,
				   (amp_s8_t *)&option,
				   sizeof(option));
	if (rc) {
		AMP_ERROR("__amp_tcp_general_init: close linger2 error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}

	option = 1;
	rc = sock_setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, 
                            (char *)&option, sizeof(option));
	if (rc < 0) {
		AMP_ERROR("__amp_tcp_general_init: set keepalive error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}

	option = AMP_KEEP_IDLE;
	rc = sock->ops->setsockopt(sock, 
                                   SOL_TCP, 
                                   TCP_KEEPIDLE, 
                                   (char *)&option, 
                                   sizeof(option));
	if (rc < 0) {
		AMP_ERROR("__amp_tcp_general_init: set keep idle error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}

	option = AMP_KEEP_INTVL;
	rc = sock->ops->setsockopt(sock, 
                                   SOL_TCP, 
                                   TCP_KEEPINTVL, 
                                   (char *)&option, 
                                   sizeof(option));
	if (rc < 0) {
		AMP_ERROR("__amp_tcp_general_init: set keep intvl error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}

	option = AMP_KEEP_COUNT;
	rc = sock->ops->setsockopt(sock, 
                                   SOL_TCP, 
                                   TCP_KEEPCNT, 
                                   (char *)&option, 
                                   sizeof(option));
	if (rc < 0) {
		AMP_ERROR("__amp_tcp_general_init: set keep count error, rc:%d\n", rc);
		err = rc;
		goto EXIT;
	}

		
EXIT:
	set_fs(oldmm);
	AMP_LEAVE("__amp_tcp_general_init: leave\n");
	return err;
}

/* 
 * send data to peer by tcp protocol.
 * return: 0 - normal, <0 - abnormal
 */ 
int
__amp_tcp_sendmsg (void *protodata,
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
	amp_s32_t  slen;
	amp_s8_t  *tmpbufp = NULL;

	AMP_ENTER("__amp_tcp_sendmsg enter, len:%d\n", len);

	sock = (struct socket *)protodata;

	if (!sock) {
		AMP_ERROR("__amp_tcp_sendmsg: no sock\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!bufp || !len) {
		AMP_ERROR("__amp_tcp_sendmsg: no buffer\n");
		err = -EINVAL;
		goto EXIT;
	}

	tmpbufp = bufp;
	slen = len;
	oldmm = get_fs();
	set_fs(KERNEL_DS);

	while (slen) {
		fragiov.iov_base = (char *)tmpbufp;
		fragiov.iov_len = slen;
		msg.msg_name = NULL;
		msg.msg_namelen = 0;
		msg.msg_iov = &fragiov;
		msg.msg_iovlen = 1;
		msg.msg_control = NULL;
		msg.msg_flags = flags;

		rc = sock_sendmsg(sock, &msg, slen);

		if (rc <= 0) {
			if (rc == -EAGAIN || rc == -EINTR)
				continue;
			if (rc == 0)
				rc = -ECONNABORTED;
			AMP_ERROR("__amp_tcp_sendmsg: send msg error, rc:%d\n", rc);

			err = rc;
			goto EXIT;
		}
		if (rc < slen) 
			AMP_DMSG("__amp_tcp_sendmsg: sent:%c, slen:%d, less\n", rc, slen);

		tmpbufp += rc;
		slen -= rc;
	}

	set_fs(oldmm);

EXIT:
	AMP_LEAVE("__amp_tcp_sendmsg: leave\n");
	return err;
}

/*
 * send data to peer by tcp protocol.
 */
int 
__amp_tcp_senddata(void *protodata, 
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
	char *bufp;
	mm_segment_t oldmm = get_fs();
	amp_u32_t this_flags = 0;


	AMP_ENTER("__amp_tcp_senddata: enter\n");
	sock = (struct socket *)protodata;


	if (!sock) {
		AMP_ERROR("__amp_tcp_senddata: no sock\n");
		err = EINVAL;
		goto EXIT;
	}

	if(!iov || !niov) {
		AMP_ERROR("__amp_tcp_senddata: no iov\n");
		err = -EINVAL;
		goto EXIT;
	}

	set_fs(KERNEL_DS);

	iovs = niov;
	kiov = iov;

	AMP_DMSG("__amp_tcp_senddata: niov:%d, iov:%p\n", iovs, kiov);
	while (iovs) {
		if ((kiov->ak_len + kiov->ak_offset) > PAGE_SIZE) {
			AMP_ERROR("__amp_tcp_senddata: ak_len:%d, ak_offset:%d, too larger\n",
				  kiov->ak_len, kiov->ak_offset);
			err = -EINVAL;
			goto EXIT;

		}
			
		page = kiov->ak_page;

		if (!page) {
			AMP_ERROR("__amp_tcp_senddata: no page\n");
			err = -EINVAL;
			goto EXIT;
		}
		bufp = kmap(page);
		bufp +=  kiov->ak_offset;
		slen = kiov->ak_len;
		
		AMP_ENTER("__amp_tcp_senddata: page:%p, slen:%d\n", page, slen);

		if (iovs > 1)
			this_flags = flags | MSG_MORE;
		while (slen) {
			fragiov.iov_base = bufp;
			fragiov.iov_len = slen;
			msg.msg_name = NULL;
			msg.msg_namelen = 0;
			msg.msg_iov = &fragiov;
			msg.msg_iovlen = 1;
			msg.msg_control = NULL;
			msg.msg_controllen = 0;
			msg.msg_flags = this_flags;
			AMP_DMSG("__amp_tcp_senddata: before sendmsg, slen:%d\n", slen);

			rc = sock_sendmsg(sock, &msg, slen);
			if (rc <= 0) {
				if (rc == -EAGAIN || rc == -EINTR)
					continue;

				AMP_ERROR("__amp_tcp_senddata: send error, rc:%d\n", rc);

				if (rc == 0)
					rc = -ECONNABORTED;
				err = rc;
				kunmap(page);
				goto EXIT;
			}
			if (rc < slen) 
				AMP_DMSG("__amp_tcp_senddata: sent:%d, slen:%d, less\n", rc, slen);
			AMP_DMSG("__amp_tcp_sendata: total sent:%d\n", rc);

			bufp += rc;
			slen -= rc;
		}

		kunmap(page);

		iovs --;
		kiov ++;
	}

EXIT:
	set_fs(oldmm);
	AMP_LEAVE("__amp_tcp_senddata: leave\n");
	return err;
}

/* 
 * recvmsg
 */ 
int 
__amp_tcp_recvmsg(void *protodata,
		  void *addr,
		  amp_u32_t addr_len,
		  amp_u32_t len,
		  void *bufp,
		  amp_u32_t flags)
{
	amp_s32_t err = 0;
	amp_s32_t rc = 0;
	amp_u32_t slen;
	amp_u32_t first = 1;
	amp_u32_t thisflags = 0;
	struct socket *sock = NULL;
	struct iovec fragiov;
	struct msghdr msg;
	

	char  *tmpbufp = NULL;
	mm_segment_t  oldmm = get_fs();

	AMP_ENTER("__amp_tcp_recvmsg: enter, len:%d\n", len);

	sock = (struct socket *)protodata;
	
	if (!sock) {
		AMP_ERROR("__amp_tcp_recvmsg: no socket\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!bufp || !len) {
		AMP_ERROR("__amp_tcp_recvmsg: no buffer\n");
		err = -EINVAL;
		goto EXIT;
	}

	tmpbufp = (amp_s8_t *)bufp;

	slen = len;

	set_fs(KERNEL_DS);

	while (slen) {
		fragiov.iov_base = tmpbufp;
		fragiov.iov_len = slen;
		msg.msg_name = NULL;
		msg.msg_namelen = 0;
		msg.msg_iov = &fragiov;
		msg.msg_iovlen = 1;
		msg.msg_control = NULL;
		msg.msg_controllen = 0;
		if (first) {
				msg.msg_flags = flags;
				thisflags = flags;
				first = 0;
		} else {
				msg.msg_flags = MSG_WAITALL;
				thisflags = MSG_WAITALL;
		}	
#if 0
		AMP_DMSG("__amp_tcp_recvmsg: brh, ops:%p, ioctl:%p\n", sock->ops, sock->ops->ioctl);
		err = sock->ops->ioctl(sock, TIOCINQ, (unsigned long)&avail);
		AMP_DMSG("__amp_tcp_recvmsg: after ioctl\n");
		if(err < 0) {
			AMP_ERROR("__amp_tcp_recvmsg: before receive header ioctl error, err:%d\n", err);
		}
		if (avail < slen) {
			AMP_DMSG("__amp_tcp_recvmsg: before recv header, avail:%d, slen:%d\n", \
				     avail, slen);
		}
		AMP_WARNING("__amp_tcp_recvmsg: ai, avail:%d, slen:%d\n", avail, slen);

#endif
		rc = sock_recvmsg (sock, &msg, slen, thisflags);

		if (rc <= 0) {
			if (rc != -EAGAIN && rc != -EINTR)
				AMP_ERROR("__amp_tcp_recvmsg: recv error, rc:%d\n", rc);

			err = rc;
			if (rc == 0)
				err = -ECONNABORTED;
			goto EXIT;
		}

		AMP_DMSG("__amp_tcp_recvmsg: returned rc:%d\n", rc);
#if 0
		if (rc < slen) {
			avail = 0;
			AMP_ERROR("__amp_tcp_recvmsg: recved:%d, slen:%d, less\n", rc, slen);
WAIT_MSG:
			err = sock->ops->ioctl(sock, TIOCINQ, (unsigned long)&avail);
			if (err < 0) {
				AMP_ERROR("__amp_tcp_recvmsg: ioctl error, err:%d\n", err);
				goto EXIT;
			}
			if (avail < (slen - rc)) {
				printk("__amp_tcp_recvmsg: avail:%d, slen:%d\n", avail, slen);
				yield();
				//AMP_DMSG("__amp_tcp_recvmsg: conn_resched returned:%d\n", err);
				goto WAIT_MSG;
			}
		}
#endif
		tmpbufp += rc;
		slen -= rc;
	}
	
	err = 0;

EXIT:
	set_fs(oldmm);
	AMP_LEAVE("__amp_tcp_recvmsg: leave\n");
	return err;

}


/*
 * recv data blocks
 */ 
int 
__amp_tcp_recvdata (void *protodata,
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
	amp_kiov_t *kiov = NULL;
	char *bufp = NULL;
	mm_segment_t  oldmm = get_fs();

	AMP_ENTER("_amp_tcp_recvdata: enter\n");

	set_fs(KERNEL_DS);

	sock = (struct socket *)protodata;

	if (!sock) {
		AMP_ERROR("__amp_tcp_recvdata: no sock\n");
		err = -EINVAL;
		goto EXIT;
	}

	if (!iov || !niov) {
		AMP_ERROR("__amp_tcp_recvdata: no kiov\n");
		err = -EINVAL;
		goto EXIT;
	}

	AMP_DMSG("__amp_tcp_recvdata: niov:%d, iov:%p\n", niov, iov);

	kiov = iov;
	iovs = niov;

	while (iovs) {
		if (kiov->ak_len + kiov->ak_offset > PAGE_SIZE) {
			AMP_ERROR("__amp_tcp_recvdata: ak_len:%d, ak_offset:%d, too large\n",
					   kiov->ak_len, kiov->ak_offset);
			err = -EINVAL;
			goto EXIT;
		}
		AMP_ENTER("__amp_tcp_recvdata: iovs:%d, page:%p, off:%d\n", \
                         iovs, kiov->ak_page, kiov->ak_offset);

		bufp = kmap(kiov->ak_page);
		bufp += kiov->ak_offset;

		fragiov.iov_base = bufp;
		slen = kiov->ak_len;
		while (slen) {
			fragiov.iov_base = bufp;
			fragiov.iov_len = slen;

			msg.msg_name = NULL;
			msg.msg_namelen = 0;
			msg.msg_iov = &fragiov;
			msg.msg_iovlen = 1;
			msg.msg_control = NULL;
			msg.msg_controllen = 0;
			msg.msg_flags = flags | MSG_WAITALL;
			AMP_DMSG("__amp_tcp_recvdata: before sock_recvmsg, sock:%p, msg:%p, slen:%d\n", \
				  sock, &msg, slen);
			AMP_DMSG("__amp_tcp_recvdata: iov_base:%p, iov_len:%ld\n", \
                                  fragiov.iov_base, fragiov.iov_len);
#if 0
WAIT:
			avail = 0;
			err = sock->ops->ioctl(sock, TIOCINQ, (unsigned long)&avail);
			if (err < 0) {
				AMP_ERROR("__amp_tcp_recvdata: ioctl error, err:%d\n", err);
				goto EXIT;
			}
			if (avail < slen) {	
				yield();
				//AMP_DMSG("__amp_tcp_recvdata: cond_resched returned: %d\n", err);
				//AMP_ENTER("__amp_tcp_recvdata: avail:%d, slen:%d\n", avail, slen);
				goto WAIT;
			}
#endif
			rc = sock_recvmsg (sock, &msg, slen, flags);
			if (rc <= 0) {
				if (rc == -EAGAIN) {
					AMP_DMSG("__amp_tcp_recvdata: get EAGAIN\n");
					continue;
				}

				if (rc == -EINTR) {
					AMP_DMSG("__amp_tcp_recvdata: get EINTR\n");
					continue;
				}
				AMP_ERROR("__amp_tcp_recvdata: recv error, rc:%d\n", rc);

				if (rc == 0)
					err = -ECONNABORTED;
				else
					err = rc;
				kunmap(kiov->ak_page);
				goto EXIT;
			}
			AMP_DMSG("__amp_tcp_recvdata: return rc:%d\n", rc);
			if (rc < slen)
				__amp_tcp_debug_hook();

			bufp += rc;
			slen -= rc;
		}

		kunmap(kiov->ak_page);
		iovs --;
		kiov ++;
	}
EXIT:
	set_fs(oldmm);
	AMP_LEAVE("__amp_tcp_recvdata: leave\n");
	return err;
}

/*
 * doing a connect
 */ 
int 
__amp_tcp_connect (void *protodata_parent,
		   void **protodata_child,
		   void *addr,
		   amp_u32_t direction)
{
	amp_s32_t err;
	struct socket *parent_sock, *new_sock;
	struct sockaddr_in *saddrp;

	AMP_ENTER("__amp_tcp_connect: enter\n");

	switch (direction) {
		case AMP_CONN_DIRECTION_LISTEN:
			AMP_DEBUG(AMP_DEBUG_TCP|AMP_DEBUG_MSG, "__amp_tcp_connect: listen\n");
			if (!addr) {
				AMP_ERROR("__amp_tcp_connect: no address\n");
				err = -EINVAL;
				goto EXIT;
			}
			saddrp = (struct sockaddr_in *)addr;
			err = sock_create_kern (PF_INET, SOCK_STREAM, IPPROTO_TCP, &new_sock);
			if (err < 0) {
				AMP_ERROR("__amp_tcp_connect: create socket error, err:%d\n", err);
				goto EXIT;
			}
			new_sock->sk->sk_reuse = 1;

			err = new_sock->ops->bind(new_sock, 
					                  (struct sockaddr *)saddrp, 
									  sizeof(*saddrp));
			if (err < 0) {
				AMP_ERROR("__amp_tcp_connect: bind error, err:%d\n", err);
				sock_release(new_sock);
				goto EXIT;
			}

			err = new_sock->ops->listen(new_sock, 64);
			if (err < 0) {
				AMP_ERROR("__amp_tcp_connect: listen error, err:%d\n", err);
				sock_release(new_sock);
				goto EXIT;
			}
			break;

		case AMP_CONN_DIRECTION_ACCEPT:
			AMP_DEBUG(AMP_DEBUG_TCP|AMP_DEBUG_MSG, "__amp_tcp_connect: accept\n");
			parent_sock = (struct socket *)protodata_parent;
			if (!parent_sock) {
				AMP_ERROR("__amp_tcp_connect: no parent socket\n");
				err = -EINVAL;
				goto EXIT;
			}

#if(LINUX_VERSION_CODE < (KERNEL_VERSION(2,5,72)))
			new_sock = sock_alloc();

			if (!new_sock) {
				AMP_ERROR("__amp_tcp_connect: alloc sock error\n");
				err = -ENOMEM;
				goto EXIT;
			}
			new_sock->type = parent_sock->type;
			new_sock->ops = parent_sock->ops;
#else
			err = sock_create_lite (PF_INET, SOCK_STREAM, IPPROTO_TCP, &new_sock);
			if (err) {
				AMP_ERROR("__amp_tcp_connect: alloc sock error, err:%d\n", err);
				goto EXIT;
			}
			new_sock->ops = parent_sock->ops;
#endif
			err = parent_sock->ops->accept(parent_sock, new_sock, O_NONBLOCK);
			if (err < 0) {
				AMP_ERROR("__amp_tcp_connect: accept error, err:%d\n", err);
				sock_release(new_sock);
				goto EXIT;
			}

			break;
		case AMP_CONN_DIRECTION_CONNECT:
			AMP_DEBUG(AMP_DEBUG_TCP|AMP_DEBUG_MSG, "__amp_tcp_connect: connect\n");
			if (!addr) {
				AMP_ERROR("__amp_tcp_connect: no address\n");
				err = -EINVAL;
				goto EXIT;
			}
			saddrp = (struct sockaddr_in *)addr;

			err = sock_create_kern(PF_INET, SOCK_STREAM, IPPROTO_TCP, &new_sock);
			if (err < 0) {
				AMP_ERROR("__amp_tcp_connect: create socket error, err:%d\n", err);
				goto EXIT;
			}

			err = new_sock->ops->connect(new_sock, 
				                     (struct sockaddr *)saddrp, 
						     sizeof(*saddrp),
						     0);
										 /*O_NONBLOCK);*/
			if (err < 0) {
				AMP_ERROR("__amp_tcp_connect: connect to server error, err:%d\n", err);
				sock_release(new_sock);
				goto EXIT;
			}
			
			break;
		default:
			AMP_ERROR("__amp_tcp_connect: wrong direction:%d\n", direction);
			err = -EINVAL;
			break;
	}

	/*
	 * now doing some initialization
	 */

	err = __amp_tcp_general_init(new_sock);
	if (err) {
		sock_release(new_sock);
		goto EXIT;
	}

	/*
	 * return it to connection
	 */ 
	*protodata_child = new_sock;

EXIT:
	AMP_LEAVE("__amp_tcp_connect: leave\n");
	return err;
}

/*
 * disconnect 
 */ 
int
__amp_tcp_disconnect (void *protodata)
{
	
	amp_s32_t err = 0;
 	struct socket *sock;
	amp_connection_t  *conn = NULL;
				    
       AMP_ENTER("__amp_tcp_disconnect: enter\n");
						 
       if (!protodata) {
	      AMP_ERROR("__amp_tcp_disconnect: no sock\n");
              err = -EINVAL;
              goto EXIT;
       }
       sock = (struct socket *)protodata;
       if (!sock->sk) {
	      AMP_ERROR("__amp_tcp_disconnect: no sk for sock:%p\n", sock);
	      err = -EINVAL;
	      goto EXIT;
       }
       AMP_DMSG("__amp_tcp_disconnect: get sock:%p\n", sock);

       conn = (amp_connection_t *)(sock->sk->sk_user_data);
       if (!conn) {
	   	AMP_ERROR("__amp_tcp_disconnect: no conn in sock\n");
		err = -EINVAL;
		goto EXIT;
       }
       AMP_ERROR("__amp_tcp_disconnect: get conn:%p\n", conn);

       sock->sk->sk_data_ready = conn->ac_saved_data_ready;
       sock->sk->sk_write_space = conn->ac_saved_write_space;
       sock->sk->sk_state_change = conn->ac_saved_state_change;
       sock->sk->sk_user_data = NULL;

       if (sock->ops && sock->ops->shutdown) {
		AMP_DMSG("__amp_tcp_disconnect: shutdown\n");
		sock->ops->shutdown(sock, SEND_SHUTDOWN|RCV_SHUTDOWN);
       }
       sock_release(sock);
									    
EXIT:
       AMP_WARNING("__amp_tcp_disconnect: leave\n");
       return err;
}

/*
 * init the  socket
 */
int 
__amp_tcp_init (void * protodata, amp_u32_t direction)
{
	amp_s32_t  err = 0;
	struct socket *sock = NULL;
	amp_connection_t  *conn = NULL;
	
	AMP_ENTER("__amp_tcp_init: enter\n");

	if (!protodata) {
		AMP_ERROR("__amp_tcp_init: no proto data\n");
		err = -EINVAL;
		goto EXIT;
	}

	sock = (struct socket *)protodata;

	conn = (amp_connection_t *)(sock->sk->sk_user_data);
	if (!conn) {
		AMP_ERROR("__amp_tcp_init: no conn in sock\n");
		err = -EINVAL;
		goto EXIT;
	}

	conn->ac_saved_state_change = sock->sk->sk_state_change;
	conn->ac_saved_data_ready = sock->sk->sk_data_ready;
	conn->ac_saved_write_space = sock->sk->sk_write_space;

	switch (direction)  {
		case AMP_CONN_DIRECTION_ACCEPT:
		case AMP_CONN_DIRECTION_CONNECT:
			sock->sk->sk_data_ready = __amp_tcp_data_ready;
			sock->sk->sk_write_space = __amp_tcp_write_space;
			sock->sk->sk_state_change = __amp_tcp_state_change;
			break;
			
		case AMP_CONN_DIRECTION_LISTEN:
			sock->sk->sk_data_ready = __amp_tcp_listen_data_ready;
			break;
		default:
			AMP_ERROR("__amp_tcp_init: error direction: %d\n", direction);
			break;
	}

EXIT:
	AMP_LEAVE("__amp_tcp_init: leave\n");
	return err;
}

/*
 * callback when data ready
 */ 
void 
__amp_tcp_data_ready (struct sock *sk, int count)
{
	amp_connection_t *conn;
	amp_u32_t  added = 0;

	AMP_ENTER("__amp_tcp_data_ready: enter\n");

	conn = (amp_connection_t *)(sk->sk_user_data);
	

	if(!conn) {
		sk->sk_data_ready(sk, count);
	}
	AMP_DMSG("__amp_tcp_data_ready: conn:%p\n", conn);
	
	if (sk->sk_state == TCP_CLOSE_WAIT) {
		AMP_DMSG("__amp_tcp_data_ready: it's TCP_CLOSE_WAIT state\n");
		goto EXIT;
	}

	if (sk->sk_state == TCP_CLOSE) {
		AMP_DMSG("__amp_tcp_data_ready: it's TCP_CLOSE state\n");
		goto EXIT;
	}

	spin_lock_bh(&amp_bh_req_list_lock);
	if (conn->ac_bhstate == AMP_CONN_BH_STATECHANGE) {
		AMP_DMSG("__amp_tcp_data_ready: it's already in statechange state\n");
		spin_unlock_bh(&amp_bh_req_list_lock);
		goto EXIT;
	}

	if (conn->ac_state != AMP_CONN_OK) {
		AMP_ERROR("__amp_tcp_data_ready: conn:%p, state:%d, not ok\n", \
                          conn, conn->ac_state);
		spin_unlock_bh(&amp_bh_req_list_lock);
		goto EXIT;
	}

	conn->ac_bhstate = AMP_CONN_BH_DATAREADY;
	if (list_empty(&conn->ac_protobh_list)) {
		AMP_DMSG("__amp_tcp_data_ready: add conn:%p to protobh_list\n", conn);
		list_add_tail(&conn->ac_protobh_list, &amp_bh_req_list);
		added = 1;
	}
	spin_unlock_bh(&amp_bh_req_list_lock);
	if (added) 
		amp_sem_up(&amp_bh_sem);

EXIT:
	AMP_LEAVE("__amp_tcp_data_ready: leave\n");
}

/*
 * callback when connect coming
 *
 * The above fs component can using a dedicated thread to accept remote acceptions
 * so that we don't need any list_data_ready callbacks.
 */ 
/*
 * data ready for listen connection
 */
void
__amp_tcp_listen_data_ready (struct sock *sk, int count)
{
	amp_connection_t *conn = NULL;

	AMP_WARNING("__amp_tcp_listen_data_ready: enter\n");
	conn = (amp_connection_t *)(sk->sk_user_data);
	if(!conn) {
		AMP_ERROR("__amp_tcp_listen_data_ready: no connection in socket\n");
		goto EXIT;		
	}

	amp_sem_up(&conn->ac_listen_sem);

EXIT:
	AMP_WARNING("__amp_tcp_listen_data_ready: leave:%p\n", conn);
	return;
}



/*
 * callback when having free space
 *
 * maybe we do not need it.
 */ 
void
__amp_tcp_write_space (struct sock *sk)
{
	AMP_ENTER("__amp_tcp_write_space: enter\n");
	if (sk->sk_sleep && waitqueue_active(sk->sk_sleep))
		wake_up_interruptible(sk->sk_sleep);
	AMP_LEAVE("__amp_tcp_write_space: leave\n");
}

/*
 * callback when the 
 */
void 
__amp_tcp_state_change (struct sock *sk)
{
	amp_connection_t  *conn = NULL;
	amp_u32_t  added = 0;

	AMP_ENTER("__amp_tcp_state_change: enter\n");
	
	conn = (amp_connection_t *)(sk->sk_user_data);
	if (!conn) {
		AMP_DMSG("__amp_tcp_state_change: no connection in sock \n");
		goto EXIT;
	}
	spin_lock_bh(&amp_bh_req_list_lock);

	if (conn->ac_state != AMP_CONN_OK) {
		AMP_DMSG("__amp_tcp_state_change: conn:%p, state already not ok\n", conn);
		spin_unlock_bh(&amp_bh_req_list_lock);

		goto EXIT;
	}

	if (conn->ac_bhstate == AMP_CONN_BH_STATECHANGE) {
		AMP_ERROR("__amp_tcp_state_change: conn:%p, already changed to statechange\n", \
                           conn);
		spin_unlock_bh(&amp_bh_req_list_lock);
		goto EXIT;
	}
	conn->ac_bhstate = AMP_CONN_BH_STATECHANGE;
	if (list_empty(&conn->ac_protobh_list)) {
		AMP_DMSG("__amp_tcp_state_change: add conn:%p to protobh_list\n", conn);
		list_add_tail(&conn->ac_protobh_list, &amp_bh_req_list);
		added = 1;
	}
	spin_unlock_bh(&amp_bh_req_list_lock);
	
	if (added) 
		amp_sem_up(&amp_bh_sem);

EXIT:
	if (sk->sk_sleep && waitqueue_active(sk->sk_sleep))
		wake_up_interruptible_all(sk->sk_sleep);
	AMP_LEAVE("__amp_tcp_state_change: leave\n");
}

amp_proto_interface_t  amp_tcp_proto_interface = {
	type: AMP_CONN_TYPE_TCP,
	amp_proto_sendmsg: __amp_tcp_sendmsg,
	amp_proto_senddata: __amp_tcp_senddata,
	amp_proto_recvmsg: __amp_tcp_recvmsg,
	amp_proto_recvdata: __amp_tcp_recvdata,
	amp_proto_connect: __amp_tcp_connect,
	amp_proto_disconnect: __amp_tcp_disconnect,
	amp_proto_init: __amp_tcp_init
};



/*end of file*/

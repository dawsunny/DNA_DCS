/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_TCP_H_
#define __AMP_TCP_H_

#include <amp_sys.h>
#include <amp_types.h>
#include <amp_protos.h>
#include <amp_help.h>
#include <amp_conn.h>


int __amp_tcp_sendmsg (void *protodata, 
		   void *addr,
		   amp_u32_t addr_len,
		   amp_u32_t len, 
		   void *bufp,
		   amp_u32_t flags);
int __amp_tcp_senddata (void *protodata, 
	                void *addr,
			amp_u32_t addr_len,
			amp_u32_t niov, 
			amp_kiov_t *iov,
			amp_u32_t flags);
int __amp_tcp_recvmsg (void *protodata, 
		   void *addr,
		   amp_u32_t addr_len,
		   amp_u32_t len, 
		   void *bufp,
		   amp_u32_t flags);
int __amp_tcp_recvdata (void *protodata, 
	                void *addr,
			amp_u32_t addr_len,
			amp_u32_t niov, 
			amp_kiov_t *iov,
			amp_u32_t flags);
int __amp_tcp_connect (void *protodata_parent, 
	               void **protodata_child,
	               void *addr, 
	   	       amp_u32_t direction);

int __amp_tcp_disconnect (void *protodata);
int __amp_tcp_init (void * protodata, amp_u32_t direction);

#ifdef __KERNEL__
void __amp_tcp_data_ready (struct sock *sk, int count);
void __amp_tcp_listen_data_ready (struct sock *sk, int count);
void __amp_tcp_write_space (struct sock *sk);
void __amp_tcp_state_change (struct sock *sk);
void __amp_tcp_listen_data_ready(struct sock *sk, int count);
void __amp_tcp_state_change (struct sock *sk);
#endif

extern amp_proto_interface_t  amp_tcp_proto_interface;
#endif

/*end of file*/

/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_UDP_H_
#define __AMP_UDP_H_

#include <amp_sys.h>
#include <amp_types.h>
#include <amp_protos.h>
#include <amp_help.h>
#include <amp_conn.h>


int __amp_udp_sendmsg (void *protodata, 
                       void *addr,
                       amp_u32_t addr_len,
                       amp_u32_t len, 
                       void *bufp,
                       amp_u32_t flags);
int __amp_udp_senddata (void *protodata, 
                        void *addr,
                        amp_u32_t addr_len,
                        amp_u32_t niov, 
                        amp_kiov_t *iov,
                        amp_u32_t flags);
int __amp_udp_recvmsg (void *protodata, 
                       void *addr,
                       amp_u32_t addr_len,
                       amp_u32_t len, 
                       void *bufp,
                       amp_u32_t flags);
int __amp_udp_recvdata (void *protodata, 
                        void *addr,
                        amp_u32_t addr_len,
                        amp_u32_t niov, 
                        amp_kiov_t *iov,
                        amp_u32_t flags);
int __amp_udp_connect (void *protodata_parent, 
                       void **protodata_child,
                       void *addr, 
                       amp_u32_t direction);

int __amp_udp_disconnect (void *protodata);
int __amp_udp_init (void * protodata, amp_u32_t direction);


extern amp_proto_interface_t  amp_udp_proto_interface;
#endif

/*end of file*/

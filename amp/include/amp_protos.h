/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_PROTOS_H_
#define __AMP_PROTOS_H_

#include <amp_types.h>

/*some alias*/

/*send and receive timeout interval*/
#define AMP_ETHER_SNDTIMEO (5)  /*seconds*/
#define AMP_ETHER_RCVTIMEO (5)  /*seconds*/
#define AMP_ETHER_SNDBUF   (8388608)
#define AMP_ETHER_RCVBUF   (8388608)

/*for keep alive*/
#define AMP_KEEP_IDLE   (15)
#define AMP_KEEP_COUNT  (5)
#define AMP_KEEP_INTVL  (5)

/*unified interface table*/
struct __amp_proto_interface {
    amp_u32_t type;  /*protocal type*/
    int (*amp_proto_sendmsg) (void *protodata, 
                              void *addr,
                              amp_u32_t addr_len,
                              amp_u32_t len, 
                              void *bufp,
                              amp_u32_t flags);
    int (*amp_proto_senddata) (void *protodata, 
                               void *addr,
                               amp_u32_t addr_len,
                               amp_u32_t niov,
                               amp_kiov_t *iov,
                               amp_u32_t flags);
    int (*amp_proto_recvmsg) (void *protodata,
                              void *addr,
                              amp_u32_t addr_len,
                              amp_u32_t len,
                              void *bufp,
                              amp_u32_t flags);
    int (*amp_proto_recvdata) (void *protodata,
                               void *addr,
                               amp_u32_t addr_len,
                               amp_u32_t niov,
                               amp_kiov_t *iov,
                               amp_u32_t flags);
    int (*amp_proto_connect) (void *protodata_parent,
                              void **protodata_child,
                              void *addr,
                              amp_u32_t direction);
    int (*amp_proto_disconnect) (void *protodata);
    int (*amp_proto_init)(void * protodata, amp_u32_t direction);
};
typedef struct __amp_proto_interface  amp_proto_interface_t;

/*a global table*/
extern amp_proto_interface_t *amp_protocol_interface_table[];

/*init function*/
extern void amp_proto_interface_table_init (void);

/*help macro*/
#define TBLOP(tblp,__op) ((tblp)->amp_##__op)
#define AMP_OP(__type, __op)  TBLOP(amp_protocol_interface_table[(__type)],__op)
#define AMP_HAS_TYPE(__tp)   (amp_protocol_interface_table[(__tp)] && (amp_protocol_interface_table[(__tp)]->type == (__tp)))


#endif
/*end of file*/

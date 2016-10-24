/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_IOCTL_H_
#define __AMP_IOCTL_H_

#include <amp_sys.h>
#include <amp_types.h>

/*add a connection*/
struct __amp_conf_add_conn {
	amp_u32_t ipaddr;    /*ip address*/
	amp_u32_t port;      /*port*/
	amp_u32_t type;      /*mds or client, etc.*/
	amp_u32_t remote_id; /*unique id of this component*/
};
typedef struct __amp_conf_add_conn  amp_conf_add_conn_t;

/*break a connection*/
struct __amp_conf_disconn {
	amp_u32_t  ipaddr;
	amp_u32_t  port;
	amp_u32_t  type;
	amp_u32_t  remote_id;
};
typedef struct __amp_conf_disconn  amp_conf_disconn_t;


#ifdef __KERNEL__
#define IOC_AMP_TYPE       'f'
#define IOC_AMP_MIN_NR     50
#define IOC_AMP_ADD_CONN   _IOWR('f', 50, amp_conf_add_conn_t)
#define IOC_AMP_DISCONN    _IOWR('f', 51, amp_conf_disconn_t)
#define IOC_AMP_MAX_NR     51
#endif 

#endif
/*end of file*/

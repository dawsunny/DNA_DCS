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

#endif
/*end of file*/

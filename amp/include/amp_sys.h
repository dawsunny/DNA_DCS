/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_SYS_H_
#define __AMP_SYS_H_

#ifndef __u16
typedef unsigned short __u16;
typedef unsigned char  __u8;
typedef unsigned int   __u32;
#endif

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/time.h>

#ifdef __AMP_LISTEN_POLL
#include <sys/poll.h>
#endif

#ifdef __AMP_LISTEN_EPOLL
#include <sys/epoll.h>		// by Chen Zhuan at 2008-11-03
#endif

#ifdef __AMP_LISTEN_SELECT
#include <sys/select.h>
#endif

#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#include <semaphore.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <amp_list.h>
#include <linux/filter.h>
#include <netinet/ip.h>
#include <netinet/ip_icmp.h>

#endif

/*end of file*/

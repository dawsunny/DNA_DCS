/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_SYS_H_
#define __AMP_SYS_H_

#ifdef __KERNEL__

#include <linux/autoconf.h>
#include <linux/version.h>
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/mm.h>
#include <linux/string.h>
#include <linux/stat.h>
#include <linux/errno.h>
#include <linux/smp_lock.h>
#include <linux/unistd.h>

#include <net/sock.h>
#include <net/tcp.h>

#include <asm/system.h>
#include <asm/uaccess.h>
#include <asm/irq.h>
#include <asm/semaphore.h>

#include <linux/init.h>
#include <linux/fs.h>
#include <linux/file.h>
#include <linux/list.h>
#include <linux/kmod.h>
#include <linux/sysctl.h>
#include <linux/vmalloc.h>
#include <linux/miscdevice.h>
#include <linux/inet.h>
#include <asm/ioctls.h>
#include <linux/icmp.h>
#include <linux/highmem.h>
#else

#ifndef __u16
typedef unsigned short __u16;
typedef unsigned char  __u8;
typedef unsigned int   __u32;
#endif

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/poll.h>
#include <sys/epoll.h>		// by Chen Zhuan at 2008-11-03
#include <sys/select.h>
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

#endif

/*end of file*/

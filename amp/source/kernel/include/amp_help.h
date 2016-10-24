/***********************************************/
/*       Async Message Passing Library         */
/*       Copyright  2005                       */
/*                                             */
/*  Author:                                    */ 
/*          Rongfeng Tang                      */
/***********************************************/
#ifndef __AMP_HELP_H_
#define __AMP_HELP_H_

#include "amp_sys.h"
#include "amp_types.h"

/*allow debug bit*/
#define AMP_DEBUG_ENTRY    0x0000000000000001
#define AMP_DEBUG_LEAVE    0x0000000000000002
#define AMP_DEBUG_ALLOC    0x0000000000000004
#define AMP_DEBUG_FREE     0x0000000000000008
#define AMP_DEBUG_MSG      0x0000000000000010
#define AMP_DEBUG_ERROR    0x0000000000000020
#define AMP_DEBUG_WARNING  0x0000000000000040

/*for specific files*/
#define AMP_DEBUG_TCP      0x0000000000000100
#define AMP_DEBUG_UDP      0x0000000000000200
#define AMP_DEBUG_REQUEST  0x0000000000000400
#define AMP_DEBUG_CONN     0x0000000000000800
#define AMP_DEBUG_THREAD   0x0000000000001000



extern  amp_u64_t  amp_debug_mask;  /*only bits in this mask can be printed*/

		

#ifdef __KERNEL__
#define AMP_DEBUG(__msk, format, a...) \
do { \
	if((__msk) & amp_debug_mask) \
/*	printk("(%s, %d): "format, __FILE__, __LINE__, ##a); \ */ \
		printk("[%d]:"format, current->pid, ##a); \
} while (0)


#else

#define AMP_DEBUG(__msk,format,a...) \
do { \
	if ((__msk) & amp_debug_mask) \
		fprintf(stderr, "[%ld] "format, pthread_self(), ##a); \
} while (0)

#endif

#define AMP_ERROR(format, a...)   AMP_DEBUG(AMP_DEBUG_ERROR, format, ##a)
#define AMP_WARNING(format, a...) AMP_DEBUG(AMP_DEBUG_WARNING, format, ##a)
#define AMP_ENTER(format, a...)   AMP_DEBUG(AMP_DEBUG_ENTRY, format, ##a)
#define AMP_LEAVE(format, a...)   AMP_DEBUG(AMP_DEBUG_LEAVE, format, ##a)
#define AMP_DMSG(format, a...)     AMP_DEBUG(AMP_DEBUG_MSG, format, ##a)
#define AMP_ALLOC(format, a...)   AMP_DEBUG(AMP_DEBUG_ALLOC, format, ##a)
#define AMP_FREE(format, a...)    AMP_DEBUG(AMP_DEBUG_FREE, format, ##a)

/*function for set the amp_debug_mask*/
extern void amp_reset_debug_mask (amp_u64_t newmask);
extern void amp_add_debug_bits (amp_u64_t mask_bits);
extern void amp_clear_debug_bits (amp_u64_t mask_bits);

/*defination for locks*/
#ifdef __KERNEL__
#define amp_lock_init(__lckp) spin_lock_init((__lckp))
#define amp_lock(__lckp)  spin_lock((__lckp))
#define amp_unlock(__lckp)  spin_unlock((__lckp))
#define amp_sem_init(__semp) init_MUTEX((__semp))
#define amp_sem_init_locked(__semp)  init_MUTEX_LOCKED((__semp))
#define amp_sem_down(__semp) down((__semp)) 
#define amp_sem_down_interruptible(__semp) down_interruptible((__semp))
#define amp_sem_up(__semp)   up((__semp))
#else
#define amp_lock_init(__lckp) pthread_mutex_init(__lckp, NULL)
#define amp_lock(__lckp)  pthread_mutex_lock(__lckp)
#define amp_unlock(__lckp) pthread_mutex_unlock(__lckp)

#define amp_sem_init(__semp) sem_init(__semp, 0, 1)
#define amp_sem_init_locked(__semp) sem_init(__semp, 0, 0)
#define amp_sem_down(__semp)  sem_wait(__semp)
#define amp_sem_up(__semp) sem_post(__semp)
#endif

/*definition for memory malloc*/
#ifdef __KERNEL__
static inline amp_s8_t * amp_alloc( amp_u32_t __size)   
{
	amp_s8_t* ret =NULL;
	if (__size > 4096) 
		ret = vmalloc(__size); 
	else   
		ret = kmalloc(__size, GFP_KERNEL); 
	if (ret)
		memset(ret, 0, __size); 
	return ret;
}

#define amp_free(__pt, __size)  \
do { \
	if ((__size) > 4096) \
		vfree((__pt)); \
	else \
		kfree ((__pt)); \
	(__pt) = NULL; \
} while (0)

#else

#define amp_alloc(__size)  malloc((__size))
#define amp_free(__pt, __size)  do { \
	free((__pt)); \
	(__pt) = NULL; \
} while (0)
	

#endif



/*about signals*/
#ifdef __KERNEL__

#if (LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,0))
#define AMP_SIGNAL_MASK_LOCK(task, flags)   \
	spin_lock_irqsave(&((task)->sighand->siglock), (flags))
	
#define AMP_SIGNAL_MASK_UNLOCK(task, flags) \
	spin_unlock_irqrestore(&((task)->sighand->siglock), (flags))

#define AMP_RECALC_SIGPENDING  recalc_sigpending()

#elif defined (CONFIG_RH_2_4_20)

#define AMP_SIGNAL_MASK_LOCK(task, flags)   \
	spin_lock_irqsave(&task->sighand->siglock, flags)
	
#define AMP_SIGNAL_MASK_UNLOCK(task, flags) \
	spin_unlock_irqstore(&task->sighand->siglock, flags)

#define AMP_RECALC_SIGPENDING  recalc_sigpending()

#else /*2.4.x*/

#define AMP_SIGNAL_MASK_LOCK(task, flags)   \
	spin_lock_irqsave(&task->sigmask_lock, flags)
	
#define AMP_SIGNAL_MASK_UNLOCK(task, flags) \
	spin_unlock_irqrestore(&task->sigmask_lock, flags)

#define AMP_RECALC_SIGPENDING  recalc_sigpending(current)

#endif

#endif

/*about gettimeofday*/
#ifdef __KERNEL__
#define amp_gettimeofday(__thistmp)  do_gettimeofday((__thistmp))
#else
#define amp_gettimeofday(__thistmp) gettimeofday((__thistmp), NULL)
#endif

static inline amp_u16_t
__amp_nm_cksum(const amp_u16_t *addr, register amp_s32_t len)
{
	register amp_s32_t nleft = len;
	const amp_u16_t *w = addr;
	register amp_u16_t answer;
	register amp_s32_t sum = 0;

	/*
	 *  Our algorithm is simple, using a 32 bit accumulator (sum),
	 *  we add sequential 16 bit words to it, and at the end, fold
	 *  back all the carry bits from the top 16 bits into the lower
	 *  16 bits.
	 */
	while (nleft > 1)  {
		sum += *w++;
		nleft -= 2;
	}

	/* mop up an odd byte, if necessary */
	if (nleft == 1)
		sum += htons(*(amp_u8_t *)w << 8);

	/*
	 * add back carry outs from top 16 bits to low 16 bits
	 */
	sum = (sum >> 16) + (sum & 0xffff);	/* add hi 16 to low 16 */
	sum += (sum >> 16);			/* add carry */
	answer = ~sum;				/* truncate to 16 bits */
	return (answer);
}




#endif /*#ifdef __AMP_HELP_H*/

/*end of file*/

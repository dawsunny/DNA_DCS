#include <string.h>
#include "hash.h"
#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"
#include "hash.h"
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include "bloom.h"

/* A Simple Hash Function */
dcs_u64_t simple_hash(dcs_u8_t *str)
{
	register dcs_u64_t hash;
	register dcs_u8_t *p;

	for(hash = 0, p = (dcs_u8_t *)str; *p ; p++)
		hash = 1023 * hash + *p;

	return (hash & 0x7FFFFFFFFFFFFFFF);
}

/* RS Hash Function */
dcs_u64_t RS_hash(dcs_u8_t *str)
{
         dcs_u64_t b = 378551;
         dcs_u64_t a = 63689;
         dcs_u64_t hash = 0;

         while (*str)
         {
                 hash = hash * a + (*str++);
                 a *= b;
         }

         return (hash & 0x7FFFFFFFFFFFFFFF);
}

/* JS Hash Function */
dcs_u64_t JS_hash(dcs_u8_t *str)
{
         dcs_u64_t hash = 1315423911;

         while (*str)
         {
                 hash ^= ((hash << 5) + (*str++) + (hash >> 2));
         }
        
         return (hash & 0x7FFFFFFFFFFFFFFF);
}

/* P. J. Weinberger Hash Function */
dcs_u64_t PJW_hash(dcs_u8_t *str)
{
         dcs_u64_t BitsInUnignedInt = (dcs_u64_t)(sizeof(dcs_u64_t) * 8);
         dcs_u64_t ThreeQuarters     = (dcs_u64_t)((BitsInUnignedInt   * 3) / 4);
         dcs_u64_t OneEighth         = (dcs_u64_t)(BitsInUnignedInt / 8);

         dcs_u64_t HighBits          = (dcs_u64_t)(0xFFFFFFFFFFFFFFFF) << (BitsInUnignedInt - OneEighth);
         dcs_u64_t hash              = 0;
         dcs_u64_t test              = 0;

         while (*str)
         {
                 hash = (hash << OneEighth) + (*str++);
                 if ((test = hash & HighBits) != 0)
                 {
                         hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
                 }
         }

         return (hash & 0x7FFFFFFFFFFFFFFF);
}

/* ELF Hash Function */
dcs_u64_t ELF_hash(dcs_u8_t *str)
{
         dcs_u64_t hash = 0;
         dcs_u64_t x    = 0;

         while (*str)
         {
                 hash = (hash << 4) + (*str++);
                 if ((x = hash & 0xF0000000L) != 0)
                 {
                         hash ^= (x >> 24);
                         hash &= ~x;
                 }
         }

         return (hash & 0x7FFFFFFFFFFFFFFF);
}

/* BKDR Hash Function */
dcs_u64_t BKDR_hash(dcs_u8_t *str)
{
         dcs_u64_t seed = 131; // 31 131 1313 13131 131313 etc..
         dcs_u64_t hash = 0;

         while (*str)
         {
                 hash = hash * seed + (*str++);
         }

         return (hash & 0x7FFFFFFFFFFFFFFF);
}

/* SDBM Hash Function */
dcs_u64_t SDBM_hash(dcs_u8_t *str)
{
         dcs_u64_t hash = 0;

         while (*str)
         {
                 hash = (*str++) + (hash << 6) + (hash << 16) - hash;
         }

         return (hash & 0x7FFFFFFFFFFFFFFF);
}

/* DJB Hash Function */
dcs_u64_t DJB_hash(dcs_u8_t *str)
{
         dcs_u64_t hash = 5381;

         while (*str)
         {
                 hash += (hash << 5) + (*str++);
         }

         return (hash & 0x7FFFFFFFFFFFFFFF);
}

/* AP Hash Function */
dcs_u64_t AP_hash(dcs_u8_t *str)
{
         dcs_u64_t hash = 0;
         int i;
         for (i=0; *str; i++)
         {
                 if ((i & 1) == 0)
                 {
                         hash ^= ((hash << 7) ^ (*str++) ^ (hash >> 3));
                 }
                 else
                 {
                         hash ^= (~((hash << 11) ^ (*str++) ^ (hash >> 5)));
                 }
         }

         return (hash & 0x7FFFFFFFFFFFFFFF);
}

/* CRC Hash Function */
dcs_u64_t CRC_hash(dcs_u8_t *str)
{
    dcs_u64_t        nleft   = strlen((dcs_s8_t *)str);
    unsigned long long  sum     = 0;
    unsigned short int *w       = (unsigned short int *)str;
    unsigned short int  answer  = 0;

    /*
     * Our algorithm is simple, using a 32 bit accumulator (sum), we add
     * sequential 16 bit words to it, and at the end, fold back all the
     * carry bits from the top 16 bits into the lower 16 bits.
     */
    while ( nleft > 1 ) {
        sum += *w++;
        nleft -= 2;
    }
    /*
     * mop up an odd byte, if necessary
     */
    if ( 1 == nleft ) {
        *( dcs_u8_t * )( &answer ) = *( dcs_u8_t * )w ;
        sum += answer;
    }
    /*
     * add back carry outs from top 16 bits to low 16 bits
     * add hi 16 to low 16
     */
    sum = ( sum >> 32 ) + ( sum & 0xFFFFFFFF );
    /* add carry */
    sum += ( sum >> 16 );
    /* truncate to 16 bits */
    answer = ~sum;

    return (answer & 0xFFFFFFFFFFFFFFFF);
}


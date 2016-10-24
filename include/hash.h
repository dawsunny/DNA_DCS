#ifndef _HASH_H
#define _HASH_H

#include "dcs_type.h"
#include "dcs_const.h"

#ifdef __cplusplus
extern "C" {
#endif

/* A Simple Hash Function */
dcs_u64_t simple_hash(dcs_u8_t *str);

/* RS Hash Function */
dcs_u64_t RS_hash(dcs_u8_t *str);

/* JS Hash Function */
dcs_u64_t JS_hash(dcs_u8_t *str);

/* P. J. Weinberger Hash Function */
dcs_u64_t PJW_hash(dcs_u8_t *str);

/* ELF Hash Function */
dcs_u64_t ELF_hash(dcs_u8_t *str);

/* BKDR Hash Function */
dcs_u64_t BKDR_hash(dcs_u8_t *str);

/* SDBM Hash Function */
dcs_u64_t SDBM_hash(dcs_u8_t *str);

/* DJB Hash Function */
dcs_u64_t DJB_hash(dcs_u8_t *str);

/* AP Hash Function */
dcs_u64_t AP_hash(dcs_u8_t *str);

/* CRC Hash Function */
dcs_u64_t CRC_hash(dcs_u8_t *str);

#ifdef __cplusplus
}
#endif

#endif

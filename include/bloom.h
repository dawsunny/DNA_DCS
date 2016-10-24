/*bloom filter head file*/

#ifndef _BLOOM_H
#define _BLOOM_H

#include <stdlib.h>
#include <stdint.h>

typedef struct {
	dcs_u64_t asize;
	dcs_u8_t *a;
} BLOOM;

typedef struct {
        dcs_u64_t asize;
        dcs_u8_t *a;
}BMAP;

typedef struct {
	dcs_u8_t **a;
	dcs_u8_t *bmap;
        pthread_mutex_t lock;
	dcs_u8_t level_num;
	dcs_u8_t real_level_num;
}tier_bloom_block;

typedef struct {
        tier_bloom_block **blocks;
	dcs_u8_t *bmap;
        pthread_mutex_t lock; 
	dcs_u64_t blocks_num;
	//dcs_u64_t asize;
}tier_bloom;


BLOOM *bloom_create(dcs_u64_t size);
dcs_s32_t bloom_destroy(BLOOM *bloom);
dcs_s32_t bloom_setbit(BLOOM *bloom, dcs_s32_t n, ...);
dcs_s32_t bloom_check(BLOOM *bloom, dcs_s32_t n, ...);

BMAP *bmap_create(dcs_u64_t size);
dcs_s32_t bmap_destroy(BMAP *bmap);
dcs_s32_t bmap_setbit(BMAP *bmap, dcs_s32_t n, ...);
dcs_s32_t bmap_check(BMAP *bmap, dcs_s32_t n, ...);


tier_bloom * bloom_create1(dcs_u64_t size);
dcs_s32_t bloom_destroy1(tier_bloom * bloom);
dcs_s32_t bloom_block_init(tier_bloom * bloom, dcs_s32_t index);
dcs_s32_t bloom_block_init1(tier_bloom * bloom, dcs_s32_t index, dcs_s32_t n, ...);
dcs_s32_t bloom_block_expand(tier_bloom *bloom, dcs_s32_t index);

dcs_s32_t bloom_bit_add(tier_bloom *bloom, dcs_u64_t index, dcs_s32_t num);
dcs_s32_t bloom_bit_minus(tier_bloom * bloom, dcs_u64_t index, dcs_s32_t num);
#endif


/*bloom filter source file*/

#include "dcs_debug.h"
#include "amp.h"
#include "dcs_const.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"
#include "hash.h"

//#include "compressor_cnd.h"
//#include "compressor_cache.h"
//#include "compressor_op.h"
//#include "compressor_thread.h"
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include "bloom.h"

#define SETBIT(a, n) (a[n/CHAR_BIT] |= (1<<(n%CHAR_BIT)))
#define GETBIT(a, n) (a[n/CHAR_BIT] & (1<<(n%CHAR_BIT)))

BLOOM *bloom_create(dcs_u64_t size)
{
	BLOOM *bloom;
	
	if(!(bloom = (BLOOM *)malloc(sizeof(BLOOM)))) {
        DCS_ERROR("bloom_create err: %d\n", errno);
		return NULL;
	}

	if(!(bloom->a = (dcs_u8_t *)calloc((size + CHAR_BIT-1)/CHAR_BIT, sizeof(dcs_s8_t)))) {
		free(bloom);
        DCS_ERROR("bloom_create err: %d\n", errno);
		return NULL;
	}
	bloom->asize = size;

	return bloom;
}

dcs_s32_t bloom_destroy(BLOOM *bloom)
{
	free(bloom->a);
	free(bloom);

	return 0;
}

dcs_s32_t bloom_setbit(BLOOM *bloom, dcs_s32_t n, ...)
{
	va_list l;
	dcs_u64_t pos;
	dcs_s32_t i;

	va_start(l, n);
	for (i = 0; i < n; i++) {
		pos = va_arg(l, dcs_u64_t);
		SETBIT(bloom->a, pos % bloom->asize);
	}
	va_end(l);

	return 0;
}

dcs_s32_t bloom_check(BLOOM *bloom, dcs_s32_t n, ...)
{
	va_list l;
	dcs_u64_t pos;
	dcs_s32_t i;

	va_start(l, n);
	for (i = 0; i < n; i++) {
		pos = va_arg(l, dcs_u64_t );
		if(!(GETBIT(bloom->a, pos % bloom->asize))) {
			return 0;
		}
	}
	va_end(l);

	return 1;
}

/*tier_bloom*/
tier_bloom *bloom_create1(dcs_u64_t size)
{
	tier_bloom *bloom;
	dcs_s32_t i;
	dcs_u64_t num = (size + BLOOM_BLOCK_SIZE - 1) / BLOOM_BLOCK_SIZE;
        
	bloom = (tier_bloom *)malloc(sizeof(tier_bloom));	
	if(NULL == bloom) {
		DCS_ERROR("bloom_create err: %d\n", errno);
		goto EXIT;
	}

	bloom->blocks = (tier_bloom_block **)malloc(sizeof(tier_bloom_block *) * num);
	if(NULL == bloom->blocks){
		DCS_ERROR("bloom_create1 malloc for bloom->blocks failed\n");
		goto EXIT;
	}
	
	for(i = 0; i < num; i++){
		bloom->blocks[i] = NULL;	
	}
	
	bloom->bmap = (dcs_u8_t *)malloc((num + CHAR_BIT - 1) / CHAR_BIT);
	if(NULL == bloom->bmap){
		DCS_ERROR("bloom_create1 malloc for bloom->bmap failed\n");
		goto EXIT;
	}
	memset(bloom->bmap, 0, (num + CHAR_BIT - 1) / CHAR_BIT);
	
	pthread_mutex_init(&bloom->lock, NULL);
        bloom->blocks_num = num;
        DCS_MSG("bloom_create1 bloom->blocks_num: %ld\n",num);
	return bloom;
EXIT:
	if(bloom){
		if(bloom->blocks){
			free(bloom->blocks);
			bloom->blocks = NULL;
		}
		if(bloom->bmap){
			free(bloom->bmap);
			bloom->bmap = NULL;
		}
		pthread_mutex_destroy(&bloom->lock);
		free(bloom);
		bloom = NULL;
	}	
	return NULL;
}

dcs_s32_t bloom_block_expand(tier_bloom * bloom, dcs_s32_t index){
	dcs_s32_t rc = 0;
	dcs_s32_t i = 0;
	dcs_s32_t tmp_real_level_num = bloom->blocks[index]->real_level_num;
	dcs_s32_t tmp_level_num = bloom->blocks[index]->level_num;

	if(NULL == bloom || index < 0){
                DCS_ERROR("bloom_block_expand parameter error, exit\n");
                rc = -1;
                goto EXIT;
        }

	if(tmp_real_level_num == tmp_level_num){
		dcs_u8_t ** tmp = (dcs_u8_t **)malloc(tmp_level_num + BLOOM_LEVEL_NUM);
		
		if(tmp){
			for(i = 0; i < tmp_level_num + BLOOM_LEVEL_NUM; i++){
				tmp[i] = NULL;
				if(i < tmp_real_level_num){
					tmp[i] = bloom->blocks[index]->a[i];
				}
			}		
			
			free(bloom->blocks[index]->a);
			bloom->blocks[index]->a = tmp;
			bloom->blocks[index]->level_num += BLOOM_LEVEL_NUM;
			bloom->blocks[index]->a[tmp_real_level_num] = (dcs_u8_t *)malloc(sizeof(char) * BLOOM_BLOCK_SIZE);
			if(NULL == bloom->blocks[index]->a[tmp_real_level_num]){
				DCS_ERROR("bloom_block_expand malloc for bloom->blocks[index]->a[%d] failed\n",tmp_real_level_num);
				rc = -1;
				goto EXIT;
			}
			bloom->blocks[index]->real_level_num ++;
		}else{
			DCS_ERROR("bloom_block_expand remalloc for bloom->blocks[%d]->a failed\n", index);
			rc = -1;
			goto EXIT;
		}
	}else{
		DCS_ERROR("bloom_block_expand bloom->blocks[%d] have enough space to store data, level_num is %d and real_level_num is %d\n", index, tmp_level_num, tmp_real_level_num);
		rc = 1;
		goto EXIT;
	}

EXIT:
	return rc;
}

dcs_s32_t bloom_block_init1(tier_bloom * bloom, dcs_s32_t index, dcs_s32_t n, ...){
	dcs_s32_t rc = 0;
	dcs_s32_t i = 0;
	va_list l;
	dcs_s32_t level_num = BLOOM_LEVEL_NUM;
	dcs_s32_t real_level_num = 1;

	if(n == 2){//n==2， extract parameter of level_num and real_level_num
		va_start(l, n);
		level_num = va_arg(l, dcs_s32_t); 
		real_level_num = va_arg(l, dcs_s32_t);
		va_end(l);
	}

	if(NULL == bloom || index < 0){
		DCS_ERROR("bloom_block_init parameter error, exit\n");
		rc = -1;
		goto EXIT;
	}
	
	if(bloom->blocks[index]){
		DCS_ERROR("bloom_block_init bloom->blocks[%d] is exist\n",index);
		rc = -1;
		goto EXIT;
	}

	bloom->blocks[index] = (tier_bloom_block *)malloc(sizeof(tier_bloom_block));
	if(NULL == bloom->blocks[index]){
		DCS_ERROR("bloom_block_init malloc for bloom->blocks[%d] failed\n", index);
		rc = -1;
		goto EXIT;
	}

	memset(bloom->blocks[index], 0, sizeof(tier_bloom_block));
	
	bloom->blocks[index]->a = (dcs_u8_t **)malloc(sizeof(dcs_u8_t*) * level_num);
	if(NULL == bloom->blocks[index]->a){
		DCS_ERROR("bloom_block_init malloc for bloom->blocks[%d]->a failed\n", index);
		rc = -1;
		goto EXIT;
	}
	
	bloom->blocks[index]->bmap = (dcs_u8_t *)malloc(sizeof(char) * (BLOOM_BLOCK_SIZE + CHAR_BIT -1) / CHAR_BIT);

	bloom->blocks[index]->level_num = level_num;
	
	for(i = 0; i < level_num; i++){
		bloom->blocks[index]->a[i] = NULL;
	}
	
	for(i = 0; i < real_level_num; i++){
		bloom->blocks[index]->a[i] = (dcs_u8_t*)malloc(sizeof(char) * BLOOM_BLOCK_SIZE);
		if(NULL == bloom->blocks[index]->a[i]){
			DCS_ERROR("bloom_block_init malloc for bloom->blocks[%d]->a[0] failed\n", index);
			rc = -1;
			goto EXIT;
		}
	}

	bloom->blocks[index]->real_level_num = real_level_num;
	
	pthread_mutex_init(&bloom->blocks[index]->lock, NULL);

	return rc;	
EXIT:
	for(i = 0; i < real_level_num; i++){
		if(bloom->blocks[index]->a[i]){
			free(bloom->blocks[index]->a[i]);
			bloom->blocks[index]->a[i] = NULL;
		}
	}
	if(bloom->blocks[index]->a){
		free(bloom->blocks[index]->a);
		bloom->blocks[index]->a = NULL;
	}
	if(bloom->blocks[index]->bmap){
		free(bloom->blocks[index]->bmap);
	}
	if(bloom->blocks[index]){
		free(bloom->blocks[index]);
		bloom->blocks[index] = NULL;
	}
	return rc;
}

dcs_s32_t bloom_block_init(tier_bloom * bloom, dcs_s32_t index){
	dcs_s32_t rc = 0;
	dcs_s32_t i = 0;	
	if(NULL == bloom || index < 0){
		DCS_ERROR("bloom_block_init parameter error, exit\n");
		rc = -1;
		goto EXIT;
	}
	
	if(bloom->blocks[index]){
		DCS_ERROR("bloom_block_init bloom->blocks[%d] is exist\n",index);
		rc = -1;
		goto EXIT;
	}

	bloom->blocks[index] = (tier_bloom_block *)malloc(sizeof(tier_bloom_block));
	if(NULL == bloom->blocks[index]){
		DCS_ERROR("bloom_block_init malloc for bloom->blocks[%d] failed\n", index);
		rc = -1;
		goto EXIT;
	}

	memset(bloom->blocks[index], 0, sizeof(tier_bloom_block));
	
	bloom->blocks[index]->a = (dcs_u8_t **)malloc(sizeof(dcs_u8_t*) * BLOOM_LEVEL_NUM);
	if(NULL == bloom->blocks[index]->a){
		DCS_ERROR("bloom_block_init malloc for bloom->blocks[%d]->a failed\n", index);
		rc = -1;
		goto EXIT;
	}
	
	bloom->blocks[index]->bmap = (dcs_u8_t *)malloc(sizeof(char) * (BLOOM_BLOCK_SIZE + CHAR_BIT -1) / CHAR_BIT);

	bloom->blocks[index]->level_num = BLOOM_LEVEL_NUM;
	
	for(i = 0; i < BLOOM_LEVEL_NUM; i++){
		bloom->blocks[index]->a[i] = NULL;
	}
	
	bloom->blocks[index]->a[0] = (dcs_u8_t*)malloc(sizeof(char) * BLOOM_BLOCK_SIZE);
	if(NULL == bloom->blocks[index]){
		DCS_ERROR("bloom_block_init malloc for bloom->blocks[%d]->a[0] failed\n", index);
		rc = -1;
		goto EXIT;
	}

	bloom->blocks[index]->real_level_num = 1;
	
	pthread_mutex_init(&bloom->blocks[index]->lock, NULL);

	return rc;	
EXIT:
	if(bloom->blocks[index]->a[0]){
		free(bloom->blocks[index]->a[0]);
		bloom->blocks[index]->a[0] = NULL;
	}
	if(bloom->blocks[index]->a){
		free(bloom->blocks[index]->a);
		bloom->blocks[index]->a = NULL;
	}
	if(bloom->blocks[index]->bmap){
		free(bloom->blocks[index]->bmap);
	}
	if(bloom->blocks[index]){
		free(bloom->blocks[index]);
		bloom->blocks[index] = NULL;
	}
	return rc;
}

dcs_s32_t bloom_destroy1(tier_bloom *bloom)
{
	dcs_s32_t rc = 0;
	dcs_s32_t i = 0;
	dcs_s32_t j = 0;
	if(bloom){
		if(bloom->blocks){
			/*free blocks*/			
			for(i = 0; i < bloom->blocks_num; i++){
				if(bloom->blocks[i]){
					for(j = 0; j < bloom->blocks[i]->real_level_num; j++){
						if(bloom->blocks[i]->a[j]){
							free(bloom->blocks[i]->a[j]);
						}
					}
					free(bloom->blocks[i]->a);
					free(bloom->blocks[i]->bmap);
					pthread_mutex_destroy(&bloom->blocks[i]->lock);
					free(bloom->blocks[i]);
				}
				
			}
			free(bloom->blocks);
		}
		if(bloom->bmap){
			free(bloom->bmap);
		}
		pthread_mutex_destroy(&bloom->lock);
		free(bloom);
		bloom = NULL;
	}

	return rc;
}
/*
inline dcs_s32_t bloom_bit_add(tier_bloom *bloom, dcs_u64_t index, dcs_s32_t num){
	dcs_s32_t rc = 0;
	dcs_s32_t i = 0;
	dcs_s32_t blocks_index = index / BLOOM_BLOCK_SIZE;
	dcs_s32_t block_index = index % BLOOM_BLOCK_SIZE;
	
	if()	
		

EXIT:
	return rc;
}

inline dcs_s32_t bloom_bit_check(tier_bloom * bloom, dcs_u64_t index){
	return GETBIT(bloom->bmap, index % bloom->asize);
}

inline dcs_s32_t bloom_bit_minus(tier_bloom * bloom, dcs_u64_t index, dcs_s32_t num){
	dcs_s32_t rc = 0;
	dcs_s32_t i = 0;
	dcs_s32_t blocks_index = index / BLOOM_BLOCK_SIZE;
	dcs_s32_t block_index = index % BLOOM_BLOCK_SIZE;

EXIT:
	return rc;
}*/

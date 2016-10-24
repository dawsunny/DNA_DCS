/*master operation head file*/
#ifndef __MASTER_OP_H_
#define __MASTER_OP_H_


/*analyse the msg to determine next operation*/
dcs_s32_t __dcs_master_process_req(amp_request_t *req);
/*master process query service*/
dcs_s32_t __dcs_master_query(amp_request_t *req);
/*master process update service*/
dcs_s32_t __dcs_master_update(amp_request_t *req);
/*master bloom filter update*/
dcs_s32_t bloom_update(dcs_s32_t cache_id, dcs_s32_t bf_id);
/*query bloom filter*/
dcs_s32_t bloom_query(dcs_u8_t *sha, dcs_s32_t pos, dcs_s32_t bf_id);
/*hash functions for bloom filter*/
dcs_u64_t hashfunc(dcs_u8_t *sha, dcs_s32_t i);

dcs_s32_t __dcs_master_bloom_reset(amp_request_t *req);

dcs_s32_t __dcs_tier_bloom_upload(amp_request_t *req);
#endif

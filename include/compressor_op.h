/*compressor operation header*/

#ifndef __COMPRESSOR_OP_H_
#define __COMPRESSOR_OP_H_

#include "dcs_const.h"
#include "dcs_type.h"
#include <map>
#include <string>
using namespace std;

//#define DATA_CONTAINER_PATH "/tmp/data/container"
//#define FP_CONTAINER_PATH "/tmp/fp/container"

/*analyze the msg find the request type*/
dcs_s32_t __dcs_compressor_process_req(amp_request_t *req, dcs_thread_t *threadp);
/*if it is a write request, 
 * then get the data and do the dcslication*/
dcs_s32_t __dcs_compressor_write(amp_request_t *req, dcs_thread_t *threadp);
dcs_s32_t __dcs_compressor_delete(amp_request_t *req);
dcs_s32_t __dcs_compressor_read(amp_request_t *req);
dcs_s32_t __dcs_compressor_upload(amp_request_t *req);
/*get sample FPs*/
sha_sample_t *get_sample_fp(dcs_u8_t *sha, dcs_u32_t chunk_num);
/*find champion container and return container id*/
dcs_u64_t find_champion_container(container_t *con_id);

//by bxz
dcs_s32_t get_location_fa(dcs_s8_t *, dcs_s8_t *, dcs_u32_t);
dcs_s32_t get_location_fq(dcs_s8_t *, dcs_s8_t *, dcs_u32_t, dcs_u64_t, dcs_u32_t);
dcs_s32_t do_write_map(map<string, string> &, dcs_u32_t);
dcs_s32_t do_read_map(map<string, string> &, dcs_u32_t);
extern pthread_mutex_t compressor_location_fa_lock;
extern pthread_mutex_t compressor_location_fq_lock;
extern map<string, compressor_hash_t> compressor_location_fa;
extern map<string, compressor_hash_t> compressor_location_fq;
extern dcs_u32_t compressor_location_fa_cnt;
extern dcs_u32_t compressor_location_fq_cnt;
extern dcs_u32_t compressor_location_fq_cnt_local;

#endif

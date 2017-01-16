/*server operation head file*/

#ifndef __SERVER_OP_H_
#define __SERVER_OP_H_
#include "dcs_type.h"
/*server process req will analyze the msg to determine operation type */
dcs_s32_t __dcs_server_process_req(amp_request_t *req, 
                                       dcs_thread_t *threadp);
/*dcs write server*/
dcs_s32_t __dcs_write_server(amp_request_t *req, dcs_thread_t *threadp);
/*send the target compressor which dcs the superchunk and the cache id*/
dcs_u32_t __dcs_server_updata_master(dcs_u32_t target, 
                                         dcs_u32_t *master_cache_id);
/*decide which target*/
dcs_s32_t __dcs_server_data_routing(dcs_u32_t *mark);
/*merge the chunk common info and position info into mapinfo*/
dcs_datamap_t *__dcs_server_chunkinfo_merge(dcs_datapos_t *data_pos, 
                                                sha_array_t *sha_array,
                                                dcs_u32_t chunk_num);
/*get the power of master*/
dcs_s32_t get_master_power();

/*process read server*/
dcs_s32_t __dcs_read_server(amp_request_t *req);
dcs_s32_t __dcs_readquery_server(amp_request_t *req);  //bxz

/*process read finish request*/
dcs_s32_t __dcs_read_finish(dcs_u64_t inode, 
                                dcs_u64_t timestamp,
                                dcs_u32_t client_id,
                                amp_request_t *req);
/*send finish reply to client*/
dcs_u32_t __dcs_server_write_finish(dcs_s8_t *,
                                    dcs_u32_t,
                                        amp_request_t *req);
/*collect disk usage info from compressor*/
dcs_s32_t server_collect_diskinfo(); 
/*init the disk info*/
dcs_s32_t __dcs_server_init_diskinfo();
/*stateless datarouting use the*/
dcs_s32_t __dcs_server_stateless_routing(dcs_u8_t *sha);

dcs_s32_t __dcs_delete_server(amp_request_t *);


#endif 

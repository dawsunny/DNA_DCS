/*client_op header*/
#ifndef __CLIENT_OP_H_
#define __CLIENT_OP_H_

#include "amp.h"
#define shell_ins  "mkdir -p "


/*get dir name from the config*/
//dcs_s32_t __dcs_clt_get_dirname();

/*read or write operation type*/
dcs_s32_t __dcs_clt_op();
/*get file name from the dir*/
dcs_s32_t __dcs_clt_get_filename(dcs_s8_t *path);

dcs_s32_t __dcs_clt_get_read_filename(dcs_s8_t *path);

/*write file to server*/
dcs_s32_t __dcs_clt_write_file(dcs_s8_t *filename, 
                                   dcs_thread_t *threadp);
/*read file from server*/
dcs_s32_t __dcs_clt_read_file(dcs_s8_t *filename, dcs_thread_t *threadp);
/*send data to server*/
dcs_s32_t clt_send_data(dcs_u64_t, dcs_s8_t *, dcs_c2s_req_t c2s_datainfo,
                          dcs_s8_t *buf,
                          dcs_thread_t *threadp);
/*create a dir to store meta data*/
dcs_s32_t clt_create_dir(dcs_s8_t *path);
/*get meta file path*/
dcs_s8_t *get_meta_path(dcs_s8_t *pathname);
/*send finish msg*/
dcs_s32_t __dcs_clt_finish_msg(dcs_u64_t, dcs_s8_t *, dcs_c2s_req_t c2s_datainfo,
                                   dcs_thread_t *threadp);
/*send read finish msg*/
dcs_s32_t __client_send_finish_msg(dcs_u64_t inode, dcs_u64_t timestamp, dcs_thread_t *threadp);
/*add to dir queue*/
dcs_s32_t add_to_dirqueue(dcs_s8_t *dirpath);
dcs_s32_t __dcs_clt_list(dcs_s8_t *path);
dcs_s32_t __dcs_clt_rmdir(dcs_s8_t *path);
dcs_s32_t __dcs_clt_delete(dcs_s8_t *path);
dcs_s32_t check_dir(dcs_s8_t * path);
#endif

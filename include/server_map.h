/*server map header file*/

#ifndef __SERVER_MAP_H_
#define __SERVER_MAP_H_

#include "server_op.h"
#include "dcs_const.h"

struct map_table_type{
    dcs_datamap_t *datamap;
    struct map_table_type *next;
    dcs_s8_t  *inode_cid;  /*inode and client id*/
    dcs_u32_t chunk_num;
};
typedef struct map_table_type map_table_t;

struct map_name_type{
    dcs_s8_t *map_name;
};
typedef struct map_name_type map_name_t;

#define INODE_LEN     20
#define CLIENTID_LEN  16
#define MAP_TABLE_NUM 100
#define MAX_BUF_NUM 1000
//#define MAP_FILE_PATH1 "/mnt/disk2/liuhougui/mapfile1/"
//#define MAP_FILE_PATH2 "/mnt/disk2/liuhougui/mapfile2/"
//#define MAP_FILE_PATH1 "/tmp/mapfile1/"
//#define MAP_FILE_PATH2 "/tmp/mapfile2/"

/*init a array for store the chunk file mapping info*/
dcs_s32_t __dcs_server_maptable_init();
/*init a array to cache the map info
 * to improve read request performance*/
dcs_s32_t __dcs_server_mapbuf_init();
/*insert the chunk file mapping info to table*/
dcs_u32_t __dcs_server_insert_mapinfo(dcs_datamap_t *datamap,
                                          dcs_u64_t filenode,
                                          dcs_u64_t timestamp,
                                          dcs_u32_t clientid,
                                          dcs_u32_t new_chunk_num);
/*write the chunk-file mapping info to disk*/
dcs_u32_t __dcs_server_mapinfo_wb(dcs_u64_t inode,
                                      dcs_u64_t timestamp,
                                      dcs_u32_t clientid,
                                      dcs_u64_t filesize);
/*turn the inode number to char*/
dcs_s8_t *get_map_name(dcs_u64_t inode, dcs_u32_t client_id, dcs_u64_t timestamp);
/*get map file path*/
dcs_s8_t *get_map_path(dcs_s8_t *filename);
/*read mapinfo into buf*/
dcs_mapbuf_t *read_map_buf(dcs_s8_t *map_name, 
                              dcs_u64_t fileoffset,
                              dcs_u32_t reqsize);
/*get needed map info from whole file mapping info*/
dcs_mapbuf_t *get_map_buf(dcs_s8_t *map_name,
                             dcs_u64_t fileoffset,
                             dcs_u32_t reqsize,
                             dcs_u32_t pos);
/*check the if the mapinfo had been cached*/
dcs_s32_t check_map_buf(dcs_s8_t *map_name);
/*free the map buf according the map_name */
dcs_s32_t free_map_buf(dcs_s8_t *map_name);

dcs_s32_t free_map_table(dcs_s8_t *map_name);

dcs_s32_t sha_bf_upload_to_master();
dcs_s32_t dir_read_sha_bf_upload(dcs_s8_t * filepath);

#endif

/*dcs types*/

#ifndef __DCS_TYPE_H_
#define __DCS_TYPE_H_

#include <pthread.h>
#include <semaphore.h>
#include <map>
#include <string>
using namespace std;

typedef unsigned char      dcs_u8_t;
typedef char               dcs_s8_t;
typedef unsigned short     dcs_u16_t;
typedef short              dcs_s16_t;
typedef unsigned int       dcs_u32_t;
typedef int                dcs_s32_t;
typedef unsigned long      dcs_u64_t;
typedef long               dcs_s64_t;

//by bxz
struct _server_hash
{
    dcs_u32_t filetype;
    dcs_u64_t filesize;
    dcs_u32_t compressor_id;
    dcs_u64_t inode;
    dcs_u64_t timestamp;
    dcs_s8_t md5[33];
};
typedef struct _server_hash server_hash_t;

struct _compressor_hash
{
    dcs_u32_t chunk_num;
    string location;    
    map<dcs_u64_t, string> off_loc;
};
typedef struct _compressor_hash compressor_hash_t;

struct __dcs_thread
{
    pthread_t thread;
    sem_t startsem;
    sem_t stopsem;
    dcs_u32_t is_up;
    dcs_u32_t to_shutdown;
    dcs_u32_t seqno;
};
typedef struct __dcs_thread dcs_thread_t;

/*chunk info */
struct sha_word{
    dcs_u64_t offset;
    dcs_u32_t chunksize;
    dcs_u8_t  sha[20];
};
typedef struct sha_word sha_array_t;

/*chunk info array*/
struct chunk_info{
    sha_array_t *sha_array;
    dcs_u32_t chunk_num;
};
typedef struct chunk_info chunk_info_t;

struct sha_sample{
    dcs_u8_t *sha;
    dcs_u32_t sha_num;
};
typedef struct sha_sample sha_sample_t;

struct container_id{
    dcs_u64_t *c_id;
    dcs_u32_t con_num;
};
typedef struct container_id container_t;

struct compressor_container_chunknum{
    dcs_u64_t container_id;
    dcs_u32_t chunk_num ;
};
typedef struct compressor_container_chunknum container_chunk_t;

/*struct for data position*/
struct __dcs_data_pos{
    dcs_u32_t compressor_id;
    dcs_u64_t container_id;
    dcs_u32_t container_offset;
};
typedef struct __dcs_data_pos dcs_datapos_t;

/*chunk file map info*/
struct __dcs_data_mapinfo{
    dcs_u8_t  sha[20];//store sha
    dcs_u32_t compressor_id;
    dcs_u64_t container_id;
    dcs_u32_t container_offset;
    dcs_u32_t chunksize;
    dcs_u64_t offset;
};
typedef struct __dcs_data_mapinfo dcs_datamap_t;

struct __sha_bf{
    dcs_u8_t sha[20];
    dcs_u32_t bf_id;
};
typedef struct __sha_bf sha_bf_t;

struct conid_sha_block{
    dcs_u64_t con_id;
    dcs_u8_t sha[20];
};
typedef struct conid_sha_block conid_sha_block_t;

struct __dcs_read_info{
    dcs_u32_t begin;
    dcs_u32_t end;
    dcs_u32_t compressor_id;
};
typedef struct __dcs_read_info dcs_readinfo_t;

struct __dcs_map_buf{
    dcs_datamap_t *datamap;
    dcs_u32_t     chunk_num;
};
typedef struct __dcs_map_buf dcs_mapbuf_t;

/*
 * struct request_data{
 *     dcs_s8_t *buf;
 *     dcs_u32_t fromid;
 *     dcs_u32_t seqno;
 *     dcs_u64_t fileinode;
 *     dcs_u64_t fileoffset;
 *     dcs_u32_t bufsize;
 *     dcs_s32_t finish;
 *
 *}
 **/

#endif

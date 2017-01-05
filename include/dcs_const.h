/*const paramatter*/
    
#ifndef __DCS_CONST_H_
#define __DCS_CONST_H_

#include "amp.h"
#include "dcs_msg.h"
#include "dcs_type.h"
#include <limits.h>
#include <time.h>
#define DCS_READ   0
#define DCS_WRITE  1
#define DCS_QUERY  2
#define DCS_DCS  3
#define DCS_GET    4
#define DCS_UPDATE 5
#define DCS_LIST   6
#define DCS_DELETE 7
#define DCS_RMDIR  8
#define DCS_UPLOAD 9 

//add by bxz
#define DCS_READ_QUERY 10
#define DCS_FILETYPE_FASTA 100
#define DCS_FILETYPE_FASTQ 200

enum dcs_comp_type{
    DCS_CLIENT = 1,
    DCS_SERVER = 2,
    DCS_MASTER = 3,
    DCS_NODE  =  4,
};

/*get or set a bit*/
#define SETBIT(a, n) (a[n/CHAR_BIT] |= (1<<(n%CHAR_BIT)))
#define GETBIT(a, n) (a[n/CHAR_BIT] & (1<<(n%CHAR_BIT)))
#define RESETBIT(a, n) (a[n/CHAR_BIT] &= (~(1<<(n%CHAR_BIT))))
/*when create a file, use this file name size*/
#define FILENAME_LEN 256
#define PATH_LEN     256

/*thread num*/
#define DCS_CLIENT_THREAD_NUM  1 
#define DCS_SERVER_THREAD_NUM  4 
#define DCS_MASTER_THREAD_NUM  4 
#define DCS_COMPRESSOR_THREAD_NUM 4
#define ADDR_LEN 80
/*port id*/
#define SERVER_PORT 5446
#define MASTER_PORT 5440
#define COMPRESSOR_PORT 5445

/*superchunk and chunk size*/
#define DCS_SEND_SIZE (1*1024*1024)
#define SUPER_CHUNK_SIZE (1*1024*1024)
#define CONTAINER_SIZE   (8*1024*1024)

//by bxz
#define FA_CHUNK_SIZE (400*1024*1024)
#define FQ_CHUNK_SIZE (40*1024*1024)

#define FQ_LINE_SIZE 512
#define FQ_CNT_REC (1 << 14)
#define COMPRESSOR_OUTPUT_FILE_LEN 6

#define MD5_READ_DATA_SIZE	4096
#define MD5_SIZE		16
#define MD5_STR_LEN		(MD5_SIZE * 2)

/*bloom filter size*/
/*multiple of 4k*/
#define MASTER_BLOOM_SIZE  (1024*1024*128)
#define COMPRESSOR_BLOOM_SIZE (1024*1024*1024)

/*the number of server master and compressor*/
#define DCS_SERVER_NUM 1 
#define DCS_COMPRESSOR_NUM 1
#define DCS_MASTER_NUM 1

/*cache table configuration*/
#define TOTAL_CACHE_NUM (4*1024*1024)
#define EACH_BLOCK_NUM  1024

/*data routing*/
#define ROUTING_ON 1
//collect info per 30s
#define SLEEP_TIME 300
//con_table's data and fp write sync to thd disk if no write in 30, not include data delete
#define DATA_UPDATE_INTERVAL (5)

#define MSGHEAD_SIZE ((dcs_u32_t)(((dcs_msg_t *)0)->u.m2s_reply))
#define DCS_MSGHEAD_SIZE (AMP_MESSAGE_HEADER_LEN + MSGHEAD_SIZE)

#define SHA_LEN   20
#define FIX_CHUNK 0
#define VAR_CHUNK 1
#define DCS_CHUNK_SIZE 4096 

/*compressor configure*/
#define SAMPLE_RATE 1 
#define INDEX_BLOCK_SIZE 512

#define HASH_FUNC_NUM 6
#define BLOOM_LEVEL_NUM 4//u32:4 u64:8  1 byte present 256 
#define BLOOM_BLOCK_SIZE 512//multiple of CHAR_BIT
#define FP_FILE_SIZE (256*1024*1024/16/1024)//256M/fp_file

#define MAX_CONTAINER_DIR_NUM 2

#define CONF_DIR 		"/DNA_DCS"
#define MASTER_BLOOM_PATH 	CONF_DIR"/master/bf_"
#define COMPRESSOR_BLOOM_PATH	CONF_DIR"/compressor/bf"
#define MAP_FILE_PATH1 		CONF_DIR"/server/mapfile1"
#define MAP_FILE_PATH2 		CONF_DIR"/server/mapfile2"
#define DATA_CONTAINER_PATH 	CONF_DIR"/compressor/data"
#define FASTA_CONTAINER_PATH 	CONF_DIR"/compressor/data/fa"
#define FASTQ_CONTAINER_PATH 	CONF_DIR"/compressor/data/fq"
#define FP_CONTAINER_PATH 	CONF_DIR"/compressor/fp"
#define FASTA_MAP_PATH 	CONF_DIR"/compressor/map/fa.map"
#define FASTQ_MAP_PATH 	CONF_DIR"/compressor/map/fq.map"
#define CLIENT_MD_PATH 		CONF_DIR"/client/md"
/*by zhj fp file size*/
#define INDEX_PATH 		CONF_DIR"/compressor/hash_index/index"
#define BMAP_PATH 		CONF_DIR"/compressor/hash_index/bmap"
/*configure file*/
#define DCS_CONF              CONF_DIR"/conf"
#define CLIENT_ADDR_CONF	DCS_CONF"/client_addr"
#define SERVER_ADDR_CONF	DCS_CONF"/server_addr"
#define MASTER_ADDR_CONF	DCS_CONF"/master_addr"
#define COMPRESSOR_ADDR_CONF	DCS_CONF"/compressor_addr"

//bxz
#define FASTA_REF_PATH  CONF_DIR"/compressor/fa_ref/ref.fasta"

#ifndef DCS_LOG_FILE_PATH
#define DCS_LOG_FILE_PATH 	CONF_DIR"/log/"
#endif

#endif

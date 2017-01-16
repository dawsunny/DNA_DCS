/*
 * message header for dcslication storage system
 */

#ifndef __DCS_MSG_H_
#define __DCS_MSG_H_
 
#include "dcs_type.h"
#include "dcs_const.h"

/*data info sended form client to server*/

#define is_req 1
#define is_rep 2

struct __dcs_c2s_req
{
    dcs_u32_t size;
    dcs_u32_t finish;
    dcs_u64_t offset;
    dcs_u64_t inode;
    dcs_u64_t timestamp;
    dcs_u32_t target_server;    //bxz
    dcs_s8_t filename[256];
};
typedef struct __dcs_c2s_req dcs_c2s_req_t;

struct __dcs_s2c_reply
{
    dcs_u32_t size;
    dcs_u32_t offset;
};
typedef struct __dcs_s2c_reply dcs_s2c_reply_t;

struct __dcs_c2s_del_req
{
	dcs_u32_t size;
	dcs_u64_t inode;
        dcs_u64_t timestamp;
};
typedef struct __dcs_c2s_del_req dcs_c2s_del_req_t;

/*server query sha num and update bloom filter id*/
struct __dcs_s2m_req
{
    dcs_u32_t sha_num;
    dcs_s32_t bf_id;
    dcs_s32_t cache_id;
};
typedef struct __dcs_s2m_req dcs_s2m_req_t;

struct __dcs_s2m_upload_req
{
    dcs_u32_t sha_num;
    dcs_u32_t scsize;
};
typedef struct __dcs_s2m_upload_req dcs_s2m_upload_req_t;

/*master reply info*/
struct __dcs_m2s_reply
{
    dcs_u32_t mark_num;    
    dcs_u32_t cache_id;   /*use cache_id sha word to update*/
};
typedef struct __dcs_m2s_reply dcs_m2s_reply_t;

/*server to compressor req*/
struct __dcs_s2d_req
{
    dcs_u32_t chunk_num;
    dcs_u32_t scsize;    /*superchunk size*/
    dcs_u64_t offset;
    dcs_u32_t filetype;
    dcs_u32_t finish;
};
typedef struct __dcs_s2d_req dcs_s2d_req_t;

struct __dcs_s2d_upload_req
{
    dcs_u32_t sha_num;
    dcs_u32_t scsize;    /*superchunk size*/
};
typedef struct __dcs_s2d_upload_req dcs_s2d_upload_req_t;

/*compressor reply data position to*/
struct __dcs_d2s_reply
{
    dcs_u32_t chunk_num;
    dcs_u32_t bufsize;
    dcs_u64_t diskinfo;
};
typedef struct __dcs_d2s_reply dcs_d2s_reply_t;

/*data position info from dcs node to server*/
struct __dcs_d2s_msg
{
    dcs_u8_t sha_string[20];
    dcs_u64_t containerid;
};
typedef struct dcs_d2s_msg dcs_d2s_datainfo_t;

struct __dcs_s2m_del_req
{
    dcs_u32_t sha_num;
    dcs_u32_t scsize;    /*superchunk size*/
};
typedef struct __dcs_s2m_del_req dcs_s2m_del_req_t;

struct __dcs_s2d_del_req
{
    dcs_u32_t sha_num;
    dcs_u32_t scsize;    /*superchunk size*/
};
typedef struct __dcs_s2d_del_req dcs_s2d_del_req_t;

struct __dcs_m2d_del_req
{
    dcs_u32_t sha_num;
    dcs_u32_t scsize;    /*superchunk size*/
};
typedef struct __dcs_m2d_del_req dcs_m2d_del_req_t;


/*
struct __dcs_d2s_msg
{
    dcs_u32_t sha_num;

}
typedef struct dcs
*/

/*msg type*/
struct __dcs_msg
{
    dcs_u32_t size;       /*msg size*/
    dcs_u32_t seqno;      /*thread id*/
    dcs_u32_t msg_type;   /*msg type*/
    dcs_u32_t fromid;     /*from id*/
    dcs_u32_t fromtype;   /*from type*/
    dcs_u32_t optype;     /*operation type*/
    dcs_u32_t ack;        /*common ack usual 1 means sucess*/
    dcs_u32_t filetype;  //FASTA(0)/FASTQ(1)   ***by bxz
    dcs_u64_t filesize; //by bxz
    dcs_u64_t inode;
    dcs_u64_t timestamp;
    dcs_s8_t md5[33];  //by bxz
    union{
        /*client related*/
        dcs_c2s_req_t c2s_req;               
        /*server related*/
        dcs_s2m_req_t s2m_req;
        dcs_s2m_upload_req_t s2m_upload_req;
        dcs_s2d_req_t s2d_req;
        dcs_s2d_upload_req_t s2d_upload_req;
        dcs_c2s_del_req_t c2s_del_req;
        dcs_s2d_del_req_t s2d_del_req;
        dcs_s2m_del_req_t s2m_del_req;
        dcs_m2d_del_req_t m2d_del_req;
        dcs_s2c_reply_t s2c_reply;
        //dcs_chunkinfo_t shaword[DCS_CHUNK_NUM];
        /*master related*/
        dcs_m2s_reply_t m2s_reply;
        /*compressor related*/
        dcs_d2s_reply_t d2s_reply;
    }u;
    /*if reply has to be included some data, use this point*/
    dcs_s8_t buf[0];
};
typedef struct __dcs_msg dcs_msg_t;


#endif

/*server operation*/

#include "amp.h"
#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"

#include "server_cnd.h"
#include "server_thread.h"
#include "chunker.h"
#include "server_map.h"
#include "server_op.h"

//#include <stropts.h>
#include <stdint.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <string.h>
#include <semaphore.h>
#include <openssl/sha.h>

#include <map>
#include <string>
using namespace std;

dcs_u32_t sign_num = 0;
dcs_u64_t diskinfo[DCS_COMPRESSOR_NUM];
dcs_u64_t disk_avg;
pthread_mutex_t diskinfo_lock = PTHREAD_MUTEX_INITIALIZER;
map<string, server_hash_t> server_table;
pthread_mutex_t server_table_lock = PTHREAD_MUTEX_INITIALIZER;

dcs_s32_t __dcs_server_write_mapinfo() {
    dcs_s32_t rc = 0;
    FILE *filep = NULL;
    dcs_u32_t tmp = 0;
    dcs_u64_t tmp64 = 0;
    map<string, server_hash_t>::iterator it;
    DCS_ENTER("__dcs_server_write_mapinfo enter\n");
    if ((filep = fopen(SERVER_MAP_PATH, "wb")) == NULL) {
        DCS_ERROR("__dcs_server_write_mapinfo open SERVER_MAP_PATH error[%d]\n", errno);
        rc = errno;
        goto EXIT;
    }
    tmp = server_table.size();
    if ((fwrite(&tmp, sizeof(dcs_u32_t), 1, filep)) != 1) {
        DCS_ERROR("__dcs_server_write_mapinfo write server table's size error\n");
        rc = -1;
        goto EXIT;
    }
    
    for (it = server_table.begin(); it != server_table.end(); ++it) {
        tmp = it->first.size();
        if ((fwrite(&tmp, sizeof(dcs_u32_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_write_mapinfo write server_table.first.size error\n");
            rc = -1;
            goto EXIT;
        }
        
        if ((fwrite(it->first.c_str(), 1, tmp, filep)) != tmp) {
            DCS_ERROR("__dcs_server_write_mapinfo write server_table.first error\n");
            rc = -1;
            goto EXIT;
        }
        
        tmp = it->second.filetype;
        if ((fwrite(&tmp, sizeof(dcs_u32_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_write_mapinfo write server_table.second.filetype error\n");
            rc = -1;
            goto EXIT;
        }
        tmp = it->second.compressor_id;
        if ((fwrite(&tmp, sizeof(dcs_u32_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_write_mapinfo write server_table.second.compressor_id error\n");
            rc = -1;
            goto EXIT;
        }
        tmp64 = it->second.filesize;
        if ((fwrite(&tmp64, sizeof(dcs_u64_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_write_mapinfo write server_table.second.filesize error\n");
            rc = -1;
            goto EXIT;
        }
        tmp64 = it->second.inode;
        if ((fwrite(&tmp64, sizeof(dcs_u64_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_write_mapinfo write server_table.second.inode error\n");
            rc = -1;
            goto EXIT;
        }
        tmp64 = it->second.timestamp;
        if ((fwrite(&tmp64, sizeof(dcs_u64_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_write_mapinfo write server_table.second.timestamp error\n");
            rc = -1;
            goto EXIT;
        }
        if ((fwrite(it->second.md5, 1, 33, filep)) != 33) {
            DCS_ERROR("__dcs_server_write_mapinfo write server_table.second.timestamp error\n");
            rc = -1;
            goto EXIT;
        }
    }
    
EXIT:
    if (filep) {
        fclose(filep);
    }
    DCS_LEAVE("__dcs_server_write_mapinfo leave\n");
    return rc;
}
dcs_s32_t __dcs_server_read_mapinfo() {
    dcs_s32_t rc = 0;
    FILE *filep = NULL;
    dcs_u32_t tmp = 0, i = 0, size = 0;
    dcs_u64_t tmp64 = 0;
    dcs_s8_t filename[PATH_LEN];//, tmp_md5[33];
    string str = "";
    server_table.clear();
    
    DCS_ENTER("__dcs_server_read_mapinfo enter\n");

    
    if ((filep = fopen(SERVER_MAP_PATH, "rb")) == NULL) {
        DCS_ERROR("__dcs_server_read_mapinfo open SERVER_MAP_PATH error\n");
        rc = -1;
        goto EXIT;
    }
    
    if ((fread(&size, sizeof(dcs_u32_t), 1, filep)) != 1) {
        DCS_ERROR("__dcs_server_read_mapinfo read server table's size error\n");
        rc = -1;
        goto EXIT;
    }
    
    for (i = 0; i < size; ++i) {
        if ((fread(&tmp, sizeof(dcs_u32_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_read_mapinfo read server_table.first's size error\n");
            rc = -1;
            goto EXIT;
        }
        memset(filename, 0, PATH_LEN);
        if ((fread(filename, 1, tmp, filep)) != tmp) {
            DCS_ERROR("__dcs_server_read_mapinfo read server_table.first error\n");
            rc = -1;
            goto EXIT;
        }
        str = filename;
        
        if ((fread(&tmp, sizeof(dcs_u32_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_read_mapinfo read filetype error\n");
            rc = -1;
            goto EXIT;
        }
        server_table[str].filetype = tmp;
        
        if ((fread(&tmp, sizeof(dcs_u32_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_read_mapinfo read compressor_id error\n");
            rc = -1;
            goto EXIT;
        }
        server_table[str].compressor_id = tmp;
        
        if ((fread(&tmp64, sizeof(dcs_u64_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_read_mapinfo read filesize error\n");
            rc = -1;
            goto EXIT;
        }
        server_table[str].filesize = tmp64;
        
        if ((fread(&tmp64, sizeof(dcs_u64_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_read_mapinfo read inode error\n");
            rc = -1;
            goto EXIT;
        }
        server_table[str].inode = tmp64;
        
        if ((fread(&tmp64, sizeof(dcs_u64_t), 1, filep)) != 1) {
            DCS_ERROR("__dcs_server_read_mapinfo read timestamp error\n");
            rc = -1;
            goto EXIT;
        }
        server_table[str].timestamp = tmp64;
        
        //memset(tmp_md5, 0, 33);
        if ((fread(server_table[str].md5, 1, 33, filep)) != 33) {
            DCS_ERROR("__dcs_server_read_mapinfo read md5 error\n");
            rc = -1;
            goto EXIT;
        }
        //memcpy(server_table[str].md5, tmp_md5, 33);
    }
    
EXIT:
    if (filep) {
        fclose(filep);
    }
    DCS_LEAVE("__dcs_server_read_mapinfo leave\n");
    return rc;
}

/*init the disk info*/
dcs_s32_t __dcs_server_init_diskinfo()
{
    dcs_s32_t i;
    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        diskinfo[i] = 0;
    }

    disk_avg = 0;
    return 0;
}

/*server process req will analyze the msg to determine 
 * operation type 
 * if the operation type is read call __dcs_server_read
 * if the operation type is write call __dcs_server_write
 */
dcs_s32_t __dcs_server_process_req(amp_request_t *req, dcs_thread_t *threadp)
{
    dcs_s32_t rc = 0;
    dcs_msg_t *msgp = NULL;
    dcs_u32_t op_type;

    DCS_ENTER("__dcs_server_process_req enter \n");

    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    op_type = msgp->optype;
    if(op_type == DCS_WRITE)
    {
        rc = __dcs_write_server(req, threadp);
    } else if (op_type == DCS_READ_QUERY) {
        rc = __dcs_readquery_server(req);
    }
    else if(op_type == DCS_READ){
        rc = __dcs_read_server(req);
    }
    else if (op_type == DCS_DELETE){
        rc = __dcs_delete_server(req);
    }
    else{
        DCS_ERROR("__dcs_server_process_req recieve wrong optype from client \n");
        rc = -1;
        goto EXIT;
    }

EXIT:
    DCS_LEAVE("__dcs_server_process_req leave \n");
    return rc;
}

//by bxz
dcs_s32_t __dcs_readquery_server(amp_request_t *req) {
    dcs_s32_t rc = 0;
    dcs_msg_t *msgp = NULL;
    amp_message_t *repmsgp = NULL;
    dcs_u64_t reqsize;
    dcs_s8_t filename1[PATH_LEN];
    string filename;
    dcs_u32_t clientid;
    dcs_u32_t size = 0;
    dcs_s32_t query_result = -1;
    dcs_u32_t filetype = 0;
    dcs_u64_t filesize = 0;
    dcs_u64_t inode = 0;
    dcs_u64_t timestamp;
    DCS_ENTER("__dcs_readquery_server enter\n");
    
    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    reqsize = msgp->u.c2s_req.size;
    clientid = msgp->fromid;
    //filename = (dcs_s8_t *)req->req_iov->ak_addr;
    memset(filename1, 0, PATH_LEN);
    memcpy(filename1, req->req_iov->ak_addr, req->req_iov->ak_len);
    filename = filename1;
    //filetype = msgp->filetype;
    printf("get from client:\nreqsize: %d, clientid: %d, filename: %s\n", reqsize, clientid, filename1);
    
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    repmsgp = (amp_message_t *)malloc(size);
    if (repmsgp == NULL) {
        DCS_ERROR("__dcs_readquery_server malloc for repmsgp error\n");
        rc = -1;
        goto EXIT;
    }
    memset(repmsgp, 0, size);
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);
    if (req->req_msg) {
        free(req->req_msg);
        req->req_msg = NULL;
    }
    
    pthread_mutex_lock(&server_table_lock);
    if (server_table.find(filename) != server_table.end()) {
        query_result = 1;
        filetype = server_table[filename].filetype;
        filesize = server_table[filename].filesize;
        inode = server_table[filename].inode;
        timestamp = server_table[filename].timestamp;
    } else {
        query_result = 0;
    }
    pthread_mutex_unlock(&server_table_lock);
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->msg_type = is_rep;
    msgp->size = size;
    msgp->fromtype = DCS_SERVER;
    msgp->fromid = server_this_id;
    if (query_result == 1) {
        msgp->filetype = filetype;
        msgp->filesize = filesize;
        msgp->inode = inode;
        msgp->timestamp = timestamp;
        msgp->ack = 1;
        //req->req_iov = NULL;
        req->req_type = AMP_REPLY | AMP_DATA;
        printf("||||got!\n");
    } else {
        msgp->filetype = 0;
        msgp->filesize = 0;
        msgp->inode = 0;
        msgp->timestamp = 0;
        msgp->ack = 0;
        //req->req_iov = NULL;
        req->req_type = AMP_REPLY | AMP_MSG;
        printf("||||not got!\n");
    }
    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 0;
    
    rc = amp_send_sync(server_comp_context, req, DCS_CLIENT, clientid, 0);
    printf("send to client\n");
    if (rc != 0) {
        DCS_ERROR("__dcs_readquery_server send data to client error\n");
        goto EXIT;
    }
    
EXIT:
    if (repmsgp) {
        free(repmsgp);
        repmsgp = NULL;
    }
    if (req->req_iov) {
        __server_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
        req->req_iov = NULL;
    }
    if (req) {
        __amp_free_request(req);
    }
    DCS_LEAVE("__dcs_readquery_server leave\n");
    return rc;
}

/*dcs write server
 * 1. judge if it is a finished req if yes write the map info back to disk
 * 2. get data from req
 * 3. chunk the data and comput sha1
 * 4. classify the FP by the header bits according algorithm
 * 5. prepared the req and msg send the FP to masters
 * 6. get the mark and decide which compressor to go
 * 7. send data to the compressor
 * 8. update the master according the compressor id
 */
dcs_s32_t __dcs_write_server(amp_request_t *req, dcs_thread_t *threadp)
{
        /*
        for(j=0; j<power; j++){
            if(GETBIT(tmpsha,j))
                SETBIT(sign, j);
        }
        */
    dcs_s32_t rc = 0;
    dcs_u32_t i,j = 0;
    dcs_u32_t size = 0;
    dcs_s32_t power = 0;
    dcs_u32_t seqno = 0;
    dcs_u32_t fromid = 0;
    dcs_u32_t filetype = 0;     //by bxz
    dcs_u64_t fileinode = 0;
    dcs_u64_t timestamp = 0;
    dcs_u64_t fileoffset = 0;
    dcs_u32_t bufsize = 0;
    dcs_u32_t total_sha_num = 0;
    dcs_u32_t sign = 0;
    dcs_u32_t finish = 0;
    dcs_u8_t  *tmpsha = NULL;
    //dcs_s8_t  *tmp = NULL;
    dcs_s8_t  *buf = NULL;

    //dcs_u32_t highest = 0;
    dcs_u32_t target = 0;
    //dcs_u32_t tmp_mark[DCS_COMPRESSOR_NUM];
    dcs_u32_t *tmp_mark = NULL;
    dcs_u32_t mark[DCS_COMPRESSOR_NUM];
    //dcs_u32_t master_cache_id[DCS_MASTER_NUM];
    //dcs_u32_t sha_num[DCS_MASTER_NUM];
    //dcs_u8_t  *tmp_sha_v[DCS_MASTER_NUM];
    //dcs_u8_t  *sha_v[DCS_MASTER_NUM];
    amp_message_t *repmsgp  = NULL;
    //amp_request_t *req2m[DCS_MASTER_NUM];
    //amp_message_t *reqmsgp2m[DCS_MASTER_NUM];
    //amp_message_t *repmsgp2m[DCS_MASTER_NUM];

    amp_request_t *req2d = NULL;
    amp_message_t *reqmsgp2d = NULL;
    amp_message_t *repmsgp2d = NULL;
    dcs_datapos_t *data_pos = NULL;

    dcs_msg_t *msgp = NULL;
    //chunk_info_t *chunk_detail = NULL;
    sha_array_t *sha_array = NULL;
    dcs_datamap_t *datamap = NULL;
    

    dcs_s8_t tmp_filename[PATH_LEN];
    string filename;
    dcs_u64_t filesize;
    dcs_s8_t file_md5[MD5_STR_LEN + 1];
    DCS_ENTER("__dcs_write_server enter \n");
    
    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        mark[i] = 0;
    }
    /*
    for(i=0; i<DCS_MASTER_NUM; i++){
        tmp_sha_v[i] = NULL;
        sha_v[i] = NULL;
        req2m[i] = NULL;
        reqmsgp2m[i] = NULL;
        repmsgp2m[i] = NULL;
    }
*/

    //printf("||||||got msg from client: %s[%d]\n", (dcs_s8_t *)req->req_iov->ak_addr, req->req_iov->ak_len);
    
    //get msg info from client msg
    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    seqno = msgp->seqno;
    fromid = msgp->fromid;
    filetype = msgp->filetype;      //by bxz
    filesize = msgp->filesize;      //by bxz
    memcpy(file_md5, msgp->md5, MD5_STR_LEN);
    fileinode = msgp->u.c2s_req.inode;
    timestamp = msgp->u.c2s_req.timestamp;
    fileoffset = msgp->u.c2s_req.offset;
    bufsize = msgp->u.c2s_req.size;
    finish = msgp->u.c2s_req.finish;
    printf("||||||MD5: %s\n", file_md5);

    //by bxz
    sprintf(tmp_filename, "%s_%u_%lu", msgp->u.c2s_req.filename, fromid, timestamp);
    filename = tmp_filename;
    printf("filename: %s, offset: %lu\n", tmp_filename, fileoffset);
    target = (file_md5[0] * 256 + file_md5[1]) % DCS_COMPRESSOR_NUM + 1;
    
    if(finish){
        DCS_MSG("__dcs_write_server file %ld write finish \n", fileinode);
        rc = __dcs_server_write_finish(file_md5, filetype, target, req);
        goto EXIT;
    }

    //by bxz
    pthread_mutex_lock(&server_table_lock);
    server_table[filename].filetype = filetype;
    server_table[filename].filesize = filesize;
    server_table[filename].compressor_id = target;
    server_table[filename].inode = fileinode;
    server_table[filename].timestamp = timestamp;
    memcpy(server_table[filename].md5, file_md5, MD5_STR_LEN + 1);
    pthread_mutex_unlock(&server_table_lock);
    /*
    //do chunking job, fix or var chunk*
    if(server_chunk_type == FIX_CHUNK){
        chunk_detail = __dcs_get_fix_chunk((dcs_s8_t *)req->req_iov->ak_addr, req->req_iov->ak_len, fileoffset);
    } else if(server_chunk_type == VAR_CHUNK){
        chunk_detail = __dcs_get_var_chunk((dcs_s8_t *)req->req_iov->ak_addr, req->req_iov->ak_len, fileoffset);
    } else{
        DCS_ERROR("__dcs_write_server wrong server_chunk_type \n");
        rc = -1;
        goto EXIT;
    }

    if(chunk_detail == NULL){
        DCS_ERROR("__dcs_write_server chunking err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }

    sha_array = chunk_detail->sha_array;

    if(sha_array == NULL){
        DCS_ERROR("__dcs_write_server get sha_array err:%d \n", errno);
        rc = -1;
        goto EXIT;
    }

    total_sha_num = chunk_detail->chunk_num;

    DCS_MSG("total sha num is %d \n", total_sha_num);
    tmpsha = (dcs_u8_t *)malloc(SHA_LEN);
    if(tmpsha == NULL){
        DCS_ERROR("__dcs_write_server malloc for tmpsha err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    power = get_master_power();
    
    //init some array for sending query requst
    for(i=0; i<DCS_MASTER_NUM; i++){
        tmp_sha_v[i] = NULL;
        sha_v[i] = NULL;
        sha_num[i] = 0;
        master_cache_id[i] = 0;
        req2m[i] = NULL;
        reqmsgp2m[i] = NULL;
        repmsgp2m[i] = NULL;
    }

    

    for(i=0; i<DCS_MASTER_NUM; i++){
        tmp_sha_v[i] = (dcs_u8_t *)malloc(SHA_LEN*total_sha_num);
        if(tmp_sha_v[i] == NULL){
            DCS_ERROR("__dcs_write_server malloc for tmp_sha_v array err:%d \n", errno);
            rc = errno;
            goto EXIT;
        }
        memset(tmp_sha_v[i], 0, (SHA_LEN*total_sha_num));
    }

    //category the sha value decide the sha value should be sended to which master
    for(i=0; i<total_sha_num; i++){
        sign = 0;
        memcpy(tmpsha, sha_array[i].sha, SHA_LEN);

        //DCS_MSG("__dcs_write_server the power is %d \n", power);
        sign = tmpsha[0] & ((1 << power) - 1);
        memcpy(tmp_sha_v[sign] + SHA_LEN*sha_num[sign], sha_array[i].sha, SHA_LEN);
        sha_num[sign]++;
    }

    for(i=0; i<DCS_MASTER_NUM; i++){
        sha_v[i] = (dcs_u8_t *)malloc(SHA_LEN*sha_num[i]);
        if(sha_v[i] == NULL && sha_num[i] != 0){
            DCS_ERROR("__dcs_write_server malloc init the %dth array err:%d \n",
                        i, errno);
            rc = errno;
            goto EXIT;
        }

        //DCS_MSG("__dcs_write_server len of tmp_sha_v is %ld \n", strlen(tmp));
        memcpy(sha_v[i], tmp_sha_v[i], sha_num[i]*SHA_LEN);
        DCS_MSG("__dcs_write_server sha num %d is %d \n",i , sha_num[i]);
    }

    //send sha vlue to the responding master
    for(i=0; i<DCS_MASTER_NUM; i++){
        rc = __amp_alloc_request(&req2m[i]);
        if(rc < 0){
            DCS_ERROR("__dcs_write_server alloc for %dth req err:%d \n",
                        i, errno);
            rc = errno;
            goto EXIT;
        }

        size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
        reqmsgp2m[i] = (amp_message_t *)malloc(size);
        if(!reqmsgp2m[i]){
            DCS_ERROR("__dcs_write_server alloc for %dth reqmsgp err:%d \n",
                        i, errno);
            rc = errno;
            goto EXIT;
        }
        memset(reqmsgp2m[i], 0, size);
        msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp2m[i] + AMP_MESSAGE_HEADER_LEN);
        msgp->size = size;
        msgp->msg_type = is_req;
        msgp->fromid = server_this_id;
        msgp->fromtype = DCS_SERVER;
        msgp->optype = DCS_QUERY;
        msgp->u.s2m_req.sha_num = sha_num[i];
        msgp->u.s2m_req.bf_id = -1;
        msgp->u.s2m_req.cache_id = -1;

        req2m[i]->req_iov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
        if(req2m[i]->req_iov == NULL){
            DCS_ERROR("__dcs_write_server malloc for %dth err:%d \n",
                        i, errno);
            rc = errno;
            goto EXIT;
        }

        DCS_MSG("__dcs_write_server init iov to send sha value \n");

        req2m[i]->req_iov->ak_addr = sha_v[i];
     
        //dcs_s8_t *tmp;
        //tmp = (dcs_s8_t *)sha_v[i];
        //DCS_MSG("the len of sha_v %ld \n", strlen(tmp));
     
        if(req2m[i]->req_iov->ak_addr == NULL){
            DCS_ERROR("__dcs_write_server ak_addr is null \n");
        }

        req2m[i]->req_iov->ak_len  = sha_num[i]*SHA_LEN;
        if(req2m[i]->req_iov->ak_len){
            DCS_MSG("__dcs_write_server req2m[i]->req_iov->ak_len is %d \n",
                                                    req2m[i]->req_iov->ak_len);
        }

        req2m[i]->req_iov->ak_offset = 0;
        req2m[i]->req_iov->ak_flag = 0;
        req2m[i]->req_niov = 1;

        req2m[i]->req_msg = reqmsgp2m[i];
        req2m[i]->req_msglen = size;
        req2m[i]->req_need_ack = 1;
        req2m[i]->req_resent = 1;
        req2m[i]->req_type = AMP_REQUEST | AMP_DATA;
    }

    for(i=0; i<DCS_MASTER_NUM; i++){
        DCS_MSG("send to %dth master \n", i+1);
        rc = amp_send_sync(server_comp_context, 
                        req2m[i], 
                        DCS_MASTER, 
                        (i+1), 
                        0);
        DCS_MSG("after send %dth master msg\n",i+1);
        if(rc < 0){
            DCS_ERROR("__dcs_write_server send req to master err:%d \n",rc);
            //rc = errno;
            goto EXIT;
        }
    }

    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        mark[i] = 0;
    }
    
    //master finish query ,recieve marks
    for(i=0; i<DCS_MASTER_NUM; i++){
        DCS_MSG("before get reply msg \n");
        repmsgp2m[i] = req2m[i]->req_reply;
        if(!repmsgp2m[i]){
            DCS_ERROR("__dcs_write_server recieve reply %dth  msg err:%d \n",
                        i, errno );
            rc = -1;
            goto EXIT;
        }

        DCS_MSG("after get reply msg \n");
        msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp2m[i]+ AMP_MESSAGE_HEADER_LEN);
        if(msgp->ack == 0){
            DCS_ERROR("__dcs_write_server server fail to query the %dth master \n",(i+1));
            rc = -1;
            goto EXIT;
        }

        //store the cache id in maser, it will be used in update BF
        master_cache_id[i] = msgp->u.m2s_reply.cache_id;
        DCS_MSG("the master cache id is %d \n",master_cache_id[i]);
        
        //tmp_mark = (dcs_u32_t *)malloc(sizeof(dcs_u32_t) * DCS_COMPRESSOR_NUM);
        //tmp_mark = (dcs_u32_t *)((dcs_s8_t *)repmsgp2m[i] + AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t));
        tmp_mark = (dcs_u32_t *)(msgp->buf);
        //memcpy(tmp_mark, repmsgp2m[i] + AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t), sizeof(dcs_u32_t) * DCS_COMPRESSOR_NUM);
        DCS_MSG("offset : %lu \n", (dcs_u64_t)((dcs_s8_t *)tmp_mark - (dcs_s8_t *)repmsgp2m[i]));
        DCS_MSG("msg_size is %d \n", req2m[i]->req_replylen);
        DCS_MSG("__dcs_write_server the tmp_mark is %u \n", tmp_mark[0]);

        if(req2m[i]->req_iov){
            //tmp_mark = (dcs_u32_t *)req2m[i]->req_iov->ak_addr;
            if(tmp_mark == NULL){
                DCS_ERROR("__dcs_write_server get mark from master err: -1  \n");
                rc = -1;
                goto EXIT;
            }
        }

     
        for(j=0; j<DCS_COMPRESSOR_NUM; j++){
            //DCS_MSG("__dcs_write_server %dth mark is %d, tmp_mark is %d \n", j, mark[j], tmp_mark[j]);
            mark[j] = mark[j] + tmp_mark[j];
            //DCS_MSG("__dcs_write_server %dth mark is %d \n",j, mark[j]);
        }

        if(req2m[i]->req_iov){
            __server_freebuf(req2m[i]->req_niov, req2m[i]->req_iov);
            free(req2m[i]->req_iov);
            req2m[i]->req_iov = NULL;
            req2m[i]->req_niov = 0;
        }

        if(repmsgp2m[i]){
            free(repmsgp2m[i]);
            repmsgp2m[i] = NULL;
        }
    }
     */

    /*
    //do the routing and make decision on which target compressor data will route to
    if(ROUTING_ON == 1)
        target = __dcs_server_data_routing(mark);
    else
        target = __dcs_server_stateless_routing(sha_array[0].sha);

    if(target < 0){
        DCS_ERROR("__dcs_write_server routing err:%d \n", target);
        goto EXIT;
    }
     */
    
    //DCS_MSG("__dcs_write_server the hishest mark is:%d and the target compressor is:%d \n",
                //highest, target);
    
    //add by bxz
    tmpsha = (dcs_u8_t *)malloc(SHA_LEN);
    if(tmpsha == NULL){
        DCS_ERROR("__dcs_write_server malloc for tmpsha err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    SHA1((dcs_u8_t *)req->req_iov->ak_addr, req->req_iov->ak_len, tmpsha);
    //target = tmpsha[0] % DCS_COMPRESSOR_NUM; //no need to + 1 because already + 1 when send data
    target = fileinode % DCS_COMPRESSOR_NUM; //no need to + 1 because already + 1 when send data
    //add by bxz end

    if(rc != 0){
        DCS_ERROR("__dcs_write_server update master err:%d \n", rc);
        goto EXIT;
    }

    rc = __amp_alloc_request(&req2d);
    if(rc < 0){
        DCS_ERROR("__dcs_write_server alloc for request to compressor err: %d\n",errno);
        rc = errno;
        goto EXIT;
    }

    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    
    reqmsgp2d = (amp_message_t *)malloc(size);
    if(!reqmsgp2d){
        DCS_ERROR("__dcs_write_server malloc for repmsg2d err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
     
    memset(reqmsgp2d, 0, size);

    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp2d + AMP_MESSAGE_HEADER_LEN);
    msgp->size = size;
    msgp->msg_type = is_req;
    msgp->fromid = server_this_id;
    msgp->fromtype = DCS_SERVER;
    msgp->optype = DCS_WRITE;
    msgp->filetype = filetype;      //by bxz
    memcpy(msgp->md5, file_md5, MD5_STR_LEN + 1);
    //msgp->u.s2d_req.chunk_num = total_sha_num;
    msgp->u.s2d_req.chunk_num = 1;  //by bxz
    msgp->u.s2d_req.scsize = bufsize;
    msgp->u.s2d_req.offset = fileoffset;

    req2d->req_iov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if(req2d->req_iov == NULL){
        DCS_ERROR("__dcs_write_server malloc for req2d req_iov err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    //bufsize = bufsize + strlen((dcs_s8_t *)sha_array);
    //bufsize = bufsize + total_sha_num*sizeof(sha_array_t);
    //by bxz
    sha_array = (sha_array_t *)malloc(sizeof(sha_array_t));
    sha_array->chunksize = bufsize;
    sha_array->offset = fileoffset;
    memcpy(sha_array->sha, tmpsha, SHA_LEN);
    //printf("||send chunk info: size: %d, offset: %lu, sha: %s\n", sha_array->chunksize, sha_array->offset, sha_array->sha);
    //by bxz end
    
    bufsize = bufsize + sizeof(sha_array_t);
    buf = (dcs_s8_t *)malloc(bufsize);
    if(buf == NULL){
        DCS_ERROR("__dcs_write_server malloc data buf for compressor err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(buf, 0 , bufsize);

    DCS_MSG("__dcs_write_server bufsize is %d \n", bufsize);
    /*send FP + data to compressor*/
    //memcpy(buf, sha_array, total_sha_num*sizeof(sha_array_t));
    //memcpy(buf + total_sha_num*sizeof(sha_array_t), req->req_iov->ak_addr, req->req_iov->ak_len);
    memcpy(buf, (dcs_s8_t *)sha_array, sizeof(sha_array_t));
    memcpy(buf + sizeof(sha_array_t), req->req_iov->ak_addr, req->req_iov->ak_len);

    //DCS_MSG("__dcs_write_server ready to send data to the %dth compressor \n", (target + 1));
    req2d->req_iov->ak_addr = buf;
    req2d->req_iov->ak_len = bufsize;
    req2d->req_iov->ak_offset = 0;
    req2d->req_iov->ak_flag = 0;
    req2d->req_niov = 1;

    req2d->req_msg = reqmsgp2d;
    req2d->req_msglen = size;
    req2d->req_need_ack = 1;
    req2d->req_resent = 1;
    req2d->req_type = AMP_REQUEST | AMP_DATA;

    rc = amp_send_sync(server_comp_context,
                        req2d,
                        DCS_NODE,
                        (target + 1),
                        0);

    DCS_MSG("__dcs_write_server target %d \n", target);
    if(rc < 0){
        DCS_ERROR("__dcs_write_server amp send data to compressor err: %d \n", rc);
        goto EXIT;
        }

    repmsgp2d = req2d->req_reply;
    if(repmsgp2d == NULL){
        DCS_ERROR("__dcs_write_server cannot recieve err:%d \n", errno);
        rc = -1;
        goto EXIT;
    }
    
    //get data stored position from the compressor
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp2d + AMP_MESSAGE_HEADER_LEN);
    data_pos = (dcs_datapos_t *)((dcs_s8_t *)msgp->buf);
    /*
    if(data_pos == NULL){
        DCS_ERROR("__dcs_write_server get data_pos from reply msg err");
        rc = -1;
        goto EXIT;
    }
     */

    //merge the data position and other chunkinfo to chunk mapping info
    //datamap = __dcs_server_chunkinfo_merge(data_pos, sha_array, total_sha_num);

    //buf the chunk mapping info until a file is finished
    //rc = __dcs_server_insert_mapinfo(datamap, fileinode, timestamp, fromid, total_sha_num);
    if(rc != 0){
        DCS_ERROR("__dcs_write_server insert mapinfo err:%d \n", rc);
        goto EXIT;
    }

    //update target bloom filter in master
    //rc = __dcs_server_updata_master(target, master_cache_id);
    if(rc != 0){
        DCS_ERROR("__dcs_write_server err:%d \n", rc);
        goto EXIT;
    }

    //send reply to client
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    repmsgp = (amp_message_t *)malloc(size);
    if(repmsgp == NULL){
        DCS_ERROR("__dcs_write_server malloc for repmsgp err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);

    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->ack = 1;
    msgp->msg_type = is_rep;
    msgp->u.s2c_reply.size = 0;
    msgp->u.s2c_reply.offset = 0;

    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 0;
    req->req_type = AMP_REPLY | AMP_MSG;

    rc = amp_send_sync(server_comp_context, 
                       req, 
                       req->req_remote_type, 
                       req->req_remote_id, 
                       0);
    if(rc < 0){
        DCS_ERROR("__dcs_write_server reply to client err:%d \n", errno);
        goto EXIT;
    }
    //printf("||||||got msg from client: %s[%d]\n", (dcs_s8_t *)req->req_iov->ak_addr, req->req_iov->ak_len);
    /*
    if (filetype == DCS_FILETYPE_FASTA) {
        printf("got file type: FASTA!\n");
    } else if (filetype == DCS_FILETYPE_FASTQ) {
        printf("got file type: FASTQ!\n");
    } else {
        printf("filetype error!\n");
    }
     */

EXIT:
    //DCS_MSG("__dcs_write_server before free space \n");

    /*
    if(data_pos != NULL){
        free(data_pos);
        data_pos = NULL;
    }
    */

    //DCS_MSG("1 \n");
    /*
    if(chunk_detail != NULL){
        if(sha_array != NULL){
            free(sha_array);
            sha_array = NULL;
        }
        
        free(chunk_detail);
        chunk_detail = NULL;
    }
     */

    if(tmpsha != NULL){
        free(tmpsha);
        tmpsha = NULL;
    }

    //DCS_MSG("2 \n");
    /*
    if(buf != NULL){
        free(buf);
        buf = NULL;
    }
    */

    /*
    for(i=0; i<DCS_MASTER_NUM; i++){
        if(tmp_sha_v[i] != NULL){
            free(tmp_sha_v[i]);
            tmp_sha_v[i] = NULL;
        }
    }
     */
   //DCS_MSG("3 \n");

    /*
    for(i=0; i<DCS_MASTER_NUM; i++){
        if(sha_v[i] != NULL){
            free(sha_v[i]);
            sha_v[i] = NULL;
        }
    }
    */

    /*
   //DCS_MSG("4 \n");
    //free request msg to master
    for(i=0; i<DCS_MASTER_NUM; i++){
        if(reqmsgp2m[i] != NULL){
            free(reqmsgp2m[i]);
            reqmsgp2m[i] = NULL;
        }
    }

   //DCS_MSG("5 \n");
    //free reply msg from master
    for(i=0; i<DCS_MASTER_NUM; i++){
        if(repmsgp2m[i] != NULL){
            free(repmsgp2m[i]);
            repmsgp2m[i] = NULL;
        }
    }

   //DCS_MSG("6 \n");
    //free request to master
    for(i=0; i<DCS_MASTER_NUM; i++){
        if(req2m[i] != NULL){
            if(req2m[i]->req_iov != NULL){
                __server_freebuf(req2m[i]->req_niov, req2m[i]->req_iov);
                free(req2m[i]->req_iov);
                req2m[i]->req_iov = NULL;
            }
            __amp_free_request(req2m[i]);
            req2m[i] = NULL;
        }
    }
     */

   //DCS_MSG("7 \n");
    if(reqmsgp2d != NULL){
        free(reqmsgp2d);
        reqmsgp2d = NULL;
    }

   //DCS_MSG("8 \n");
    if(repmsgp2d != NULL){
        free(repmsgp2d);
        repmsgp2d = NULL;
    }

   //DCS_MSG("9 \n");
    if(req2d != NULL){
        if(req2d->req_iov != NULL){
            __server_freebuf(req2d->req_niov, req2d->req_iov);
            free(req2d->req_iov);
            req2d->req_iov = NULL;
        }
        __amp_free_request(req2d);
        req2d = NULL;
    }

   //DCS_MSG("10 \n");
    if(repmsgp != NULL){
        free(repmsgp);
        repmsgp = NULL;
    }

   //DCS_MSG("11 \n");
    if(req->req_msg){
        amp_free(req->req_msg, req->req_msglen);
    }

   //DCS_MSG("12 \n");
    if(req->req_iov){
        __server_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
        req->req_iov = NULL;
    }

   //DCS_MSG("13 \n");
    if(req != NULL){
        __amp_free_request(req);
    }

    DCS_LEAVE("__dcs_write_server leave\n");
    return rc;

}

/*data routing according the query reulst from master */
dcs_s32_t __dcs_server_data_routing(dcs_u32_t *mark)
{
    dcs_s32_t i = 0;
    //dcs_u32_t max = 0;
    dcs_s32_t target = 0;
    double      max = 0.0;
    double      tmp_avg = 0.0;
    double      tmp_mark[DCS_COMPRESSOR_NUM];

    if(DCS_COMPRESSOR_NUM == 1){
        target = 0;
        goto EXIT;
    }

    /*
    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        if(sign_num < (i+1)*1){
            target = i;
            goto EXIT;
        }
    }
    */

    DCS_ENTER("__dcs_server_data_routing enter \n");

    pthread_mutex_lock(&diskinfo_lock);
    tmp_avg = (double)disk_avg;
    DCS_MSG("__dcs_server_data_routing avg is %lf \n", tmp_avg);
    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        if(ROUTING_ON && diskinfo[i] > disk_avg && diskinfo[i] != 0)
            tmp_mark[i] = (double)mark[i]*tmp_avg/diskinfo[i];
        else
            tmp_mark[i] = (double)mark[i];
    }
    pthread_mutex_unlock(&diskinfo_lock);

    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        DCS_MSG("mark %d is %lf\n",i, tmp_mark[i]);
        if(tmp_mark[i] > max ){
            max = tmp_mark[i];
            target = i;
        }
    }

    DCS_MSG("data routing the max is %lf \n", max);

    if(max == 0.0){
        target = rand()%(DCS_COMPRESSOR_NUM);
    }

    //DCS_MSG("__dcs_server_data_routing target is %d \n", target);

EXIT:

    sign_num++;

    DCS_LEAVE("__dcs_server_data_routing leave \n");
    return target;
}

/*stateless datarouting use the*/
dcs_s32_t __dcs_server_stateless_routing(dcs_u8_t *sha)
{
    dcs_u8_t  tmpsha;
    dcs_u32_t sign = 0;
    dcs_s32_t target = 0;

    if(DCS_COMPRESSOR_NUM == 1){
        target = 0;
        goto EXIT;
    }

    DCS_ENTER("__dcs_server_stateless_routing enter \n");

    tmpsha = sha[0];
    sign = (dcs_u32_t)tmpsha;
    target = sign % DCS_COMPRESSOR_NUM;
    DCS_MSG("sign is %d target is %d \n", sign, target);

EXIT:
    DCS_LEAVE("__dcs_server_stateless_routing leave \n");

    return target;
}

/* the sha value been sended to master has been cache in a buf
 * when data routing is finish we update the bloom filter 
 * send the target compressor which dcs the superchunk and the cache id
 * to the master to update corresponding bloom filter
 */
dcs_u32_t __dcs_server_updata_master(dcs_u32_t target, 
                                         dcs_u32_t *master_cache_id)
{
    dcs_u32_t rc = 0;
    dcs_u32_t i = 0;
    dcs_u32_t size = 0;

    amp_request_t *req[DCS_MASTER_NUM];
    amp_message_t *reqmsgp[DCS_MASTER_NUM];
    amp_message_t *repmsgp[DCS_MASTER_NUM];
    dcs_msg_t   *msgp;

    DCS_ENTER("__dcs_server_updata_master enter \n");

    for(i=0; i<DCS_MASTER_NUM; i++){
        rc = __amp_alloc_request(&req[i]);
        if(rc < 0){
            DCS_ERROR("__dcs_server_updata_master malloc for %dth req err:%d \n", i, errno);
            rc = errno;
            goto EXIT;
        }
    }
    
    for(i=0; i<DCS_MASTER_NUM; i++){
        size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
        reqmsgp[i] = (amp_message_t *)malloc(size);
        if(reqmsgp == NULL){
            DCS_ERROR("__dcs_server_updata_master malloc for %dth reqmsgp err:%d \n", i, errno);
            rc = errno;
            goto EXIT;
        }
        memset(reqmsgp[i], 0, size);

        msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp[i] + AMP_MESSAGE_HEADER_LEN);
        msgp->size = size;
        msgp->msg_type = is_req;
        msgp->fromid = server_this_id;
        msgp->fromtype = DCS_SERVER;
        msgp->optype = DCS_UPDATE;
    
        msgp->u.s2m_req.bf_id = target;
        msgp->u.s2m_req.cache_id = master_cache_id[i];
        
        req[i]->req_iov = NULL;
        req[i]->req_niov = 0;

        req[i]->req_msg = reqmsgp[i];
        req[i]->req_msglen = size;
        req[i]->req_need_ack = 1;
        req[i]->req_resent = 1;
        req[i]->req_type = AMP_REQUEST|AMP_MSG;
    }

    for(i=0; i<DCS_MASTER_NUM; i++){
        rc = amp_send_sync(server_comp_context,
                           req[i],
                           DCS_MASTER,
                           (i+1),
                           0);
        if(rc < 0){
            DCS_ERROR("__dcs_server_updata_master send %dth req err:%d \n", i, errno);
            rc = errno;
            goto EXIT;
        }
    }

    for(i=0; i<DCS_MASTER_NUM; i++){
        repmsgp[i] = req[i]->req_reply;
        if(!repmsgp[i]){
            DCS_ERROR("__dcs_server_updata_master cannot recieve %dth msg from master err:%d \n", i, errno);
            rc = errno;
            goto EXIT;
        }

        msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp[i] + AMP_MESSAGE_HEADER_LEN);
        if(msgp->ack){
            //DCS_MSG("__dcs_server_updata_master update bf in %dth master sucessed\n", i);
        }
        else{
            //DCS_MSG("__dcs_server_updata_master update bf in %dth master err \n", i);
            rc = -1;
            goto EXIT;
        }
    }

EXIT:
    for(i=0; i<DCS_MASTER_NUM; i++){
        if(repmsgp[i]){
            free(repmsgp[i]);
            repmsgp[i] = NULL;
        }

        if(reqmsgp[i]){
            free(reqmsgp[i]);
            reqmsgp[i] = NULL;
        }

        if(req[i]){
            __amp_free_request(req[i]);
        }
    }

    DCS_LEAVE("__dcs_server_updata_master leave \n");
    return rc;
}

/*merge the chunk common info and position info into mapinfo*/
dcs_datamap_t *__dcs_server_chunkinfo_merge(dcs_datapos_t *data_pos,
                                                sha_array_t *sha_array,
                                                dcs_u32_t chunk_num)
{
    dcs_u32_t rc = 0;
    dcs_s32_t i;
    
    dcs_datamap_t *datamap = NULL;

    DCS_ENTER("__dcs_server_chunkinfo_merge enter \n");

    if(!chunk_num){
        DCS_ERROR("__dcs_server_chunkinfo_merge get chunk num err \n");
        rc = -1;
        datamap = NULL;
        goto EXIT;
    }

    datamap = (dcs_datamap_t *)malloc(sizeof(dcs_datamap_t) * chunk_num);
    if(datamap == NULL){
        DCS_ERROR("__dcs_server_chunkinfo_merge malloc for datamap err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(datamap, 0, sizeof(dcs_datamap_t)*chunk_num);

    for(i=0; i<chunk_num; i++){
        memcpy(datamap[i].sha, sha_array[i].sha, SHA_LEN);//store sha 
        datamap[i].chunksize = sha_array[i].chunksize;
        datamap[i].offset = sha_array[i].offset;
        datamap[i].compressor_id = data_pos[i].compressor_id;
        datamap[i].container_id = data_pos[i].container_id;
        datamap[i].container_offset = data_pos[i].container_offset;
        if(i==253){
            DCS_MSG("the datamap compressor_id %d datapos compressor id %d \n", 
                    datamap[i].compressor_id, data_pos[i].compressor_id);
        }
        if(i>0 && data_pos[i].compressor_id != data_pos[i-1].compressor_id){
            DCS_MSG("the %dth datamap compressor_id is %d and %dth datamap compressor id is %d\n",
                     i, data_pos[i].compressor_id, i-1, data_pos[i-1].compressor_id);
        }
        
        if(datamap[i].compressor_id > 255){
            DCS_ERROR("the %dth datamap compressor_id is %d and %dth datamap compressor id is %d\n",i, data_pos[i].compressor_id, i-1, data_pos[i-1].compressor_id);
        }
    }

EXIT:
    DCS_LEAVE("__dcs_server_chunkinfo_merge leave \n");

    if(rc != 0)
        return NULL;
    else
        return datamap;

}

/*when a file is been compressed, write back its mapinfo
 * and send reply msg back*/
dcs_u32_t __dcs_server_write_finish(dcs_s8_t *md5,
                                    dcs_u32_t filetype,
                                    dcs_u32_t target_compressor,
                                        amp_request_t *req)
{
    dcs_s32_t rc = 0;
    dcs_u32_t size = 0;

    amp_message_t *repmsgp = NULL;
    dcs_msg_t   *msgp = NULL;
    
    //by bxz
    amp_request_t *req2d = NULL;
    amp_message_t *reqmsgp2d = NULL;
    amp_message_t *repmsgp2d = NULL;

    DCS_ENTER("__dcs_server_write_finish enter \n");
    printf("~~~~~~~~~~~~~~~server write finish IN\n");

    //write back file mapping info
    /*
    rc = __dcs_server_mapinfo_wb(inode, timestamp, clientid, size);
    if(rc != 0){
        DCS_ERROR("__dcs_server_write_finish mapinfo wb err:%d \n", rc);
        goto EXIT;
    }
     */
    pthread_mutex_lock(&server_table_lock);
    __dcs_server_write_mapinfo();
    pthread_mutex_unlock(&server_table_lock);
    
    //send finish msg to compressor
    rc = __amp_alloc_request(&req2d);
    if (rc < 0) {
        DCS_ERROR("__dcs_server_write_finish alloc for req2d error[%d]\n", rc);
        goto EXIT;
    }
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    reqmsgp2d = (amp_message_t *)malloc(size);
    if (reqmsgp2d == NULL) {
        DCS_ERROR("__dcs_server_write_finish malloc for reqmsgp2d error[%d]", errno);
        rc = errno;
        goto EXIT;
    }
    memset(reqmsgp2d, 0, size);
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp2d + AMP_MESSAGE_HEADER_LEN);
    msgp->size = size;
    msgp->filetype = filetype;
    msgp->optype = DCS_WRITE;
    msgp->msg_type = is_req;
    msgp->fromtype = DCS_SERVER;
    msgp->fromid = server_this_id;
    memcpy(msgp->md5, md5, MD5_STR_LEN + 1);
    msgp->u.s2d_req.finish = 1;
    
    req2d->req_iov = NULL;
    req2d->req_niov = 0;
    req2d->req_msg = reqmsgp2d;
    req2d->req_msglen = size;
    req2d->req_need_ack = 1;
    req2d->req_resent = 1;
    req2d->req_type = AMP_REQUEST | AMP_MSG;
    
    rc = amp_send_sync(server_comp_context,
                       req2d,
                       DCS_NODE,
                       target_compressor,
                       0);
    if (rc < 0) {
        DCS_ERROR("__dcs_server_write_finish send req to compressor error[%d]\n", rc);
        goto EXIT;
    }
    
    //get the reply from compressor
    repmsgp2d = req2d->req_reply;
    if (repmsgp2d == NULL) {
        DCS_ERROR("__dcs_server_write_finish cannot recieve the reply from compressor\n");
        rc = -1;
        goto EXIT;
    }
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp2d + AMP_MESSAGE_HEADER_LEN);
    if (msgp->ack == 0) {
        DCS_ERROR("__dcs_server_write_finish compressor fail to store the map info\n");
        rc = -1;
        goto EXIT;
    }
    if (req2d->req_iov) {
        free(req2d->req_iov);
        req2d->req_iov = NULL;
        req2d->req_niov = 0;
    }
    amp_free(repmsgp2d, req2d->req_replylen);

    //send finish reply to client
    size = sizeof(dcs_msg_t) + AMP_MESSAGE_HEADER_LEN;
    repmsgp = (amp_message_t *)malloc(size);
    if(repmsgp == NULL){
        DCS_ERROR("__dcs_server_write_finish malloc for repmsgp err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
    memset(repmsgp, 0, size);
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);

    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);

    msgp->ack = 1;
    
    req->req_iov = NULL;
    req->req_niov = 0;

    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 0;
    req->req_type = AMP_REPLY | AMP_MSG;

    rc = amp_send_sync(server_comp_context, 
                       req, 
                       req->req_remote_type, 
                       req->req_remote_id, 
                       0);

    if(rc != 0){
        DCS_ERROR("__dcs_server_write_finish send reply err: %d \n", rc);
        goto EXIT;
    }

EXIT:
    if (reqmsgp2d) {
        free(reqmsgp2d);
        reqmsgp2d = NULL;
    }
    if (req2d) {
        __amp_free_request(req2d);
    }

    if(repmsgp != NULL){
        free(repmsgp);
        repmsgp = NULL;
    }
    
    return rc;
}

/*get the power of master*/
dcs_s32_t get_master_power()
{
    dcs_s32_t rc = 0;
    dcs_u32_t count = 0;
    dcs_u32_t a;

    a = DCS_MASTER_NUM;
    while(a){
        a = a/2;
        count++;
    }

    rc = count - 1;

    return rc;
}

dcs_s32_t __dcs_delete_server(amp_request_t *req){
    dcs_s32_t rc = 0;
    dcs_s32_t i,j;
    dcs_u64_t fileinode;
    dcs_u32_t client_id;
    dcs_s32_t pos = -1;
    dcs_s32_t begin = -1;
    dcs_s32_t size = 0;
    dcs_s32_t end = 0;
    dcs_u64_t reqsize;
    dcs_u64_t fileoffset = 0;
    dcs_u32_t chunk_num = 0;
    dcs_s32_t target_num = 0;
    dcs_s8_t *map_name = NULL;
    dcs_mapbuf_t *map_buf = NULL;
    dcs_u8_t tmpsha[SHA_LEN];

    dcs_u8_t  *tmp_sha_v[DCS_MASTER_NUM];
    dcs_u8_t  *sha_v[DCS_MASTER_NUM];
    dcs_u32_t master_cache_id[DCS_MASTER_NUM];
    dcs_u32_t sha_num[DCS_MASTER_NUM];
    amp_message_t *repmsgp  = NULL;
    amp_request_t *req2m[DCS_MASTER_NUM];
    amp_message_t *reqmsgp2m[DCS_MASTER_NUM];
    amp_message_t *repmsgp2m[DCS_MASTER_NUM];
    dcs_datamap_t *tmp_sha_bf[DCS_MASTER_NUM];
    dcs_datamap_t *sha_bf[DCS_MASTER_NUM];
    dcs_u32_t sha_bf_num[DCS_MASTER_NUM];
    dcs_u32_t sign = 0;
    dcs_s32_t power = 0;
    dcs_readinfo_t *read_info = NULL;
    dcs_msg_t   *msgp = NULL;
    dcs_s8_t    *tmpdata = NULL;
    dcs_s8_t    *map_path = NULL;
    dcs_u64_t   timestamp;
    DCS_ENTER("__dcs_delete_server enter \n");
    
    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    reqsize = msgp->u.c2s_del_req.size;
    fileinode = msgp->u.c2s_del_req.inode;
    client_id = msgp->fromid;
    timestamp = msgp->u.c2s_del_req.timestamp;
    
    dcs_u32_t super_size = SUPER_CHUNK_SIZE;


    //DCS_MSG("2\n");
    map_name = get_map_name(fileinode, client_id, timestamp);
    if(map_name == NULL){
        DCS_ERROR("__dcs_delete_server get map file name err:%d \n",errno);
        rc = errno;
        goto SEND_ACK;
    }

    //DCS_MSG("3\n");
    if((pos = check_map_buf(map_name)) != -1){
        DCS_MSG("__dcs_delete_server map info is in buf \n");
        map_buf = get_map_buf(map_name, fileoffset, reqsize, pos);
    }
    else{
        DCS_MSG("__dcs_delete_server map info is not in buf \n");
        map_buf = read_map_buf(map_name, fileoffset, reqsize);
    }
    
    if(map_buf == NULL){
        DCS_ERROR("__dcs_delete_server get map_buf err:%d \n", errno);
        rc = -1;
        goto SEND_ACK;
    }

    chunk_num = map_buf->chunk_num;
    DCS_MSG("after get buf chunk num is %d \n", chunk_num);
    /*reset bloom filter of each master*/
    power = get_master_power();
    
    /*init some array for sending query requst*/
    for(i=0; i<DCS_MASTER_NUM; i++){
        tmp_sha_v[i] = NULL;
        sha_v[i] = NULL;
        sha_num[i] = 0;
        master_cache_id[i] = 0;
	req2m[i] = NULL;
        reqmsgp2m[i] = NULL;
        repmsgp2m[i] = NULL;
        tmp_sha_bf[i] = NULL;
        sha_bf[i] = NULL;
        sha_bf_num[i] = 0;
    }

    for(i=0; i<DCS_MASTER_NUM; i++){
        tmp_sha_bf[i] = (dcs_datamap_t *)malloc(sizeof(dcs_datamap_t) * chunk_num);
        if(tmp_sha_bf[i] == NULL){
            DCS_ERROR("__dcs_delete_server malloc for tmp_sha_bf array err:%d \n", errno);
            rc = errno;
            goto EXIT;
        }
        memset(tmp_sha_bf[i], 0, (sizeof(dcs_datamap_t) * chunk_num));
    }

    /*delete infomation of compressor,include index fp and data*/
    //DCS_MSG("4\n");
    target_num = (reqsize/super_size);
    read_info = (dcs_readinfo_t *)malloc(sizeof(dcs_readinfo_t)*target_num);
    if(read_info == NULL){
        DCS_ERROR("__dcs_delete_server malloc for read info err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    /*classify all the chunk into some compressor id
     * here use a simple way
     * if the compressor id is not continue
     * we consider it to be a different one*/
    j=0;
    DCS_MSG("chunk num is %d \n", chunk_num);
    for(i=0; i<chunk_num; i++){
        if(i == 0){
            begin = 0;
            end = 0;
        }
        if(map_buf->datamap[begin].compressor_id != map_buf->datamap[i].compressor_id){
            end = i-1;
            read_info[j].begin = begin;
            read_info[j].end = end;
            read_info[j].compressor_id = map_buf->datamap[begin].compressor_id;
            j++;
            begin = end = i;
        }
        
        sign = 0;
        memcpy(tmpsha, map_buf->datamap[i].sha, SHA_LEN);
        sign = tmpsha[0] & ((1 << power) - 1);
        
        memcpy((dcs_datamap_t *)tmp_sha_bf[sign] + sha_bf_num[sign], &map_buf->datamap[i], sizeof(dcs_datamap_t));
        sha_bf_num[sign]++;
        if(map_buf->datamap[i].compressor_id > 255){
            DCS_ERROR("dcs_delete_server map_buf->datamap[%d].compressor_id: %d\n", i, map_buf->datamap[i].compressor_id);
        }
    }


    for(i=0; i<DCS_MASTER_NUM; i++){
        sha_bf[i] = (dcs_datamap_t *)malloc(sizeof(dcs_datamap_t)*sha_bf_num[i]);
        if(sha_bf[i] == NULL && sha_bf_num[i] != 0){
            DCS_ERROR("__dcs_delete_server malloc init the %dth array err:%d \n", i, errno);
            rc = errno;
            goto EXIT;
        }
        memset(sha_bf[i], 0, sha_bf_num[i] * sizeof(dcs_datamap_t));
        memcpy(sha_bf[i], tmp_sha_bf[i], sha_bf_num[i] * sizeof(dcs_datamap_t));
   }



    /*send req to master, to reset bf of compressor_id of each master's blooms*/

    for(i=0; i<DCS_MASTER_NUM; i++){
        rc = __amp_alloc_request(&req2m[i]);
        if(rc < 0){
            DCS_ERROR("__dcs_delete_server alloc for %dth req err:%d \n",
                        i, errno);
            rc = errno;
            goto EXIT;
        }

        size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
        reqmsgp2m[i] = (amp_message_t *)malloc(size);
        if(!reqmsgp2m[i]){
            DCS_ERROR("__dcs_delete_server alloc for %dth reqmsgp err:%d \n",
                        i, errno);
            rc = errno;
            goto EXIT;
        }
        memset(reqmsgp2m[i], 0, size);
        msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp2m[i] + AMP_MESSAGE_HEADER_LEN);
        msgp->size = size;
        msgp->msg_type = is_req;
        msgp->fromid = server_this_id;
        msgp->fromtype = DCS_SERVER;
        msgp->optype = DCS_DELETE;
        msgp->u.s2m_del_req.sha_num = sha_bf_num[i];
        msgp->u.s2m_del_req.scsize = sha_bf_num[i] * sizeof(dcs_datamap_t);

        req2m[i]->req_iov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
        if(req2m[i]->req_iov == NULL){
            DCS_ERROR("__dcs_delete_server malloc for %dth err:%d \n",
                        i, errno);
            rc = errno;
            goto EXIT;
        }

        DCS_MSG("__dcs_delete_server init iov to send sha value \n");

        req2m[i]->req_iov->ak_addr = sha_bf[i];
        req2m[i]->req_iov->ak_len  = sha_bf_num[i]*sizeof(dcs_datamap_t);

        DCS_MSG("__dcs_delete_server req2m[i]->req_iov->ak_len is %d, scsize:%ld \n",req2m[i]->req_iov->ak_len,sha_bf_num[i] * sizeof(dcs_datamap_t));

        req2m[i]->req_iov->ak_offset = 0;
        req2m[i]->req_iov->ak_flag = 0;
        req2m[i]->req_niov = 1;

        req2m[i]->req_msg = reqmsgp2m[i];
        req2m[i]->req_msglen = size;
        req2m[i]->req_need_ack = 1;
        req2m[i]->req_resent = 1;
        req2m[i]->req_type = AMP_REQUEST | AMP_DATA;
    }

    for(i=0; i<DCS_MASTER_NUM; i++){
        DCS_MSG("send to %dth master \n", i+1);
        rc = amp_send_sync(server_comp_context, 
                        req2m[i], 
                        DCS_MASTER, 
                        (i+1), 
                        0);
        DCS_MSG("after send %dth master msg\n",i+1);
        if(rc < 0){
            DCS_ERROR("__dcs_delete_server send req to master err:%d \n",rc);
            rc = errno;
            goto SEND_ACK;
        }
    }

    /*master finish query ,recieve marks*/
    for(i=0; i<DCS_MASTER_NUM; i++){
        DCS_MSG("before get reply msg \n");
        repmsgp2m[i] = req2m[i]->req_reply;
        if(!repmsgp2m[i]){
            DCS_ERROR("__dcs_delete_server recieve reply %dth  msg err:%d \n", i, errno );
            rc = -1;
            goto SEND_ACK;
        }

        DCS_MSG("after get reply msg \n");
        msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp2m[i]+ AMP_MESSAGE_HEADER_LEN);

        if(msgp->ack == 1){
            //DCS_MSG("__dcs_delete_server reset bloom filter ");
        }else{
            DCS_ERROR("__dcs_delete_server fail to reset bf of the %dth master \n",(i+1));
            rc = -1;
            goto SEND_ACK;
        }


        if(req2m[i]->req_iov){
            __server_freebuf(req2m[i]->req_niov, req2m[i]->req_iov);
            free(req2m[i]->req_iov);
            req2m[i]->req_iov = NULL;
            req2m[i]->req_niov = 0;
        }

        if(repmsgp2m[i]){
            free(repmsgp2m[i]);
            repmsgp2m[i] = NULL;
        }
        
        if(req2m[i]){
            __amp_free_request(req2m[i]);
            req2m[i] = NULL;
        }      
    }
 
    rc = free_map_buf(map_name);
    if(rc){
        DCS_ERROR("__dcs_delete_server free map buf failed, map_name:%s\n", map_name);
        goto SEND_ACK;
    }
    
    map_path = get_map_path(map_name);    

    rc = remove(map_path);
    if(!rc){
        struct stat f_state;

        rc = stat(map_path, &f_state);
        if(rc == -1 && errno == ENOENT){
            rc = 0;
        }else{
            if(!rc){
                rc = remove(map_path);
                if(!rc){
                    DCS_ERROR("__dcs_delete_server delete map file %s failed\n", map_path);
                }
            }
        }
    }

    free_map_table(map_name);

SEND_ACK:
    //send delete ack infomation;
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t) ;
    repmsgp = (amp_message_t *)malloc(size);
    if(repmsgp == NULL){
        DCS_ERROR("__dcs_delete_server malloc for repmsgp err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
    memset(repmsgp, 0, size);
    
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);
    if(req->req_msg != NULL){
        free(req->req_msg);
        req->req_msg = NULL;
    }

    //DCS_MSG("13 \n");
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->msg_type = is_rep;
    msgp->size = size;
    msgp->fromtype = DCS_SERVER;
    msgp->fromid = server_this_id;
    msgp->ack = (rc == 0) ? 1 : 0;
    
    //DCS_MSG("14 \n");
    req->req_iov = NULL;
    req->req_niov = 0;
    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 1;
    req->req_type = AMP_REPLY | AMP_MSG;

    //DCS_MSG("16 \n");
    rc = amp_send_sync(server_comp_context,
                       req,
                       req->req_remote_type,
                       req->req_remote_id,
                       0);
    if(rc < 0){
        DCS_ERROR("__dcs_delete_server send read data to client err :%d \n", rc);
        goto EXIT;
    }
    //DCS_MSG("17 \n");

EXIT:
    if(map_buf != NULL){
        if(map_buf->datamap != NULL){
            free(map_buf->datamap);
            map_buf->datamap = NULL;
        }
        free(map_buf);
        map_buf = NULL;
    }

    if(map_name != NULL){
        free(map_name);
        map_name = NULL;
    }

    if(read_info){
        free(read_info);
        read_info = NULL;
    }

    if(tmpdata){
        free(tmpdata);
        tmpdata = NULL;
    }
/*
    for(i=0; i<target_num; i++){
        DCS_MSG("i is %d , target num is %d \n", i, target_num);
        if(reqmsgp2d[i] != NULL){
            free(reqmsgp2d[i]);
            reqmsgp2d[i] = NULL;
        }
            
        //DCS_MSG("1\n");
        if(repmsgp2d[i] != NULL){
            free(repmsgp2d[i]);
            repmsgp2d[i] = NULL;
        }

        //DCS_MSG("2\n");
        if(req2d[i] != NULL){
            if(req2d[i]->req_iov){
                __server_freebuf(req2d[i]->req_niov, req2d[i]->req_iov);
        //DCS_MSG("4\n");
                free(req2d[i]->req_iov);
        //DCS_MSG("5\n");
                req2d[i]->req_iov = NULL;
            }

        //DCS_MSG("3\n");
            __amp_free_request(req2d[i]);
        }
    }
*/
    //DCS_MSG("1\n");
    /*
    if(buf != NULL){
        free(buf);
        buf = NULL;
    }
    */
    
    //DCS_MSG("12\n");
    if(req->req_msg){
        //amp_free(req->req_msg, req->req_msglen);
    }

    //DCS_MSG("13\n");
    if(req->req_iov){
        __server_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
        req->req_iov = NULL;
    }

    //DCS_MSG("14\n");
    if(req != NULL){
        __amp_free_request(req);
    }


    DCS_LEAVE("__dcs_delete_server leave \n");
    return rc;

}

/*process read service
 * 1. anlyse the request
 * 2. get mapping info in this step, it may access to disk,
 *    when some data of a file has been read, the whole mapping info file
 *    will be read into buf, until all read request about this file is finish 
 * 3. according the mapping info, send read data request to compressor
 * 4. recieve data from compressor
 *    in this step, when we get data from compressor, it may a problem that 
 *    offset of the data been requested may not the beginning of a chunk so we have to 
 *    do some cuting job.
 * 5. send data to the client
 */
dcs_s32_t __dcs_read_server(amp_request_t *req)
{
    dcs_s32_t rc = 0;
    //dcs_s32_t i,j;
    dcs_u64_t fileinode;
    dcs_u32_t client_id;
    //dcs_s32_t pos = -1;
    //dcs_s32_t begin = -1;
    dcs_s32_t size = 0;
    //dcs_u32_t tmpbuf_size = 0;
    //dcs_u32_t tmpdata_size = 0;
    //dcs_u32_t buf_offset = 0;
    //dcs_s32_t end = 0;
    dcs_u64_t reqsize;
    dcs_u64_t fileoffset;
    //dcs_u32_t chunk_num = 0;
    //dcs_u32_t tmp_num = 0;
    //dcs_s32_t target[DCS_COMPRESSOR_NUM];
    //dcs_s32_t target_num = 0;
    //dcs_s8_t *map_name = NULL;
    //dcs_u8_t *tmpbuf = NULL;
    //dcs_datamap_t *map_buf = NULL;
    //dcs_mapbuf_t *map_buf = NULL;

    //dcs_readinfo_t *read_info = NULL;
    dcs_msg_t   *msgp = NULL;
    amp_message_t *repmsgp = NULL;
    //amp_request_t **req2d = NULL;
    //amp_message_t **reqmsgp2d = NULL;
    //repmsgp2d means reply message from compressor
    //amp_message_t **repmsgp2d = NULL;
    amp_request_t *req2d = NULL;
    amp_message_t *reqmsgp2d = NULL;
    amp_message_t *repmsgp2d = NULL;
    dcs_s8_t    *tmpdata = NULL;
    //dcs_s8_t    *buf = NULL;
    dcs_u64_t   timestamp;
    
    //bxz
    dcs_s8_t filename1[PATH_LEN];
    memset(filename1, 0, PATH_LEN);
    string filename;
    dcs_u32_t target_compressor = 0;
    dcs_s8_t file_md5[MD5_STR_LEN + 1];
    dcs_s8_t query_result = 0;
    dcs_u32_t filetype = 0;
    dcs_u32_t tmpdata_size;

    DCS_ENTER("__dcs_read_server enter \n");
    
    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    reqsize = msgp->u.c2s_req.size;
    printf("__dcs_read_server reqsize is %ld \n", reqsize);
    fileinode = msgp->u.c2s_req.inode;
    timestamp = msgp->u.c2s_req.timestamp;
    memcpy(filename1, msgp->u.c2s_req.filename, msgp->u.c2s_req.filename_len);   //bxz
    memcpy(file_md5, msgp->md5, MD5_STR_LEN + 1);
    filename = filename1;
    //request data offset in a file
    fileoffset = msgp->u.c2s_req.offset;
    client_id = msgp->fromid;
    printf("read: md5: %s\nclient_id: %d\noffset: %d\n", file_md5, client_id, fileoffset);

    //dcs_u32_t super_size = SUPER_CHUNK_SIZE;
    printf("reqsize: %lu, filename: %s[%d], fileoffset: %lu\n", reqsize, filename1, strlen(filename1), fileoffset);

    if(msgp->u.c2s_req.finish){
        printf("__dcs_read_server this is finish message \n");
        rc = __dcs_read_finish(fileinode, timestamp, client_id, req);
        goto EXIT;
    }
    
    printf("1\n");
    pthread_mutex_lock(&server_table_lock);
    if (server_table.find(filename) != server_table.end()) {
        query_result = 1;
        target_compressor = server_table[filename].compressor_id;
        memcpy(file_md5, server_table[filename].md5, MD5_STR_LEN + 1);
        filetype = server_table[filename].filetype;
    }
    pthread_mutex_unlock(&server_table_lock);
    printf("2\n");
    
    if (query_result == 0 || target_compressor <= 0 || (filetype != DCS_FILETYPE_FASTA && filetype != DCS_FILETYPE_FASTQ)) {
        query_result = 0;
        goto SEND_ACK;
    }
    printf("3\n");
    rc = __amp_alloc_request(&req2d);
    if (rc < 0) {
        DCS_ERROR("__dcs_read_server alloc for req2d error[%d]\n", rc);
        goto EXIT;
    }
    printf("4\n");
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    reqmsgp2d = (amp_message_t *)malloc(size);
    if (reqmsgp2d == NULL) {
        DCS_ERROR("__dcs_read_server malloc for reqmsgp2d error[%d]\n", errno);
        rc = -1;
        goto EXIT;
    }
    memset(reqmsgp2d, 0, size);
    //memcpy(reqmsgp2d, req->req_msg, AMP_MESSAGE_HEADER_LEN);
    
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp2d + AMP_MESSAGE_HEADER_LEN);
    msgp->size = size;
    msgp->optype = DCS_READ;
    msgp->msg_type = is_req;
    msgp->fromtype = DCS_SERVER;
    msgp->fromid = server_this_id;
    memcpy(msgp->md5, file_md5, MD5_STR_LEN + 1);
    msgp->u.s2d_req.offset = fileoffset;
    msgp->u.s2d_req.filetype = filetype;
    
    req2d->req_iov = NULL;
    req2d->req_niov = 0;
    req2d->req_msg = reqmsgp2d;
    req2d->req_msglen = size;
    req2d->req_need_ack = 1;
    req2d->req_resent = 1;
    req2d->req_type = AMP_REQUEST | AMP_MSG;
    
    printf("5\n");
    
    rc = amp_send_sync(server_comp_context,
                       req2d,
                       DCS_NODE,
                       target_compressor,
                       0);
    printf("6\n");
    if (rc < 0) {
        DCS_ERROR("__dcs_read_server send read req error[%d]\n", rc);
        goto EXIT;
    }
    printf("7\n");
    //get the data from compressor
    repmsgp2d = req2d->req_reply;
    if (repmsgp2d == NULL) {
        DCS_ERROR("__dcs_read_server cannot recieve the fata from compressor\n");
        rc = -1;
        goto EXIT;
    }
    if (req2d->req_iov == NULL) {
        DCS_ERROR("__dcs_read_server get no reply data from compressor\n");
        rc = -1;
        goto EXIT;
    }
    printf("8\n");
    
    if (filetype == DCS_FILETYPE_FASTA) {
        tmpdata_size = FA_CHUNK_SIZE;
    } else {
        tmpdata_size = FQ_CHUNK_SIZE;
    }
    tmpdata = (dcs_s8_t *)malloc(tmpdata_size);
    if (tmpdata == NULL) {
        DCS_ERROR("__dcs_read_server malloc for tmpdata error[%d]\n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(tmpdata, 0, tmpdata_size);
    printf("data:\n%s[%d]\n", req2d->req_iov->ak_addr, req2d->req_iov->ak_len);

    /*
    tmpdata = (dcs_s8_t *)malloc(reqsize);
    if (tmpdata == NULL) {
        DCS_ERROR("__dcs_read_server malloc for tmpdata error[%d]\n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(tmpdata, 0, reqsize);
     */
    memcpy(tmpdata, req2d->req_iov->ak_addr, req2d->req_iov->ak_len);
    printf("9\n");
    
SEND_ACK:
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    repmsgp = (amp_message_t *)malloc(size);
    if (repmsgp == NULL) {
        DCS_ERROR("__dcs_read_server malloc for repmsgp error[%d]\n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(repmsgp, 0, size);
    
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);
    /*
    if (req->req_msg != NULL) {
        free(req->req_msg);
        req->req_msg = NULL;
    }
     */
    
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->msg_type = is_rep;
    msgp->size = size;
    msgp->fromtype = DCS_SERVER;
    msgp->fromid = server_this_id;
    
    printf("query result: %d\n", query_result);
    printf("remote type: %d, remote id: %d\n", req->req_remote_type, req->req_remote_id);
    if (query_result == 1) {
        msgp->u.s2c_reply.offset = fileoffset;
        msgp->u.s2c_reply.size = strlen(tmpdata);
        msgp->ack = 1;
        req->req_iov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
        if (req->req_iov == NULL) {
            DCS_ERROR("__dcs_read_server malloc for reply iov error[%d]\n", errno);
            rc = errno;
            goto EXIT;
        }
        
        req->req_iov->ak_addr = tmpdata;
        req->req_iov->ak_len = strlen(tmpdata);
        req->req_iov->ak_flag = 0;
        req->req_iov->ak_offset = 0;
        req->req_niov = 1;
        req->req_type = AMP_REPLY | AMP_DATA;
    } else {
        msgp->u.s2c_reply.offset = 0;
        msgp->u.s2c_reply.size = 0;
        msgp->ack = 0;
        req->req_iov = NULL;
        req->req_niov = 0;
        req->req_type = AMP_REPLY | AMP_MSG;
    }
    
    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 1;
    printf("10\n");
    
    rc = amp_send_sync(server_comp_context,
                       req,
                       DCS_CLIENT,
                       client_id,
                       0);
    if (rc < 0) {
        DCS_ERROR("__dcs_read_server send reply to client error[%d]\n", rc);
        goto EXIT;
    }
    
EXIT:
    
    if (reqmsgp2d != NULL) {
        free(reqmsgp2d);
        reqmsgp2d = NULL;
    }
    
    if (repmsgp2d != NULL) {
        free(repmsgp2d);
        repmsgp2d = NULL;
    }
    
    if (req2d != NULL) {
        if (req2d->req_iov) {
            __server_freebuf(req2d->req_niov, req2d->req_iov);
            free(req2d->req_iov);
            req2d->req_iov = NULL;
        }
        __amp_free_request(req2d);
    }
    
    if(req->req_iov != NULL){
        __server_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
        req->req_iov = NULL;
        //req->req_niov = 0;
    }
    
    
    if (repmsgp) {
        free(repmsgp);
        repmsgp = NULL;
    }
    if(req != NULL){
        __amp_free_request(req);
    }
    
    DCS_LEAVE("__dcs_read_server leave \n");
    return rc;
    /*
    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        target[i] = 0;
    }

    //DCS_MSG("2\n");
    map_name = get_map_name(fileinode, client_id, timestamp);
    if(map_name == NULL){
        DCS_ERROR("__dcs_read_server get map file name err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }

    DCS_MSG("map file name is %s \n", map_name);
    //DCS_MSG("3\n");
    if((pos = check_map_buf(map_name)) != -1){
        DCS_MSG("__dcs_read_server map info is in buf \n");
        map_buf = get_map_buf(map_name, fileoffset, reqsize, pos);
    }
    else{
        DCS_MSG("__dcs_read_server map info is not in buf \n");
        map_buf = read_map_buf(map_name, fileoffset, reqsize);
    }
    
    if(map_buf == NULL){
        DCS_ERROR("__dcs_read_server get map_buf err:%d \n", errno);
        rc = ENOENT;
        goto SEND_ACK;
    }
    chunk_num = map_buf->chunk_num;
    DCS_MSG("after get buf chunk num is %d \n", chunk_num);
    
    //DCS_MSG("4\n");
    DCS_MSG("requsize is %ld , super_chunk_size is %d \n", reqsize, SUPER_CHUNK_SIZE);
    target_num = (reqsize/super_size);
    DCS_MSG("target_num is %d , size of readinfo t %ld \n", target_num, sizeof(dcs_readinfo_t));
    read_info = (dcs_readinfo_t *)malloc(sizeof(dcs_readinfo_t)*target_num);
    if(read_info == NULL){
        DCS_ERROR("__dcs_read_server malloc for read info err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    //*classify all the chunk into some compressor id
     //* here use a simple way
     //* if the compressor id is not continue
     //* we consider it to be a different one
    j=0;
    DCS_MSG("chunk num is %d \n", chunk_num);
    for(i=0; i<chunk_num; i++){
        if(i == 0){
            //DCS_MSG("6\n");
            begin = 0;
            end = 0;
        }
        if(map_buf->datamap[begin].compressor_id != map_buf->datamap[i].compressor_id){
            end = i-1;
            read_info[j].begin = begin;
            read_info[j].end = end;
            read_info[j].compressor_id = map_buf->datamap[begin].compressor_id;
            j++;
            begin = end = i;
        }
    }

    //DCS_MSG("8\n");
    DCS_MSG("the begin is %d end is %d i is %d j is %d \n", begin, end , i, j);
    if(map_buf->datamap[begin].compressor_id == map_buf->datamap[i-1].compressor_id){
        //DCS_MSG("09\n");
        read_info[j].begin = begin;
        read_info[j].end = i-1;
        DCS_MSG("the begin is %d end is %d i is %d j is %d \n", read_info[j].begin, read_info[j].end , i, j);
        read_info[j].compressor_id = map_buf->datamap[begin].compressor_id;
        j++;
    }

    target_num = j;
    DCS_MSG("the begin compressor is %d and the last compressor is %d \n", 
        map_buf->datamap[begin].compressor_id, map_buf->datamap[i-1].compressor_id);

    req2d = (amp_request_t **)malloc(sizeof(amp_request_t *)*target_num);
    if(req2d == NULL){
        DCS_ERROR("__dcs_read_server malloc for req2d array err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    //DCS_MSG("9\n");
    for(i=0; i<target_num; i++){
        req2d[i] = NULL;
    }

    reqmsgp2d = (amp_message_t **)malloc(sizeof(amp_message_t *)*target_num);
    if(reqmsgp2d == NULL){
        DCS_ERROR("__dcs_read_server malloc for reqmsgp2d err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
    for(i=0; i<target_num; i++){
        reqmsgp2d[i] = NULL;
    }

    //DCS_MSG("10\n");
    repmsgp2d = (amp_message_t **)malloc(sizeof(amp_message_t *)*target_num);
    if(repmsgp2d == NULL){
        DCS_ERROR("__dcs_read_server malloc for repmsgp2d err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    for(i=0; i<target_num; i++){
        repmsgp2d[i] = NULL;
    }

    //*malloc for server to client buf
    tmpdata_size = map_buf->datamap[chunk_num - 1].offset - map_buf->datamap[0].offset + map_buf->datamap[chunk_num - 1].chunksize;
    tmpdata = (dcs_s8_t *)malloc(tmpdata_size);
    if(tmpdata == NULL){
        DCS_ERROR("__dcs_read_server malloc for tmpdata err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(tmpdata, 0, tmpdata_size);

    //*buf_offset is offset in the databuf which get data from compressor
    buf_offset = 0;

    for(i=0 ; i<target_num; i++){
        DCS_MSG("target_num is %d and j is %d \n", target_num, j);
        __amp_alloc_request(&req2d[i]);
        if(req2d[i] == NULL){
            DCS_ERROR("__dcs_read_server malloc for %dth req2d err:%d \n", i, errno);
            rc = errno;
            goto EXIT;
        }
    
        tmp_num = read_info[i].end - read_info[i].begin + 1;
        DCS_MSG("end is %d begin is %d tmp_num is %d \n", read_info[i].end, read_info[i].begin, tmp_num);

        size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t) + sizeof(dcs_datamap_t)*tmp_num ;
        reqmsgp2d[i] = (amp_message_t *)malloc(size);
        if(reqmsgp2d[i] == NULL){
            DCS_ERROR("__dcs_read_server malloc for %dth reqmsgp2d err:%d \n",i ,errno);
            rc = errno;
            goto EXIT;
        }

        msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp2d[i] + AMP_MESSAGE_HEADER_LEN);
        msgp->size = size;
        msgp->optype = DCS_READ;
        msgp->msg_type = is_req;
        msgp->fromtype = DCS_SERVER;
        msgp->fromid = server_this_id;
        msgp->u.s2d_req.chunk_num = tmp_num;

        memcpy(msgp->buf, 
                map_buf->datamap + read_info[i].begin*sizeof(dcs_datamap_t), 
                sizeof(dcs_datamap_t)*tmp_num);

        req2d[i]->req_iov = NULL;
        req2d[i]->req_niov = 0;

        req2d[i]->req_msg = reqmsgp2d[i];
        req2d[i]->req_msglen = size;
        req2d[i]->req_need_ack = 1;
        req2d[i]->req_resent = 1;
        req2d[i]->req_type = AMP_REQUEST | AMP_MSG;

        DCS_MSG("destination is %dth compressor\n", read_info[i].compressor_id);
        rc = amp_send_sync(server_comp_context,
                           req2d[i],
                           DCS_NODE,
                           read_info[i].compressor_id,
                           0);

        if(rc < 0){
            DCS_ERROR("__dcs_read_server send read req err:%d \n", rc);
            //rc = errno;
            goto EXIT;
        }

        //*get data from the compressor
        repmsgp2d[i] = req2d[i]->req_reply;
        if(repmsgp2d[i] == NULL){
            DCS_ERROR("__dcs_read_server cannot recieve data from %dth compressor\n",i);
            rc = -1;
            goto EXIT;
        }

        if(req2d[i]->req_iov == NULL){
            DCS_ERROR("__dcs_read_server %dth req got no reply data \n", i);
            rc = -1;
            goto EXIT;
        }

        memcpy(tmpdata + buf_offset, req2d[i]->req_iov->ak_addr, req2d[i]->req_iov->ak_len);
        buf_offset = buf_offset + req2d[i]->req_iov->ak_len;

    }
    
    //*as the offset of request data is may not the begin from a chunk,
      //we have to cut some data from the beginning and the tail.
    //*this buf is use for retrun to the client
    //DCS_MSG("11 \n");
    buf = (dcs_s8_t *)malloc(reqsize);
    if(buf == NULL){
        DCS_ERROR("__dcs_read_server malloc for buf err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    DCS_MSG("the fileoffset is %ld and the first chunk offset is %ld \n",
            fileoffset, map_buf->datamap[0].offset);
    //memcpy(buf, tmpdata + (map_buf->datamap[0].offset - fileoffset), reqsize);
    memcpy(buf, tmpdata + (fileoffset - map_buf->datamap[0].offset), reqsize);

SEND_ACK:
    //DCS_MSG("12 \n");
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t) ;
    repmsgp = (amp_message_t *)malloc(size);
    if(repmsgp == NULL){
        DCS_ERROR("__dcs_read_server malloc for repmsgp err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
    memset(repmsgp, 0, size);
    
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);
    if(req->req_msg != NULL){
        free(req->req_msg);
        req->req_msg = NULL;
    }

    //DCS_MSG("13 \n");
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->msg_type = is_rep;
    msgp->size = size;
    msgp->fromtype = DCS_SERVER;
    msgp->fromid = server_this_id;
    

    if(rc == 0){
        msgp->u.s2c_reply.offset = fileoffset;
        msgp->u.s2c_reply.size = reqsize;
        msgp->ack = 1;

        req->req_iov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
        if(req->req_iov == NULL){
            DCS_ERROR("__dcs_read_server malloc for reply iov err:%d \n", errno);
            rc = errno;
            goto EXIT;
        }
    
        //DCS_MSG("14 \n");
        req->req_iov->ak_addr = buf;
        req->req_iov->ak_len = reqsize;
        req->req_iov->ak_flag = 0;
        req->req_iov->ak_offset = 0;
        req->req_niov = 1;
        req->req_type = AMP_REPLY | AMP_DATA;
    }else{
        msgp->u.s2c_reply.offset = 0;
        msgp->u.s2c_reply.size = 0;
        msgp->ack = 0;
        req->req_iov = NULL;
        req->req_niov = 0;
        req->req_type = AMP_REPLY | AMP_MSG;
    }
    
    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 1;

    //DCS_MSG("16 \n");
    rc = amp_send_sync(server_comp_context,
                       req,
                       req->req_remote_type,
                       req->req_remote_id,
                       0);
    if(rc < 0){
        DCS_ERROR("__dcs_read_server send read data to client err :%d \n", rc);
        goto EXIT;
    }
    //DCS_MSG("17 \n");

EXIT:
    if(map_buf != NULL){
        if(map_buf->datamap != NULL){
            free(map_buf->datamap);
            map_buf->datamap = NULL;
        }
        free(map_buf);
        map_buf = NULL;
    }

    if(map_name != NULL){
        free(map_name);
        map_name = NULL;
    }

    if(read_info){
        free(read_info);
        read_info = NULL;
    }

    if(tmpdata){
        free(tmpdata);
        tmpdata = NULL;
    }

    for(i=0; i<target_num; i++){
        DCS_MSG("i is %d , target num is %d \n", i, target_num);
        if(reqmsgp2d[i] != NULL){
            free(reqmsgp2d[i]);
            reqmsgp2d[i] = NULL;
        }
            
        //DCS_MSG("1\n");
        if(repmsgp2d[i] != NULL){
            free(repmsgp2d[i]);
            repmsgp2d[i] = NULL;
        }

        //DCS_MSG("2\n");
        if(req2d[i] != NULL){
            if(req2d[i]->req_iov){
                __server_freebuf(req2d[i]->req_niov, req2d[i]->req_iov);
        //DCS_MSG("4\n");
                free(req2d[i]->req_iov);
        //DCS_MSG("5\n");
                req2d[i]->req_iov = NULL;
            }

        //DCS_MSG("3\n");
            __amp_free_request(req2d[i]);
        }
    }

    //DCS_MSG("1\n");
    //
    //if(buf != NULL){
    //    free(buf);
      //  buf = NULL;
    //}
    
    
    //DCS_MSG("12\n");
    if(req->req_msg){
        //amp_free(req->req_msg, req->req_msglen);
    }

    //DCS_MSG("13\n");
    if(req->req_iov){
        __server_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
        req->req_iov = NULL;
    }

    //DCS_MSG("14\n");
    if(req != NULL){
        __amp_free_request(req);
    }


    DCS_LEAVE("__dcs_read_server leave \n");
    return rc;
     */
}

/*process finish request
 * as we had buffer the whole file mapping info 
 * so we have to chose a chance to free it 
 * finish reading a file is the chance
 */
dcs_s32_t __dcs_read_finish(dcs_u64_t inode,
                                dcs_u64_t timestamp, 
                                dcs_u32_t client_id,
                                amp_request_t *req)
{
    dcs_s32_t rc = 0;
    dcs_u32_t size = 0;
    //dcs_s8_t  *map_name = NULL;
    
    amp_message_t *repmsgp = NULL;
    dcs_msg_t   *msgp = NULL;

    DCS_ENTER("_dcs_read_finish enter \n");

    /*
    map_name = get_map_name(inode, client_id, timestamp);
    if(map_name == NULL){
        DCS_ERROR("__dcs_read_finish get map name err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
     */
    
    /*free map buf*/
    //rc = free_map_buf(map_name);
    if(rc != 0){
        DCS_ERROR("__dcs_read_finish free map buf err:%d \n", rc);
        goto EXIT;
    }

    DCS_MSG("after free map buf \n");
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    repmsgp = (amp_message_t *)malloc(size);
    if(repmsgp == NULL){
        DCS_ERROR("__dcs_read_finish malloc for repmsgp err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(repmsgp, 0, size);
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);

    free(req->req_msg);
    req->req_msg = NULL;
    //DCS_MSG("2 \n");

    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);

    msgp->msg_type = is_rep;
    msgp->fromtype = DCS_SERVER;
    msgp->fromid = server_this_id;
    msgp->ack = 1;

    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 0;
    req->req_type = AMP_REPLY | AMP_MSG;

    DCS_MSG("before send reply \n");
    rc = amp_send_sync(server_comp_context, 
                       req, 
                       req->req_remote_type, 
                       req->req_remote_id, 
                       0);
    if(rc < 0){
        DCS_ERROR("__dcs_read_finish send reply msg to server err:%d \n", rc);
        goto EXIT;
    } 

EXIT:
    //DCS_MSG("3\n");
    /*
    if(map_name != NULL){
        free(map_name);
        map_name = NULL;
    }
    */

    //DCS_MSG("4\n");
    if(repmsgp != NULL){
        amp_free(repmsgp, size);
        repmsgp = NULL;
    }

    //DCS_MSG("5\n");
    DCS_LEAVE("__dcs_read_finish leave \n");
    
    return rc;
}

dcs_s32_t server_collect_diskinfo() 
{
    dcs_s32_t rc = 0;
    dcs_u32_t size = 0;
    dcs_s32_t i = 0;
    dcs_u64_t tmp[DCS_COMPRESSOR_NUM];

    amp_request_t *req[DCS_COMPRESSOR_NUM];
    amp_message_t *reqmsgp[DCS_COMPRESSOR_NUM];
    amp_message_t *repmsgp[DCS_COMPRESSOR_NUM];
    dcs_msg_t   *msgp = NULL;
    
    DCS_ENTER("server_collect_diskinfo enter \n");

    for(i=0; i < DCS_COMPRESSOR_NUM; i++){
        req[i] = NULL;
        reqmsgp[i] = NULL;
        repmsgp[i] = NULL;
        tmp[i] = 0;
    }

    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        rc = __amp_alloc_request(&req[i]);
        if(rc < 0){
            DCS_ERROR("server_collect_diskinfo malloc for req err: %d \n", rc);
            rc = errno;
            goto EXIT;
        }

        size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
        reqmsgp[i] = (amp_message_t *)malloc(size);
        if(!reqmsgp[i]){
            DCS_ERROR("clt_send_data alloc for reqmsgp err: %d \n", errno);
            rc = errno;
            goto EXIT;
        }

        memset(reqmsgp[i], 0, size);
        msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp[i] + AMP_MESSAGE_HEADER_LEN);
        msgp->size = size;
        msgp->msg_type = is_req;
        msgp->fromid = server_this_id;
        msgp->fromtype = DCS_SERVER;
        msgp->optype = DCS_QUERY;

        req[i]->req_iov = NULL;
        req[i]->req_niov = 0;

        req[i]->req_msg = reqmsgp[i];
        req[i]->req_msglen = size;
        req[i]->req_need_ack = 1;
        req[i]->req_resent = 1;
        req[i]->req_type = AMP_REQUEST | AMP_MSG;

        DCS_MSG("before send query req to compressor \n ");
       
        rc = amp_send_sync(server_comp_context, 
                        req[i], 
                        DCS_NODE, 
                        (i+1), 
                        0);
        DCS_MSG("after send %dth compressor msg\n",i+1);
        if(rc < 0){
            DCS_ERROR("__dcs_server_collect send req to master err:%d \n",rc);
            goto EXIT;
        }

        DCS_MSG("after send query req to compressor \n ");

        repmsgp[i] = req[i]->req_reply;
        if(!repmsgp[i]){
            DCS_ERROR("server_collect_diskinfo cannot recieve reply from the compressor \n");
            rc = -1;
            goto EXIT;
        }

        msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp[i] + AMP_MESSAGE_HEADER_LEN);
        DCS_MSG("the disk info is %ld \n", msgp->u.d2s_reply.diskinfo);
        /*get disk info from compressor*/
        tmp[i] = msgp->u.d2s_reply.diskinfo; 
    }

    /*update the disk usage info*/
    pthread_mutex_lock(&diskinfo_lock);
    disk_avg = 0;
    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        diskinfo[i] = tmp[i];
        DCS_MSG("the %dth compressor disk usage is %ld \n ", i+1, diskinfo[i]);
        disk_avg = disk_avg + tmp[i];
    }
    disk_avg = disk_avg / DCS_COMPRESSOR_NUM;
    DCS_MSG("the avg disk usage info is %ld \n", disk_avg);
    pthread_mutex_unlock(&diskinfo_lock);

EXIT:

    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        if(reqmsgp[i] != NULL){
            amp_free(reqmsgp[i],size);
            reqmsgp[i] = NULL;
        }

        if(repmsgp[i] != NULL){
            amp_free(repmsgp[i], req[i]->req_replylen);
            repmsgp[i] = NULL;
        }

        if(req[i] != NULL){
            if(req[i]->req_iov != NULL){
                __server_freebuf(req[i]->req_niov, req[i]->req_iov);
                free(req[i]->req_iov);
                req[i]->req_iov = NULL;
                req[i]->req_niov = 0;
            }

            __amp_free_request(req[i]);
            req[i] = NULL;
        }
    }

    DCS_LEAVE("server_collect_diskinfo leave \n");
    return rc;
}

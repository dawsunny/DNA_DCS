/*master operation*/


#include "amp.h"
#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"

#include "master_cnd.h"
#include "master_cache.h"
#include "master_op.h"
#include "master_thread.h"
#include "master_cache.h"
#include "bloom.h"
#include "hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/ioctl.h>
#include <errno.h>
//#include <stropts.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <string.h>
#include <semaphore.h>

/*analyse the msg to determine next operation 
 * if the optype is query call __dcs_master_query
 * if the optype is update call __dcs_master_update
 */
dcs_s32_t __dcs_master_process_req(amp_request_t *req)
{
    dcs_s32_t rc;
    dcs_msg_t *msgp = NULL;
    dcs_u32_t op_type;

    DCS_ENTER("__dcs_master_process_req enter \n");
    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    op_type = msgp->optype;
    if(op_type == DCS_QUERY){
        rc = __dcs_master_query(req);
    }else if(op_type == DCS_UPDATE){
        rc = __dcs_master_update(req);//bloom update
    }else if(op_type == DCS_DELETE){
        rc = __dcs_master_bloom_reset(req);
    }else if(op_type == DCS_UPLOAD){
        rc = __dcs_tier_bloom_upload(req);
    }else{
        DCS_ERROR("__dcs_master_process_req recieve wrong optype from server \n");
        rc = -1;
        goto EXIT;
    }

EXIT:
    DCS_LEAVE("__dcs_master_process_req leave \n");
    return rc;
}

/*master query service
 * query bloom filter use sha value
 * cache the sha value
 * reply the result
 */
dcs_s32_t __dcs_master_query(amp_request_t *req)
{
    dcs_s32_t i,j;
    dcs_s32_t rc;
    dcs_u32_t sha_num;
    dcs_u32_t size;
    dcs_u8_t  *sha = NULL;
    //dcs_u32_t  *buf = NULL;
    dcs_u32_t *tmp_mark = NULL;
    //dcs_u32_t tmp_mark = 0;
    dcs_s32_t cache_id;
    
    dcs_u32_t mark[DCS_COMPRESSOR_NUM];
    amp_message_t *repmsgp = NULL;
    dcs_msg_t *msgp = NULL; 

    DCS_ENTER("__dcs_master_query enter\n");

    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        mark[i] = 0;
    }

    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    sha_num = msgp->u.s2m_req.sha_num;

    sha = (dcs_u8_t *)malloc(SHA_LEN*sha_num);
    if(sha == NULL){
        DCS_ERROR("__dcs_master_query malloc for sha err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }

    if(req->req_iov->ak_len != SHA_LEN*sha_num){
        DCS_ERROR("__dcs_master_query err in send buf len or sha_num \n");
        rc = -1;
        goto EXIT;
    }
    memcpy(sha, req->req_iov->ak_addr, req->req_iov->ak_len);
    
    for(i=0; i<sha_num; i++){
        for(j=0; j<DCS_COMPRESSOR_NUM; j++){
            if(bloom_query(sha, i, j)){
                mark[j]++;
            }
        }
    }

    //mark[0] = 10;
    /*
    buf = (dcs_u32_t *)malloc(sizeof(dcs_u32_t)*DCS_COMPRESSOR_NUM);
    if(buf == NULL){
        DCS_ERROR("__dcs_master_query malloc for buf err:%d, \n",errno);
        rc = errno;
        goto EXIT;
    }
    memcpy(buf, mark, sizeof(dcs_u32_t)*DCS_COMPRESSOR_NUM);
*/
    /*cache the sha value for later process*/
    cache_id = __dcs_master_cache_insert(sha, sha_num);
    if(cache_id < 0){
        DCS_ERROR("__dcs_master_query cache sha err:%d \n", cache_id);
        rc = cache_id;
        goto EXIT;
    }

    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    DCS_MSG("original msg size is %d \n", size);
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t) + sizeof(dcs_u32_t)*DCS_COMPRESSOR_NUM;
    DCS_MSG("msg size is %d \n", size);
    repmsgp = (amp_message_t *)malloc(size);
    if(repmsgp == NULL){
        DCS_ERROR("__dcs_master_query malloc for repmsgp err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);
    
    /*init dcs msg and add buf to the tail*/
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->ack = 1;
    msgp->msg_type = is_rep;
    msgp->u.m2s_reply.mark_num = DCS_COMPRESSOR_NUM;
    msgp->u.m2s_reply.cache_id = cache_id;

    //msgp = NULL;
    //msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    //DCS_MSG("ack is %d \n", msgp->ack);

    //memcpy(repmsgp + AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t), mark, sizeof(dcs_u32_t)*DCS_COMPRESSOR_NUM);
    memcpy(msgp->buf, mark, sizeof(dcs_u32_t)*DCS_COMPRESSOR_NUM);

    //tmp_mark = (dcs_u32_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t));

    DCS_MSG("__dcs_master_query mark 0 is %u \n", mark[0]);

    //tmp_mark = (dcs_u32_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t));
    tmp_mark = (dcs_u32_t *)(msgp->buf);
    //DCS_MSG("offset :%lu \n", (dcs_u64_t)((void *)tmp_mark - (void *)repmsgp));
    DCS_MSG("__dcs_master_query tmp_mark 0 is %u \n", tmp_mark[0]);
    DCS_MSG("__dcs_master_query mark is %d cache id is %d\n", mark[0], cache_id);


    DCS_MSG("__dcs_master_query before free the req_iov \n");
    if(req->req_iov){
        __master_freebuf(req->req_niov, req->req_iov);
          free(req->req_iov);
    }

    if(req->req_msg){
        amp_free(req->req_msg, size);
    }

    /*
    kiov.ak_addr = buf;
    kiov.ak_len = strlen(buf);
    kiov.ak_len = strlen(buf);
    kiov.ak_flag = 0;
    kiov.ak_offset= 0;
    req->req_iov = &kiov;
    */

    /*
    req->req_iov->ak_addr = buf;
    req->req_iov->ak_len = strlen(buf);

    req->req_iov->ak_flag = 0;
    req->req_iov->ak_offset = 0;
    req->req_niov = 1;
    */

    req->req_iov = NULL;
    req->req_niov = 0;

    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 0;
    req->req_type = AMP_REPLY|AMP_MSG;

    rc = amp_send_sync(master_comp_context,
                       req,
                       req->req_remote_type,
                       req->req_remote_id,
                       0);
    if(rc < 0){
        DCS_ERROR("__dcs_master_query send reply msg err:%d \n", rc);
        goto EXIT;
    }

    DCS_MSG("send reply msg succseed \n");
EXIT:

    /*
    if(req->req_msg){
        amp_free(req->req_msg, req->req_msglen);
    }

    if(req->req_iov){
        __master_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
    }

    if(req != NULL){
        __amp_free_request(req);
    }
    */

    if(repmsgp){
        free(repmsgp);
        repmsgp = NULL;
    }

    /*
    if(msgp){
        free(msgp);
        msgp = NULL;
    }
    */

    /*
    if(buf){
        free(buf);
        buf = NULL;
    }
    */

    DCS_LEAVE("__dcs_master_query leave \n ");
    return rc;
}

dcs_s32_t __dcs_master_update(amp_request_t *req)
{
    dcs_s32_t rc = 0;
    dcs_s32_t cache_id;
    dcs_s32_t bf_id;
    dcs_u32_t size;

    dcs_msg_t *msgp = NULL;
    amp_message_t *reqmsgp = NULL;
    amp_message_t *repmsgp = NULL;

    DCS_ENTER("__dcs_master_update enter \n");

    reqmsgp = (amp_message_t *)req->req_msg;
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp + AMP_MESSAGE_HEADER_LEN);
    cache_id = msgp->u.s2m_req.cache_id;
    bf_id = msgp->u.s2m_req.bf_id;

    rc = bloom_update(cache_id, bf_id);
    if(rc < 0){
        DCS_ERROR("bloom update err:%d \n", rc);
        goto EXIT;
    }
    
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    repmsgp = (amp_message_t *)malloc(size);
    if(!repmsgp){
        DCS_ERROR("__dcs_master_update err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(repmsgp, 0, size);
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);

    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->size = size;
    msgp->msg_type = is_rep;
    msgp->fromtype = DCS_MASTER;
    msgp->fromid = master_this_id;
    msgp->ack = 1;

    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 0;
    req->req_type = AMP_REPLY | AMP_MSG;

    rc = amp_send_sync(master_comp_context, 
                       req, 
                       req->req_remote_type, 
                       req->req_remote_id, 
                       0);
    if(rc < 0){
        DCS_ERROR("__dcs_master_update send reply msg to server err:%d \n", rc);
        goto EXIT;
    } 

EXIT:
    if(repmsgp){
        free(repmsgp);
        repmsgp = NULL;
    }

    /*
    if(msgp){
        free(msgp);
        msgp = NULL;
    }
    */


    DCS_LEAVE("__dcs_master_update leave \n");
    return rc;
}

dcs_s32_t __dcs_tier_bloom_upload(amp_request_t *req)
{
    dcs_s32_t rc = 0;
    dcs_s32_t flag = 0;
    dcs_u32_t i;
    dcs_u32_t j;
    dcs_u32_t len = 0;
    dcs_u32_t scsize;
    dcs_s32_t bf_id;
    dcs_u32_t size;
    dcs_u32_t sha_num;
    dcs_u64_t key[HASH_FUNC_NUM];
    dcs_msg_t *msgp = NULL;
    amp_message_t *reqmsgp = NULL;
    amp_message_t *repmsgp = NULL;
    sha_bf_t *sha_bf = NULL;
    sha_bf_t tmp_sha_bf;
    amp_kiov_t * kiovp = NULL;
    dcs_u64_t blocks_num = 0;
    dcs_u64_t block_num = 0;
    tier_bloom_block * tbb = NULL;

    DCS_ENTER("__dcs_master_bloom_reset enter \n");

    reqmsgp = (amp_message_t *)req->req_msg;
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp + AMP_MESSAGE_HEADER_LEN);
    sha_num = msgp->u.s2m_upload_req.sha_num;
    scsize = msgp->u.s2m_upload_req.scsize;

    sha_bf = (sha_bf_t *)malloc(sha_num * sizeof(sha_bf_t));
    if (NULL == sha_bf){
        DCS_ERROR("__dcs_master_bloom_reset malloc for dcs_datamap_t array failed\n");
        rc = -1;
        goto SEND_ACK;
    }
    
    if(NULL != req->req_iov && 0 != req->req_niov){
        kiovp = req->req_iov;
        for(i = 0; i < req->req_niov;i++)
        {
            memcpy((dcs_s8_t *)sha_bf + len, kiovp->ak_addr, kiovp->ak_len);
            len += kiovp->ak_len;
        }     
    }
    
    if(len !=  scsize){
        DCS_ERROR("__dcs_master_bloom_reset donot receive enough data, should receive:%d, real receive:%d, niov:%d\n", scsize, len, req->req_niov);
        rc = -1;
        goto SEND_ACK;
    }

    for( i = 0; i < sha_num; i++){
       flag = 0;
       memcpy(&tmp_sha_bf, (dcs_s8_t *)sha_bf + sizeof(sha_bf_t) * i, sizeof(sha_bf_t));
       bf_id = tmp_sha_bf.bf_id;
       for(j = 0; j < HASH_FUNC_NUM; j++ ){
           key[j] = hashfunc(tmp_sha_bf.sha, j);
       }
       
       for(j = 0; j < HASH_FUNC_NUM; j++ ){
           dcs_s32_t k = 0;
           dcs_u8_t tmp = 0;
           dcs_u64_t key_tmp = 0; 
           key_tmp = key[j] % MASTER_BLOOM_SIZE;
           blocks_num = (key_tmp + BLOOM_BLOCK_SIZE - 1)/BLOOM_BLOCK_SIZE;
           block_num = key_tmp % BLOOM_BLOCK_SIZE;

           pthread_mutex_lock(&tbf[bf_id]->lock);
           if(!GETBIT(tbf[bf_id]->bmap, blocks_num)){
               SETBIT(tbf[bf_id]->bmap, blocks_num);
               if(NULL == tbf[bf_id]->blocks[blocks_num]){
                   bloom_block_init1(tbf[bf_id], blocks_num, 0);
               }
           }
           tbb = tbf[bf_id]->blocks[blocks_num];

           pthread_mutex_lock(&tbb->lock);
           SETBIT(tbb->bmap, block_num);
                
           for(k = 0; k < tbb->real_level_num; k++){
               tmp = tbb->a[k][block_num];
               tmp++;
               tbb->a[k][block_num] = tmp;
               flag = 0;
               if(tmp){
                   break;
               }
               flag = 1;
           }
           if(flag){
               if(tbb->real_level_num < tbb->level_num){
                   tbb->a[k] = (dcs_u8_t *)malloc(sizeof(dcs_u8_t) * BLOOM_BLOCK_SIZE);
                   memset(tbb->a[k], 0, sizeof(dcs_u8_t) * BLOOM_BLOCK_SIZE);
                   tbb->a[k][block_num]++;
                   tbb->real_level_num++;
               }else{
                   bloom_block_expand(tbf[bf_id], blocks_num);
                   tbb->a[k][block_num]++;
               }
           }    

           pthread_mutex_unlock(&tbb->lock);
           pthread_mutex_unlock(&tbf[bf_id]->lock);
       }
    }

SEND_ACK:
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    repmsgp = (amp_message_t *)malloc(size);
    if(!repmsgp){
        DCS_ERROR("__dcs_master_bloom_reset err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(repmsgp, 0, size);
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);

    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->size = size;
    msgp->msg_type = is_rep;
    msgp->fromtype = DCS_MASTER;
    msgp->fromid = master_this_id;
    msgp->ack = ((rc == 0) ? 1 : 0);

    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 0;
    req->req_type = AMP_REPLY | AMP_MSG;

    rc = amp_send_sync(master_comp_context, 
                       req, 
                       req->req_remote_type, 
                       req->req_remote_id, 
                       0);
    if(rc < 0){
        DCS_ERROR("__dcs_master_bloom_reset send reply msg to server err:%d \n", rc);
        goto EXIT;
    } 

EXIT:
    if(repmsgp){
        free(repmsgp);
        repmsgp = NULL;
    }

    DCS_LEAVE("__dcs_tier_bloom_upload leave \n");
    return rc;
}

dcs_s32_t __dcs_master_bloom_reset(amp_request_t *req)
{
    dcs_s32_t rc = 0;
    dcs_s32_t flag = 0;
    dcs_u32_t i;
    dcs_u32_t j;
    dcs_u32_t len = 0;
    dcs_u32_t scsize;
    dcs_s32_t bf_id;
    dcs_u32_t size;
    dcs_u32_t sha_num;
    dcs_u32_t sha_bf_num[DCS_COMPRESSOR_NUM];
    dcs_datamap_t  *sha_bf_2d[DCS_COMPRESSOR_NUM] = {NULL};
    dcs_u64_t key[HASH_FUNC_NUM];
    dcs_msg_t *msgp = NULL;
    amp_message_t *reqmsgp = NULL;
    amp_message_t *repmsgp = NULL;
    dcs_datamap_t *sha_bf = NULL;
    dcs_datamap_t tmp_sha_bf;
    amp_kiov_t * kiovp = NULL;
    dcs_u64_t blocks_num = 0;
    dcs_u64_t block_num = 0;
    tier_bloom_block * tbb = NULL;
    amp_request_t *req2d[DCS_COMPRESSOR_NUM];
    amp_message_t *reqmsgp2d[DCS_COMPRESSOR_NUM];

    DCS_ENTER("__dcs_master_bloom_reset enter \n");

    reqmsgp = (amp_message_t *)req->req_msg;
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp + AMP_MESSAGE_HEADER_LEN);
    sha_num = msgp->u.s2m_del_req.sha_num;
    scsize = msgp->u.s2m_del_req.scsize;

    sha_bf = (dcs_datamap_t *)malloc(sha_num * sizeof(dcs_datamap_t));
    if (NULL == sha_bf){
        DCS_ERROR("__dcs_master_bloom_reset malloc for dcs_datamap_t array failed\n");
        rc = -1;
        goto SEND_ACK;
    }
    
    if(NULL != req->req_iov && 0 != req->req_niov){
        kiovp = req->req_iov;
        for(i = 0; i < req->req_niov;i++)
        {
            memcpy((dcs_s8_t *)sha_bf + len, kiovp->ak_addr, kiovp->ak_len);
            len += kiovp->ak_len;
        }     
    }
    
    if(len !=  scsize){
        DCS_ERROR("__dcs_master_bloom_reset donot receive enough data, should receive:%d, real receive:%d, niov:%d\n", scsize, len, req->req_niov);
        rc = -1;
        goto SEND_ACK;
    }

    for(i = 0; i < DCS_COMPRESSOR_NUM; i++){
        sha_bf_2d[i] = NULL;
        sha_bf_2d[i] = (dcs_datamap_t *)malloc(sha_num * sizeof(dcs_datamap_t));
        if(NULL == sha_bf_2d[i]){
            DCS_ERROR("__dcs_master_bloom_reset malloc for sha_bf[%d] failed\n",i);
            continue;
        }
        memset(sha_bf_2d[i], 0, sha_num * sizeof(dcs_datamap_t));
        sha_bf_num[i] = 0;
        req2d[i] = NULL;
        reqmsgp2d[i] = NULL;
    }

    for( i = 0; i < sha_num; i++){
       flag = 0;
       memcpy(&tmp_sha_bf, (dcs_s8_t *)sha_bf + sizeof(dcs_datamap_t) * i, sizeof(dcs_datamap_t));
       bf_id = tmp_sha_bf.compressor_id - 1;
       for(j = 0; j < HASH_FUNC_NUM; j++ ){
           key[j] = hashfunc(tmp_sha_bf.sha, j);
       }
       
       for(j = 0; j < HASH_FUNC_NUM; j++ ){
           dcs_s32_t k = 0;
           dcs_u8_t tmp = 0;
           dcs_u64_t key_tmp = 0; 
           key_tmp = key[j] % MASTER_BLOOM_SIZE;
           blocks_num = (key_tmp + BLOOM_BLOCK_SIZE - 1)/BLOOM_BLOCK_SIZE;
           block_num = key_tmp % BLOOM_BLOCK_SIZE;
           pthread_mutex_lock(&tbf[bf_id]->lock);
           if(!GETBIT(tbf[bf_id]->bmap, blocks_num)){
               if(tbf[bf_id]->blocks[blocks_num]){
	           DCS_ERROR("__dcs_master_bloom_reset blocks_num %ld bitmap is reset already, but response blocks is not null\n",blocks_num);
               }
               pthread_mutex_unlock(&tbf[bf_id]->lock);
               continue;
           }
           tbb = tbf[bf_id]->blocks[blocks_num];
           pthread_mutex_lock(&tbb->lock);

           for(k = 0; k < tbb->real_level_num; k++){
                tmp = tbb->a[k][block_num];
                tbb->a[k][block_num] = tmp - 1;
                if(tmp){
                    break;
                }else{
                    dcs_s32_t l = 0;
                    for(l = k + 1; l < tbb->real_level_num; l++){
                        if(tbb->a[l][block_num] != 0){
                            break;
                        }
                    }
                    if(l == tbb->real_level_num){
                        for(l = 0; l < tbb->real_level_num; l ++){
                            tbb->a[l][block_num] = 0;
                        }
                        break;
                    }
                }
           }
           
           for(k = 0; k < tbb->real_level_num; k++){
               if(tbb->a[k][block_num]){
                   break;
               }
           }
           if(k == tbb->real_level_num){
               RESETBIT(tbb->bmap, block_num);
               flag ++;
           }
//DCS_MSG("SHA: key:%ld, hash_key:%ld, blocks_num:%ld, block_num:%ld, flag:%d, con_id:%ld\n",key[j], key_tmp, blocks_num, block_num,flag, tmp_sha_bf.container_id);
           pthread_mutex_unlock(&tbb->lock);
           tmp = 0;
           for(k = 0; k < (BLOOM_BLOCK_SIZE + CHAR_BIT - 1)/CHAR_BIT; k++){
              if(tbb->bmap[k]){
                  tmp = 1;
              }
           } 

           if(!tmp){
               for(k = 0; k < tbb->real_level_num; k++){
                   if(tbb->a[k]){
                       free(tbb->a[k]);
                   }
               }
               free(tbb->a);
               free(tbb->bmap);
               pthread_mutex_destroy(&tbb->lock);
               free(tbf[bf_id]->blocks[blocks_num]);
               tbf[bf_id]->blocks[blocks_num] = NULL;
               RESETBIT(tbf[bf_id]->bmap, blocks_num);
           }
           pthread_mutex_unlock(&tbf[bf_id]->lock);
       }

       if(/*flag && */NULL != sha_bf_2d[bf_id]){
               memcpy((dcs_datamap_t *)sha_bf_2d[bf_id] + sha_bf_num[bf_id], &tmp_sha_bf, sizeof(dcs_datamap_t));
               sha_bf_num[bf_id] ++;
       } 
    }

    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    for(i = 0; i < DCS_COMPRESSOR_NUM; i++){
        if(sha_bf_num[i] && sha_bf_2d[i]){
            __amp_alloc_request(&req2d[i]);
            if(NULL == req2d[i]){
                DCS_ERROR("__dcs_master_bloom_reset call __amp_alloc_request for req2d[%d] failed\n",i);
                rc = -1;
                goto EXIT;
            }

            reqmsgp2d[i] = (amp_message_t *)malloc(size);
            if(NULL ==  reqmsgp2d[i]){
                DCS_ERROR("__dcs_master_bloom_reset malloc for reqmsgp2d[%d] failed\n", i);
                rc = -1;
                goto EXIT;
            }
            memset(reqmsgp2d[i], 0, size);
            msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp2d[i] + AMP_MESSAGE_HEADER_LEN);
            msgp->size = size;
            msgp->msg_type = is_req;
            msgp->optype = DCS_DELETE;
            msgp->fromtype = DCS_MASTER;
            msgp->fromid = master_this_id;
            msgp->u.m2d_del_req.sha_num = sha_bf_num[i];
            msgp->u.m2d_del_req.scsize = sha_bf_num[i] * SHA_LEN;

            req2d[i]->req_need_ack = 0;
            req2d[i]->req_resent = 1;
            req2d[i]->req_type = AMP_REQUEST|AMP_DATA;
            req2d[i]->req_msg = reqmsgp2d[i];
            req2d[i]->req_msglen = size; 
            req2d[i]->req_niov = 1;
            req2d[i]->req_iov = (amp_kiov_t*)malloc(sizeof(amp_kiov_t));
            if(NULL == req2d[i]->req_iov){
                DCS_ERROR("__dcs_master_bloom_reset malloc for req2d[%d]->req_iov failed\n", i);
                rc = -1;
                goto EXIT;
            }
            req2d[i]->req_iov->ak_addr = (dcs_s8_t *)malloc(sha_bf_num[i] * sizeof(dcs_datamap_t));
            if(NULL == req2d[i]->req_iov->ak_addr){
                DCS_ERROR("__dcs_master_bloom_reset malloc for req2d[%d]->req_iov->ak_addr failed\n", i);
                rc = -1;
                goto EXIT;
            }
            req2d[i]->req_iov->ak_len = sha_bf_num[i] * sizeof(dcs_datamap_t);
            memset(req2d[i]->req_iov->ak_addr, 0, sha_bf_num[i] * sizeof(dcs_datamap_t));
            memcpy(req2d[i]->req_iov->ak_addr, sha_bf_2d[i], sha_bf_num[i] * sizeof(dcs_datamap_t));
            req2d[i]->req_iov->ak_offset = 0;
            req2d[i]->req_iov->ak_flag = 0;
            amp_send_async(master_comp_context ,req2d[i], DCS_NODE, i+1, 1);
            DCS_MSG("__dcs_master_bloom_reset send_async to compressor %d to clean sha and data, sha_num: %d\n", i, sha_bf_num[i]);
        }
    }

SEND_ACK:
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    repmsgp = (amp_message_t *)malloc(size);
    if(!repmsgp){
        DCS_ERROR("__dcs_master_bloom_reset err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(repmsgp, 0, size);
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);

    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->size = size;
    msgp->msg_type = is_rep;
    msgp->fromtype = DCS_MASTER;
    msgp->fromid = master_this_id;
    msgp->ack = ((rc == 0) ? 1 : 0);

    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 0;
    req->req_type = AMP_REPLY | AMP_MSG;

    rc = amp_send_sync(master_comp_context, 
                       req, 
                       req->req_remote_type, 
                       req->req_remote_id, 
                       0);
    if(rc < 0){
        DCS_ERROR("__dcs_master_bloom_reset send reply msg to server err:%d \n", rc);
        goto EXIT;
    } 

EXIT:
    if(repmsgp){
        free(repmsgp);
        repmsgp = NULL;
    }
   
    for(i = 0; i < DCS_COMPRESSOR_NUM; i++){
        if(sha_bf_2d[i]){
            free(sha_bf_2d[i]);
            sha_bf_2d[i] = NULL;
        }
    }

    /*
    if(msgp){
        free(msgp);
        msgp = NULL;
    }
    */


    DCS_LEAVE("__dcs_master_bloom_reset leave \n");
    return rc;
}

/*bloom filter update*/
dcs_s32_t bloom_update(dcs_s32_t cache_id, dcs_s32_t bf_id)
{
    dcs_s32_t i,j;
    dcs_u64_t blocks_num, block_num;
    dcs_s32_t rc = 0;
    dcs_u32_t sha_num;
    dcs_u8_t  *sha = NULL;
    dcs_u8_t  *tmpsha = NULL;
    dcs_u64_t key;
    dcs_u8_t tmp = 0;
    dcs_u8_t flag = 0;

    DCS_ENTER("bloom_update enter \n");
    
    tmpsha = (dcs_u8_t *)malloc(SHA_LEN);
    if(tmpsha == NULL){
        DCS_ERROR("bloom_update err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }

    pthread_mutex_lock(&sha_cache[cache_id].cache_lock);
    sha = sha_cache[cache_id].sha;
    if(sha == NULL){
        DCS_ERROR("bloom_update get sha err:-1 \n");
        rc = -1;
        pthread_mutex_unlock(&sha_cache[cache_id].cache_lock);
        goto EXIT;
    }
    //sha_num = strlen((dcs_s8_t *)sha)/SHA_LEN;
    sha_num = sha_cache[cache_id].sha_num;
    
    //pthread_mutex_lock(&bloom_lock[bf_id]);
    for(i=0; i<sha_num; i++){
        memset(tmpsha, 0, SHA_LEN);
        memcpy(tmpsha, sha + (SHA_LEN*i), SHA_LEN);
        for(j=0; j<HASH_FUNC_NUM; j++){
            tier_bloom_block * tbb = NULL;
            dcs_s32_t k;
            key = hashfunc(tmpsha, j) % MASTER_BLOOM_SIZE;   
            //SETBIT(bf[bf_id]->a, key % bf[bf_id]->asize);
            blocks_num = (key + BLOOM_BLOCK_SIZE - 1) / BLOOM_BLOCK_SIZE;
            block_num = key % BLOOM_BLOCK_SIZE;

            pthread_mutex_lock(&tbf[bf_id]->lock);

            SETBIT(tbf[bf_id]->bmap, blocks_num);
            if(tbf[bf_id]->blocks[blocks_num] == NULL){
                bloom_block_init1(tbf[bf_id], blocks_num, 0);
            }

            tbb = tbf[bf_id]->blocks[blocks_num];            

            pthread_mutex_lock(&tbb->lock);
            
            SETBIT(tbb->bmap, block_num);
                
            for(k = 0; k < tbb->real_level_num; k++){
                    tmp = tbb->a[k][block_num];
                    tmp++;
                    tbb->a[k][block_num] = tmp;
                    flag = 0;
                    if(tmp){
                        break;
                    }
                    flag = 1;
            }
            if(flag){
                if(tbb->real_level_num < tbb->level_num){
                    tbb->a[k] = (dcs_u8_t *)malloc(sizeof(dcs_u8_t) * BLOOM_BLOCK_SIZE);
                    memset(tbb->a[k], 0, sizeof(dcs_u8_t) * BLOOM_BLOCK_SIZE);
                    tbb->a[k][block_num]++;
                    tbb->real_level_num++;
                }else{
                    bloom_block_expand(tbf[bf_id], blocks_num);
                    tbb->a[k][block_num]++;
            	}
            }    

            pthread_mutex_unlock(&tbb->lock);
            pthread_mutex_unlock(&tbf[bf_id]->lock);
        }
    }
    //pthread_mutex_unlock(&bloom_lock[bf_id]);

    free(sha_cache[cache_id].sha);
    sha_cache[cache_id].sha = NULL;
    sha_cache[cache_id].sha_num = 0;
    pthread_mutex_unlock(&sha_cache[cache_id].cache_lock);

EXIT:
    if(tmpsha){
        free(tmpsha);
        tmpsha = NULL;
    }

    DCS_LEAVE("bloom_update leave \n");
    return rc;
}

/*query bloom filter*/
dcs_s32_t bloom_query(dcs_u8_t *sha, dcs_s32_t pos, dcs_s32_t bf_id)
{
    dcs_s32_t i;
    dcs_s32_t rc = 1;
    dcs_u64_t key;
    dcs_u8_t  *tmpsha = NULL;
    dcs_u64_t blocks_num;
    dcs_u64_t block_num;
    DCS_ENTER("bloom_query enter \n");
    
    tmpsha = (dcs_u8_t *)malloc(SHA_LEN);
    if(!tmpsha){
        DCS_ERROR("bloom_query malloc for tmpsha err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memcpy(tmpsha, sha + pos*SHA_LEN, SHA_LEN);
    
    for(i=0; i<HASH_FUNC_NUM; i++){
        key = hashfunc(tmpsha,i);
        key = key % MASTER_BLOOM_SIZE;
	blocks_num = (key + BLOOM_BLOCK_SIZE -1) / BLOOM_BLOCK_SIZE;
        block_num = key % BLOOM_BLOCK_SIZE;

	
	pthread_mutex_lock(&tbf[bf_id]->lock);
	if(!GETBIT(tbf[bf_id]->bmap, blocks_num)){
	    pthread_mutex_unlock(&tbf[bf_id]->lock);
            rc = 0;
            break;
	}
	
        pthread_mutex_lock(&tbf[bf_id]->blocks[blocks_num]->lock);
	if(!GETBIT(tbf[bf_id]->blocks[blocks_num]->bmap, block_num)){
            pthread_mutex_unlock(&tbf[bf_id]->blocks[blocks_num]->lock);
	    pthread_mutex_unlock(&tbf[bf_id]->lock);
            rc = 0;
            break;
	}
        pthread_mutex_unlock(&tbf[bf_id]->blocks[blocks_num]->lock);
	pthread_mutex_unlock(&tbf[bf_id]->lock);
    }

    /*pthread_mutex_lock(&bloom_lock[bf_id]);
    rc = 1;
    for(i=0; i<HASH_FUNC_NUM; i++){
       // if(bloom_check(bf[bf_id], 1, key[i]) == 0){
        if(!(GETBIT(bf[bf_id]->a, key[i] % bf[bf_id]->asize))){
            rc = 0;
            break;  
        }
    }
    pthread_mutex_unlock(&bloom_lock[bf_id]);*/

EXIT:
    if(tmpsha){
        free(tmpsha);
        tmpsha = NULL;
    }

    DCS_LEAVE("bloom_query leave \n");
    return rc;
}

/*hash function for bloom filter*/
dcs_u64_t hashfunc(dcs_u8_t *sha, dcs_s32_t i)
{
    dcs_u64_t key = 0;

    if(i == 0)
        key = simple_hash(sha);
    else if(i == 1)
        key = RS_hash(sha);
    else if(i == 2)
        key = JS_hash(sha);
    else if(i == 3)
        key = PJW_hash(sha);
    else if(i == 4)
        key = ELF_hash(sha);
    else if(i == 5)
        key = BKDR_hash(sha);
    else if(i == 6)
        key = SDBM_hash(sha);
    else if(i == 7)
        key = DJB_hash(sha);
    else if(i == 8)
        key = AP_hash(sha);
    else if(i == 9)
        key = CRC_hash(sha);
    else
        key = 0;

    return key;
}

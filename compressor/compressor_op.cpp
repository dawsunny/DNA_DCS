/*compressor operation*/

#include "amp.h"
#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"

#include "compressor_op.h"
#include "compressor_cache.h"
#include "compressor_cnd.h"
#include "compressor_con.h"
#include "compressor_thread.h"
#include "compressor_index.h"
#include "bloom.h"
#include "hash.h"
#include "compressor_op.h"

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

#include <sstream>
#include <string>

#include "dc_c_global.h"
#include "ds_dsrc.h"
using namespace std;

pthread_mutex_t compressor_location_lock = PTHREAD_MUTEX_INITIALIZER;
map<string, string> compressor_location;
dcs_u32_t compressor_location_cnt = 0;

//by bxz
dcs_s32_t get_location(dcs_s8_t *res, dcs_u8_t *sha, dcs_u32_t optype) {
    dcs_s32_t rc = 0;
    dcs_s32_t fd = 0;
    memset(res, 0, PATH_LEN);
    memcpy(res, DATA_CONTAINER_PATH, strlen(DATA_CONTAINER_PATH));
    res[strlen(res)] = '/';
    if (optype == DCS_WRITE) {
        string strstr = "";
        stringstream is;
        
        pthread_mutex_lock(&compressor_location_lock);
        is << compressor_location_cnt;
        is >> strstr;
        for (int i = 0, j = strstr.size() - 1; i < j; i++, j--) {
            swap(strstr[i], strstr[j]);
        }
        while (strstr.size() < COMPRESSOR_OUTPUT_FILE_LEN) {
            strstr += '0';
        }
        for (int i = 0, j = strstr.size() - 1; i < j; i++, j--) {
            swap(strstr[i], strstr[j]);
        }
        compressor_location_cnt++;
        string shatmptmp = (dcs_s8_t *)sha;
        compressor_location[shatmptmp] = strstr;
        pthread_mutex_unlock(&compressor_location_lock);
        
        memcpy(res + strlen(res), strstr.c_str(), strstr.size());
        
        printf("||location: %s\n", res);
        if (access(res, 0)) {
            if ((fd = open(res, O_WRONLY | O_CREAT, 0666)) < 0) {
                DCS_ERROR("get_location: open dir error\n");
                rc = -1;
                goto EXIT1;
            }
            close(fd);
        } else {
            printf("get location: dir already exists!\n");
            rc = -1;
            goto EXIT1;
        }
    } else {
        
    }
    goto EXIT;
    
EXIT1:
    if (res != NULL) {
        free(res);
        res = NULL;
    }
    
EXIT:
    return rc;
}


/*analyze the msg find the request type*/
dcs_s32_t __dcs_compressor_process_req(amp_request_t *req, dcs_thread_t *threadp)
{
    dcs_s32_t rc = 0;
    dcs_msg_t *msgp = NULL;
    dcs_u32_t op_type;

    DCS_ENTER("__dcs_compressor_process_req enter \n");

    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    op_type = msgp->optype;
    if(op_type == DCS_WRITE)
    {
        rc = __dcs_compressor_write(req, threadp);
        if(0 != rc){
            DCS_ERROR("__dcs_compressor_write rc : %d\n", rc);
        }
    }
    else if(op_type == DCS_READ){
        rc = __dcs_compressor_read(req);
        if(0 != rc){
            DCS_ERROR("__dcs_compressor_write read : %d\n", rc);
        }
    }
    else if(op_type == DCS_QUERY){
        rc = __dcs_compressor_answer(req);
        if(0 != rc){
            DCS_ERROR("__dcs_compressor_answer rc : %d\n", rc);
        }
    }
    else if(op_type == DCS_DELETE){
        rc = __dcs_compressor_delete(req);
        if(0 != rc){
            DCS_ERROR("__dcs_compressor_delete rc : %d\n", rc);
        }

    }else if(op_type == DCS_UPLOAD){
        rc = __dcs_compressor_upload(req);
        if(0 != rc){
            DCS_ERROR("__dcs_compressor_upload rc : %d\n", rc);
        }

    }else{
        DCS_ERROR("__dcs_compressor_process_req recieve wrong optype from server \n");
        rc = -1;
        goto EXIT;
    }

EXIT:
    DCS_LEAVE("__dcs_compressor_process_req leave \n");
    return rc;
}

dcs_s32_t __dcs_compressor_upload(amp_request_t *req){
    dcs_s32_t rc = 0;
    dcs_s32_t chunk_num;
    dcs_s32_t i;
    dcs_s32_t data_size;
    conid_sha_block_t *csb_array = NULL;
    dcs_u32_t size = 0;
    amp_message_t *repmsgp = NULL;
    dcs_msg_t   *msgp = NULL;
#ifdef __DCS_TIER_BLOOM__    
    dcs_u8_t  sha_array = NULL;
#endif
    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    chunk_num = msgp->u.s2d_upload_req.sha_num;
    data_size = msgp->u.s2d_upload_req.scsize;

    DCS_ERROR("__dcs_compressor_upload receive fp info from server: %d, sha_num:%d, process...\n", req->req_remote_id, chunk_num);

    if(req->req_iov->ak_len != chunk_num*sizeof(conid_sha_block_t)){
        rc = -1;
        DCS_ERROR("__dcs_compressor_upload recieve err: %d", rc);
        goto EXIT;
    }

    csb_array = (conid_sha_block_t *)malloc(sizeof(conid_sha_block_t) * chunk_num);
    if(csb_array == NULL){
        DCS_ERROR("__dcs_compressor_upload malloc for csb_array err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    memcpy(csb_array, req->req_iov->ak_addr, sizeof(conid_sha_block_t)*chunk_num);

#ifdef __DCS_TIER_BLOOM__
    sha_array = (dcs_u8_t *)malloc(sizeof(dcs_u8_t) * chunk_num);
    if(NULL == sha_array){
        DCS_ERROR("__dcs_compressor_upload malloc for sha_array err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
#endif

    for(i=0; i<chunk_num; i++){
       index_insert(csb_array[i].sha, csb_array[i].con_id, 1); 
#ifdef __DCS_TIER_BLOOM__
       memcpy(sha_array + i * SHA_LEN, csb_array[i].sha, SHA_LEN);
#endif
    }


#ifdef __DCS_TIER_BLOOM__
    bloom_update(sha_array, chunk_num);
#endif

    DCS_ERROR("__dcs_compressor_upload receive fp info from server: %d, sha_num:%d, process end, send ack to server\n", req->req_remote_id, chunk_num);
    size = sizeof(dcs_msg_t) + AMP_MESSAGE_HEADER_LEN;
    repmsgp = (amp_message_t *)malloc(size);
    if(repmsgp == NULL){
        DCS_ERROR("__dcs_compressor_write malloc for repmsgp err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(repmsgp, 0, size);

    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);
    if(req->req_msg){
        free(req->req_msg);
        req->req_msg = NULL;
    }
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    
    msgp->msg_type = is_rep;
    msgp->fromtype = DCS_NODE;
    msgp->fromid = compressor_this_id;
    msgp->ack = 1;

    if(req->req_iov){
        __compressor_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
        req->req_iov = NULL;
    }

    req->req_iov = NULL;
    req->req_niov = 0;

    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 0;
    req->req_type = AMP_REPLY|AMP_MSG;

    rc = amp_send_sync(compressor_comp_context,
                       req,
                       req->req_remote_type,
                       req->req_remote_id,
                       0);
    if(rc < 0){
        DCS_ERROR("__dcs_compressor_write send reply msg err:%d \n", rc);
        goto EXIT;
    }

EXIT:
    return rc;
}
dcs_s32_t __dcs_compressor_delete(amp_request_t *req)
{
    dcs_s32_t rc = 0;
    dcs_s32_t chunk_num = 0;
    dcs_u32_t datasize = 0;
    dcs_s32_t ii = 0;
    dcs_s32_t iii = 0;
    dcs_u8_t    *sha = NULL;
    dcs_s8_t    *datap = NULL;
    sha_array_t   *chunk_info = NULL;
    dcs_datamap_t *data_map = NULL;
    amp_message_t *repmsgp = NULL;
    dcs_msg_t   *msgp = NULL;
    dcs_s32_t   i = 0, j = 0;
    DCS_ENTER("__dcs_compressor_delete enter \n");

    /*get msg from the req*/
    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    chunk_num = msgp->u.m2d_del_req.sha_num;
    datasize = msgp->u.m2d_del_req.scsize;
    
    /*verification weather the data recieved is correct*/
    if(req->req_iov->ak_len != chunk_num*sizeof(dcs_datamap_t)){
        rc = -1;
        DCS_ERROR("__dcs_compressor_delete recieve err: %d", rc);
        goto EXIT;
    }
    
    data_map = (dcs_datamap_t *)malloc(chunk_num * sizeof(dcs_datamap_t));
    if(NULL == data_map){
        DCS_ERROR("__dcs_compressor_delete malloc for data_map failed, errno:%d\n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(data_map, 0, sizeof(dcs_datamap_t) * chunk_num);
    memcpy(data_map, req->req_iov->ak_addr, chunk_num * sizeof(dcs_datamap_t));
   
    dcs_delete_index(data_map, &chunk_num);

    for(i = 0; i < chunk_num - 1; i ++){
        dcs_s32_t tmp = data_map[i].container_id;
        dcs_s32_t tmp_off = data_map[i].container_offset;
        dcs_s32_t index = i;
        dcs_datamap_t tmpmap;
        for(j = i + 1; j < chunk_num; j ++){
            if(tmp > data_map[j].container_id){
                index = j;
                tmp = data_map[j].container_id;
                tmp_off = data_map[j].container_offset;
            }else if(tmp == data_map[j].container_id){
                if(tmp_off < data_map[j].container_offset ){
                    index = j;
                    tmp = data_map[j].container_id;
                    tmp_off = data_map[j].container_offset;
                }
            } 
        }
        if(index != i){
            memcpy(&tmpmap, &data_map[i], sizeof(dcs_datamap_t));
            memcpy(&data_map[i], &data_map[index], sizeof(dcs_datamap_t));
            memcpy(&data_map[index], &tmpmap, sizeof(dcs_datamap_t));
        }
    }

    for( i = 0; i < chunk_num; i = j){
        //dcs_s32_t con_num = 0;
        dcs_u64_t con_id = data_map[i].container_id;
        struct stat f_state,d_state;
        dcs_s32_t fd = 0;
        dcs_s32_t k = 0;
        dcs_s32_t filesize = 0;
        dcs_s32_t total_num = 0;
        dcs_s32_t tsize;
        dcs_s8_t  *fp_container_name = NULL;
        dcs_s8_t  *data_container_name = NULL;
        con_sha_t *con_sha = NULL;
        dcs_s32_t all_shot_flag = 0;
        for(j = i; j < chunk_num; j++){
            all_shot_flag = 0;
            if((j == chunk_num - 1 && data_map[j].container_id == con_id)|| data_map[j].container_id != con_id){
                if(j == chunk_num - 1 && data_map[j].container_id == con_id){
                    j++;
                }

                for(k=0; k<data_cache->con_num; k++){
                    pthread_rwlock_wrlock(&data_cache->cache_lock[k]);
                    if(data_cache->container_id[k] == con_id){
                        data_cache->container_id[k] = 0;
                        data_cache->con_size[k] = 0;
                        free(data_cache->data_con[k]);
                        data_cache->data_con[k] = NULL;
                   }
                   pthread_rwlock_unlock(&data_cache->cache_lock[k]);
               }

                fp_container_name = get_fpcon_name(con_id);
                data_container_name = get_datacon_name(con_id);
                
                if(0 != (rc = stat(data_container_name, &d_state))){
                        rc = errno;
                }

                if(0 != (rc = stat(fp_container_name, &f_state))){
                        rc = errno;
                }
                
                if((rc != 0 && errno == ENOENT) || (rc == 0 /*&& d_state.st_size < CONTAINER_SIZE && f_state.st_size > 0*/)) {
                    for(ii=0; ii<DCS_COMPRESSOR_THREAD_NUM; ii++){
                        pthread_rwlock_wrlock(&con_table->con_lock[ii]);
                        if(NULL != con_table->fp_con[ii] && con_table->fp_con[ii]->container_id == con_id){
                            dcs_s32_t cnum = con_table->fp_con[ii]->chunk_num;
                            fp_container_t *fp = con_table->fp_con[ii];
                            for(k = i; k < j; k++){
                                for(iii = 0; iii < cnum; iii++){
                                    if(fp->offset[iii] == data_map[k].container_offset && 
                                       0 == shastr_cmp(data_map[k].sha, fp->con[iii].sha)){
                                        cnum--;
                                        memcpy(fp->con[iii].sha, fp->con[cnum].sha, SHA_LEN);
                                        con_table->fp_con[ii]->chunk_num = cnum;
                                        con_table->data_con[ii]->timestamp = time(NULL);
                                        fp->offset[iii] = fp->offset[cnum];
                                        fp->offset[cnum] = 0;
                                        memset(fp->con[cnum].sha, 0, SHA_LEN);
                                        break;
                                    }
                                }
                            }
                            if(cnum == 0){
                                con_table->data_con[ii]->offset = 0;
                                memset(con_table->data_con[ii]->con, 0, CONTAINER_SIZE);
                                con_table->data_con[ii]->timestamp = time(NULL);
                            }
                            if(rc == 0){
                                DCS_ERROR("__dcs_compressor_delete call dcs_compressor_data_fp_trunc container_id: %ld data_offset: %d\n", con_id, con_table->data_con[ii]->offset); 
                                dcs_compressor_data_fp_trunc(con_table->data_con[ii], con_table->fp_con[ii]);
                            }
                            pthread_rwlock_unlock(&con_table->con_lock[ii]);
                            all_shot_flag = 1;
                            break;
                        }
                        pthread_rwlock_unlock(&con_table->con_lock[ii]);
                    }
                }
                if(rc){
                      if(all_shot_flag == 0){
                          DCS_ERROR("__dcs_compressor_delete con_id[%ld] not in con_table, and file %s not exist, errno:%d\n ", con_id, fp_container_name, rc);
                          goto EXIT;
                      }
                      rc = 0;
                }else if(rc == 0 && all_shot_flag == 0){

                    fd = open(fp_container_name, O_RDONLY, 0666);
                    if(fd < 0){
                        DCS_ERROR("cache_fp open fp file err: %d \n", errno);
                        rc = errno;
                        goto EXIT;
                    }

                    filesize = (dcs_u64_t)(f_state.st_size);
                    total_num = filesize/sizeof(con_sha_t);
                    DCS_MSG("total num is %d \n", total_num);

                    con_sha = (con_sha_t *)malloc(sizeof(con_sha_t)*total_num);
                    if(con_sha == NULL){
                        DCS_ERROR("cache_fp malloc for con sha err:%d \n", errno);
                        rc = errno;
                        goto EXIT;
                    }
                    memset(con_sha, 0, sizeof(con_sha_t)*total_num);

                    tsize = read(fd, con_sha, filesize);
                    if(tsize != filesize){
                        DCS_ERROR("cache_fp read sha container err: %d \n" ,errno );
                        goto EXIT;
                    }
                    close(fd);
                
                    for(k = i; k < j; k++){
                        dcs_s32_t l = 0;
                        dcs_s32_t lflag = 0;
                        for(l = 0; l < total_num; l++){
                            if(con_sha[l].offset == data_map[k].container_offset 
                               && 0 == shastr_cmp(con_sha[l].sha, data_map[k].sha)){
                                total_num--;
                                lflag = 1;
                                if(l <  total_num)
                                    memcpy(&con_sha[l], &con_sha[total_num], sizeof(con_sha_t));
                                break;
                            }
                        }
                        if(l == total_num&&!lflag){
                            DCS_ERROR("dcs_compressor_delete read fp_con cannot read fp:%s , container_total_num: %d, current_con_id:%ld, datamap_id:%ld\n", data_map[k].sha,total_num,con_id,data_map[k].container_id);
                        }
                    }
                    if(total_num == 0){
                        fd = open(data_container_name, O_RDWR|O_TRUNC|O_CREAT, 0666);
                        if(fd <= 0){
                            DCS_ERROR("read container get read_fd err:%d \n", errno);
                            rc = errno;
                            close(fd);
                            goto EXIT;
                        }
                        close(fd);
                        if(!all_shot_flag){
                            con_block_t * tmp_con_block = (con_block_t *)malloc(sizeof(con_block_t));
                            if(NULL == tmp_con_block){
                                DCS_ERROR("dcs_compressor_delete malloc con_block_t for recycle container file failed, errno:%d\n", errno);
                                rc = errno;
                                goto EXIT;
                            }
                            pthread_mutex_lock(&container_recycle_lock);
                            INIT_LIST_HEAD(&tmp_con_block->con_list);
                            tmp_con_block->con_id = con_id;
                            tmp_con_block->timestamp = time(NULL);
                            list_add_tail(&tmp_con_block->con_list, &container_recycle_list);
                            pthread_mutex_unlock(&container_recycle_lock);
                        }
                    }
                    if(total_num * sizeof(con_sha_t) < filesize){
                        fd = open(fp_container_name, O_WRONLY | O_CREAT|O_TRUNC, 0666);
                        tsize = write(fd, con_sha, total_num*(sizeof(con_sha_t)));
                        if(tsize != total_num*(sizeof(con_sha_t))){
                            DCS_ERROR("dcs_compressor_delete err:%d \n", errno);
                            rc = errno;
                            close(fd);
                            goto EXIT;
                        }
                        close(fd);
                    }
                
                }
                if(data_container_name != NULL){
                    free(data_container_name);
                    data_container_name = NULL;
                }
                if(fp_container_name != NULL){
                    free(fp_container_name);
                    fp_container_name = NULL;
                }
                if(con_sha != NULL){
                    free(con_sha);
                    con_sha = NULL;
                }

                break;
            }
        }
    }

EXIT:
    if(req->req_iov){
        __compressor_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
        req->req_iov = NULL;
    }
    
    if(sha != NULL){
        free(sha);
        sha = NULL;
    }

    if(chunk_info != NULL){
        free(chunk_info);
        chunk_info = NULL;
    }

    if(datap){
        free(datap);
        datap = NULL;
    }

    if(repmsgp != NULL){
        free(repmsgp);
        repmsgp = NULL;
    }

    if(req != NULL){
        __amp_free_request(req);
        req = NULL;
    }

    DCS_LEAVE("__dcs_compressor_delete leave \n");

    return rc;
}

/*if it is a write request, 
 * then get the data and do the dcslication*/
dcs_s32_t __dcs_compressor_write(amp_request_t *req, dcs_thread_t *threadp)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_u32_t size = 0;
    dcs_u32_t filetype = 0;     //by bxz
    dcs_u32_t chunk_num = 0;
    dcs_u32_t datasize = 0;
    dcs_u64_t container_id = 0;
    dcs_u8_t    *sha = NULL;
    dcs_s8_t    *datap = NULL;
    dcs_s8_t    *tmpdatap = NULL;
    sha_sample_t  *hook = NULL;
    sha_array_t   *chunk_info = NULL;
    container_t   *con_id = NULL;
    dcs_datapos_t *data_pos = NULL;

    amp_message_t *repmsgp = NULL;
    dcs_msg_t   *msgp = NULL;

    FILE *filep = NULL; //bxz
    static int seqno = 0;
    char *input_name, *output_name;

    DCS_ENTER("__dcs_compressor_write enter \n");

    /*get msg from the req*/
    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    filetype = msgp->filetype;      //by bxz
    chunk_num = msgp->u.s2d_req.chunk_num;
    datasize = msgp->u.s2d_req.scsize;

    /*verification weather the data recieved is correct*/
    if(req->req_iov->ak_len != (chunk_num*sizeof(sha_array_t)) + datasize){
        rc = -1;
        DCS_ERROR("__dcs_compressor_write recieve err: %d", rc);
        goto EXIT;
    }

    sha = (dcs_u8_t *)malloc(chunk_num*SHA_LEN);
    if(sha == NULL){
        DCS_ERROR("__dcs_compressor_write malloc for sha err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    //DCS_MSG("5\n");
    chunk_info = (sha_array_t *)malloc(sizeof(sha_array_t)*chunk_num);
    if(chunk_info == NULL){
        DCS_ERROR("__dcs_compressor_write malloc for chunk info err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    memcpy(chunk_info, req->req_iov->ak_addr, sizeof(sha_array_t)*chunk_num);
    printf("||got chunk info: size: %d, offset: %lu, sha: %s\n", chunk_info->chunksize, chunk_info->offset, chunk_info->sha);

    for(i=0; i<chunk_num; i++){
        memcpy(sha + i*SHA_LEN, chunk_info[i].sha, SHA_LEN);
    }

    tmpdatap = (dcs_s8_t *)((dcs_s8_t *)req->req_iov->ak_addr + chunk_num*sizeof(sha_array_t));
    
    datap = (dcs_s8_t *)malloc(datasize + 1);   //modified(+1) by bxz
    if(datap == NULL){
        DCS_ERROR("__dcs_compressor_write malloc for datap err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(datap, 0, datasize + 1);     //by bxz
    memcpy(datap, tmpdatap, datasize);
    
    //filep = fopen("./input.fa", "w+");
    //fwrite(datap, 1, datasize, filep);
    //fclose(filep);
    input_name = (char *)malloc(PATH_LEN);
    output_name = (char *)malloc(PATH_LEN);
    if (filetype == DCS_FILETYPE_FASTA) {
        get_location(output_name, chunk_info->sha, DCS_WRITE);
        dc_c_main(datap, datasize, output_name);
    } else if (filetype == DCS_FILETYPE_FASTQ) {
        sprintf(input_name, "./input.fq");
        sprintf(output_name, "./output.ds");

        filep = fopen(input_name, "w+");
        fwrite(datap, 1, datasize, filep);
        fclose(filep);
        
        dsrc_main(DCS_WRITE, input_name, output_name);
    } else {
        DCS_ERROR("__dcs_compressor_write got file type[%d] error\n", filetype);
        rc = -1;
        goto EXIT;
    }
    //DCS_MSG("6\n");
    /*get sample FPs*/
    /*
    hook = get_sample_fp(sha, chunk_num);
    if(hook == NULL || hook->sha == NULL){
        rc = -1;
        DCS_ERROR("__dcs_compressor_write get sample fp err:%d \n", rc);
        goto EXIT;
    }
    */
    
    /*
    //search the index, find correspoding container id
    con_id = dcs_query_index(sha, chunk_num);
    //find the champion container which most sample hit it
    container_id = find_champion_container(con_id);
    //dcslication
    data_pos = dcslication(chunk_info, datap, chunk_num, container_id, threadp);
    if(data_pos == NULL){
        DCS_ERROR("__dcs_compressor_write get sample fp err:%d \n", rc);
        rc = errno;
        goto EXIT;
    }
     
     */

    size = sizeof(dcs_msg_t) + AMP_MESSAGE_HEADER_LEN + sizeof(dcs_datapos_t)*chunk_num ;
    repmsgp = (amp_message_t *)malloc(size);
    if(repmsgp == NULL){
        DCS_ERROR("__dcs_compressor_write malloc for repmsgp err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(repmsgp, 0, size);

    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);
    if(req->req_msg){
        free(req->req_msg);
        req->req_msg = NULL;
    }
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    
    msgp->msg_type = is_rep;
    msgp->fromtype = DCS_NODE;
    msgp->fromid = compressor_this_id;
    msgp->u.d2s_reply.chunk_num = chunk_num;
    //memcpy(msgp->buf, data_pos, sizeof(dcs_datapos_t)*chunk_num);

    if(req->req_iov){
        __compressor_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
        req->req_iov = NULL;
    }

    req->req_iov = NULL;
    req->req_niov = 0;

    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 0;
    req->req_type = AMP_REPLY|AMP_MSG;

    rc = amp_send_sync(compressor_comp_context,
                       req,
                       req->req_remote_type,
                       req->req_remote_id,
                       0);
    if(rc < 0){
        DCS_ERROR("__dcs_compressor_write send reply msg err:%d \n", rc);
        goto EXIT;
    }
    //printf("got msg: %s$\n", datap);
    if (filetype == DCS_FILETYPE_FASTA) {
        printf("got file type: FASTA!\n");
    } else if (filetype == DCS_FILETYPE_FASTQ) {
        printf("got file type: FASTQ!\n");
    } else {
        printf("filetype error!\n");
    }
    
EXIT:
    if(sha != NULL){
        free(sha);
        sha = NULL;
    }

    if(chunk_info != NULL){
        free(chunk_info);
        chunk_info = NULL;
    }

    if(hook != NULL){
        if(hook->sha != NULL){
            free(hook->sha);
            hook->sha = NULL;
        }
        free(hook);
        hook = NULL;
    }

    if(con_id != NULL){
        free(con_id);
        con_id = NULL;
    }

    if(datap){
        free(datap);
        datap = NULL;
    }

    if(repmsgp != NULL){
        free(repmsgp);
        repmsgp = NULL;
    }

    if(req != NULL){
        __amp_free_request(req);
        req = NULL;
    }
    
    //bxz
    if (output_name) {
        free(output_name);
        output_name = NULL;
    }

    DCS_LEAVE("__dcs_compressor_write leave \n");

    return rc;
}

/*compressor read operation
 * get the datamap info from the request type 
 * then read data from the container. when read data from data container
 * we will use cache to improve performance
 */
dcs_s32_t __dcs_compressor_read(amp_request_t *req)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s32_t j = 0;
    dcs_u32_t chunk_num = 0;
    dcs_u32_t container_num = 0;
    dcs_u32_t size = 0;
    dcs_u32_t datasize = 0;
    dcs_u64_t begin_offset = 0;
    
    dcs_s8_t      *data = NULL;
    dcs_s8_t      *container_data = NULL;
    container_chunk_t *con_chunk_info = NULL;
    dcs_datamap_t *datamap = NULL;
    dcs_msg_t     *msgp = NULL;
    amp_message_t   *repmsgp = NULL;
    //amp_message_t   *reqmsgp = NULL;

    DCS_ENTER("__dcs_compressor_read enter \n");
    
    msgp = (dcs_msg_t *)((dcs_s8_t *)req->req_msg + AMP_MESSAGE_HEADER_LEN);
    chunk_num = msgp->u.s2d_req.chunk_num;
    DCS_MSG("chunk num is %d \n", chunk_num);
    if(chunk_num == 0){
        DCS_ERROR("__dcs_compressor_read get chunk_num err:%d \n", errno);
        rc = -1;
        goto EXIT;
    }

    con_chunk_info = (container_chunk_t *)malloc(sizeof(container_chunk_t)*chunk_num);
    if(con_chunk_info == NULL){
        DCS_ERROR("__dcs_compressor_read malloc for container chunk info err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
    memset(con_chunk_info, 0, sizeof(container_chunk_t)*chunk_num);

    datamap = (dcs_datamap_t *)(msgp->buf);
    if(datamap == NULL){
        DCS_ERROR("__dcs_compressor_read get datamap err :%d \n", errno);
        rc = -1;
        goto EXIT;
    }

    /*get the data begin offset*/
    begin_offset = datamap[0].offset;
    /*request data size*/
    //DCS_MSG("chunk_num is %d \n", chunk_num);
    datasize = datamap[chunk_num - 1].offset - datamap[0].offset + datamap[chunk_num - 1].chunksize;
    data = (dcs_s8_t *)malloc(datasize);
    DCS_MSG("compressor read datasize is %d \n", datasize);
    if(data == NULL){
        DCS_ERROR("__dcs_compressor_read malloc for data err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    
    for(i=0; i<chunk_num ;i++){
        for(j=0; j<container_num; j++){
            if(con_chunk_info[j].container_id == datamap[i].container_id){
                con_chunk_info[j].chunk_num ++;
                break;
            }
        }

        /*new container*/
        if(j == container_num){
            con_chunk_info[container_num].container_id = datamap[i].container_id;
            con_chunk_info[container_num].chunk_num = 1;
            container_num++;
        }
    }

    /*finish anlyze the datamap we get the container number*/
    //con_chunk_info = (container_chunk_t *)realloc(con_chunk_info, sizeof(container_chunk_t)*container_num);
    
    /*get the data from the container*/
    //DCS_MSG("container_num is %d \n", container_num);
    for(i=0; i<container_num; i++){
        dcs_u32_t con_size = 0;
        DCS_MSG("container_num is %d , i is %d  \n", container_num, i);

        container_data = get_data_container(con_chunk_info[i].container_id, &con_size);
        //DCS_MSG("container len is %ld \n", strlen(container_data));
        //DCS_MSG("begin offset is %ld \n", begin_offset);
        if(container_data == NULL){
            DCS_MSG("get_data_container err \n");
            goto EXIT;
        }

        /*get chunk data from a container*/
        for(j=0; j<chunk_num; j++){
            //DCS_MSG("chunk_num is %d, j is %d  \n", chunk_num, j);
            if(datamap[j].container_id == con_chunk_info[i].container_id){
                memcpy(data + (datamap[j].offset - begin_offset),
                       container_data + datamap[j].container_offset,
                       datamap[j].chunksize);

                con_chunk_info[i].chunk_num--;
            }
        }

        /*free the tmp buf*/
        /*
        if(container_data != NULL){
            DCS_MSG("before free data container \n");
            free(container_data);
            container_data = NULL;
        }
        */

        if(con_chunk_info[i].chunk_num != 0){
            rc = -1;
            DCS_ERROR("__dcs_compressor_read do not get enough chunk from container %dth err:%d\n", i, rc);
            goto EXIT;
        }
    }

    //DCS_MSG("3 \n");
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    repmsgp = (amp_message_t *)malloc(size);
    if(repmsgp == NULL){
        DCS_ERROR("__dcs_compressor_read malloc for repmsgp err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    //DCS_MSG("4 \n");
    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);
    if(req->req_msg != NULL){
        free(req->req_msg);
        req->req_msg = NULL;
    }
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);

    msgp->size = size;
    msgp->ack  = 1;
    msgp->fromtype = DCS_NODE;
    msgp->fromid = compressor_this_id;
    msgp->u.d2s_reply.bufsize = datasize;
    DCS_MSG("datasize is %d \n", datasize);

    req->req_iov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if(req->req_iov == NULL){
        DCS_ERROR("__dcs_compressor_read malloc for reply iov err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    req->req_iov->ak_addr = data;
    req->req_iov->ak_len = datasize;
    req->req_iov->ak_flag = 0;
    req->req_iov->ak_offset = 0;
    req->req_niov = 1;

    req->req_reply = repmsgp;
    req->req_replylen = size;
    req->req_need_ack = 0;
    req->req_resent = 0;
    req->req_type = AMP_REPLY | AMP_DATA;
    
    //DCS_MSG("5 \n");
    rc = amp_send_sync(compressor_comp_context,
                       req,
                       req->req_remote_type,
                       req->req_remote_id,
                       0);
    if(rc < 0){
        DCS_ERROR("__dcs_compressor_read reply data to server err: %d \n", rc);
        goto EXIT;
    }

EXIT:

    //DCS_MSG("6 \n");
    if(con_chunk_info != NULL){
        free(con_chunk_info);
        con_chunk_info = NULL;
    }

    //DCS_MSG("7 \n");
    if(req->req_iov != NULL){
        __compressor_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
        req->req_iov = NULL;
        req->req_niov = 0;
    }

    //DCS_MSG("8 \n");
    if(repmsgp != NULL){
        free(repmsgp);
        repmsgp = NULL;
    }

    //DCS_MSG("9 \n");
    if(req != NULL){
        __amp_free_request(req);
    }

    if(data != NULL){
        //free(data);
        //data = NULL;
    }

    DCS_LEAVE("__dcs_compressor_read leave \n");
    return rc;
}

/*get sample FPs*/
sha_sample_t *get_sample_fp(dcs_u8_t *sha, dcs_u32_t chunk_num)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_u32_t sign = 0;
    dcs_u32_t sha_num = 0;
    dcs_u8_t  tmpsha ;

    sha_sample_t *hook = NULL;

    DCS_ENTER("get_sample_fp enter \n");

    hook = (sha_sample_t *)malloc(sizeof(sha_sample_t));
    if(hook == NULL){
        DCS_ERROR("get_sample_fp malloc for hook err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
    hook->sha = NULL;
    hook->sha_num = 0;

    DCS_MSG("get_sample_fp chunk num is %d \n", chunk_num);
    hook->sha = (dcs_u8_t *)malloc(SHA_LEN*chunk_num);
    if(hook->sha == NULL){
        DCS_ERROR("get_sample_fp malloc for hook->sha err:%d \n", errno);
        rc = -1;
        goto EXIT;
    }

    /*the first sha must be gotten*/
    DCS_MSG("before get the first chunk sha \n");
    memcpy(hook->sha, sha, SHA_LEN);
    sha_num ++;
    DCS_MSG("get the first chunk sha \n");

    /*get hook according sampleing*/
    DCS_MSG("the sample rate is 1/%d , power is %d\n", SAMPLE_RATE, power);
    
    if(SAMPLE_RATE == 1){
        memcpy(hook->sha, sha, SHA_LEN*chunk_num);
        hook->sha_num = chunk_num;
        goto EXIT;
    }

    /*
    for(i=1; i<chunk_num; i++){
        tmpsha = sha[i*SHA_LEN];
        sign = (dcs_u32_t)(tmpsha);
        DCS_MSG("get_sample_fp sign is %d \n", sign);
        sign = (dcs_u32_t)(sign%SAMPLE_RATE);
        DCS_MSG("get_sample_fp sign is %d , power is %d\n", sign, power);
        //sign = sha[i*SHA_LEN] & ((1 << power) - 1);
        if(sign == 0){
            sha_num ++;
            //hook->sha = (dcs_u8_t *)realloc(hook->sha, sha_num);
            memcpy(hook->sha + (sha_num-1)*SHA_LEN, sha + i*SHA_LEN, SHA_LEN);
        }
    }
    */

    for(i=1; i<chunk_num; i++){
        tmpsha = sha[i*SHA_LEN];
        sign = (dcs_u32_t)tmpsha;
        DCS_MSG("sign is %d \n", sign);
        sign = (dcs_u32_t)(sign >> (8-power));
        DCS_MSG("get_sample_fp sign is %d power is %d \n", sign, power);
        //sign = sha[i*SHA_LEN] & ((1 << power) - 1);
        if(sign == 0){
            sha_num ++;
            //hook->sha = (dcs_u8_t *)realloc(hook->sha, sha_num);
            memcpy(hook->sha + (sha_num-1)*SHA_LEN, sha + i*SHA_LEN, SHA_LEN);
        }
    }


    DCS_MSG("get_sample_fp get sha num is %d\n", sha_num);
    hook->sha_num = sha_num;
    
EXIT:
    DCS_LEAVE("get_sample_fp leave \n");

    if(rc == 0){
        return hook;
    }
    else{
        DCS_MSG("get_sample_fp err\n");

        if(hook->sha){
            free(hook->sha);
            hook->sha = NULL;
        }

        if(hook != NULL){
            free(hook);
            hook = NULL;
        }

        return NULL;
    }
}

/*find champion container and return container id*/
dcs_u64_t find_champion_container(container_t *con_id)
{
    dcs_u64_t container_id = 0;
    dcs_u32_t container_num = 0;
    dcs_u32_t max_mark = 0;
    dcs_u32_t tmpcount = 0;
    dcs_s32_t i = 0;
    dcs_s32_t j = 0;
    dcs_s32_t rc = 0;

    dcs_u64_t *contmp = NULL;
    dcs_u32_t *mark = NULL;

    DCS_ENTER("find_champion_container enter \n");

    container_num = con_id->con_num;

    /*malloc contmp for storing each container id state*/
    contmp = (dcs_u64_t *)malloc(sizeof(dcs_u64_t)*container_num);
    if(contmp == NULL){
        DCS_ERROR("find_champion_container malloc for tmp con err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
    //memset(contmp, 0, sizeof(dcs_u64_t)*container_num);
    for(i=0; i<container_num; i++)
        contmp[i] = -1;

    /*mark is use for telling the frequence of con_id*/
    mark = (dcs_u32_t *)malloc(sizeof(dcs_u32_t)*container_num);
    if(mark == NULL){
        DCS_ERROR("find_champion_container malloc for mark err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(mark, 0, sizeof(dcs_u32_t)*container_num);
        

    if(container_num == 0){
        container_id = 0;
        goto EXIT;
    }

    for(i=0; i<container_num; i++){
        for(j=0; j<tmpcount; j++){
            if(contmp[j] == con_id->c_id[i]){
                mark[j]++;
                break;
            }
        }
        
        /*if j is less than tmpcount means that the same c_id appear again
         * else it is a new c_id
         */
        if(j < tmpcount)
            continue;
        else{
            contmp[tmpcount] = con_id->c_id[i];
            tmpcount ++;
            mark[tmpcount]++;
        }
    }

    DCS_MSG("container num hit is %d \n", tmpcount);
    for(i=0; i<tmpcount; i++){
        DCS_MSG("hit container id is %ld \n", contmp[i]);
        if(contmp[i] != 0){
            rc = search_cache_list(contmp[i]);
            if(rc == 0){
                DCS_MSG("dcslication the fp container we wanted is not in cache \n");
                /*if the fps of container_id is not in the cache,reand and add them to cache*/
                rc = cache_fp(contmp[i]);
                if(rc != 0){
                    DCS_ERROR("dcslication read fp from disk err:%d \n", rc);
                    goto EXIT;
                }
            }
            else{
                DCS_MSG("dcslication the fp is in the cache \n");
                rc = 0;
            }
        }
    }

    container_id = contmp[0];
    max_mark = mark[0];
    for(i=1; i<tmpcount; i++){
        if(max_mark < mark[i]){
            container_id = contmp[i];
            max_mark = mark[i];
        }
    }
    
    DCS_MSG("find_champion_container container id is %ld \n", container_id);
EXIT:
    DCS_LEAVE("find_champion_container leave \n");

    if(mark != NULL){
        free(mark);
        mark = NULL;
    }

    if(contmp != NULL){
        free(contmp);
        contmp = NULL;
    }

    return container_id;
}

/*compressor cache source file*/

#include "amp.h"
#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"

#include "compressor_cache.h"
#include "compressor_cnd.h"
#include "compressor_con.h"
#include "compressor_thread.h"
#include "compressor_index.h"
#include "bloom.h"
#include "hash.h"
#include "compressor_op.h"

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

container_list_t *con_list = NULL;
pthread_rwlock_t cache_list_lock;
cache_table_t    *cache_table = NULL; 
dcs_u32_t bucket_num = TOTAL_CACHE_NUM/EACH_BLOCK_NUM;
dcs_u32_t bucket_power = 0;
tmp_table_t     *tmp_cache = NULL;

data_cache_t    *data_cache = NULL;
dcs_u32_t     insert_pos = 0;

/*this function will record all the container id which beed cache
 * in memery
 */
dcs_s32_t container_list_init()
{
    dcs_s32_t rc = 0;

    pthread_rwlock_init(&cache_list_lock, NULL);
    con_list = NULL;

    return rc;
}

/*init cache hash table*/
dcs_s32_t cache_table_init()
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;


    cache_table = (cache_table_t *)malloc(sizeof(cache_table_t));
    if(cache_table == NULL){
        DCS_ERROR("cache_table_init malloc for cache_table err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    bucket_power = get_bucket_power(); 
    //DCS_MSG("2 \n");
    for(i=0; i<bucket_num; i++){
        cache_table->sha_num[i] = 0;
        pthread_rwlock_init(&cache_table->cache_lock[i], NULL);
        cache_table->sha_cache[i] = (sha_cache_t *)malloc(sizeof(sha_cache_t)*EACH_BLOCK_NUM);
        if(cache_table == NULL){
            DCS_ERROR("cache_table_init malloc for sha_cache err:%d \n", errno);
            rc = errno;
            goto EXIT;
        }
    }

    //DCS_MSG("4 \n");
    tmp_cache = (tmp_table_t *)malloc(sizeof(tmp_table_t));
    if(tmp_cache == NULL){
        DCS_ERROR("cache_table_init malloc for tmp_cache err:%d \n", errno);
        rc = errno; 
        goto EXIT;
    }

    tmp_cache->block_num = 0;
    for(i=0; i<TMP_BLOCK_NUM; i++){
        tmp_cache->sha_num[i] = 0;
        tmp_cache->bucket_id[i] = 0;
        pthread_rwlock_init(&tmp_cache->cache_lock[i], NULL);
        tmp_cache->sha_cache[i] = NULL;
    }
    
EXIT:

    return rc;
}

/*data cache init*/
dcs_s32_t data_cache_init()
{
    dcs_s32_t rc = 0;
    dcs_u32_t i = 0;


    data_cache = (data_cache_t *) malloc(sizeof(data_cache_t));
    if(data_cache == NULL){
        DCS_ERROR("data_cache_init malloc for data cache struct err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    data_cache->con_num = 0;
    
    for(i=0; i<DATA_CACHE_SIZE; i++){
        data_cache->container_id[i] = 0;
        data_cache->data_con[i] = NULL;
        data_cache->con_size[i] = 0;
        pthread_rwlock_init(&data_cache->cache_lock[i], NULL);
    }
    
EXIT:

    return rc;
}

#if 0
dcs_datapos_t *get_dcslication_info(sha_array_t *chunk_info, dcs_u32_t chunk_num, dcs_u64_t container_id)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_u64_t begin = 0;

    dcs_datapos_t *data_pos = NULL;
    dcs_datapos_t *tmppos = NULL;
    //sha_array_t     *tmpinfo = NULL;

    
    //DCS_MSG("chunksize in first  %d \n", chunk_info[0].chunksize);

    DCS_MSG("before malloc for datapos chunk num is %d \n", chunk_num);
    data_pos = (dcs_datapos_t *)malloc(sizeof(dcs_datapos_t)*chunk_num);
    if(data_pos == NULL){
        DCS_ERROR("get_dcslication_info malloc for data position err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    //DCS_MSG("chunksize in first chunksize %d, first offset is  %ld \n",
    //            chunk_info[0].chunksize,chunk_info[0].offset);
    if(container_id != 0){
        rc = search_cache_list(container_id);
        if(rc == 0){
            DCS_MSG("get_dcslication_info the fp container we wanted is not in cache \n");
            /*if the fps of container_id is not in the cache,reand and add them to cache*/
            rc = cache_fp(container_id);
            if(rc != 0){
                DCS_ERROR("get_dcslication_info read fp from disk err:%d \n", rc);
                goto EXIT;
            }
        }
        else{
            DCS_MSG("get_dcslication_info the fp is in the cache \n");
            rc = 0;
        }
    }

    begin = chunk_info[0].offset;
    //tmp_datap = datap;

    for(i=0; i<chunk_num; i++){
        //DCS_MSG("chunksize in chunkinfo is %d \n", chunk_info[i].chunksize);
        //DCS_MSG("the %dth chunk \n", i);
        tmppos = cache_query(chunk_info[i].sha);
        if(tmppos == NULL){
            DCS_ERROR("get_dcslication_info chunk %d: %s is not found in container \n", i, chunk_info[i].sha);
            /*set the data pointer offset*/
            /*tmppos = add_to_container(tmp_datap, chunk_info[i].chunksize, threadp, chunk_info[i].sha);
            DCS_MSG("dcslication chunk %d is new data container id is %ld \n",
                    i, tmppos->container_id);
            //DCS_MSG("dcslication datap addr is %p, and tmp_datap is %p \n", datap, tmp_datap);
            //DCS_MSG("chunk num is %d, i is %d \n", chunk_num, i);
            DCS_MSG("container id is %ld \n", tmppos->container_id);
            //memcpy(data_pos + i*(sizeof(dcs_datapos_t)), tmppos, sizeof(dcs_datapos_t));
            data_pos[i].compressor_id = compressor_this_id;
            data_pos[i].container_id = tmppos->container_id;
            data_pos[i].container_offset = tmppos->container_offset;
            tmp_datap = tmp_datap + chunk_info[i].chunksize;
            free(tmppos);
            tmppos = NULL;*/
        }
        else{
            DCS_MSG("get_dcslication_info chunk %d is existed and container id is %ld \n",
                    i, tmppos->container_id);
            //memcpy((dcs_s8_t *)data_pos + i*(sizeof(dcs_datapos_t)), tmppos, sizeof(dcs_datapos_t));
            data_pos[i].compressor_id = compressor_this_id;
            data_pos[i].container_id = tmppos->container_id;
            data_pos[i].container_offset = tmppos->container_offset;
            //tmp_datap = tmp_datap + chunk_info[i].chunksize;
            free(tmppos);
            tmppos = NULL;
        }
    }
    
EXIT:

    if(rc != 0){
        if(data_pos){
            free(data_pos);
            data_pos = NULL;
        }
    }

    return data_pos;
}
#endif

/*do the dcslication 
 * check out if the FP of container_id we got is in the cache
 * if yes let the coming FP query the cache 
 * else read the FPs with container_id from disk, then query the cache
 * while query the cache ,new data will be copy to the new container 
 * after query all the FPs return data position info
 */
dcs_datapos_t *dcslication(sha_array_t *chunk_info,
                               dcs_s8_t  *datap,
                               dcs_u32_t chunk_num,
                               dcs_u64_t container_id,
                               dcs_thread_t *threadp)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_u64_t begin = 0;

    dcs_s8_t      *tmp_datap = NULL;
    dcs_datapos_t *data_pos = NULL;
    dcs_datapos_t *tmppos = NULL;
    //sha_array_t     *tmpinfo = NULL;

    //DCS_MSG("chunksize in first  %d \n", chunk_info[0].chunksize);

    DCS_MSG("before malloc for datapos chunk num is %d \n", chunk_num);
    data_pos = (dcs_datapos_t *)malloc(sizeof(dcs_datapos_t)*chunk_num);
    if(data_pos == NULL){
        DCS_ERROR("dcslication malloc for data position err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(data_pos, 0, sizeof(dcs_datapos_t)*chunk_num);

    //DCS_MSG("chunksize in first chunksize %d, first offset is  %ld \n",
    //            chunk_info[0].chunksize,chunk_info[0].offset);
    if(container_id != 0){
        rc = search_cache_list(container_id);
        if(rc == 0){
            DCS_MSG("dcslication the fp container we wanted is not in cache \n");
            /*if the fps of container_id is not in the cache,reand and add them to cache*/
            rc = cache_fp(container_id);
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

    //DCS_MSG("chunksize in first  %d \n", chunk_info[0].chunksize);
    //DCS_MSG("chunksize addr %p \n", &chunk_info[0].chunksize);

    begin = chunk_info[0].offset;
    tmp_datap = datap;

    for(i=0; i<chunk_num; i++){
        tmppos = cache_query(chunk_info[i].sha);
        if(tmppos == NULL){
            tmppos = add_to_container(tmp_datap, chunk_info[i].chunksize, threadp, chunk_info[i].sha);
            DCS_MSG("dcslication chunk %d is new data container id is %ld \n",
                    i, tmppos->container_id);
            DCS_MSG("container id is %ld \n", tmppos->container_id);
            data_pos[i].compressor_id = compressor_this_id;
            data_pos[i].container_id = tmppos->container_id;
            data_pos[i].container_offset = tmppos->container_offset;
            tmp_datap = tmp_datap + chunk_info[i].chunksize;
            free(tmppos);
            tmppos = NULL;
        }
        else{
            DCS_ERROR("dcslicaiton chunk %d is existed and container id is %ld \n",
                    i, tmppos->container_id);
            data_pos[i].compressor_id = compressor_this_id;
            data_pos[i].container_id = tmppos->container_id;
            data_pos[i].container_offset = tmppos->container_offset;
            tmp_datap = tmp_datap + chunk_info[i].chunksize;
            free(tmppos);
            tmppos = NULL;
/*
                for(ii=0; ii<DCS_COMPRESSOR_THREAD_NUM; ii++){
                    if(con_table->fp_con[ii] != NULL){
                        pthread_rwlock_rdlock(&con_table->con_lock[ii]);
                        for(jj=0; jj<con_table->fp_con[ii]->chunk_num; jj++){
                            if(data_pos[i].container_id == con_table->fp_con[ii]->container_id
                               &&(shastr_cmp(chunk_info[i].sha, con_table->fp_con[ii]->con[jj].sha)) == 0){
                                con_table->fp_con[ii]->ref_count[jj]++;
                                pthread_rwlock_unlock(&con_table->con_lock[ii]);
                                goto CONTINUE;
                            }  
                        }
                        pthread_rwlock_unlock(&con_table->con_lock[ii]);
                    }
                }
                index_insert(chunk_info[i].sha, data_pos[i].container_id, 1);*/
        }
        index_insert(chunk_info[i].sha, data_pos[i].container_id, 1);
//CONTINUE:
//        ;
    }

    /*update the cache data container*/
    rc = update_data_cache(threadp->seqno);
    if(rc != 0){
        DCS_ERROR("dcslication update data cache err:%d \n",rc);
    }
    
EXIT:

    if(rc != 0){
        if(data_pos){
            free(data_pos);
            data_pos = NULL;
        }
    }

    return data_pos;
}

dcs_u32_t cache_delete(dcs_u8_t *sha, dcs_u64_t container_id)
{
    dcs_s32_t     i = 0;
    dcs_s32_t     j = 0;
    dcs_u32_t     block_num = 0;
    dcs_u32_t     bucket_id = 0;
    dcs_u32_t     sha_num = 0;
    dcs_u32_t     rc = 0;
    dcs_u32_t     sign = 0;
    dcs_u8_t      *tmpsha = NULL;


    /*get bucket_id according the front power bit of sha*/
    if(bucket_power < 8 || bucket_power == 8){
        //DCS_MSG("bucket power is less than 8 \n");
        bucket_id =(dcs_u32_t)(sign | sha[0]);
        bucket_id = sign >> (8-bucket_power);
    }
    else{
        //DCS_MSG("bucket power is larger than 8 \n");
        bucket_id = (dcs_u32_t)(sign | sha[0]);
        bucket_id = bucket_id << (bucket_power - 8);
        bucket_id = (dcs_u32_t)(bucket_id | (sha[1] >> (16-bucket_power)));
    }
    DCS_MSG("cache_query bucket id is %d \n", bucket_id);
    
    /*lock the bucket*/
    pthread_rwlock_wrlock(&cache_table->cache_lock[bucket_id]);

    sha_num = cache_table->sha_num[bucket_id];
    for(i=0; i<sha_num; i++){
        tmpsha = cache_table->sha_cache[bucket_id][i].sha;
        if(shastr_cmp(sha, tmpsha) == 0 && cache_table->sha_cache[bucket_id][i].container_id == container_id){
            sha_num--;
            memcpy(&cache_table->sha_cache[bucket_id][i], &cache_table->sha_cache[bucket_id][sha_num], sizeof(sha_cache_t));
            memset(&cache_table->sha_cache[bucket_id][sha_num], 0, sizeof(sha_cache_t));
            cache_table->sha_num[bucket_id] = sha_num;
            break;
        }
    }
    pthread_rwlock_unlock(&cache_table->cache_lock[bucket_id]);

    /*query the tmp cache table*/
    block_num = tmp_cache->block_num;
    for(i=0; i<block_num; i++){
        pthread_rwlock_wrlock(&tmp_cache->cache_lock[i]);
        sha_num = tmp_cache->sha_num[i];
        for(j=0; j<sha_num; j++){
            if((shastr_cmp(tmp_cache->sha_cache[i][j].sha, sha)) == 0 && tmp_cache->sha_cache[i][j].container_id == container_id){
                sha_num--;
                memcpy(&tmp_cache->sha_cache[i][j], &tmp_cache->sha_cache[i][sha_num], sizeof(sha_cache_t));
                memset(&tmp_cache->sha_cache[i][sha_num], 0 , sizeof(sha_cache_t));
                tmp_cache->sha_num[i] = sha_num;
                break;
            }
        }
        pthread_rwlock_unlock(&tmp_cache->cache_lock[i]);
    }
    return rc;
}

/*query the fp cache find out if the fp
 * is in the cache if yes return the dcs_datapos_t 
 * else return NULL
 */
dcs_datapos_t *cache_query(dcs_u8_t *sha)
{
    dcs_datapos_t *datapos = NULL;
    dcs_s32_t     i = 0;
    dcs_s32_t     j = 0;
    dcs_u32_t     block_num = 0;
    dcs_u32_t     bucket_id = 0;
    dcs_u32_t     sha_num = 0;
    dcs_u32_t     find = 0;
    dcs_u32_t     sign = 0;
    dcs_u8_t      *tmpsha = NULL;


    /*get bucket_id according the front power bit of sha*/
    if(bucket_power < 8 || bucket_power == 8){
        //DCS_MSG("bucket power is less than 8 \n");
        bucket_id =(dcs_u32_t)(sign | sha[0]);
        bucket_id = sign >> (8-bucket_power);
    }
    else{
        //DCS_MSG("bucket power is larger than 8 \n");
        bucket_id = (dcs_u32_t)(sign | sha[0]);
        bucket_id = bucket_id << (bucket_power - 8);
        bucket_id = (dcs_u32_t)(bucket_id | (sha[1] >> (16-bucket_power)));
    }
    DCS_MSG("cache_query bucket id is %d \n", bucket_id);
    
    /*lock the bucket*/
    pthread_rwlock_rdlock(&cache_table->cache_lock[bucket_id]);

    sha_num = cache_table->sha_num[bucket_id];
    for(i=0; i<sha_num; i++){
        tmpsha = cache_table->sha_cache[bucket_id][i].sha;
        if(shastr_cmp(sha, tmpsha) == 0){
            find = 1;
            datapos = (dcs_datapos_t *)malloc(sizeof(dcs_datapos_t));
            if(datapos == NULL){
                DCS_ERROR("cache_query malloc for datapos err:%d \n", errno);
                pthread_rwlock_unlock(&cache_table->cache_lock[bucket_id]);
                find = 0;
                goto EXIT;
            }
            datapos->compressor_id = compressor_this_id;
            datapos->container_id = cache_table->sha_cache[bucket_id][i].container_id;
            datapos->container_offset = cache_table->sha_cache[bucket_id][i].offset;

            pthread_rwlock_unlock(&cache_table->cache_lock[bucket_id]);
            goto EXIT;
        }
    }
    pthread_rwlock_unlock(&cache_table->cache_lock[bucket_id]);

    /*query the tmp cache table*/
    block_num = tmp_cache->block_num;
    for(i=0; i<block_num; i++){
        pthread_rwlock_rdlock(&tmp_cache->cache_lock[i]);

        sha_num = tmp_cache->sha_num[i];
        for(j=0; j<sha_num; j++){
            if((shastr_cmp(tmp_cache->sha_cache[i][j].sha, sha)) == 0){
                find = 1;
                datapos = (dcs_datapos_t *)malloc(sizeof(dcs_datapos_t));
                if(datapos == NULL){
                    DCS_ERROR("cache_query malloc for datapos err:%d \n", errno);
                    pthread_rwlock_unlock(&tmp_cache->cache_lock[i]);
                    find = 0;
                    goto EXIT;
                }
                datapos->compressor_id = compressor_this_id;
                datapos->container_id = tmp_cache->sha_cache[i][j].container_id;
                datapos->container_offset = tmp_cache->sha_cache[i][j].offset;
                
                pthread_rwlock_unlock(&tmp_cache->cache_lock[i]);
                goto EXIT;
            }
        }

        pthread_rwlock_unlock(&tmp_cache->cache_lock[i]);
    }

    datapos = query_container_buf(sha);
    if(datapos != NULL){
        find = 1;
        DCS_ERROR("query_container successed, find it in container buf \n");
    }

EXIT:
    if(find == 0){
        if(datapos != NULL){
            free(datapos);
            datapos = NULL;
        }
    }

    return datapos;
}

/*search  the cache list juge weather the container is in cache list*/
dcs_s32_t search_cache_list(dcs_u64_t container_id)
{
    dcs_s32_t rc = 0;
    container_list_t *head = NULL;


    pthread_rwlock_rdlock(&cache_list_lock);
    if(con_list == NULL){
        DCS_MSG("search_cache_list list is empty \n");
        pthread_rwlock_unlock(&cache_list_lock);
        goto EXIT;
    }

    head = con_list;
    while(head != NULL){
        /*if the container is the same, find the container*/
        if(head->container_id == container_id){
            DCS_MSG("search_cache_list find the container we wanted \n");
            rc = 1;
            pthread_rwlock_unlock(&cache_list_lock);
            goto EXIT;
        }

        head = head->next;
    }
    pthread_rwlock_unlock(&cache_list_lock);

EXIT:
    return rc;
}

/*if the container fp we wanted is not in the cache, 
 * we have to read them from the disk and insert them
 * into the cache hash table
 */
dcs_s32_t cache_fp(dcs_u64_t container_id)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s32_t read_fd = 0;
    dcs_u64_t filesize = 0;
    dcs_u32_t total_num = 0;
    //dcs_u32_t block_num = 0;
    dcs_u32_t sign = 0;

    dcs_u32_t tmp_num[bucket_num];
    dcs_u8_t *sha = NULL;
    dcs_s8_t *con_file = NULL;
    con_sha_t  *con_sha = NULL;
    container_list_t *tmp = NULL;
    container_list_t *head = NULL;
    struct stat f_state;


    /*get the whole container file path*/
    con_file = get_fpcon_name(container_id);
    read_fd = open(con_file, O_RDONLY | O_APPEND, 0666);
    if(read_fd < 0){
        DCS_ERROR("cache_fp open fp file %s err: %d \n",con_file, errno);
        rc = errno;
        goto EXIT;
    }

    if(stat(con_file, &f_state)){
        DCS_ERROR("cache_fp get container file state err:%d \n", errno);
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

    rc = read(read_fd, con_sha, filesize);
    if(rc != filesize){
        DCS_ERROR("cache_fp read sha container err: %d \n" ,rc );
        goto EXIT;
    }

    sha = (dcs_u8_t *)malloc(SHA_LEN);
    if(sha == NULL){
        DCS_ERROR("cache_fp malloc for sha err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    /*init the statistics array, tmp_num array is used for caculate 
     * the number of sha that each bucket would insert.
     * it is vary useful for helping cache eviction*/
    for(i=0; i<bucket_num; i++){
        tmp_num[i] = 0;
    }

    DCS_MSG("cache_fp the bucket power is %d \n", bucket_power);
    for(i=0; i<total_num; i++){
        DCS_MSG("i is %d \n", i);
        memcpy(sha, con_sha[i].sha, SHA_LEN);
        sign = 0;
        if(bucket_power < 8 || bucket_power == 8){
            sign =(dcs_u32_t)(sign | sha[0]);
            sign = sign >> (8-bucket_power);
        }
        else{
            sign = (dcs_u32_t)(sign | sha[0]);
            DCS_MSG("sign is %d \n", sign);
            sign = sign << (bucket_power - 8);
            DCS_MSG("sign is %d \n", sign);
            sign = (dcs_u32_t)(sign | (sha[1] >> (16-bucket_power)));
            DCS_MSG("sign is %d \n", sign);
        }
        DCS_MSG("sign is %d \n", sign);
        tmp_num[sign]++;
    }

    /*
    for(i=0; i<bucket_num; i++){
        if(tmp_num[i] > EACH_BLOCK_NUM){
            if((TMP_BLOCK_NUM - block_num) < BLOCK_NEED){
                DCS_MSG("cache_fp not enough room for tmp cache, so clean it\n");
                rc = clean_tmp_cache();
                if(rc != 0){
                    DCS_ERROR("cache_fp clean tmp cache err:%d \n", rc);
                    goto EXIT;
                }
                tmp_cache->block_num = 0;
                block_num = 0;        
            }
        }
    }
    */

    DCS_MSG("begin cache the fp \n");
    for(i=0; i<total_num; i++){
        rc = insert_cache(con_sha[i], tmp_num, container_id);
        if(rc != 0){
            DCS_ERROR("cache_fp insert sha value to cache err:%d \n", rc);
            goto EXIT;
        }
    }    

    /*insert the container id into container id list*/
    tmp = (container_list_t *)malloc(sizeof(container_list_t));
    tmp->container_id = container_id;
    
    pthread_rwlock_wrlock(&cache_list_lock);
    head = con_list;
    if(head == NULL){
        con_list = tmp;
        tmp->next = NULL;
        pthread_rwlock_unlock(&cache_list_lock);
        goto EXIT;
    }

    while(head->next){
        head = head->next;
    }
    head->next = tmp;
    tmp->next = NULL;
    pthread_rwlock_unlock(&cache_list_lock);

EXIT:
    if(sha != NULL){
        free(sha);
        sha = NULL;
    }

    if(read_fd){
        close(read_fd);
        read_fd = 0;
    }

    if(con_file != NULL){
        free(con_file);
        con_file = NULL;
    }

    if(con_sha != NULL){
        free(con_sha);
        con_sha = NULL;
    }

    return rc ;
}

/*after reading a sha container from the disk
 * we insert all the FPs of that container into 
 * the hash table*/
dcs_s32_t insert_cache(con_sha_t con_sha, dcs_u32_t *tmp_num, dcs_u64_t container_id)
{
    dcs_s32_t rc = 0;
    dcs_u32_t i = 0;
    dcs_u32_t sign = 0;
    dcs_u32_t bucket_id = 0;
    dcs_u32_t block_num = 0;
    dcs_u32_t sha_pos = 0;

    dcs_u8_t  *sha = NULL;

    
    sha = con_sha.sha;
    DCS_MSG("insert_cache bucket power is %d \n", bucket_power);

    /*get bucket_id according the front power bit of sha*/
    if(bucket_power < 8 || bucket_power == 8){
        bucket_id =(dcs_u32_t)(sign | sha[0]);
        bucket_id = sign >> (8-bucket_power);
    }
    else{
        bucket_id = (dcs_u32_t)(sign | sha[0]);
        bucket_id = bucket_id << (bucket_power - 8);
        bucket_id = (dcs_u32_t)(bucket_id | (sha[1] >> (16-bucket_power)));
    }
    //tmp_num[bucket_id] = 0;

    if(tmp_num[bucket_id] > EACH_BLOCK_NUM){
        DCS_MSG("insert_cache bucket:%d cannot cache all the %d fps \n",
                                        bucket_id ,tmp_num[bucket_id]);
        DCS_MSG("insert_cache we insert it to tmp cache\n");
        block_num = tmp_cache->block_num;
        /*
        if((TMP_BLOCK_NUM - block_num) < BLOCK_NEED){
            rc = clean_tmp_cache();
            if(rc != 0){
                DCS_ERROR("insert_cache clean tmp cache err:%d \n", rc);
                goto EXIT;
            }
            tmp_cache->block_num = 0;
            block_num = 0;        
        }
        */

        /*first sha insert into tmp_cache*/
        if(block_num == 0){
            pthread_rwlock_wrlock(&tmp_cache->cache_lock[block_num]);
            tmp_cache->bucket_id[block_num] = bucket_id;
            sha_pos = tmp_cache->sha_num[block_num];
            if(tmp_cache->sha_num[block_num] == 0){
                tmp_cache->sha_cache[block_num] = (sha_cache_t *)malloc(sizeof(sha_cache_t)*TMP_BLOCK_NUM);
                if(tmp_cache->sha_cache[block_num] == NULL){
                    DCS_ERROR("insert_cache malloc for %dth tmp cache err:%d \n", block_num, errno);
                    pthread_rwlock_unlock(&tmp_cache->cache_lock[block_num]);
                    rc = errno;
                    goto EXIT;
                }
            }
            /*block num + 1*/
            tmp_cache->block_num++;
            memcpy(tmp_cache->sha_cache[block_num][sha_pos].sha, con_sha.sha, SHA_LEN);
            tmp_cache->sha_cache[block_num][sha_pos].offset = con_sha.offset;
            tmp_cache->sha_cache[block_num][sha_pos].container_id = container_id;
            /*sha_num +1 */
            tmp_cache->sha_num[block_num]++;
            pthread_rwlock_unlock(&tmp_cache->cache_lock[block_num]);
        }
        else{
            /*it is not the first sha in tmp cache
             * find out weather it has the same bucket_id
             */
            for(i=0; i<block_num; i++){
                if(tmp_cache->bucket_id[i] == bucket_id){
                    sha_pos = tmp_cache->sha_num[i];
                    pthread_rwlock_wrlock(&tmp_cache->cache_lock[i]);
                    memcpy(tmp_cache->sha_cache[i][sha_pos].sha, con_sha.sha, SHA_LEN);
                    tmp_cache->sha_cache[i][sha_pos].offset = con_sha.offset;
                    tmp_cache->sha_cache[i][sha_pos].container_id = container_id;
                    tmp_cache->sha_num[i] = sha_pos + 1; 
                    pthread_rwlock_unlock(&tmp_cache->cache_lock[i]);
                }
            }

            if(i == block_num){
                DCS_MSG("insert_cache if tmp cache is full clean the cache\n");
                if(i == TMP_BLOCK_NUM){
                    rc = clean_tmp_cache();
                    if(rc != 0){
                        DCS_ERROR("insert_cache clean tmp cache err:%d \n", rc);
                        goto EXIT;
                    }
                    tmp_cache->block_num = 0;
                    block_num = 0;
                }

                pthread_rwlock_wrlock(&tmp_cache->cache_lock[block_num]);
                tmp_cache->bucket_id[block_num] = bucket_id;
                tmp_cache->sha_num[block_num] = 0;
                sha_pos = 0;

                tmp_cache->sha_cache[block_num] = (sha_cache_t *)malloc(sizeof(sha_cache_t)*TMP_BLOCK_NUM);
                if(tmp_cache->sha_cache[block_num] == NULL){
                    DCS_ERROR("insert_cache malloc for %dth tmp cache err:%d \n", block_num, errno);
                    rc = errno;
                    pthread_rwlock_unlock(&tmp_cache->cache_lock[block_num]);
                    goto EXIT;
                }

                /*block num + 1*/
                tmp_cache->block_num++;
                memcpy(tmp_cache->sha_cache[block_num][sha_pos].sha, con_sha.sha, SHA_LEN);
                tmp_cache->sha_cache[block_num][sha_pos].offset = con_sha.offset;
                tmp_cache->sha_cache[block_num][sha_pos].container_id = container_id;
                /*sha_num +1 */
                tmp_cache->sha_num[block_num]++;
                pthread_rwlock_unlock(&tmp_cache->cache_lock[block_num]);
            }
        }
    }
    else if((tmp_num[bucket_id] + cache_table->sha_num[bucket_id]) > EACH_BLOCK_NUM){
        /*in this situation the bucket has no enough room for new
         * enter sha value, so cache table will evict all sha from the 
         * same bucket.
         */
        DCS_MSG("insert_cache the %dth bucket is full clean it\n",bucket_id);
        DCS_MSG("insert_cache it will have %d shas will insert this bucket \n", tmp_num[bucket_id]);
        rc = clean_cache_bucket(bucket_id);
        if(rc != 0){
            DCS_ERROR("insert_cache clean %dth table bucket err:%d \n",bucket_id,  rc);
            goto EXIT;
        }

        pthread_rwlock_wrlock(&cache_table->cache_lock[bucket_id]);
        sha_pos = cache_table->sha_num[bucket_id];

        memcpy(cache_table->sha_cache[bucket_id][sha_pos].sha, con_sha.sha, SHA_LEN);
        cache_table->sha_cache[bucket_id][sha_pos].offset = con_sha.offset;
        cache_table->sha_cache[bucket_id][sha_pos].container_id = container_id;

        cache_table->sha_num[bucket_id]++;
        pthread_rwlock_unlock(&cache_table->cache_lock[bucket_id]);
        //tmp_num[bucket_id]--;
        DCS_MSG("insert_cache now have %d sha leaved to be inserted such bucket\n", tmp_num[bucket_id]);
    } 
    else{
        /*the bucket is not full, and have enough room for inserting all sha value
         * in the same bucket
         */
        DCS_MSG("insert_cache %dth bucket is not full,have enough room for all this bucket sha\n", bucket_id);
        
        pthread_rwlock_wrlock(&cache_table->cache_lock[bucket_id]);
        sha_pos = cache_table->sha_num[bucket_id];

        memcpy(cache_table->sha_cache[bucket_id][sha_pos].sha, con_sha.sha, SHA_LEN);
        cache_table->sha_cache[bucket_id][sha_pos].offset = con_sha.offset;
        cache_table->sha_cache[bucket_id][sha_pos].container_id = container_id;

        cache_table->sha_num[bucket_id] = sha_pos + 1;
        pthread_rwlock_unlock(&cache_table->cache_lock[bucket_id]);
        //tmp_num[bucket_id]--;
    }

    tmp_num[bucket_id]--;
EXIT:

    return rc;
}
    
/*clean the cache bucket*/
dcs_s32_t clean_cache_bucket(dcs_u32_t bucket_id)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_u32_t sha_num = 0;
    dcs_u64_t container_id = 0;

    container_list_t *head = NULL;
    container_list_t *tmp  = NULL;

    
    pthread_rwlock_wrlock(&cache_table->cache_lock[bucket_id]);

    sha_num = cache_table->sha_num[bucket_id];
    for(i=0; i<sha_num; i++){
        container_id = cache_table->sha_cache[bucket_id][i].container_id;
        pthread_rwlock_wrlock(&cache_list_lock);
        head = con_list; 
        if(head != NULL && head->container_id == container_id){
            con_list = head->next;
            free(head);
            head = NULL;
        }
        if(head != NULL && head->next != NULL){
            while(head->next){
                if(head->next->container_id == container_id){
                    tmp = head->next->next;
                    free(head->next);
                    head->next = tmp;
                    break;
                }
                head = head->next;
            }
        }
        pthread_rwlock_unlock(&cache_list_lock);
    }
    //memset(cache_table->sha_cache[bucket_id], 0, sizeof(sha_cache_t)*EACH_BLOCK_NUM);
    cache_table->sha_num[bucket_id] = 0;

    pthread_rwlock_unlock(&cache_table->cache_lock[bucket_id]);

    return rc;
}

/*when the tmp_cache is not enough for next sha container
 * we clean the tmp_cache ,as tmp_cache is not used usually
 * so we clean it with a simple stratage*/
dcs_s32_t clean_tmp_cache()
{
    dcs_s32_t rc = 0;
    dcs_u32_t i = 0;
    dcs_u32_t j = 0;
    dcs_u32_t sha_num = 0;
    dcs_u32_t block_num = 0;
    dcs_u64_t container_id = 0;

    container_list_t *head = NULL;
    container_list_t *tmp = NULL;

    
    block_num = tmp_cache->block_num;
    for(i=0; i<block_num; i++){
        pthread_rwlock_wrlock(&tmp_cache->cache_lock[i]);
        
        sha_num = tmp_cache->sha_num[i];
        for(j=0; j<sha_num; j++){
            container_id = tmp_cache->sha_cache[i][j].container_id;
            head = con_list; 
            if(head->container_id == container_id){
                con_list = head->next;
                free(head);
                head = NULL;
            }
            else{
                while(head->next){
                    if(head->next->container_id == container_id){
                        tmp = head->next->next;
                        free(head->next);
                        head->next = tmp;
                    }
                }
            }
        }
        tmp_cache->sha_num[i] = 0;
        tmp_cache->block_num = 0;

        pthread_rwlock_unlock(&tmp_cache->cache_lock[i]);
    }

    return rc;
}

dcs_u32_t get_bucket_power()
{
    dcs_s32_t rc = 0;
    dcs_u32_t count = 0;
    dcs_u32_t a;

    a = bucket_num;
    while(a){
        a = a/2;
        count++;
    }

    rc = count - 1;

    return rc; 
}

/*get data container*/
dcs_s8_t *get_data_container(dcs_u64_t  container_id, dcs_u32_t * size)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s32_t pos = -1;
    dcs_u32_t tmp_con_num = 0;

    dcs_s8_t *container_data = NULL;


    tmp_con_num = data_cache->con_num;

    //DCS_MSG("1 \n");
    DCS_MSG("the tmp con num is %d \n", tmp_con_num);
    for(i=0; i<tmp_con_num; i++){
        //DCS_MSG("i is %d \n", i);
        pthread_rwlock_rdlock(&data_cache->cache_lock[i]);
        //DCS_MSG("get the lock \n");
        if(data_cache->container_id[i] == container_id){
            DCS_MSG("get_data_container find the data container in the cache \n");
            pthread_rwlock_unlock(&data_cache->cache_lock[i]);
            pos = i;
            break;
        }
        pthread_rwlock_unlock(&data_cache->cache_lock[i]);
    }
    
    //DCS_MSG("2 \n");
    if(i == tmp_con_num){
        /*the container is not in the cache, read it from disk*/
        //DCS_MSG("3 \n");
        container_data = read_container(container_id, size);
        if(container_data == NULL){
            DCS_ERROR("get_data_container read container err:%d \n", errno);
            rc = errno;
            goto EXIT;
        }

        pthread_rwlock_wrlock(&data_cache->cache_lock[insert_pos]) ;

        if(data_cache->data_con[insert_pos] != NULL){
            free(data_cache->data_con[insert_pos]);
        }
        data_cache->data_con[insert_pos] = NULL;
        data_cache->data_con[insert_pos] = container_data;
        data_cache->container_id[insert_pos] = container_id;
        data_cache->con_size[insert_pos] = *size;
        //DCS_MSG("the errno is %d \n", errno);
        //tmp_con_num++;
        if(tmp_con_num < DATA_CACHE_SIZE)
            tmp_con_num++;
        data_cache->con_num = tmp_con_num;
        
        pthread_rwlock_unlock(&data_cache->cache_lock[insert_pos]);

        dcs_u32_t data_cache_size = 0;
        data_cache_size = DATA_CACHE_SIZE;
        insert_pos = (insert_pos + 1)%data_cache_size;
    }
    else{
        //DCS_MSG("5 \n");
        /*the container is in the cache, get it directly*/
        DCS_MSG("the data container is in the cache \n");
        //memcpy(container_data, data_cache->data_con[pos], strlen(data_cache->data_con[pos]));
        pthread_rwlock_rdlock(&data_cache->cache_lock[pos]);
        if(data_cache->data_con[pos]){
            container_data = data_cache->data_con[pos];
            *size = data_cache->con_size[pos];
        }
        else 
            DCS_ERROR("get data container err \n");
        pthread_rwlock_unlock(&data_cache->cache_lock[pos]);
    }

    //DCS_MSG("4 \n");
EXIT:
    return container_data;
}

dcs_s32_t update_data_cache(dcs_u32_t thread_id)
{
    dcs_s32_t rc = 0;
    dcs_u64_t container_id  = 0;
    dcs_s32_t i = 0;
    dcs_u32_t size = 0;


    if(con_table->data_con[thread_id] != NULL)
        container_id = con_table->data_con[thread_id]->container_id;

    for(i=0; i<DATA_CACHE_SIZE; i++){
        if(data_cache->container_id[i] == container_id && data_cache->data_con[i] != NULL){
            DCS_MSG("update the cache\n");
            DCS_MSG("container id is %ld i is %d\n",container_id, i);
            size = con_table->data_con[thread_id]->offset;
            pthread_rwlock_wrlock(&data_cache->cache_lock[i]);
            free(data_cache->data_con[i]);
            data_cache->data_con[i] = NULL;
            data_cache->con_size[i] = 0;
            data_cache->data_con[i] = (dcs_s8_t *)malloc(size);
            if(data_cache->data_con[i] == NULL){
                DCS_ERROR("update_data_cache malloc for data con err:%d \n",errno);
                rc = errno;
                pthread_rwlock_unlock(&data_cache->cache_lock[i]);
                goto EXIT;
            }
            data_cache->con_size[i] = size;
            memcpy(data_cache->data_con[i], con_table->data_con[thread_id]->con, size);
            pthread_rwlock_unlock(&data_cache->cache_lock[i]);
        }
    }

EXIT:

    return rc;
}

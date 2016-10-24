/*compressor index source file*/

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

exten_hash_t *fp_index = NULL;
//BLOOM *bf = NULL;
//pthread_rwlock_t bloom_lock;
BMAP *bmap = NULL;
pthread_rwlock_t bmap_lock;

tier_bloom *tbf = NULL;
dcs_s32_t tmp = 0;
/********************* by zhj start *********************/
//exten_hash_t *disk_fp_index = NULL;
//BMAP *disk_bmap = NULL;
//dcs_s32_t disk_fp_index_dirty = 0;
//pthread_mutex_t fp_file_lock = PTHREAD_MUTEX_INITIALIZER; /* lock the state of disk fp index file, read or write or query*/
//dcs_s32_t disk_fp_flag = 0, tmp = 0;
/********************* by zhj end ***********************/

dcs_s32_t __dcs_fp_index_reload()
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s32_t j = 0;
    dcs_s32_t file_size;
    dcs_s32_t total_num;
    dcs_s8_t * fpcon_name = NULL;
    struct stat fstat;
    dcs_s32_t rsize = 0;
    con_sha_t * con_sha = NULL;
    dcs_s32_t fd = 0; 
    dcs_u8_t  *sha_array = NULL;

    for(i = 1; ; i++){

        fpcon_name = get_fpcon_name(i);
        if(fpcon_name == NULL){
            DCS_ERROR("__dcs_fp_index_reload get fpcon_name err:%d \n", errno);
            rc = errno;
            goto EXIT;
        }

        rc = stat(fpcon_name, &fstat);
        if(rc){
            if(errno == ENOENT){
                rc = 0;
                goto EXIT;
            }
            DCS_ERROR("__dcs_fp_index_reload get fpcon file %s failed, errno:%d\n", fpcon_name, errno);
            goto EXIT;
        }
        
        file_size = (dcs_u64_t)(fstat.st_size);
        total_num = file_size/sizeof(con_sha_t);

        con_sha = (con_sha_t *)malloc(sizeof(con_sha_t) * total_num);
        if(con_sha == NULL){
            DCS_ERROR("__dcs_fp_index_reload malloc for con_sha err:%d \n", errno) ;
            rc = errno;
            goto EXIT;
        }
        memset(con_sha, 0, sizeof(con_sha_t) * total_num);

        fd = open(fpcon_name, O_RDONLY, 0666);
        if(fd < 0){
            DCS_ERROR("__dcs_fp_index_reload open file %s failed, errno:%d\n",fpcon_name, errno);
            rc = errno;
            goto EXIT;
        }

        rsize = read(fd, con_sha, sizeof(con_sha_t) * total_num);
        if(rsize != file_size){
            DCS_ERROR("__dcs_fp_index_reload read size[%d] not equal with filesize[%d]\n", rsize, file_size);
            rc = -1;
            goto EXIT;
        }
        close(fd);        

        sha_array = (dcs_u8_t *)malloc(total_num * SHA_LEN);
        if(NULL == sha_array){
            DCS_ERROR("__dcs_fp_index_reload malloc for sha_array failed, errno:%d\n",errno);
            rc = errno;
            goto EXIT;
        }
      
        for(j = 0; j < total_num; j++)
            memcpy(sha_array + j * SHA_LEN, con_sha[j].sha, SHA_LEN);
 
        dcs_insert_index(sha_array, NULL, total_num, i);

        if(sha_array){
            free(sha_array);
            sha_array = NULL;
        }
        if(con_sha){
            free(con_sha);
            con_sha = NULL;
        }
        if(fpcon_name){
            free(fpcon_name);
            fpcon_name = NULL;
        }

    }

EXIT:
    return rc;
}


/*init extendible hash*/   /***** by zhj ** this func will be called after saving fp index every time***/
dcs_s32_t __dcs_compressor_index_init()
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    
    /*init the query bmap*/
    bmap = bmap_create(EXTEN_HASH_SIZE * BUCKET_NUM);
    pthread_rwlock_init(&bmap_lock, NULL);

    fp_index = (exten_hash_t *)malloc(sizeof(exten_hash_t));
    if(fp_index == NULL){
        DCS_ERROR("__dcs_compressor_index_init malloc for fp_index err: %d\n",errno);
        rc = errno;
        goto EXIT;
    }
    
    fp_index->total_block_num = 0;  /********* by zhj *** be used for deciding whether save to disk*****/

    /*init the lock and split_depth*/
    fp_index->split_depth = 0;
    pthread_rwlock_init(&fp_index->depth_lock, NULL);
    for(i=0; i<EXTEN_HASH_SIZE; i++){
        fp_index->index_block[i] = NULL;
    }

    //__dcs_fp_index_reload();
    
EXIT:
    return rc;
}

dcs_s32_t __dcs_tier_bloom_init(){
        dcs_s32_t rc = 0;
        dcs_s32_t i = 0;
        dcs_u64_t tmp_u64;
        dcs_u32_t j = 0;
        dcs_s8_t ** bloom_name = NULL;
        dcs_s32_t read_fd = 0;
        dcs_s32_t read_size = 0;

        tbf = (tier_bloom *)malloc(sizeof(tier_bloom));
        if(NULL == tbf){
                DCS_ERROR("__dcs_tier_bloom_init malloc for tier_bloom failed, err:%d\n", errno);
                rc = errno;
                goto EXIT;
        }
                
        tbf = bloom_create1(COMPRESSOR_BLOOM_SIZE);
        read_fd = open(COMPRESSOR_BLOOM_PATH, O_RDONLY, 0666);
        if(read_fd < 0){
            if(ENOENT == errno){
                DCS_MSG("__dcs_tier_bloom_init the %dth bloom filter is not exist\n", i);
            	goto EXIT1;
            }
            DCS_ERROR("__dcs_tier_bloom_init open %dth bloom file err:%d \n",i ,errno);
            rc = errno;
            goto EXIT;
        }

        read_size = read(read_fd, &tmp_u64, sizeof(dcs_u64_t));
        if(read_size != sizeof(dcs_u64_t)){
        	DCS_ERROR("__dcs_tier_bloom_init read blocks_num from file failed\n");
                rc = -1;
                goto EXIT1;
        }

        tbf->blocks_num = tmp_u64;

        read_size = read(read_fd, tbf->bmap, tmp_u64/CHAR_BIT);
        if(read_size * CHAR_BIT != tmp_u64){
        	DCS_ERROR("__dcs_tier_bloom_init read bmap failed\n");
                rc = -1;
                goto EXIT1;
        }

        for(j = 0; j < tbf->blocks_num; j++){
        	dcs_u8_t tmp_level_num;
                dcs_u8_t tmp_real_level_num;
                dcs_s32_t k;
                read_size = read(read_fd, &tmp_level_num, sizeof(dcs_u8_t));
                if(read_size != sizeof(dcs_u8_t)){
                	DCS_ERROR("__dcs_tier_bloom_init read tier_bloom's %d blocks' level_num failed\n", j);
                        rc = -1;
                        goto EXIT1;
                }

		if(tmp_level_num == 0){
                        DCS_ERROR("__dcs_tier_bloom_init tier_bloom's %d is empty\n", j);
                        continue;
                }

                read_size = read(read_fd, &tmp_real_level_num, sizeof(dcs_u8_t));
                if(read_size != sizeof(dcs_u8_t)){
                        DCS_ERROR("__dcs_tier_bloom_init read tier_bloom's %d blocks' real_level_num failed\n", j);
                        rc = -1;
                        goto EXIT1;
                }

                bloom_block_init1(tbf, j, 2, tmp_level_num, tmp_real_level_num);

                read_size = read(read_fd, tbf->blocks[j]->bmap, BLOOM_BLOCK_SIZE/CHAR_BIT);

                if(read_size != BLOOM_BLOCK_SIZE/CHAR_BIT){
                        DCS_ERROR("__dcs_tier_bloom_init read tier_bloom's %d blocks' bmap failed\n", j);
                        rc = -1;
                        goto EXIT1;
                }

                for(k = 0; k < tbf->blocks[j]->real_level_num; k++){
                	read_size = read(read_fd, tbf->blocks[j]->a[k],sizeof(char) * BLOOM_BLOCK_SIZE);
                        if(read_size != BLOOM_BLOCK_SIZE){
                        	DCS_ERROR("__dcs_tier_bloom_init read tier_bloom's %d blocks' %d level bloom failed\n", j, k);
                                rc = -1;
                                goto EXIT1;
                        }
                }

                close(read_fd);
                read_fd = 0;
        }

EXIT:
        if(read_fd){
                close(read_fd);
        }
        for(i=0; i<DCS_COMPRESSOR_NUM; i++){
                if(bloom_name[i]){
                        free(bloom_name[i]);
                        bloom_name[i] = NULL;
                }
        }
EXIT1:

        return rc;
}

#if 0
/*init bloom filter 
 * if the old bloom filter is exist, read it from the disk
 * else create one
 */
dcs_s32_t __dcs_compressor_bloom_init()
{
    dcs_s32_t rc = 0;
    dcs_s32_t read_fd = 0;
    dcs_u64_t filesize = 0;
    dcs_u64_t read_size = 0;

    pthread_rwlock_init(&bloom_lock, NULL);

    pthread_mutex_init(&fp_file_lock, NULL);/** by zhj ** put it here because this func is called once**/    

    struct stat f_state;

    DCS_MSG("__dcs_compressor_bloom_init enter \n");

    if(stat(COMPRESSOR_BLOOM_PATH, &f_state) == -1){
        if(errno == 2){
            DCS_MSG("__dcs_compressor_bloom_init get bloom filter err because not exist \n");
            bf = bloom_create(COMPRESSOR_BLOOM_SIZE);
            if(bf == NULL){
                DCS_ERROR("__dcs_compressor_bloom_init bloom filter create err:%d ", errno);
                rc = errno;
                goto EXIT;
            }
            DCS_MSG("__dcs_bloom_init finish create the bloom filter \n");
            DCS_MSG("_dcs_bloom_init bloom filter size is %ld \n", bf->asize);
            goto EXIT;
        }
        DCS_ERROR("__dcs_compressor_bloom_init get bloom file state err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }

    filesize = f_state.st_size;
        
    read_fd = open(COMPRESSOR_BLOOM_PATH, O_RDONLY, 0666);
    if(read_fd < 0){
        DCS_ERROR("__dcs_compressor_bloom_init open bloom file err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
        
    read_size = read(read_fd, bf->a, filesize);
    if(read_size != filesize){
        DCS_ERROR("__dcs_compressor_bloom_init read bloom file err:%d \n", errno);
        rc = -1;
        goto EXIT;
    }
    bf->asize = read_size;
        
    close(read_fd);
    read_fd = 0;

EXIT:
    if(read_fd > 0){
        close(read_fd);
        read_fd = 0;
    }

    return rc;
}
#endif

dcs_s32_t dcs_delete_index(dcs_datamap_t *mapinfo, dcs_s32_t *chunk_num){
    dcs_s32_t rc = 0;
    dcs_u32_t i = 0;
    dcs_u32_t del = 0;

    pthread_rwlock_wrlock(&fp_index->depth_lock);
    for(i = 0; i < *chunk_num; i++){
        del = 0;
        cache_delete(mapinfo[i].sha, mapinfo[i].container_id);
#ifdef __DCS_TIER_BLOOM__
        bloom_delete(mapinfo[i].sha);
#endif       
        del = index_delete(mapinfo[i].sha, mapinfo[i].container_id);
        if(!del){
            dcs_datamap_t tmp;
            *chunk_num = *chunk_num - 1;
            memcpy(&tmp, &mapinfo[i], sizeof(dcs_datamap_t));
            memcpy(&mapinfo[i], &mapinfo[*chunk_num], sizeof(dcs_datamap_t));
            memcpy(&mapinfo[*chunk_num], &tmp, sizeof(dcs_datamap_t));
            i--;
        }
    }
    pthread_rwlock_unlock(&fp_index->depth_lock);
    return rc;
}

/*query the sample index*/
container_t *dcs_query_index(dcs_u8_t *sha, dcs_s32_t sha_num)
{
    dcs_s32_t rc = 0;
    dcs_u32_t i = 0;
    dcs_u32_t find = 0;
    dcs_u32_t container_num = 0;
    dcs_u8_t  *sha_v = NULL;
    dcs_u64_t container_id = 0 ;
    container_t *con_id = NULL;


    sha_v = (dcs_u8_t *)malloc(SHA_LEN);
    if(sha_v == NULL){
        DCS_ERROR("dcs_query_index malloc for sha_v err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(sha_v, 0, SHA_LEN);

    con_id = (container_t *)malloc(sizeof(container_t));
    if(con_id == NULL){
        DCS_ERROR("dcs_query_index malloc for container id err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(con_id, 0, sizeof(container_t));
    con_id->con_num = 0;
    con_id->c_id = (dcs_u64_t *)malloc(sizeof(dcs_u64_t)*sha_num);
    if(con_id->c_id == NULL){
        DCS_ERROR("dcs_query_index malloc for con_id->container err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
    memset(con_id->c_id, 0, sizeof(dcs_u64_t)*sha_num);

    for(i=0; i<sha_num; i++){
        find = 0;
        memcpy(sha_v, sha + SHA_LEN*i, SHA_LEN);

        /*search the bloom filter if existed in bloom filter, search the index*/ 
#ifdef __DCS_TIER_BLOOM__
        if(bloom_query(sha_v))
#endif
        {
            //pthread_mutex_lock(&fp_file_lock);   /****************** by zhj ********/
            find = index_query(sha_v, &container_id);
            //pthread_mutex_unlock(&fp_file_lock); /****************** by zhj ********/ 

            if(find){
                con_id->c_id[container_num] = container_id;
                container_num ++;
            }
        }
    }
    con_id->con_num = container_num;

EXIT:
    
    if(sha_v != NULL){
        free(sha_v);
        sha_v = NULL;
    }

    if(rc != 0){
        if(con_id != NULL){
            if(con_id->c_id != NULL){
                free(con_id->c_id);
                con_id->c_id = NULL;
            }
            free(con_id);
            con_id = NULL;
        }
    }

    return con_id;
}

/*insert the sha and container id into extendible hash table*/
dcs_s32_t dcs_insert_index(dcs_u8_t *sha, dcs_s32_t *ref_count, dcs_s32_t sha_num, dcs_u64_t container_id)
{
    dcs_s32_t rc = 0;
    dcs_u32_t i = 0;

#ifdef __DCS_TIER_BLOOM__ 
    /*first update the bloom filter*/
    rc = bloom_update(sha, sha_num);
    if(rc != 0){
        DCS_ERROR("update bloom filter err:%d \n", rc);
        goto EXIT;
    }
#endif

    for(i=0; i<sha_num; i++){
        dcs_s32_t ref = 1;
        if(NULL != ref_count){
            ref = ref_count[i];
        }
        rc = index_insert(sha + SHA_LEN*i, container_id, ref);
        if(rc != 0){
            DCS_ERROR("dcs_insert_index insert the %dth sha err:%d \n", i, rc);
            goto EXIT;
        }
    }

EXIT:
    return rc;
}

/********** by zhj****** start save fp index to disk when memory is used up */
#if 0
dcs_s32_t save_fp_index()
{
    dcs_s32_t index_fd, bmap_fd, bucket_id, wrc, rc = 0;
    dcs_u32_t split_depth;
    index_block_t *tmp_block;
    save_info_t *tmp_info;
    dcs_s32_t i;
    char bmap_fname[PATH_LEN], index_fname[PATH_LEN], id_buf[10];
    exten_hash_t *tmp_fp_index = NULL;
    BMAP *tmp_bmap = NULL;


    tmp_bmap = bmap_create(EXTEN_HASH_SIZE * BUCKET_NUM);
    tmp_fp_index = (exten_hash_t *)malloc(sizeof(exten_hash_t));
    if(fp_index == NULL){
        DCS_ERROR("__dcs_compressor_index_init malloc for fp_index err: %d\n",errno);
        rc = errno;
        goto EXIT;
    }
    for(i=0; i<EXTEN_HASH_SIZE; i++){
        tmp_fp_index->index_block[i] = NULL;
    }

    pthread_rwlock_wrlock(&fp_index->depth_lock);
    
    tmp_fp_index->total_block_num = fp_index->total_block_num;
    tmp_fp_index->split_depth = fp_index->split_depth;
    memcpy(tmp_fp_index->index_block, fp_index->index_block, EXTEN_HASH_SIZE * sizeof(index_block_t *));

    fp_index->total_block_num = 0;
    fp_index->split_depth = 0;
    memset(fp_index->index_block, 0, EXTEN_HASH_SIZE * sizeof(index_block_t *));
    pthread_rwlock_unlock(&fp_index->depth_lock);


    memset(index_fname, 0, PATH_LEN);
    memset(bmap_fname, 0, PATH_LEN);
    strcpy(bmap_fname, BMAP_PATH);
    strcpy(index_fname, INDEX_PATH);
    for(i = 1; ; ++i)
    {
        sprintf(id_buf, "%d", i);
        memcpy(index_fname + strlen(index_fname), id_buf, strlen(id_buf) + 1);
        if( access(index_fname, 0) != 0 )
            break;
    }
    memcpy(bmap_fname + strlen(bmap_fname), id_buf, strlen(id_buf) + 1);

    bmap_fd = open(bmap_fname, O_WRONLY|O_CREAT|O_APPEND, 0666);
    if(bmap_fd == -1)
    {
        DCS_ERROR("save_fp_index open bmap file err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    
    wrc = write(bmap_fd, &tmp_bmap->asize, sizeof(dcs_u64_t));
    if(wrc != sizeof(dcs_u64_t))
    {
        DCS_ERROR("save_fp_index write bmap->asize err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    wrc = write( bmap_fd, tmp_bmap->a, (EXTEN_HASH_SIZE * BUCKET_NUM + CHAR_BIT - 1)/CHAR_BIT );
    if(wrc != (EXTEN_HASH_SIZE * BUCKET_NUM + CHAR_BIT - 1)/CHAR_BIT)
    {
        DCS_ERROR("save_fp_index write bmap->a err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    free(tmp_bmap->a);
    tmp_bmap->a = NULL;
    
    free(tmp_bmap);
    tmp_bmap = NULL;

    close(bmap_fd);

    index_fd = open(index_fname, O_WRONLY|O_CREAT|O_APPEND, 0666);
    if(index_fd == -1)
    {
        DCS_ERROR("save_fp_index open index file err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    split_depth = tmp_fp_index->split_depth;
    wrc = write(index_fd, &split_depth, sizeof(dcs_u32_t) );
    if(wrc != sizeof(dcs_u32_t))
    {    
        DCS_ERROR("save_fp_index write split_depth err:%d \n", errno);
        rc = errno;
        goto EXIT;
    } 
    
    tmp_info = (save_info_t *) malloc( sizeof(save_info_t) );
    if(tmp_info == NULL)
    {
        DCS_ERROR("save_fp_index malloc for tmp info err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    for(bucket_id = 0; bucket_id < EXTEN_HASH_SIZE; ++bucket_id)
    {
        while( (tmp_block = tmp_fp_index->index_block[bucket_id]) != NULL)
        {
            tmp_fp_index->index_block[bucket_id] = tmp_fp_index->index_block[bucket_id]->next;
            tmp_block->next = NULL;
            tmp_info->block_id = tmp_block->block_id;
            tmp_info->bucket_id = bucket_id;
            tmp_info->sha_num = tmp_block->sha_num;
            tmp_info->block_depth = tmp_block->block_depth;
            memcpy(tmp_info->sha_conid, tmp_block->sha_conid, INDEX_BLOCK_SIZE * sizeof(sha_con_t));

            wrc = write(index_fd, tmp_info, sizeof(save_info_t) );
            if(wrc != sizeof(save_info_t))
            {
                DCS_ERROR("save_fp_index write block[%d] err:%d \n", bucket_id, errno);
                rc = errno;
                goto EXIT;
            }
            free(tmp_block);
        }
    }
    free(tmp_fp_index);
    tmp_fp_index = NULL;
    
    close(index_fd);

    if(tmp_info != NULL)
    {
        free(tmp_info);
        tmp_info = NULL;
    }

EXIT:
    return rc;
}
#endif
/************by zhj********end save fp index***********/

/*insert a key into index*/
dcs_s32_t index_insert(dcs_u8_t *sha, dcs_u64_t container_id, dcs_u32_t ref_count)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_u32_t block_id = 0;
    dcs_u32_t bucket_id = 0;
    dcs_u32_t index_depth = 0;
    dcs_s32_t find = 0;

    index_block_t *head = NULL;
    index_block_t *tmp_blockp = NULL;
    index_block_t *tmpblock1 = NULL;
    index_block_t *tmpblock2 = NULL;


    pthread_rwlock_wrlock(&fp_index->depth_lock);
    index_depth = fp_index->split_depth;

    block_id = get_block_id(sha, index_depth);
    
    bucket_id = block_id / BUCKET_NUM;
    head = fp_index->index_block[bucket_id];
    /*if index depth is 0, it means it may the first sha insert to the index*/
    if(head == NULL){
            DCS_MSG("index_insert the index depth is %d \n", index_depth);
            //pthread_rwlock_wrlock(&fp_index->depth_lock);
            fp_index->index_block[bucket_id]=(index_block_t *)malloc(sizeof(index_block_t));
            fp_index->total_block_num++;    /*********** by zhj *****************/
            
            head = fp_index->index_block[bucket_id];
            if(head == NULL){
                DCS_ERROR("index_insert malloc for first index block err:%d \n", errno);
                rc = errno;
                goto EXIT;
            }
            //pthread_rwlock_unlock(&fp_index->depth_lock);
            head->block_id = block_id;
            pthread_rwlock_init(&head->block_lock, NULL);

            pthread_rwlock_wrlock(&head->block_lock);
            head->sha_num = 0;
            head->block_depth = 0;
            head->next = NULL;
            
            rc = update_bmap(head->block_id);
            if(rc != 0){
                DCS_ERROR("index_insert update bit map err:%d \n", rc);
                pthread_rwlock_unlock(&head->block_lock);
                goto EXIT;
            }

            /*insert the sha key*/
            memcpy(head->sha_conid[head->sha_num].sha, sha, SHA_LEN);
            head->sha_conid[head->sha_num].con_id = container_id;
            head->sha_conid[head->sha_num].ref_count = ref_count;
            head->sha_num++;
            pthread_rwlock_unlock(&head->block_lock);
            DCS_MSG("index_insert inserted block_id is %ld \n", head->block_id);
    }
    else{
        DCS_MSG("index_insert the index depth is %d , larger then 0 \n",index_depth);
        //DCS_MSG("find the index block \n");
        while(head != NULL){
            if(head->block_id == block_id){
                find = 1;
                break;
            }
            head = head->next;
        }

        if(find){
            pthread_rwlock_wrlock(&head->block_lock);

            DCS_MSG("find the block \n");
            if(head->sha_num < INDEX_BLOCK_SIZE){
                dcs_s32_t i = 0;
                for(i = 0; i < head->sha_num; i++){
                    if(container_id == head->sha_conid[i].con_id && 0 == shastr_cmp(head->sha_conid[i].sha, sha)){
                        head->sha_conid[i].ref_count += ref_count;
                        break;
                    }
                }
                if(i == head->sha_num){
                    memcpy(head->sha_conid[head->sha_num].sha, sha, SHA_LEN);
                    head->sha_conid[head->sha_num].con_id = container_id;
                    head->sha_conid[head->sha_num].ref_count = ref_count;
                    head->sha_num++;
                }
                DCS_MSG("index_insert inserted block_id is %ld \n", head->block_id);
            }
            else if(head->sha_num == INDEX_BLOCK_SIZE){

                DCS_MSG("it is larger enough to split \n");
                /*init the first new index block*/
                tmpblock1 = (index_block_t *)malloc(sizeof(index_block_t));
                if(tmpblock1 == NULL){
                    DCS_ERROR("index_insert malloc for tmpblock1 err:%d \n",errno);
                    rc = errno;
                    pthread_rwlock_unlock(&head->block_lock);
                    goto EXIT;
                }
                memcpy(tmpblock1, head, sizeof(index_block_t));

                memset(head->sha_conid, 0, sizeof(sha_con_t) * INDEX_BLOCK_SIZE);
                head->sha_num = 0;
                head->block_depth ++;

                //DCS_MSG("3\n");
                /*init the second index block*/
                tmpblock2 = (index_block_t *)malloc(sizeof(index_block_t));
                if(tmpblock2 == NULL){
                    DCS_ERROR("index_insert malloc for tmpblock2 err:%d \n",errno);
                    rc = errno;
                    pthread_rwlock_unlock(&head->block_lock);
                    goto EXIT;
                }
                memset(tmpblock2, 0, sizeof(tmpblock2));
                //DCS_MSG("4\n");
                /*the second index block id*/
                tmpblock2->block_id = head->block_id + (1 << (head->block_depth - 1));
                tmpblock2->sha_num = 0;
                tmpblock2->block_depth = head->block_depth;
                pthread_rwlock_init(&tmpblock2->block_lock, NULL);
                pthread_rwlock_wrlock(&tmpblock2->block_lock);
                //DCS_MSG("5\n");
                tmp_blockp = fp_index->index_block[(tmpblock2->block_id)/BUCKET_NUM];
                fp_index->index_block[(tmpblock2->block_id)/BUCKET_NUM] = tmpblock2;
                tmpblock2->next = tmp_blockp;

                /*update the index depth*/
                index_depth = fp_index->split_depth;
                
                fp_index->total_block_num++;    /****************** by zhj ************/
                if(index_depth < head->block_depth){
                    fp_index->split_depth++;
                    DCS_ERROR("index_insert the index depth is %d \n", fp_index->split_depth);
                }
                rc = update_bmap(tmpblock2->block_id);
                if(rc != 0){
                    DCS_ERROR("index_insert update bit map err:%d \n", rc);
                    pthread_rwlock_unlock(&tmpblock2->block_lock);
                    pthread_rwlock_unlock(&head->block_lock);
                    goto EXIT;
                }

                /*insert all origin index block sha into index*/
                for(i = INDEX_BLOCK_SIZE; i >= 0; i--){
                    dcs_u8_t * tmp_sha = NULL;
                    dcs_u32_t tmp_con_id = -1;
                    dcs_u32_t tmp_block_id;
                    dcs_u32_t tmp_ref_count = ref_count;
                    dcs_s32_t j = 0;
                    if(i == INDEX_BLOCK_SIZE){
                        tmp_sha = sha;
                        tmp_con_id = container_id;
                    }else{
                        tmp_sha = tmpblock1->sha_conid[i].sha;
                        tmp_con_id = tmpblock1->sha_conid[i].con_id;
                        tmp_ref_count = tmpblock1->sha_conid[i].ref_count;
                    }
                    tmp_block_id = get_block_id(tmp_sha, fp_index->split_depth); 
                    if(tmp_block_id == tmpblock2->block_id || tmp_block_id == head->block_id){
                        index_block_t * tmp_block = (tmp_block_id == tmpblock2->block_id)? tmpblock2:head;
                        if(tmp_block->sha_num  == INDEX_BLOCK_SIZE){
                            pthread_rwlock_unlock(&tmpblock2->block_lock);
                            pthread_rwlock_unlock(&head->block_lock);
                            pthread_rwlock_unlock(&fp_index->depth_lock);
                            DCS_ERROR("index_insert recall index_insert,  block_id : %d, depth: %d -  %d , sha_num: %d  sha[16-19]: %d-%d-%d-%d\n",tmp_block_id, tmp_block->block_depth, fp_index->split_depth, tmp_block->sha_num,sha[16],sha[17],sha[18],sha[19]);
                            rc = index_insert(tmp_sha,tmp_con_id, tmp_ref_count);
                            if(rc != 0){
                                DCS_ERROR("index_insert in block spliting insert the %dth sha err:%d \n", i, rc);
                            }
                            goto EXIT1;
                        }
                        for(j = 0; j < tmp_block->sha_num; j ++){
                            if(tmp_con_id == tmp_block->sha_conid[j].con_id && 0 == shastr_cmp(tmp_block->sha_conid[j].sha, tmp_sha)){
                                tmp_block->sha_conid[j].ref_count += tmp_ref_count;
                                break;
                            }
                        }
                        if(j == tmp_block->sha_num){
                            memcpy(tmp_block->sha_conid[tmp_block->sha_num].sha, tmp_sha, SHA_LEN);
                            tmp_block->sha_conid[tmp_block->sha_num].con_id = tmp_con_id;
                            tmp_block->sha_conid[tmp_block->sha_num].ref_count = tmp_ref_count;
                            tmp_block->sha_num++;
                        }
                        DCS_MSG("index_insert inserted block_id is %ld \n", tmp_block->block_id);
                    }else{
                        DCS_ERROR("index_insert can not find the index block to insert, failed\n");
                        pthread_rwlock_unlock(&tmpblock2->block_lock);
                        pthread_rwlock_unlock(&head->block_lock);
                        goto EXIT;
                    }
                }

                if(tmpblock1 != NULL){
                    free(tmpblock1);
                    tmpblock1 = NULL;
                }
                pthread_rwlock_unlock(&tmpblock2->block_lock);
            }
            pthread_rwlock_unlock(&head->block_lock);
        }
        else{
            index_block_t * tmp_block = (index_block_t *)malloc(sizeof(index_block_t));
            if(NULL == tmp_block){
                DCS_ERROR("index_insert malloc for new index_block_t failed, err:%d\n", errno);
                rc = errno;
                goto EXIT;
            }
            memset(tmp_block, 0, sizeof(index_block_t));
            tmp_block->next = fp_index->index_block[bucket_id];
            fp_index->index_block[bucket_id]=tmp_block;
            fp_index->total_block_num++;    /*********** by zhj *****************/
            
            head = fp_index->index_block[bucket_id];
            if(head == NULL){
                DCS_ERROR("index_insert malloc for first index block err:%d \n", errno);
                rc = errno;
                goto EXIT;
            }
            //pthread_rwlock_unlock(&fp_index->depth_lock);
            head->block_id = block_id;
            pthread_rwlock_init(&head->block_lock, NULL);

            pthread_rwlock_wrlock(&head->block_lock);
            head->sha_num = 0;
            head->block_depth = fp_index->split_depth;
            head->next = NULL;
            
            rc = update_bmap(head->block_id);
            if(rc != 0){
                DCS_ERROR("index_insert update bit map err:%d \n", rc);
                pthread_rwlock_unlock(&head->block_lock);
                goto EXIT;
            }

            /*insert the sha key*/
            memcpy(head->sha_conid[head->sha_num].sha, sha, SHA_LEN);
            head->sha_conid[head->sha_num].con_id = container_id;
            head->sha_conid[head->sha_num].ref_count = ref_count;
            head->sha_num++;
            pthread_rwlock_unlock(&head->block_lock);
        }
    }
EXIT: 
    pthread_rwlock_unlock(&fp_index->depth_lock);

EXIT1:
    return rc;
}

/*get block id from the split depth and bitmap query*/
dcs_u32_t get_block_id(dcs_u8_t *sha, dcs_u32_t index_depth)
{
    dcs_s32_t i = 0;
    dcs_u32_t block_id = 0;
    dcs_u32_t pos0 = 0;
    dcs_u32_t pos1 = 0;


    if(index_depth == 0){
        block_id = 0;
    }
    else{
        pos0 = index_depth / 8;
        pos1 = index_depth % 8;
        block_id = (dcs_u32_t)(sha[19 - pos0] & ((1 << pos1) - 1));
        DCS_MSG("block id is %d , pos0 is %d, pos1 is %d \n", block_id, pos0, pos1);
        for(i=pos0; i>0; i--){
            block_id = block_id << 8;
            block_id = (dcs_u32_t)(block_id | sha[SHA_LEN-i]);
        }
        DCS_MSG("get_block_id before check the bitmap the block id is %d \n", block_id);

        /*begin to check the bitmap*/
        pthread_rwlock_rdlock(&bmap_lock);
        for(i=index_depth; i>=0; i--){
            block_id = block_id & ((1 << i) - 1);
            if(GETBIT(bmap->a, (block_id%(bmap->asize)))){
                DCS_MSG("get_block_id successed check bitmap the block_id is %d \n", block_id);
                break;
            }
        }
        pthread_rwlock_unlock(&bmap_lock);
    }
    
    return block_id;
}

/*update bitmap */
dcs_s32_t update_bmap(dcs_u32_t block_id)
{
    pthread_rwlock_wrlock(&bmap_lock);
    SETBIT(bmap->a, block_id % (bmap->asize));
    pthread_rwlock_unlock(&bmap_lock);
    return 0;
}

dcs_s32_t reset_bmap(dcs_u32_t block_id){
    pthread_rwlock_wrlock(&bmap_lock);
    RESETBIT(bmap->a, block_id % (bmap->asize));
    pthread_rwlock_unlock(&bmap_lock);
    return 0;
}

#if 0
/*************** by zhj ****** read exten hash table from disk ********/
dcs_s32_t read_fp_index(char index_fname[PATH_LEN], char bmap_fname[PATH_LEN])
{
    dcs_s32_t  index_fd, bmap_fd, rdc, i, rc = 0;
    save_info_t *tmp_info;
    index_block_t *tmp_block;


    disk_fp_index = (exten_hash_t *) malloc( sizeof(exten_hash_t) );
    if(disk_fp_index == NULL)
    {
        DCS_ERROR("read_fp_index malloc for disk_fp_index err: %d\n",errno);
        rc = errno;
        goto EXIT;
    }
    disk_fp_index->split_depth = 0;
    pthread_rwlock_init(&disk_fp_index->depth_lock, NULL);
    pthread_rwlock_wrlock(&disk_fp_index->depth_lock);
    for(i = 0; i < EXTEN_HASH_SIZE; i++)
    {
        disk_fp_index->index_block[i] = NULL;
    }

    index_fd = open(index_fname, O_RDONLY);
    if( index_fd == -1 )
    {
        DCS_ERROR("read_fp_index open index err: %d\n",errno);
        rc = errno;
        goto EXIT;
    }
    
    DCS_MSG("read_fp_index read index split depth start:\n");
    rdc = read(index_fd, &disk_fp_index->split_depth, sizeof(dcs_u32_t));
    if( rdc != sizeof(dcs_u32_t) )
    {
        DCS_ERROR("read_fp_index read split_depth err: %d\n",errno);
        rc = errno;
        goto EXIT;
    }
    DCS_MSG("read_fp_index read index split depth end\n");

    DCS_MSG("read_fp_index read index block start:\n");
    tmp_info = (save_info_t *) malloc(sizeof(save_info_t));
    while( read(index_fd, tmp_info, sizeof(save_info_t)) == sizeof(save_info_t) )
    {
        tmp_block = (index_block_t *) malloc( sizeof(index_block_t) );
        if(tmp_block == NULL)
        {
            DCS_ERROR("read_fp_index for %d malloc err: %d\n",i,errno);
            rc = errno;
            goto EXIT;
        }
        memset(tmp_block, 0, sizeof(index_block_t));
        pthread_rwlock_init(&tmp_block->block_lock, NULL);
        tmp_block->block_id = tmp_info->block_id;
        tmp_block->sha_num = tmp_info->sha_num;
        tmp_block->block_depth = tmp_info->block_depth;
        memcpy(tmp_block->sha_conid, tmp_info->sha_conid, INDEX_BLOCK_SIZE * sizeof(sha_con_t));

        if(disk_fp_index->index_block[tmp_info->bucket_id] == NULL)
        {
            tmp_block->next = NULL;
            disk_fp_index->index_block[tmp_info->bucket_id] = tmp_block;
        }
        else
        {
            tmp_block->next = disk_fp_index->index_block[tmp_info->bucket_id];
            disk_fp_index->index_block[tmp_info->bucket_id] = tmp_block;
        }
        
        tmp_block = NULL;
    }
    
    DCS_MSG("read_fp_index read index block end\n");

    close(index_fd);
    pthread_rwlock_unlock(&disk_fp_index->depth_lock);

    if(tmp_info != NULL)
    {
        free(tmp_info);
        tmp_info = NULL;
    }


    disk_bmap = bmap_create(EXTEN_HASH_SIZE * BUCKET_NUM);
    
    bmap_fd = open(bmap_fname, O_RDONLY);
    if( bmap_fd == -1 )
    {
        DCS_ERROR("read_fp_index open bmap err: %d\n",errno);
        rc = errno;
        goto EXIT;
    }

    DCS_MSG("read_fp_index read bmap start:\n");
    rdc = read(bmap_fd, &disk_bmap->asize, sizeof(dcs_u64_t));
    if(rdc != sizeof(dcs_u64_t))
    {
        DCS_ERROR("read_fp_index read bmap asize err: %d\n",errno);
        rc = errno;
        goto EXIT;
    }

    rdc = read(bmap_fd, disk_bmap->a, (EXTEN_HASH_SIZE * BUCKET_NUM + CHAR_BIT - 1)/CHAR_BIT );
    if(rdc != (EXTEN_HASH_SIZE * BUCKET_NUM + CHAR_BIT - 1)/CHAR_BIT )
    {
        DCS_ERROR("read_fp_index read array a err: %d\n",errno);
        rc = errno;
        goto EXIT;
    }
    DCS_MSG("read_fp_index read bmap end\n");
    close(bmap_fd);

EXIT:
    return rc;
}
/******** by zhj *** end ***********************************/

/*********** by zhj ***** clean the disk_fp_index and disk_bmap for rewrite ***********/
void clean_disk_index()
{
    dcs_s32_t i = 0;
    index_block_t *tmp_block = NULL;
    if(disk_fp_index_dirty){
        resync_disk_fp_index(disk_fp_flag);
        disk_fp_index_dirty = 0;
    }
    if(disk_fp_index != NULL)    /* free the memory of disk_fp_index*/
    {
        for(i = 0; i < EXTEN_HASH_SIZE; ++i)
        {
            while( (tmp_block = disk_fp_index->index_block[i]) != NULL)
            {
                disk_fp_index->index_block[i] = disk_fp_index->index_block[i]->next;
                tmp_block->next = NULL;
                free(tmp_block);
                tmp_block = NULL;
            }
        }
        free(disk_fp_index);
        disk_fp_index = NULL;
    }

    if(disk_bmap != NULL)   /* free the memory of disk_bmap */
    {
        free(disk_bmap->a);
        disk_bmap->a = NULL;
        free(disk_bmap);
        disk_bmap = NULL;
    }
}
/********* by zhj ********** end ******************/

/**************** by zhj **********  query the disk extendible hash index **************/
dcs_u32_t disk_index_query(dcs_u8_t *sha, dcs_u64_t *container_id)
{
    dcs_s32_t i = 0;
    dcs_u32_t tmp_block_id = 0;
    dcs_u32_t block_id = 0;
    dcs_u32_t bucket = 0;

    dcs_u32_t pos0 = 0;
    dcs_u32_t pos1 = 0;
    dcs_u32_t depth = 0;
    
    dcs_u32_t find = 0;
    
    index_block_t *head = NULL;

    tmp_block_id = (dcs_u32_t)tmp_block_id;
    depth = disk_fp_index->split_depth;
    
    pos0 = depth / CHAR_BIT;
    pos1 = depth % CHAR_BIT;
    
    tmp_block_id = (dcs_u32_t)(sha[19 - pos0] & ((1 << pos1) - 1));
    for(i = pos0; i > 0; i--)
    {
        tmp_block_id = tmp_block_id << CHAR_BIT;
        tmp_block_id = (dcs_u32_t)(tmp_block_id | sha[SHA_LEN-i]);
    }
    
    DCS_MSG("disk_index_qeury pos0 is %d pos1 is %d \n", pos0, pos1);
    DCS_MSG("disk_index_query the block id is %d depth is %d \n", tmp_block_id, depth);

    for(i = depth; i >= 0; i--)
    {
        block_id = tmp_block_id & ((1 << i) - 1);
        if( GETBIT(disk_bmap->a, ( block_id % (EXTEN_HASH_SIZE * BUCKET_NUM) ) ) )
        {
            find = 1;
            break;
        }
    }

    if(find == 1)
    {
        find = 0;
        bucket = block_id / BUCKET_NUM;
        head = disk_fp_index->index_block[bucket];
        while(head != NULL)
        {
            if(head->block_id == block_id)
            {
                break;
            }
            head = head->next;
        }

        if(head == NULL)
        {
            DCS_MSG("disk_index_query can not find the block \n");
            goto EXIT;
        }

        for(i = 0; i < head->sha_num; i++)
        {
            if( shastr_cmp(sha, head->sha_conid[i].sha ) == 0 )
            {
                *container_id = head->sha_conid[i].con_id;
                //head->sha_conid[i].ref_count++;
                //disk_fp_index_dirty = 1;
                DCS_MSG("disk_index_query find block id is %ld the container id %ld\n",
                        head->block_id, head->sha_conid[i].con_id);
                find = 1;
                goto EXIT;
            }
        }
        
        DCS_MSG("can not find the fp in index %ld i is %d sha_num is %d \n",
                head->block_id, i, head->sha_num);
    }

EXIT:
    return find;
}

dcs_u32_t disk_index_delete(dcs_u8_t *sha, dcs_u64_t container_id, dcs_u32_t * delete)
{
    dcs_s32_t i = 0;
    dcs_u32_t tmp_block_id = 0;
    dcs_u32_t block_id = 0;
    dcs_u32_t bucket = 0;
    dcs_u32_t depth = 0;
    dcs_u32_t find = 0;
    index_block_t *head = NULL;

    tmp_block_id = (dcs_u32_t)tmp_block_id;
    depth = disk_fp_index->split_depth;
    block_id = get_block_id(sha, depth);

    //if(find == 1)
    {
        find = 0;
        bucket = block_id / BUCKET_NUM;
        head = disk_fp_index->index_block[bucket];
        while(head != NULL)
        {
            if(head->block_id == block_id)
            {
                break;
            }
            head = head->next;
        }

        if(head == NULL)
        {
            DCS_MSG("disk_index_delete can not find the block \n");
            goto EXIT;
        }
        pthread_rwlock_wrlock(&head->block_lock);
        for(i = 0; i < head->sha_num; i++)
        {
            if(container_id == head->sha_conid[i].con_id && 0 == shastr_cmp(sha, head->sha_conid[i].sha))
            {
                find = 1;
                head->sha_conid[i].ref_count --;
                if(0 == head->sha_conid[i].ref_count){
                    *delete = 1;
                    memcpy(&head->sha_conid[i], &head->sha_conid[--head->sha_num], sizeof(sha_con_t));
                    memset(&head->sha_conid[head->sha_num], 0, sizeof(sha_con_t));
                    DCS_MSG("disk_index_delete find block id is %ld the container id %ld\n", head->block_id, head->sha_conid[i].con_id);
                    //recycel mem
                }
                break;
            }
        }
        pthread_rwlock_unlock(&head->block_lock);
    }

EXIT:
    return find;
}
#endif
/************* by zhj ********* end query disk fp************/

/*query the extendible hash index*/
dcs_u32_t index_query(dcs_u8_t *sha, dcs_u64_t *container_id)
{
    dcs_s32_t i = 0;
    dcs_u32_t block_id = 0;
    dcs_u32_t bucket = 0;
    dcs_u32_t depth = 0;
    dcs_u32_t find = 0;
    index_block_t *head = NULL;

    /************* by zhj *****************/
    //dcs_s8_t index_fname[PATH_LEN], bmap_fname[PATH_LEN], id_buf[10];
    
    pthread_rwlock_rdlock(&fp_index->depth_lock);
    depth = fp_index->split_depth;
    block_id = get_block_id(sha, depth);
    
    /*according to the block id search the hash bucket*/
    //if(find == 1)
    {
        find = 0;
        bucket = block_id / BUCKET_NUM;
        head = fp_index->index_block[bucket];
        while(head != NULL){
            if(head->block_id == block_id){
                break;
            }
            head = head->next;
        }

        /*if cannot find the index block goto exit*/
        if(head == NULL){
            DCS_MSG("index_query can not find the block \n");
            pthread_rwlock_unlock(&fp_index->depth_lock);
            goto EXIT;
        }

        /*lock the index block and find the sha1 */
        pthread_rwlock_rdlock(&head->block_lock);
        for(i=0; i<head->sha_num; i++){
            if((shastr_cmp(sha, head->sha_conid[i].sha)) == 0){
                *container_id = head->sha_conid[i].con_id;
                //head->sha_conid[i].ref_count ++;
                DCS_ERROR("index_query find block id is %ld the container id %ld\n",
                        head->block_id, head->sha_conid[i].con_id);
                find = 1;
                pthread_rwlock_unlock(&head->block_lock);
                pthread_rwlock_unlock(&fp_index->depth_lock);
                goto EXIT;
            }
        }
        pthread_rwlock_unlock(&head->block_lock);
    }
    pthread_rwlock_unlock(&fp_index->depth_lock);
    goto EXIT;
    #if 0
    /*********** by zhj ******** first query the disk fp ***********/
    memset(index_fname, 0, PATH_LEN);
    memset(bmap_fname, 0, PATH_LEN);
    strcpy(index_fname, INDEX_PATH);
    strcpy(bmap_fname, BMAP_PATH);

    if( disk_fp_index != NULL ) /* if the disk_fp_index is not NULL,then query it,and skip it later */
    {
        find = disk_index_query(sha, container_id);
        if(find)
        {
            DCS_MSG("index_query disk_fp_index query 0 hit \n");
            goto EXIT;
        }
        DCS_MSG("index_query disk_fp_index query 0 not hit \n");
    }
    for(i = 1; ; ++i)
    {        
            if(i == disk_fp_flag)    /* disk_fp_flag record the number of file which has been read and queried */
                continue;

            sprintf(id_buf, "%d", i);
            memcpy(index_fname + strlen(INDEX_PATH), id_buf, strlen(id_buf) + 1);
            if( access(index_fname,0) == 0 ) 
            {/*note by weizheng, the logical is right or not?*/ 
                memcpy(bmap_fname + strlen(BMAP_PATH), id_buf, strlen(id_buf) + 1);
                clean_disk_index();
                if( read_fp_index(index_fname, bmap_fname) != 0 )
                {
                    DCS_ERROR("index_query read fp index err: %d \n",errno);
                }
                tmp = i;  /* tmp record the file that just be read */
            }
            else
            {
                DCS_MSG("index_query %s not exist", index_fname);
                break;
            }

            if( disk_fp_index != NULL)
            {   
                pthread_rwlock_rdlock(&disk_fp_index->depth_lock);         
                find = disk_index_query(sha, container_id);
                if(find)
                {
                    disk_fp_flag = tmp;  /* end of this block , disk_fp_flag record the number of the file that be read last */
                    DCS_MSG("index_query disk_fp_index query 1 hit \n");
                    pthread_rwlock_unlock(&disk_fp_index->depth_lock);
                    goto EXIT;
                }
                pthread_rwlock_unlock(&disk_fp_index->depth_lock);
                DCS_MSG("index_query disk_fp_index query 1 not hit ,index_query start\n");
            }
       
    }
    disk_fp_flag = tmp;
    /**************** by zhj *********** end *************/
    #endif
EXIT:
    return find;
}

dcs_u32_t index_delete(dcs_u8_t *sha, dcs_u64_t container_id)
{
    //dcs_u32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_u32_t block_id = 0;
    dcs_u32_t bucket = 0;
    dcs_u32_t depth = 0;
    dcs_u32_t find = 0;
    dcs_u32_t todelete = 0;
    index_block_t *head = NULL;
    index_block_t *pre = NULL;
    /************* by zhj *****************/
    //dcs_s8_t index_fname[PATH_LEN], bmap_fname[PATH_LEN], id_buf[10];
    
    //pthread_rwlock_wrlock(&fp_index->depth_lock);
    depth = fp_index->split_depth;
    block_id = get_block_id(sha, depth);
    /*according to the block id search the hash bucket*/
    //if(find == 1)
    {
        find = 0;
        bucket = block_id / BUCKET_NUM;
        head = fp_index->index_block[bucket];
        pre = head;
        while(head != NULL){
            if(head->block_id == block_id){
                break;
            }
            pre = head;
            head = head->next;
        }
        if(head == NULL){
            //pthread_rwlock_unlock(&fp_index->depth_lock);
            goto EXIT;
        }

        /*lock the index block and find the sha1 */
        pthread_rwlock_wrlock(&head->block_lock);
        for(i=0; i<head->sha_num; i++){

            if(container_id == head->sha_conid[i].con_id && shastr_cmp(sha, head->sha_conid[i].sha) == 0){
                --head->sha_conid[i].ref_count;
                find = 1;
                if(0 == head->sha_conid[i].ref_count){
                    memcpy(&head->sha_conid[i], &head->sha_conid[--head->sha_num], sizeof(sha_con_t));
                    memset(&head->sha_conid[head->sha_num], 0, sizeof(sha_con_t)); 
                    todelete = 1;
                }
                
                if(0 == head->sha_num){
                    reset_bmap(head->block_id);
                    if(pre == head){
                        fp_index->index_block[bucket] = head->next;
                    }else {
                        pre -> next = head->next;
                    }
                    pthread_rwlock_unlock(&head->block_lock);
                    pthread_rwlock_destroy(&head->block_lock);
                    memset(head, 0, sizeof(index_block_t));
                    free(head);
                }else{
                    pthread_rwlock_unlock(&head->block_lock);
                }
                //pthread_rwlock_unlock(&fp_index->depth_lock);
                goto EXIT;
            }
        }
        DCS_MSG("can not find the fp in index %ld i is %d sha_num is %d \n",
                    head->block_id, i, head->sha_num);
        pthread_rwlock_unlock(&head->block_lock);
    }
    //pthread_rwlock_unlock(&fp_index->depth_lock);
    goto EXIT;
#if 0
DISK_FP_INDEX:
    /*********** by zhj ******** first query the disk fp ***********/
    memset(index_fname, 0, PATH_LEN);
    memset(bmap_fname, 0, PATH_LEN);
    strcpy(index_fname, INDEX_PATH);
    strcpy(bmap_fname, BMAP_PATH);

    if( disk_fp_index != NULL ) /* if the disk_fp_index is not NULL,then query it,and skip it later */
    {
        find = disk_index_delete(sha, container_id, &todelete);
        if(find)
        {
            DCS_MSG("index_delete disk_fp_index query 0 hit \n");
            if(todelete){
                disk_fp_index_dirty = 1;
            }
            goto EXIT;
        }
        DCS_MSG("index_delete disk_fp_index query 0 not hit \n");
    }
    for(i = 1; ; ++i)
    {        
            sprintf(id_buf, "%d", i);
            memcpy(index_fname + strlen(INDEX_PATH), id_buf, strlen(id_buf) + 1);
            if( access(index_fname,0) == 0 ) 
            {/*note by weizheng, the logical is right or not?*/ 
                memcpy(bmap_fname + strlen(BMAP_PATH), id_buf, strlen(id_buf) + 1);
                clean_disk_index();
                if( read_fp_index(index_fname, bmap_fname) != 0 )
                {
                    DCS_ERROR("index_delete read fp index err: %d \n",errno);
                }
                disk_fp_flag = i;  /* tmp record the file that just be read */
            }
            else
            {
                DCS_MSG("index_delete file %s not exist\n", index_fname);
                break;
            }

            if( disk_fp_index != NULL)
            {            
                find = disk_index_delete(sha, container_id, &todelete);
                if(find)
                {
                    if(todelete){
                        disk_fp_index_dirty = 1;
                    }
                    DCS_MSG("index_delete disk_fp_index query 1 hit \n");
                    goto EXIT;
                }
                DCS_MSG("index_delete disk_fp_index query 1 not hit ,index_query start\n");
            }
    }
#endif
EXIT:
    return todelete;
}

#if 0
/*bloom update
 * update the bloom filter use hook gotten from the contaienr
 */
dcs_s32_t bloom_update(sha_sample_t *hook)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s32_t j = 0;
    dcs_u32_t sha_num = 0;
    dcs_u64_t key[HASH_FUNC_NUM];
    dcs_u8_t  *tmpsha = NULL;


    sha_num = hook->sha_num;

    tmpsha = (dcs_u8_t *)malloc(SHA_LEN);
    if(tmpsha == NULL){
        DCS_ERROR("bloom_update malloc for tmpsha err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    if(hook == NULL){
        DCS_MSG("hook is null \n");
    }
    
    if(hook->sha == NULL){
        DCS_MSG("hook->sha is null\n");
    }

    pthread_rwlock_wrlock(&bloom_lock);
    for(i=0; i<sha_num; i++){
        //DCS_MSG("update the %dth sha \n", i);
        memcpy(tmpsha, hook->sha + i*SHA_LEN, SHA_LEN);
        //DCS_MSG("got the %dth sha \n", i);
        for(j=0; j<HASH_FUNC_NUM; j++){
            key[j] = hashfunc(tmpsha, j);
            SETBIT(bf->a, key[j] % bf->asize);
        }        
    }

    pthread_rwlock_unlock(&bloom_lock);

EXIT:

    /*
    if(tmpsha != NULL){
        free(tmpsha);
        tmpsha = NULL;
    }
    */


    return rc;
}

/*query bloom filter 
 * caculate the key use hash function
 * check if all bits are 1
 */
dcs_s32_t bloom_query(dcs_u8_t *tmpsha)
{
    dcs_s32_t i = 0;
    dcs_s32_t rc = 0;
    dcs_u64_t key[HASH_FUNC_NUM];

    
    for(i=0; i<HASH_FUNC_NUM; i++){
        key[i] = hashfunc(tmpsha,i);
    }

    pthread_rwlock_rdlock(&bloom_lock);
    rc = 1;
    for(i=0; i<HASH_FUNC_NUM; i++){
       // if(bloom_check(bf[bf_id], 1, key[i]) == 0){
        //DCS_MSG("value of key is %ld \n", key[i]);
        if(bf == NULL){
            DCS_MSG("non bf \n");
        }
        if(!(GETBIT(bf->a, key[i] % bf->asize))){
            rc = 0;
            break;  
        }
        //DCS_MSG("rc is %d\n", rc);
    }
    pthread_rwlock_unlock(&bloom_lock);

    return rc;
}
#else
dcs_s32_t bloom_update(dcs_u8_t *sha, dcs_s32_t sha_num)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s32_t j = 0;
    dcs_u64_t key;
    dcs_u8_t  *tmpsha = NULL;
    dcs_u8_t tmp = 0;
    dcs_u8_t flag = 0;
    dcs_s32_t k;
    dcs_u64_t blocks_num, block_num;
    tier_bloom_block * tbb = NULL;


    if(sha == NULL || sha_num == 0){
        DCS_MSG("hook or hook->sha is null \n");
    }

    tmpsha = (dcs_u8_t *)malloc(SHA_LEN);
    if(tmpsha == NULL){
        DCS_ERROR("bloom_update malloc for tmpsha err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    for(i=0; i<sha_num; i++){
        memcpy(tmpsha, sha + i*SHA_LEN, SHA_LEN);
        for(j=0; j<HASH_FUNC_NUM; j++){
            key = hashfunc(tmpsha, j) % COMPRESSOR_BLOOM_SIZE;
            blocks_num = (key + BLOOM_BLOCK_SIZE - 1) / BLOOM_BLOCK_SIZE;
            block_num = key % BLOOM_BLOCK_SIZE;
            
            pthread_mutex_lock(&tbf->lock);
            SETBIT(tbf->bmap, blocks_num);
            if(tbf->blocks[blocks_num] == NULL){
                bloom_block_init1(tbf, blocks_num, 0);
            }

            tbb = tbf->blocks[blocks_num];

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
                    bloom_block_expand(tbf, blocks_num);
                    tbb->a[k][block_num]++;
                }
            }
            pthread_mutex_unlock(&tbb->lock);
            pthread_mutex_unlock(&tbf->lock);
        }        
    }

EXIT:

    if(tmpsha != NULL){
        free(tmpsha);
        tmpsha = NULL;
    }


    return rc;
}

dcs_s32_t bloom_query(dcs_u8_t *tmpsha)
{
    dcs_s32_t i = 0;
    dcs_s32_t rc = 1;
    dcs_u64_t blocks_num;
    dcs_u64_t block_num;
    dcs_u64_t key;
    
    for(i=0; i<HASH_FUNC_NUM; i++){
        key = hashfunc(tmpsha,i);

        key = key % COMPRESSOR_BLOOM_SIZE;
        blocks_num = (key + BLOOM_BLOCK_SIZE -1) / BLOOM_BLOCK_SIZE;
        block_num = key % BLOOM_BLOCK_SIZE;

        pthread_mutex_lock(&tbf->lock);
        if(!GETBIT(tbf->bmap, blocks_num)){
            pthread_mutex_unlock(&tbf->lock);
            rc = 0;
            break;
        }

        pthread_mutex_lock(&tbf->blocks[blocks_num]->lock);
        if(!GETBIT(tbf->blocks[blocks_num]->bmap, block_num)){
            pthread_mutex_unlock(&tbf->blocks[blocks_num]->lock);
            pthread_mutex_unlock(&tbf->lock);
            rc = 0;
            break;
        }
        pthread_mutex_unlock(&tbf->blocks[blocks_num]->lock);
        pthread_mutex_unlock(&tbf->lock);
    }
    return rc;
}

dcs_s32_t bloom_delete(dcs_u8_t *tmpsha){
    dcs_s32_t i = 0;
    dcs_s32_t k = 0;
    dcs_s32_t rc = 1;
    dcs_u64_t blocks_num;
    dcs_u64_t block_num;
    dcs_u64_t key;
    tier_bloom_block * tbb = NULL; 
    for(i=0; i<HASH_FUNC_NUM; i++){
        key = hashfunc(tmpsha,i);

        key = key % COMPRESSOR_BLOOM_SIZE;
        blocks_num = (key + BLOOM_BLOCK_SIZE -1) / BLOOM_BLOCK_SIZE;
        block_num = key % BLOOM_BLOCK_SIZE;

           pthread_mutex_lock(&tbf->lock);
           if(!GETBIT(tbf->bmap, blocks_num)){
               if(tbf->blocks[blocks_num]){
                   DCS_ERROR("__dcs_master_bloom_reset blocks_num %ld bitmap is reset already, but response blocks is not null\n",blocks_num);
               }
               pthread_mutex_unlock(&tbf->lock);
               continue;
           }
           tbb = tbf->blocks[blocks_num];
           pthread_mutex_lock(&tbb->lock);

           for(k = 0; k < tbb->real_level_num; k++){
                tmp = tbb->a[k][block_num];
                tbb->a[k][block_num] = tmp - 1;
                if(tmp){
                    break;
                }
           }

           for(k = 0; k < tbb->real_level_num; k++){
               if(tbb->a[k][block_num]){
                   break;
               }
           }
           if(k == tbb->real_level_num){
               RESETBIT(tbb->bmap, block_num);
           }
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
               free(tbf->blocks[blocks_num]);
               tbf->blocks[blocks_num] = NULL;
               RESETBIT(tbf->bmap, blocks_num);
           }
           pthread_mutex_unlock(&tbf->lock);

    }
    return rc;
}
#endif

#if 0
dcs_s32_t resync_disk_fp_index(dcs_s32_t id)
{
    dcs_s32_t index_fd, bmap_fd, bucket_id, wrc, rc = 0;
    dcs_u32_t split_depth;

    index_block_t *tmp_block;
    save_info_t *tmp_info;

    char bmap_fname[PATH_LEN], index_fname[PATH_LEN], id_buf[10];


    exten_hash_t *tmp_fp_index = disk_fp_index; /*replace the original pointer*/
    BMAP *tmp_bmap = disk_bmap;
    
    memset(index_fname, 0, PATH_LEN);
    memset(bmap_fname, 0, PATH_LEN);
    strcpy(bmap_fname, BMAP_PATH);
    strcpy(index_fname, INDEX_PATH);
    sprintf(id_buf, "%d", id);
    memcpy(index_fname + strlen(index_fname), id_buf, strlen(id_buf) + 1);
    if( access(index_fname, 0) != 0 ){
        DCS_ERROR("resync_disk_fp_index cannot access index_fname\n");
    }
    memcpy(bmap_fname + strlen(bmap_fname), id_buf, strlen(id_buf) + 1);

    bmap_fd = open(bmap_fname, O_WRONLY|O_CREAT|O_TRUNC, 0666);
    if(bmap_fd == -1)
    {
        DCS_ERROR("resync_disk_fp_index open bmap file err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    
    wrc = write(bmap_fd, &tmp_bmap->asize, sizeof(dcs_u64_t));
    if(wrc != sizeof(dcs_u64_t))
    {
        DCS_ERROR("resync_disk_fp_index write bmap->asize err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    wrc = write( bmap_fd, tmp_bmap->a, (EXTEN_HASH_SIZE * BUCKET_NUM + CHAR_BIT - 1)/CHAR_BIT );
    if(wrc != (EXTEN_HASH_SIZE * BUCKET_NUM + CHAR_BIT - 1)/CHAR_BIT)
    {
        DCS_ERROR("resync_disk_fp_index write bmap->a err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    close(bmap_fd);

    index_fd = open(index_fname, O_WRONLY|O_CREAT|O_TRUNC, 0666);
    if(index_fd == -1)
    {
        DCS_ERROR("resync_disk_fp_index open index file err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    split_depth = tmp_fp_index->split_depth;
    wrc = write(index_fd, &split_depth, sizeof(dcs_u32_t) );
    if(wrc != sizeof(dcs_u32_t))
    {    
        DCS_ERROR("resync_disk_fp_index write split_depth err:%d \n", errno);
        rc = errno;
        goto EXIT;
    } 
    
    tmp_info = (save_info_t *) malloc( sizeof(save_info_t) );
    if(tmp_info == NULL)
    {
        DCS_ERROR("resync_disk_fp_index malloc for tmp info err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    for(bucket_id = 0; bucket_id < EXTEN_HASH_SIZE; ++bucket_id)
    {
        tmp_block = tmp_fp_index->index_block[bucket_id];
        while(tmp_block != NULL)
        {
            tmp_info->block_id = tmp_block->block_id;
            tmp_info->bucket_id = bucket_id;
            tmp_info->sha_num = tmp_block->sha_num;
            tmp_info->block_depth = tmp_block->block_depth;
            memcpy(tmp_info->sha_conid, tmp_block->sha_conid, INDEX_BLOCK_SIZE * sizeof(sha_con_t));

            wrc = write(index_fd, tmp_info, sizeof(save_info_t) );
            if(wrc != sizeof(save_info_t))
            {
                DCS_ERROR("resync_disk_fp_index write block[%d] err:%d \n", bucket_id, errno);
                rc = errno;
                goto EXIT;
            }
            tmp_block = tmp_block->next;
        }
    }
    
    close(index_fd);

    if(tmp_info != NULL)
    {
        free(tmp_info);
        tmp_info = NULL;
    }

EXIT:
    return rc;
}
#endif
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

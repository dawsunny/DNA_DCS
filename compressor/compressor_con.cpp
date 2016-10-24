/*compressor container manager source file*/

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

container_table_t *con_table = NULL;
struct list_head  container_recycle_list;
pthread_mutex_t container_recycle_lock = PTHREAD_MUTEX_INITIALIZER;

//add by bxz
struct list_head datacon_queue;
struct list_head fpcon_queue;
struct list_head update_queue;

pthread_mutex_t datacon_queue_lock;
pthread_mutex_t fpcon_queue_lock;
pthread_mutex_t update_queue_lock;
sem_t datacon_sem;
sem_t fpcon_sem;
dcs_u64_t diskinfo = 0;
pthread_mutex_t diskinfo_lock = PTHREAD_MUTEX_INITIALIZER;

/*init container hash table which is used for cache the not full contaienr*/
dcs_s32_t container_table_init()
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;

    INIT_LIST_HEAD(&datacon_queue);
    INIT_LIST_HEAD(&fpcon_queue);
    INIT_LIST_HEAD(&update_queue);

    pthread_mutex_init(&datacon_queue_lock, NULL);
    pthread_mutex_init(&fpcon_queue_lock, NULL);
    pthread_mutex_init(&update_queue_lock, NULL);

    sem_init(&datacon_sem, 0, 0);
    sem_init(&fpcon_sem, 0, 0);

    con_table = (container_table_t *)malloc(sizeof(container_table_t));
    if(con_table == NULL){
        DCS_ERROR("container_table_init malloc for con_table err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    for(i=0; i<DCS_COMPRESSOR_THREAD_NUM; i++){
        con_table->fp_con[i] = NULL;
        con_table->data_con[i] = NULL;
        pthread_rwlock_init(&con_table->con_lock[i], NULL);
    }

    container_dir_init();
    INIT_LIST_HEAD(&container_recycle_list);

    global_conid_init();

EXIT:
    return rc;
}

dcs_s32_t global_conid_init(){
    dcs_s32_t rc = 0;
    dcs_s32_t id = 0;
    dcs_u64_t i = 0;
    dcs_s8_t *filename = NULL;
    struct stat f_state;

    filename = (dcs_s8_t *)malloc(FILENAME_LEN);
    if(filename == NULL){
        DCS_ERROR("global_conid_init malloc for filename err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    for(i = 1; ;i++){
        memset(filename, 0, FILENAME_LEN);
        id = i % MAX_CONTAINER_DIR_NUM;
        sprintf(filename, "%s/%d/container_%ld", DATA_CONTAINER_PATH, id, i);

        rc = stat(filename, &f_state);
        if(rc){
            pthread_mutex_lock(&container_lock);
            container = i;
            pthread_mutex_unlock(&container_lock);
            rc = 0;
            break;
        }

        if(f_state.st_size == 0){
            con_block_t * tmp_con_block = (con_block_t *)malloc(sizeof(con_block_t));
            if(NULL == tmp_con_block){
                DCS_ERROR("global_conid_init malloc con_block_t for recycle container file failed, errno:%d\n", errno);
                rc = errno;
                goto EXIT;
            }
            pthread_mutex_lock(&container_recycle_lock);
            INIT_LIST_HEAD(&tmp_con_block->con_list);
            tmp_con_block->con_id = i;
            tmp_con_block->timestamp = time(NULL);
            list_add_tail(&tmp_con_block->con_list, &container_recycle_list);
            pthread_mutex_unlock(&container_recycle_lock);
        }
    }

EXIT:
    return rc;
}

dcs_s32_t container_dir_init(){
    dcs_s32_t i = 0;
    dcs_s32_t rc = 0;
    dcs_s8_t dir[256];

    if(access(DATA_CONTAINER_PATH, 0)){
        if(mkdir(DATA_CONTAINER_PATH, 0755)){
            DCS_ERROR("container_list_init init dir %s failed, errno: %d\n", DATA_CONTAINER_PATH, errno);
            rc = errno;
            goto EXIT;
        }
    }

    if(access(FP_CONTAINER_PATH, 0)){
        if(mkdir(FP_CONTAINER_PATH, 0755)){
            DCS_ERROR("container_list_init init dir %s failed, errno: %d\n", FP_CONTAINER_PATH, errno);
            rc = errno;
            goto EXIT;
        }
    }

    for(i = 0; i < MAX_CONTAINER_DIR_NUM; i++){
        memset(dir, 0, 256);
        sprintf(dir, "%s/%d", DATA_CONTAINER_PATH, i);
        if(access(dir, 0) && mkdir(dir, 0755)){
            DCS_ERROR("container_list_init init dir %s failed, errno: %d\n", dir, errno);
            rc = errno;
            goto EXIT;
        }
        memset(dir, 0, 256);
        sprintf(dir, "%s/%d", FP_CONTAINER_PATH, i);
        if(access(dir, 0) && mkdir(dir, 0755)){
            DCS_ERROR("container_list_init init dir %s failed, errno: %d\n", dir, errno);
            rc = errno;
            goto EXIT;
        }
    }
EXIT:
    return rc;
}

/*this function will query the new container,
 * which may not full enough to write to disk.
 * so it means such container did not insert their sample into 
 * the main index.
 */
dcs_datapos_t *query_container_buf(dcs_u8_t *sha)
{
    dcs_s32_t rc = 0;
    dcs_u32_t i = 0;
    dcs_u32_t j = 0;
    dcs_u32_t find = 0;
    //dcs_u32_t chunk_num = 0;

    dcs_u8_t  *tmpsha = NULL;
    dcs_datapos_t *data_pos = NULL;
    sha_t  *tmpsha_con = NULL;

    
    tmpsha = (dcs_u8_t *)malloc(SHA_LEN);
    if(tmpsha == NULL){
        DCS_ERROR("query_container_buf err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    //DCS_MSG("1\n");
    for(i=0; i<DCS_COMPRESSOR_THREAD_NUM; i++){
        if(con_table->fp_con[i] != NULL){
            pthread_rwlock_rdlock(&con_table->con_lock[i]);

            //DCS_MSG("2\n");
            tmpsha_con = con_table->fp_con[i]->con;
            for(j=0; j<con_table->fp_con[i]->chunk_num; j++){
                //DCS_MSG("container query the %dth container, the %dth fp \n",i, j);
                memcpy(tmpsha, tmpsha_con[j].sha, SHA_LEN);
                if((shastr_cmp(sha, tmpsha)) == 0 /*&& con_table->fp_con[i]->ref_count[j]*/){
                    find = 1;

                    data_pos = (dcs_datapos_t *)malloc(sizeof(dcs_datapos_t));
                    if(data_pos == NULL){
                        DCS_ERROR("query_container malloc for data_pos err %d \n", errno);
                        pthread_rwlock_rdlock(&con_table->con_lock[i]);
                        rc = errno;
                        goto EXIT;
                    }

                    data_pos->compressor_id = compressor_this_id;
                    data_pos->container_id = con_table->fp_con[i]->container_id;
                    data_pos->container_offset = con_table->fp_con[i]->offset[j];

                    pthread_rwlock_unlock(&con_table->con_lock[i]);
                    goto EXIT;
                }  
            }

            pthread_rwlock_unlock(&con_table->con_lock[i]);
        }
    }


EXIT:
    if(tmpsha != NULL){
        //DCS_MSG("11\n");
        //DCS_MSG("tmpsha addr: %p \n", tmpsha);
        free(tmpsha);
        //DCS_MSG("22\n");
        tmpsha = NULL;
    }


    if(find == 0){
        if(data_pos != NULL){
            free(data_pos);
            data_pos = NULL;
        }
    }

    return data_pos;
}

/*if chunk data is new then add to the new container
 * if no new container in the buf, malloc one
 */
dcs_datapos_t *add_to_container(dcs_s8_t *datap, 
                                  dcs_u32_t chunksize, 
                                  dcs_thread_t *threadp,
                                  dcs_u8_t *sha)
{
    dcs_s32_t rc = 0;
    dcs_s32_t finish = 0;
    dcs_u32_t thread_id = 0;
    dcs_u32_t most_chunk_num = 0;
    dcs_u64_t container_id = 0;
    dcs_u32_t chunk_num = 0;
    dcs_u32_t offset = 0;

    dcs_datapos_t *data_pos = NULL;
    fp_container_t  *fp_container = NULL;
    data_container_t *data_container = NULL;


    //DCS_MSG("the addr of datap is %p \n", datap);
    data_pos = (dcs_datapos_t *)malloc(sizeof(dcs_datapos_t));
    if(data_pos == NULL){
        DCS_ERROR("add_to_container err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    //DCS_MSG("add_to_container chunksize: %d \n", chunksize);
    //DCS_MSG("1 \n");
    thread_id = threadp->seqno;
    pthread_rwlock_wrlock(&con_table->con_lock[thread_id]);

    //DCS_MSG("2 \n");
    /*if no correspoding container in the buf, malloc one*/
    if(con_table->fp_con[thread_id] == NULL){
        /*get the unique container id*/
        /*pthread_mutex_lock(&container_lock);
        container_id = container;
        container ++;
        pthread_mutex_unlock(&container_lock);*/
        container_id = get_container_id();
        //DCS_MSG("3 \n");
        con_table->fp_con[thread_id] = (fp_container_t *)malloc(sizeof(fp_container_t));
        if(con_table->fp_con[thread_id] == NULL){
            DCS_ERROR("add_to_container malloc for fp con err:%d \n", errno);
            pthread_rwlock_unlock(&con_table->con_lock[thread_id]);
            rc = errno;
            goto EXIT;
        }
        
        /*init the fp container*/
        most_chunk_num = 8*CONTAINER_SIZE / DCS_CHUNK_SIZE;
        //DCS_MSG("add to container: %ld \n", most_chunk_num*sizeof(sha_t));
        con_table->fp_con[thread_id]->con = (sha_t *)malloc(sizeof(sha_t)*(most_chunk_num));
        //DCS_MSG("add to container 44 \n");
        if(con_table->fp_con[thread_id]->con == NULL){
            DCS_ERROR("add_to_container malloc for fp con_sha_t err:%d \n", errno);
            pthread_rwlock_unlock(&con_table->con_lock[thread_id]);
            rc = errno;
            goto EXIT;
        }
        con_table->fp_con[thread_id]->chunk_num = 0;
        con_table->fp_con[thread_id]->container_id = container_id;

        //DCS_MSG("5 \n");
        /*malloc for data container*/
        con_table->data_con[thread_id] = (data_container_t *)malloc(sizeof(data_container_t));
        if(con_table->data_con[thread_id] == NULL){
            DCS_ERROR("add_to_container malloc for data con err:%d \n", errno);
            pthread_rwlock_unlock(&con_table->con_lock[thread_id]);
            rc = errno;
            goto EXIT;
        }

        /*init the data container*/
        con_table->data_con[thread_id]->con = (dcs_s8_t *)malloc(CONTAINER_SIZE);
        if(con_table->data_con[thread_id]->con == NULL){
            DCS_ERROR("add_to_container malloc for data space err:%d \n", errno);
            pthread_rwlock_unlock(&con_table->con_lock[thread_id]);
            rc = errno;
            goto EXIT;
        }
        //con_table->data_con[thread_id]->size = CONTAINER_SIZE;
        con_table->data_con[thread_id]->offset = 0;
        con_table->data_con[thread_id]->container_id = container_id;
        //DCS_MSG("7 \n");
    }

    fp_container = con_table->fp_con[thread_id];
    data_container = con_table->data_con[thread_id];
    
    //DCS_MSG("the addr of thread_id is %d ,container addr %p \n", thread_id, data_container->con);
    //DCS_MSG("add_to_container offset:%d, chunksize: %d \n", data_container->offset, chunksize);
    /*if the container have enough space for the chunk store it to the container*/
    if((data_container->offset + chunksize) < CONTAINER_SIZE || 
       (data_container->offset + chunksize) == CONTAINER_SIZE){

        chunk_num = fp_container->chunk_num;
        //DCS_MSG("22\n");
        memcpy(fp_container->con[chunk_num].sha, sha, SHA_LEN);
        fp_container->offset[chunk_num] = data_container->offset;
        //DCS_MSG("add_to_container offset:%d, chunksize: %d \n", data_container->offset, chunksize);
        //fp_container->ref_count[chunk_num] = 1;
        fp_container->chunk_num++;
        //dcs_s8_t *tmpdatap = data_container->con;
        offset = data_container->offset;
        memcpy(data_container->con + offset, datap, chunksize);
        data_pos->container_offset = data_container->offset;
        data_pos->container_id = data_container->container_id;
        data_pos->compressor_id = compressor_this_id;
        data_container->offset = data_container->offset + chunksize;
        data_container->timestamp = time(NULL); 
        pthread_rwlock_unlock(&con_table->con_lock[thread_id]);
        finish = 1;
        goto EXIT;
    }
    else if((data_container->offset + chunksize) > CONTAINER_SIZE){
        /*if the chunk size is too big for remaind space, 
         * then write the container to disk and malloc for a new one
         */

        /*add to the writing back queue*/
        rc = update_data_cache(thread_id);
        if(rc != 0){
            DCS_ERROR("add_to_container update data cache err:%d",rc);
            pthread_rwlock_unlock(&con_table->con_lock[thread_id]);
            goto EXIT;
        }

        __queue_datacon(data_container);
        __queue_fpcon(fp_container);

        con_table->data_con[thread_id] = NULL;
        con_table->fp_con[thread_id] = NULL;

        /*get the unique container id*/
        /*pthread_mutex_lock(&container_lock);
        container_id = container;
        container ++;
        pthread_mutex_unlock(&container_lock);*/

        container_id = get_container_id();

        con_table->fp_con[thread_id] = (fp_container_t *)malloc(sizeof(fp_container_t));
        if(con_table->fp_con[thread_id] == NULL){
            DCS_ERROR("add_to_container malloc for fp con err:%d \n", errno);
            pthread_rwlock_unlock(&con_table->con_lock[thread_id]);
            rc = errno;
            goto EXIT;
        }
        
        /*init the fp container*/
        most_chunk_num = 8*CONTAINER_SIZE / DCS_CHUNK_SIZE;
        con_table->fp_con[thread_id]->con = (sha_t *)malloc(sizeof(sha_t)*(most_chunk_num));
        if(con_table->fp_con[thread_id]->con == NULL){
            DCS_ERROR("add_to_container malloc for fp con_sha_t err:%d \n", errno);
            pthread_rwlock_unlock(&con_table->con_lock[thread_id]);
            rc = errno;
            goto EXIT;
        }
        con_table->fp_con[thread_id]->chunk_num = 0;
        con_table->fp_con[thread_id]->container_id = container_id;

        /*malloc for data container*/
        con_table->data_con[thread_id] = (data_container_t *)malloc(sizeof(data_container_t));
        if(con_table->data_con[thread_id] == NULL){
            DCS_ERROR("add_to_container malloc for data con err:%d \n", errno);
            pthread_rwlock_unlock(&con_table->con_lock[thread_id]);
            rc = errno;
            goto EXIT;
        }

        /*init the data container*/
        con_table->data_con[thread_id]->con = (dcs_s8_t *)malloc(CONTAINER_SIZE);
        if(con_table->data_con[thread_id]->con == NULL){
            DCS_ERROR("add_to_container malloc for data space err:%d \n", errno);
            pthread_rwlock_unlock(&con_table->con_lock[thread_id]);
            rc = errno;
            goto EXIT;
        }

        con_table->data_con[thread_id]->offset = 0;
        con_table->data_con[thread_id]->container_id = container_id;
        
        fp_container = con_table->fp_con[thread_id];
        data_container = con_table->data_con[thread_id];

        /*store the fp and data to the new container*/
        dcs_u32_t fp_chunk_num = 0;
        fp_chunk_num = fp_container->chunk_num;
        memcpy(fp_container->con[fp_chunk_num].sha, sha, SHA_LEN);
        fp_container->offset[fp_container->chunk_num] = data_container->offset;
        //fp_container->ref_count[fp_container->chunk_num] = 1;
        fp_container->chunk_num++;
        memcpy(data_container->con, datap, chunksize);
        data_pos->container_offset = data_container->offset;
        data_pos->container_id = data_container->container_id;
        data_pos->compressor_id = compressor_this_id;
        DCS_MSG("add_to_container offset:%d, chunksize:%d \n", data_container->offset, chunksize);
        data_container->offset = data_container->offset + chunksize;
        data_container->timestamp = time(NULL);
        /*finish add data to container, goto exit*/
        pthread_rwlock_unlock(&con_table->con_lock[thread_id]);
        finish = 1;
        goto EXIT;
    }

EXIT:


    if(finish)
        return data_pos;
    else{
        if(data_pos != NULL){
            free(data_pos);
            data_pos = NULL;
        }
        return NULL;
    }
}

/*queue the full data container*/
void __queue_datacon(data_container_t *data_container)
{
    pthread_mutex_lock(&datacon_queue_lock);
    list_add_tail(&data_container->con_list, &datacon_queue);
    pthread_mutex_unlock(&datacon_queue_lock);
    sem_post(&datacon_sem);
}

/*queue the responding fp container*/
void __queue_fpcon(fp_container_t *fp_container)
{
    pthread_mutex_lock(&fpcon_queue_lock);
    list_add_tail(&fp_container->con_list, &fpcon_queue);
    pthread_mutex_unlock(&fpcon_queue_lock);
    sem_post(&fpcon_sem);
}

dcs_s32_t dcs_compressor_data_fp_trunc(data_container_t *data_con, fp_container_t *fp_con){
    dcs_s32_t rc = 0;
    dcs_s32_t write_fd = 0;
    dcs_s32_t i = 0;
    dcs_u32_t chunk_num = 0;
    dcs_u32_t wsize = 0;
    con_sha_t *con_sha = NULL;
    dcs_s8_t *confile_name = NULL;
    
    confile_name = get_datacon_name(data_con->container_id);
    if(confile_name == NULL){
        DCS_ERROR("dcs_compressor_data_fp_trunc get confile_name err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
 
    if(data_con->offset == 0){
        write_fd = open(confile_name, O_WRONLY | O_CREAT| O_TRUNC, 0666);
        close(write_fd);
    }
    if(confile_name){
        free(confile_name);
        confile_name = NULL;
    }

    confile_name = get_fpcon_name(fp_con->container_id);
    if(confile_name == NULL){
        DCS_ERROR("dcs_compressor_data_fp_trunc get confile_name err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    chunk_num = fp_con->chunk_num;
    con_sha = (con_sha_t *)malloc(sizeof(con_sha_t)*chunk_num);
    if(con_sha == NULL){
        DCS_ERROR("dcs_compressor_data_fp_trunc fpsize  malloc for con_sha err:%d \n", errno) ;
        rc = errno;
        goto EXIT;
    }
    memset(con_sha, 0, sizeof(con_sha_t) * chunk_num);

    for(i=0; i<chunk_num; i++){
        memcpy(con_sha[i].sha, fp_con->con[i].sha, SHA_LEN);
        con_sha[i].offset = fp_con->offset[i];
    }

    write_fd = open(confile_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    wsize = write(write_fd, con_sha, chunk_num*(sizeof(con_sha_t)));
    if(wsize != chunk_num*(sizeof(con_sha_t))){
        DCS_ERROR("dcs_compressor_data_fp_trunc fpsize is %ld, rc is %d \n" ,chunk_num*(sizeof(con_sha_t)) , rc);
        rc = errno;
        goto EXIT;
    }
    close(write_fd);
    if(confile_name){
        free(confile_name);
        confile_name = NULL;
    }

EXIT:
    return rc;
}

dcs_s32_t dcs_compressor_data_fp_sync(data_container_t *data_con, fp_container_t *fp_con){
    dcs_s32_t rc = 0;
    dcs_s32_t write_fd = 0;
    dcs_s32_t i = 0;
    dcs_u32_t chunk_num = 0;
    dcs_u32_t wsize = 0;
    con_sha_t *con_sha = NULL;
    dcs_s8_t *confile_name = NULL;
    struct stat f_state;

    confile_name = get_datacon_name(data_con->container_id);
    if(confile_name == NULL){
        DCS_ERROR("dcs_compressor_data_fp_sync get confile_name err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    rc = stat(confile_name, &f_state); 
    if((rc != 0 && errno == ENOENT) || (rc == 0 && f_state.st_size < data_con->offset)){

        write_fd = open(confile_name, O_WRONLY | O_CREAT| O_TRUNC, 0666);
        wsize = write(write_fd, data_con->con, data_con->offset);
        if(wsize != data_con->offset){
            DCS_MSG("dcs_compressor_data_fp_sync datasize is %d, rc is %d \n",data_con->offset, rc);
            DCS_ERROR("dcs_compressor_data_fp_sync err:%d \n", rc);
            rc = errno;
            goto EXIT;
        }
        close(write_fd);
        if(confile_name){
            free(confile_name);
            confile_name = NULL;
        }
    }
    rc = 0;

    confile_name = get_fpcon_name(fp_con->container_id);
    if(confile_name == NULL){
        DCS_ERROR("dcs_compressor_data_fp_sync get confile_name err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    chunk_num = fp_con->chunk_num;
    con_sha = (con_sha_t *)malloc(sizeof(con_sha_t)*chunk_num);
    if(con_sha == NULL){
        DCS_ERROR("dcs_compressor_data_fp_sync fpsize  malloc for con_sha err:%d \n", errno) ;
        rc = errno;
        goto EXIT;
    }
    memset(con_sha, 0, sizeof(con_sha_t) * chunk_num);

    for(i=0; i<chunk_num; i++){
        memcpy(con_sha[i].sha, fp_con->con[i].sha, SHA_LEN);
        con_sha[i].offset = fp_con->offset[i];
    }

    write_fd = open(confile_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    wsize = write(write_fd, con_sha, chunk_num*(sizeof(con_sha_t)));
    if(wsize != chunk_num*(sizeof(con_sha_t))){
        DCS_MSG("dcs_compressor_data_fp_sync fpsize is %ld, rc is %d \n" ,chunk_num*(sizeof(con_sha_t)) , rc);
        DCS_ERROR("dcs_compressor_data_fp_sync err:%d \n", rc);
        rc = errno;
        goto EXIT;
    }
    close(write_fd);
    if(confile_name){
        free(confile_name);
        confile_name = NULL;
    }

EXIT:
    return rc;
}

/*write data container to disk*/
dcs_s32_t compressor_data_wb(data_container_t *data_con)
{
    dcs_s32_t rc = 0;
    dcs_s32_t write_fd = 0;

    dcs_s8_t *confile_name = NULL;


    confile_name = get_datacon_name(data_con->container_id);
    if(confile_name == NULL){
        DCS_ERROR("compressor_data_wb get confile_name err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    write_fd = open(confile_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    rc = write(write_fd, data_con->con, data_con->offset);
    if(rc != data_con->offset){
        DCS_MSG("dcs_data_wb datasize is %d, rc is %d \n",data_con->offset, rc);
        DCS_ERROR("dcs_data_wb err:%d \n", rc);
        rc = errno;
        goto EXIT;
    }
    rc = 0;

    pthread_mutex_lock(&diskinfo_lock);
    diskinfo = diskinfo + 1;
    pthread_mutex_unlock(&diskinfo_lock);

EXIT:
    if(write_fd){
        close(write_fd);
    }

    if(confile_name != NULL){
        free(confile_name);
        confile_name = NULL;
    }

    if(data_con != NULL){
        free(data_con->con);
        data_con->con = NULL;
        free(data_con);
        data_con = NULL;
    }

    return rc;
}

/*write fp container to disk*/
dcs_s32_t compressor_fp_wb(fp_container_t *fp_con)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s32_t write_fd = 0;
    dcs_u32_t chunk_num = 0;
    dcs_u32_t wsize = 0;
    dcs_u8_t  *sha = NULL;
    //sha_sample_t *hook = NULL;
    dcs_s8_t *confile_name = NULL;
    con_sha_t *con_sha = NULL;

    confile_name = get_fpcon_name(fp_con->container_id);
    if(confile_name == NULL){
        DCS_ERROR("compressor_fp_wb get confile_name err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    chunk_num = fp_con->chunk_num;
    con_sha = (con_sha_t *)malloc(sizeof(con_sha_t)*chunk_num);
    if(con_sha == NULL){
        DCS_ERROR("dcs_fp_wb fpsize  malloc for con_sha err:%d \n", errno) ;
        rc = errno;
        goto EXIT;
    }
    memset(con_sha, 0, sizeof(con_sha_t) * chunk_num);

    /*ref_count = (dcs_s32_t *)malloc(sizeof(dcs_s32_t) * chunk_num);
    if(NULL == ref_count){
        DCS_ERROR("dcs_fp_wb fpsize  malloc for ref_count err:%d \n", errno) ;
        rc = errno;
        goto EXIT;
    }*/

    for(i=0; i<chunk_num; i++){
        memcpy(con_sha[i].sha, fp_con->con[i].sha, SHA_LEN);
        con_sha[i].offset = fp_con->offset[i];
        //ref_count[i] = fp_con->ref_count[i];
    }

    write_fd = open(confile_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    wsize = write(write_fd, con_sha, chunk_num*(sizeof(con_sha_t)));
    if(wsize != chunk_num*(sizeof(con_sha_t))){
        DCS_MSG("dcs_fp_wb fpsize is %ld, rc is %d \n" ,chunk_num*(sizeof(con_sha_t)) , rc);
        DCS_ERROR("dcs_fp_wb err:%d \n", rc);
        rc = errno;
        goto EXIT;
    }

    /*sha = (dcs_u8_t *)malloc(chunk_num*SHA_LEN);
    if(sha == NULL){
        DCS_ERROR("dcs_fp_wb malloc for sha err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    for(i=0; i<chunk_num; i++){
        memcpy(sha + i*SHA_LEN, fp_con->con[i].sha, SHA_LEN);
    }
    //hook = get_sample_fp(sha, chunk_num);
    //rc = dcs_insert_index(sha, ref_count, chunk_num, fp_con->container_id);
    //if(rc != 0){
    //    DCS_ERROR("dcs_fp_wb insert sample sha value to index err:%d \n", rc);
    //    goto EXIT;
    //}
    rc = 0;*/
EXIT:
    if(write_fd){
        close(write_fd);
    }

    if(confile_name != NULL){
        free(confile_name);
        confile_name = NULL;
    }

    if(fp_con != NULL){
        free(fp_con->con);
        fp_con->con = NULL;
        free(fp_con);
    }

    /*
    if(hook != NULL){
        if(hook->sha != NULL){
            free(hook->sha);
            hook->sha = NULL;
        }
        free(hook);
        hook = NULL;
    }
    */  

    if(sha != NULL){
        free(sha);
        sha = NULL;
    }

    if(con_sha != NULL){
        free(con_sha);
        con_sha = NULL;
    }


    return rc;
}

/*get data container name including path and filename*/
dcs_s8_t *get_datacon_name(dcs_u64_t container_id)
{
    dcs_s32_t rc = 0;
    dcs_s8_t *filename = NULL;
    dcs_s32_t id = 0;

    filename = (dcs_s8_t *)malloc(FILENAME_LEN);
    if(filename == NULL){
        DCS_ERROR("get_datacon_name malloc for filename err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(filename, 0, FILENAME_LEN);

    id = container_id % MAX_CONTAINER_DIR_NUM;

    sprintf(filename, "%s/%d/container_%ld", DATA_CONTAINER_PATH, id, container_id);

EXIT:
    return filename;
}

/*get fp container name including path and filename*/
dcs_s8_t *get_fpcon_name(dcs_u64_t container_id)
{
    dcs_s32_t rc = 0;
    dcs_s8_t *filename = NULL;
    dcs_s32_t id = container_id % MAX_CONTAINER_DIR_NUM;

    filename = (dcs_s8_t *)malloc(FILENAME_LEN);
    if(filename == NULL){
        DCS_ERROR("get_fpcon_name malloc for filename err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(filename, 0, FILENAME_LEN);

    sprintf(filename, "%s/%d/container_%ld", FP_CONTAINER_PATH, id, container_id);

    DCS_MSG("get_fpcon_name the filename is %s \n", filename);

EXIT:
    return filename;
}

/*read the data container from disk*/
dcs_s8_t *read_container(dcs_u64_t container_id, dcs_u32_t *size)
{
    dcs_s32_t rc = 0;
    dcs_s32_t read_fd = 0;
    dcs_u64_t filesize = 0;

    dcs_s8_t  *data = NULL;
    dcs_s8_t  *container_name = NULL;
    struct stat f_state;
    

    container_name = get_datacon_name(container_id);
    if(stat(container_name, &f_state) == -1){
        if(errno == 2){
            DCS_MSG("read_container the container is not in the disk \n");
            data = read_container_buf(container_id, size);
            rc = 0;
            if(data == NULL)
                rc = 2;
            goto EXIT;
        }
        rc = errno;
        goto EXIT;
    }

    filesize = (dcs_u64_t)(f_state.st_size);
    *size = filesize;
    read_fd = open(container_name, O_RDWR, 0666);
    if(read_fd <= 0){
        DCS_ERROR("read container get read_fd err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    data = (dcs_s8_t *)malloc(filesize);

    rc = read(read_fd, data, filesize);
    if(rc != filesize){
        DCS_MSG("read_container filesize is:%ld rc is:%d \n",filesize, rc);
        DCS_ERROR("read_container read container err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    rc = 0;

EXIT:
    if(container_name != NULL){
        free(container_name);
        container_name = NULL;
    }
    

    if(rc != 0){
        if(data != NULL){
            free(data);
            data = NULL;
        }
    }

    if(read_fd){
        close(read_fd);
        read_fd = 0;
    }

    return data;
}

/*the container is still in the cache, did not write back*/
dcs_s8_t *read_container_buf(dcs_u64_t container_id, dcs_u32_t *size)
{
    dcs_s32_t rc = 0;
    dcs_s8_t *data = NULL;
    dcs_s32_t i = 0;

    for(i=0; i<DCS_COMPRESSOR_THREAD_NUM; i++){
        pthread_rwlock_wrlock(&con_table->con_lock[i]);
        if(con_table->data_con[i]->container_id == container_id){
            dcs_u32_t datasize = con_table->data_con[i]->offset;
            *size = datasize;
            DCS_MSG("datasize is %d \n", datasize);
            data = (dcs_s8_t *)malloc(datasize);
            if(data == NULL){
                DCS_ERROR("read_container_buf malloc for data err:%d \n", errno);
                rc = errno;
                goto EXIT;
            }

            memcpy(data, con_table->data_con[i]->con, datasize);
            pthread_rwlock_unlock(&con_table->con_lock[i]);
            break;
        }
        pthread_rwlock_unlock(&con_table->con_lock[i]);
    }

EXIT:

    if(rc != 0){
        free(data);
        data = NULL;
    }

    return data;
}

/*compare two sha string*/
dcs_s32_t shastr_cmp(dcs_u8_t *sha1, dcs_u8_t *sha2)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    
    dcs_u8_t *tmpsha1 = NULL;
    dcs_u8_t *tmpsha2 = NULL;

    tmpsha1 = (dcs_u8_t *)malloc(SHA_LEN + 1);
    if(tmpsha1 == NULL){
        DCS_ERROR("malloc for tmpsha1 err \n");
        exit(1);
    }

    tmpsha2 = (dcs_u8_t *)malloc(SHA_LEN + 1);
    if(tmpsha2 == NULL){
        DCS_ERROR("malloc for tmpsha2 err \n");
        exit(1);
    }

    memcpy(tmpsha1, sha1, SHA_LEN);
    tmpsha1[SHA_LEN] = '\0';
    memcpy(tmpsha2, sha2, SHA_LEN);
    tmpsha2[SHA_LEN] = '\0';
    //rc = strcmp(tmpsha1, tmpsha2);

    for(i=0; i<SHA_LEN; i++){
        if(tmpsha1[i] != tmpsha2[i]){
            //DCS_MSG("the %dth char is not the same \n", i);
            rc = 1;
            break;
        }
    }

    if(tmpsha1 != NULL)
        free(tmpsha1);
    if(tmpsha2 != NULL)
        free(tmpsha2);

    return rc;
}

/*compressor anwser disk usage info to server*/
dcs_s32_t __dcs_compressor_answer(amp_request_t *req)
{
    dcs_s32_t rc = 0;
    dcs_u32_t size = 0;

    amp_message_t *repmsgp = NULL;
    dcs_msg_t   *msgp = NULL; 
    

    size = sizeof(dcs_msg_t) + AMP_MESSAGE_HEADER_LEN;
    repmsgp = (amp_message_t *)malloc(size);
    if(repmsgp == NULL){
        DCS_ERROR("__dcs_compressor_answer malloc for repmsgp err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(repmsgp, 0, size);

    memcpy(repmsgp, req->req_msg, AMP_MESSAGE_HEADER_LEN);
    
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->msg_type = is_rep;
    msgp->fromtype = DCS_NODE;
    msgp->fromid = compressor_this_id;

    pthread_mutex_lock(&diskinfo_lock);
    msgp->u.d2s_reply.diskinfo = diskinfo;
    pthread_mutex_unlock(&diskinfo_lock);

    if(req->req_iov != NULL){
        __compressor_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
        req->req_iov = NULL;
        req->req_niov = 0;
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
        DCS_ERROR("__dcs_compressor_answer send reply msg err:%d \n", rc);
        goto EXIT;
    }

EXIT:
    if(req->req_msg != NULL){
        amp_free(req->req_msg, req->req_msglen);
        req->req_msg = NULL;
    }

    if(repmsgp != NULL){
        amp_free(repmsgp, size);
        repmsgp = NULL;
    }

    if(req != NULL){
        __amp_free_request(req);
        req = NULL;
    }

    return rc;
}

dcs_u64_t get_container_id(){
    dcs_u64_t id = 0;
    con_block_t * tmp_con_block = NULL;

    pthread_mutex_lock(&container_recycle_lock);
    if(!list_empty(&container_recycle_list)){
        tmp_con_block = list_entry(container_recycle_list.next, con_block_t, con_list);
        if(time(NULL) - tmp_con_block->timestamp > DATA_UPDATE_INTERVAL){
            list_del_init(&tmp_con_block->con_list);
            id = tmp_con_block->con_id;
            pthread_mutex_unlock(&container_recycle_lock);
            DCS_ERROR("get_container_id recycle container_id: %ld ...\n",id);
            goto EXIT;       
        }
    }
    pthread_mutex_unlock(&container_recycle_lock);

    pthread_mutex_lock(&container_lock);
    id = container;
    container ++;
    pthread_mutex_unlock(&container_lock);

EXIT:
    return id;
}

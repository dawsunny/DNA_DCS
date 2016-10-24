/*thread creation and init*/

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
#include <time.h>

dcs_thread_t compressor_thread[DCS_COMPRESSOR_THREAD_NUM];
dcs_thread_t compressor_datawb_thread[DCS_COMPRESSOR_THREAD_NUM];
dcs_thread_t compressor_fpwb_thread[DCS_COMPRESSOR_THREAD_NUM];
dcs_thread_t compressor_monitor_thread;

/*create compressor thread*/
dcs_s32_t __dcs_create_compressor_thread(void)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;

    dcs_thread_t *threadp = NULL;

    DCS_ENTER("__dcs_create_compressor_thread enter \n");

    DCS_MSG("__dcs_create_compressor_thread thread num:%d \n", DCS_COMPRESSOR_THREAD_NUM);

    /*create compressor server thread*/
    for(i=0; i<DCS_COMPRESSOR_THREAD_NUM; i++){
        threadp = &compressor_thread[i];
        threadp->seqno = i;
        threadp->is_up = 0;
        threadp->to_shutdown = 0;
        sem_init(&threadp->startsem, 0, 0);
        sem_init(&threadp->stopsem, 0, 0);
        rc = pthread_create(&threadp->thread,
                            NULL,
                            __dcs_compressor_thread,
                            (void *)threadp);
        if(rc){
            DCS_ERROR("__dcs_create_compressor_thread create thread error:%d \n", rc);
            goto EXIT;
        }

        sem_wait(&threadp->startsem);
    }

    /*create data container write back thread*/
    for(i=0; i<DCS_COMPRESSOR_THREAD_NUM; i++){
        threadp = &compressor_datawb_thread[i];
        threadp->seqno = i;
        threadp->is_up = 0;
        threadp->to_shutdown = 0;
        sem_init(&threadp->startsem, 0, 0);
        sem_init(&threadp->stopsem, 0, 0);
        rc = pthread_create(&threadp->thread,
                            NULL,
                            __dcs_datawb_thread,
                            (void *)threadp);
        if(rc){
            DCS_ERROR("__dcs_create_compressor_thread create datawb thread error:%d \n", rc);
            goto EXIT;
        }

        sem_wait(&threadp->startsem);
    }

    /*create FP container write back thread*/
    for(i=0; i<DCS_COMPRESSOR_THREAD_NUM; i++){
        threadp = &compressor_fpwb_thread[i];
        threadp->seqno = i;
        threadp->is_up = 0;
        threadp->to_shutdown = 0;
        sem_init(&threadp->startsem, 0, 0);
        sem_init(&threadp->stopsem, 0, 0);
        rc = pthread_create(&threadp->thread,
                            NULL,
                            __dcs_fpwb_thread,
                            (void *)threadp);
        if(rc){
            DCS_ERROR("__dcs_create_compressor_thread create fpwb thread error:%d \n", rc);
            goto EXIT;
        }

        sem_wait(&threadp->startsem);
    }

    threadp = &compressor_monitor_thread;
    threadp->seqno = 0;
    threadp->is_up = 0;
    threadp->to_shutdown = 0;
    sem_init(&threadp->startsem, 0, 0);
    sem_init(&threadp->stopsem, 0, 0);
    rc = pthread_create(&threadp->thread,
                        NULL,
                        __dcs_data_monitor_thread,
                        (void *)threadp);
    if(rc){
        DCS_ERROR("__dcs_create_compressor_thread create fpwb thread error:%d \n", rc);
        goto EXIT;
    }

    sem_wait(&threadp->startsem);

EXIT:
    DCS_LEAVE("__dcs_create_compressor_thread leave \n");
    return rc;
}

/*compressor request operation thread*/
void *__dcs_compressor_thread(void *argv)
{
    dcs_s32_t rc = 0;
    
    amp_request_t *req = NULL;
    dcs_thread_t *threadp = NULL;

    threadp = (dcs_thread_t *)argv;
    
    DCS_ENTER("__dcs_compressor_thread enter \n");

    sem_post(&threadp->startsem);
    threadp->is_up = 1;

    while(1){
        DCS_MSG("__dcs_compressor_thread thread: %d ready to work \n",
                threadp->seqno);       
        sem_wait(&request_sem);
        if(threadp->to_shutdown){
            sem_post(&request_sem);
            DCS_MSG("__dcs_compressor_thread thread: %d gona to shut down \n", 
                    threadp->seqno);
            goto EXIT;
        }
        pthread_mutex_lock(&request_queue_lock);
        req = list_entry(request_queue.next, amp_request_t, req_list);
        if(req == NULL){
            DCS_ERROR("__dcs_compressor_thread thread:%d get filename err: %d \n",
                    threadp->seqno, errno);
            pthread_mutex_unlock(&request_queue_lock);
            continue;
        }
        list_del(&req->req_list);
        pthread_mutex_unlock(&request_queue_lock);

        rc = __dcs_compressor_process_req(req, threadp);
        if(rc != 0){
            DCS_ERROR("__dcs_compressor_thread:%d fail to process req \n",
                    threadp->seqno);
        }

    }

EXIT:
    DCS_LEAVE("__dcs_compressor_thread:%d leave \n", threadp->seqno);
    return NULL;
}

/*compressor data container write back thread*/
void *__dcs_datawb_thread(void *argv)
{
    dcs_s32_t rc = 0;
    
    data_container_t *data_con = NULL;
    dcs_thread_t *threadp = NULL;

    threadp = (dcs_thread_t *)argv;
    
    DCS_ENTER("__dcs_datawb_thread enter \n");

    sem_post(&threadp->startsem);
    threadp->is_up = 1;

    while(1){
        DCS_MSG("__dcs_datawb_thread thread: %d ready to work \n",
                threadp->seqno);       
        sem_wait(&datacon_sem);
        if(threadp->to_shutdown){
            sem_post(&datacon_sem);
            DCS_MSG("__dcs_compressor_thread thread: %d gona to shut down \n", 
                    threadp->seqno);
            goto EXIT;
        }
        pthread_mutex_lock(&datacon_queue_lock);
        data_con = list_entry(datacon_queue.next, data_container_t, con_list);
        if(data_con == NULL){
            DCS_ERROR("__dcs_datawb_thread thread:%d get data container err: %d \n",
                    threadp->seqno, errno);
            pthread_mutex_unlock(&datacon_queue_lock);
            continue;
        }
        list_del(&data_con->con_list);
        pthread_mutex_unlock(&datacon_queue_lock);

        //rc = __dcs_compressor_process_req(req, threadp);
        rc = compressor_data_wb(data_con);
        if(rc != 0){
            DCS_ERROR("__dcs_datawb_thread:%d fail to process req \n",
                    threadp->seqno);
            goto EXIT;
        }

    }

EXIT:
    DCS_LEAVE("__dcs_datawb_thread:%d leave \n", threadp->seqno);
    return NULL;
}

/*compressor fp container write back thread*/
void *__dcs_fpwb_thread(void *argv)
{
    dcs_s32_t rc = 0;
    
    fp_container_t *fp_con = NULL;
    dcs_thread_t *threadp = NULL;

    threadp = (dcs_thread_t *)argv;
    
    DCS_ENTER("__dcs_fpwb_thread enter \n");

    sem_post(&threadp->startsem);
    threadp->is_up = 1;

    while(1){
        DCS_MSG("__dcs_fpwb_thread thread: %d ready to work \n",
                threadp->seqno);       
        sem_wait(&fpcon_sem);
        DCS_MSG("_dcs_fpwb_thread got fp container to write down \n");
        if(threadp->to_shutdown){
            sem_post(&fpcon_sem);
            DCS_MSG("__dcs_fpwb_thread thread: %d gona to shut down \n", 
                    threadp->seqno);
            goto EXIT;
        }
        pthread_mutex_lock(&fpcon_queue_lock);
        fp_con = list_entry(fpcon_queue.next, fp_container_t, con_list);
        if(fp_con == NULL){
            DCS_ERROR("__dcs_fpwb_thread thread:%d get fp container err: %d \n",
                    threadp->seqno, errno);
            pthread_mutex_unlock(&fpcon_queue_lock);
            continue;
        }
        list_del(&fp_con->con_list);
        pthread_mutex_unlock(&fpcon_queue_lock);

        //rc = __dcs_compressor_process_req(req, threadp);
        rc = compressor_fp_wb(fp_con);
        if(rc != 0){
            DCS_ERROR("__dcs_fpwb_thread:%d fail to process req \n",
                    threadp->seqno);
            goto EXIT;
        }

    }

EXIT:
    DCS_LEAVE("__dcs_fpwb_thread:%d leave \n", threadp->seqno);
    return NULL;
}

void *__dcs_data_monitor_thread(void *argv)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_thread_t *threadp = NULL;
    time_t now;
    threadp = (dcs_thread_t *)argv;

    DCS_ENTER("__dcs_data_monitor_thread enter \n");
    sem_post(&threadp->startsem);
    threadp->is_up = 1;


    while(1){
        if(threadp->to_shutdown){
            DCS_MSG("__dcs_fpwb_thread thread: %d gona to shut down \n", threadp->seqno);
            goto EXIT;
        }
        now = time(NULL);
        for(i = 0; i < DCS_COMPRESSOR_THREAD_NUM; i++){
            
            pthread_rwlock_wrlock(&con_table->con_lock[i]);
            
            if(NULL != con_table->data_con[i] && 
               now - con_table->data_con[i]->timestamp > DATA_UPDATE_INTERVAL){
                rc = dcs_compressor_data_fp_sync(con_table->data_con[i], con_table->fp_con[i]);
                if(rc != 0){
                    DCS_ERROR("__dcs_data_monitor_thread:%d fail to process req \n", threadp->seqno);
                    pthread_rwlock_unlock(&con_table->con_lock[i]);
                    goto EXIT;
                }
            }
            pthread_rwlock_unlock(&con_table->con_lock[i]);

        }

        /*
        if(fp_index->total_block_num >= FP_FILE_SIZE)
        {
            DCS_MSG("the index in memory is too large,save to disk and init the fp_index \n");
            rc = save_fp_index();
            if(rc != 0)
            {
                DCS_ERROR("dcs_insert_index insert the %dth sha save_fp_index err:%d \n", i, rc);
                goto EXIT;
            }
        }*/

        sleep(DATA_UPDATE_INTERVAL);
    }

EXIT:
    DCS_LEAVE("__dcs_fpwb_thread:%d leave \n", threadp->seqno);
    return NULL;
}

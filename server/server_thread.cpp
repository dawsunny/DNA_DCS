/*thread creation and init*/


#include "amp.h"
#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"

#include "server_cnd.h"
#include "server_thread.h"
#include "server_op.h"
#include "server_map.h"
#include "chunker.h"

#include <openssl/sha.h>
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

dcs_thread_t server_thread[DCS_SERVER_THREAD_NUM];
dcs_thread_t collect_thread;
dcs_thread_t upload_thread;

/*create server thread*/
dcs_s32_t __dcs_create_server_thread(void)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;

    dcs_thread_t *threadp = NULL;

    DCS_ENTER("__dcs_create_server_thread enter \n");

    DCS_MSG("__dcs_create_server_thread thread num:%d \n", DCS_SERVER_THREAD_NUM);
    
    /*
    threadp = &upload_thread;
    threadp->seqno = 0;
    threadp->is_up = 0;
    threadp->to_shutdown = 0;
    sem_init(&threadp->startsem, 0, 0);
    sem_init(&threadp->stopsem, 0, 0);
    rc = pthread_create(&threadp->thread, NULL, __dcs_server_upload_thread, (void *)threadp);
    if(rc){
        DCS_ERROR("__dcs_create_server_upload thread create thread error: %d\n", rc);
        goto EXIT;
    }
    sem_wait(&threadp->startsem);
     */

    for(i=0; i<DCS_SERVER_THREAD_NUM; i++){
        threadp = &server_thread[i];
        threadp->seqno = i;
        threadp->is_up = 0;
        threadp->to_shutdown = 0;
        sem_init(&threadp->startsem, 0, 0);
        sem_init(&threadp->stopsem, 0, 0);
        rc = pthread_create(&threadp->thread,
                            NULL,
                            __dcs_server_thread,
                            (void *)threadp);
        if(rc){
            DCS_ERROR("__dcs_create_server_thread create thread error:%d \n", rc);
            goto EXIT;
        }

        sem_wait(&threadp->startsem);
    }

    /*
    threadp = &collect_thread;
    threadp->seqno = 0;
    threadp->is_up = 0;
    threadp->to_shutdown = 0;
    sem_init(&threadp->startsem, 0, 0);
    sem_init(&threadp->stopsem, 0, 0);
    rc = pthread_create(&threadp->thread,
                        NULL,
                        __dcs_server_collect_thread,
                        (void *)threadp);
    if(rc){
        DCS_ERROR("__dcs_create_collect_server_thread create thread error:%d \n", rc);
        goto EXIT;
    }

    sem_wait(&threadp->startsem);
     */

EXIT:
    DCS_LEAVE("__dcs_create_server_thread leave \n");
    return rc;
}

void * __dcs_server_upload_thread(void *argv){
    dcs_thread_t *threadp = NULL;
    threadp = (dcs_thread_t *)argv;
    DCS_ENTER("__dcs_server_upload_thread enter \n");

    sem_post(&threadp->startsem);
    threadp->is_up = 1;

    sha_bf_upload_to_master();

    DCS_LEAVE("__dcs_server_upload_thread exit, map file upload complete\n");
    return NULL;
}

/*thread for collecting disk usage info */
void *__dcs_server_collect_thread(void *argv)
{
    dcs_s32_t rc = 0;
    
    dcs_thread_t *threadp = NULL;

    threadp = (dcs_thread_t *)argv;
    
    DCS_ENTER("__dcs_server_collect_thread enter \n");

    sem_post(&threadp->startsem);
    threadp->is_up = 1;

    while(1){
        DCS_MSG("__dcs_collect_server_thread thread: %d ready to work \n", threadp->seqno);
        if(threadp->to_shutdown){
            DCS_MSG("__dcs_collect_server_thread thread: %d  exit\n", threadp->seqno);
            goto EXIT;
        }
        rc = server_collect_diskinfo();
        if(rc != 0){
            DCS_ERROR("collect diskinfo err : %d \n",rc);
            break;
        }
        sleep(SLEEP_TIME);
    }

EXIT:
    DCS_LEAVE("__dcs_server_collect_thread leave \n");
    return NULL;
}

/*server thread*/
void *__dcs_server_thread(void *argv)
{
    dcs_s32_t rc = 0;
    
    amp_request_t *req = NULL;
    dcs_thread_t *threadp = NULL;

    threadp = (dcs_thread_t *)argv;
    
    DCS_ENTER("__dcs_server_thread enter \n");

    sem_post(&threadp->startsem);
    threadp->is_up = 1;

    while(1){
        DCS_MSG("__dcs_server_thread thread: %d ready to work \n",
                threadp->seqno);       
        sem_wait(&request_sem);
        if(threadp->to_shutdown){
            sem_post(&request_sem);
            DCS_MSG("__dcs_server_thread thread: %d gona to shut down \n", 
                    threadp->seqno);
            goto EXIT;
        }
        pthread_mutex_lock(&request_queue_lock);
        req = list_entry(request_queue.next, amp_request_t, req_list);
        if(req == NULL){
            DCS_ERROR("__dcs_server_thread thread:%d get filename err: %d \n",
                    threadp->seqno, errno);
            pthread_mutex_unlock(&request_queue_lock);
            continue;
        }
        list_del(&req->req_list);
        pthread_mutex_unlock(&request_queue_lock);

        rc = __dcs_server_process_req(req, threadp);
        if(rc != 0){
            DCS_ERROR("__dcs_server_thread:%d fail to process req \n",
                    threadp->seqno);
        }

    }

EXIT:
    DCS_LEAVE("__dcs_server_thread:%d leave \n", threadp->seqno);
    return NULL;
}

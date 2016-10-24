/*master thread creation*/


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

dcs_thread_t master_thread[DCS_MASTER_THREAD_NUM];

/*create master thread*/
dcs_s32_t __dcs_master_create_thread(void)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;

    dcs_thread_t *threadp = NULL;

    DCS_ENTER("__dcs_create_master_thread enter \n");

    DCS_MSG("__dcs_create_master_thread thread num:%d \n", DCS_MASTER_THREAD_NUM);

    for(i=0; i<DCS_MASTER_THREAD_NUM; i++){
        threadp = &master_thread[i];
        threadp->seqno = i;
        threadp->is_up = 0;
        threadp->to_shutdown = 0;
        sem_init(&threadp->startsem, 0, 0);
        sem_init(&threadp->stopsem, 0, 0);
        rc = pthread_create(&threadp->thread,
                            NULL,
                            __dcs_master_thread,
                            (void *)threadp);
        if(rc){
            DCS_ERROR("__dcs_create_master_thread create thread error:%d \n", rc);
            goto EXIT;
        }

        sem_wait(&threadp->startsem);
    }

EXIT:
    DCS_LEAVE("__dcs_create_master_thread leave \n");
    return rc;
}

/*master thread
 * get req and deal with it 
 */
void *__dcs_master_thread(void *argv)
{
    dcs_u32_t rc = 0;

    amp_request_t *req = NULL;
    dcs_thread_t *threadp = NULL;

    threadp = (dcs_thread_t *)argv;
    
    DCS_ENTER("__dcs_master_thread enter \n");

    sem_post(&threadp->startsem);
    threadp->is_up = 1;

    while(1){
        DCS_MSG("__dcs_master_thread thread: %d ready to work \n",
                threadp->seqno);       
        sem_wait(&request_sem);
        if(threadp->to_shutdown){
            sem_post(&request_sem);
            DCS_MSG("__dcs_master_thread thread: %d gona to shut down \n", 
                    threadp->seqno);
            goto EXIT;
        }

        pthread_mutex_lock(&request_queue_lock);
        req = list_entry(request_queue.next, amp_request_t, req_list);
        if(req == NULL){
            DCS_ERROR("__dcs_master_thread thread:%d get filename err: %d \n",
                    threadp->seqno, errno);
            pthread_mutex_unlock(&request_queue_lock);
            continue;
        }
        list_del(&req->req_list);
        pthread_mutex_unlock(&request_queue_lock);

        rc = __dcs_master_process_req(req);
        if(rc != 0){
            DCS_ERROR("__dcs_master_thread:%d fail to process req \n",
                    threadp->seqno);
        }
        /*modify by weizheng, free req_msg and req_iov*/ 
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
    }

EXIT:
    DCS_LEAVE("__dcs_master_thread:%d leave \n", threadp->seqno);
    return NULL;
}

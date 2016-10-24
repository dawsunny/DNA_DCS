/*thread start and stop*/

#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"

#include "amp.h"

#include "client_thread.h"
#include "client_cnd.h"
#include "client_op.h"

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
#include <semaphore.h>

struct list_head file_queue;
struct list_head read_queue;
struct list_head dir_queue;
dcs_u32_t file_on_run;
pthread_mutex_t file_num_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t clt_file_lock = PTHREAD_MUTEX_INITIALIZER; 
pthread_mutex_t dir_num_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t clt_dir_lock = PTHREAD_MUTEX_INITIALIZER; 
pthread_mutex_t clt_read_lock = PTHREAD_MUTEX_INITIALIZER; 
dcs_thread_t client_threads[DCS_CLIENT_THREAD_NUM]; //corresponding to file_queue?
dcs_thread_t dir_threads[DCS_CLIENT_THREAD_NUM];
dcs_thread_t read_threads[DCS_CLIENT_THREAD_NUM];
sem_t finish_sem;
sem_t file_sem;
sem_t dir_sem;
sem_t read_sem;

/* client thread
 * 1.get the queue mutex
 * 2.get the filename needed to write 
 * 3.write the file to the server
 */
void *__dcs_clt_thread(void *argv)
{
    dcs_s32_t rc = 0;

    dcs_thread_t *threadp = NULL;

    threadp = (dcs_thread_t *)argv;
    dcs_clt_file_t *tmp = NULL;
    
    DCS_ENTER("__dcs_clt_thread enter \n");

    sem_post(&threadp->startsem);
    threadp->is_up = 1;

    while(1){
        sem_wait(&file_sem);
        if(threadp->to_shutdown){
            DCS_MSG("__dcs_clt_thread thread: %d gona to shut down \n", threadp->seqno);
            goto EXIT;
        }

        pthread_mutex_lock(&clt_file_lock);
        
        tmp = list_entry(file_queue.next, dcs_clt_file_t, file_list);
        if(tmp == NULL){
            DCS_ERROR("__dcs_clt_thread thread: %d get filename err: %d \n",
                        threadp->seqno, errno);
            pthread_mutex_unlock(&clt_file_lock);
            continue;
        }
        list_del(&tmp->file_list);
        pthread_mutex_unlock(&clt_file_lock);

        DCS_MSG("__dcs_clt_thread filename: %s to be write \n", tmp->filename);
        rc = __dcs_clt_write_file(tmp->filename, threadp);
        if(rc != 0){
            DCS_ERROR("__dcs_clt_thread thread:%d fail to process write file \n",threadp->seqno);
            goto EXIT;
        }

        if(tmp != NULL){
            free(tmp);
        }
    }

EXIT:
    DCS_LEAVE("__dcs_clt_thread thread:%d  leave\n", threadp->seqno);
    return NULL;
}

void *__dcs_read_thread(void *argv)
{
    dcs_s32_t rc = 0;

    dcs_thread_t *threadp = NULL;

    threadp = (dcs_thread_t *)argv;
    dcs_clt_file_t *tmp = NULL;
    
    DCS_ENTER("__dcs_clt_thread enter \n");

    sem_post(&threadp->startsem);
    threadp->is_up = 1;

    while(1){
        sem_wait(&read_sem);
        if(threadp->to_shutdown){
            DCS_MSG("__dcs_read_thread thread: %d gona to shut down \n", threadp->seqno);
            goto EXIT;
        }

        pthread_mutex_lock(&clt_read_lock);
        
        tmp = list_entry(read_queue.next, dcs_clt_file_t, file_list);
        if(tmp == NULL){
            DCS_ERROR("__dcs_read_thread thread: %d get filename err: %d \n", threadp->seqno, errno);
            pthread_mutex_unlock(&clt_read_lock);
            continue;
        }
        list_del(&tmp->file_list);
        pthread_mutex_unlock(&clt_read_lock);

        DCS_MSG("__dcs_read_thread filename: %s to be write \n", tmp->filename);
        rc = __dcs_clt_read_file(tmp->filename, threadp);
        if(rc != 0){
            DCS_ERROR("__dcs_read_thread thread:%d fail to process write file \n",threadp->seqno);
            goto EXIT;
        }

        if(tmp != NULL){
            free(tmp);
        }
    }

EXIT:
    DCS_LEAVE("__dcs_read_thread thread:%d  leave\n", threadp->seqno);
    return NULL;
}

void *__dcs_dir_thread(void *argv)
{
    dcs_s32_t rc = 0;

    dcs_thread_t *threadp = NULL;

    threadp = (dcs_thread_t *)argv;
    dcs_dir_t *tmp = NULL;
    
    DCS_ENTER("__dcs_dir_thread enter \n");

    sem_post(&threadp->startsem);
    threadp->is_up = 1;

    while(1){
        DCS_MSG("__dcs_dir_thread thread: %d ready to work \n", threadp->seqno);
        sem_wait(&dir_sem);
        //DCS_MSG("__dcs_clt_thread thread: %d after file sem \n", threadp->seqno);
        DCS_MSG("__dcs_dir_thread thread: %d get work to do \n", threadp->seqno);
        if(threadp->to_shutdown){
            DCS_MSG("__dcs_dir_thread thread: %d gona to shut down \n", threadp->seqno);
            goto EXIT;
        }

        pthread_mutex_lock(&clt_dir_lock);
        /*
        if(head == NULL){
            pthread_mutex_unlock(&clt_file_lock);
        }
        tmp = head;
        head = head->next;
        */
        
        tmp = list_entry(dir_queue.next, dcs_dir_t, file_list);
        if(tmp == NULL){
            DCS_ERROR("__dcs_dir_thread thread: %d get dirname err: %d \n",
                        threadp->seqno, errno);
            pthread_mutex_unlock(&clt_dir_lock);
            continue;
        }
        list_del(&tmp->file_list);
        pthread_mutex_unlock(&clt_dir_lock);

        DCS_MSG("__dcs_dir_thread filename: %s to be process \n", tmp->pathname);
        //rc = __dcs_clt_write_file(tmp->filename, threadp);
        rc = __dcs_clt_get_filename(tmp->pathname);
        if(rc != 0){
            DCS_ERROR("__dcs_clt_thread thread:%d fail to process write file \n",threadp->seqno);
            goto EXIT;
        }

        if(tmp != NULL){
            free(tmp);
        }
    }

EXIT:
    DCS_LEAVE("__dcs_clt_thread thread:%d \n", threadp->seqno);
    return NULL;
}

dcs_s32_t __dcs_create_thread(void)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    //dcs_u32_t NUM = 1000000;
    dcs_thread_t *threadp = NULL;
    
    /*init the file queue*/
    INIT_LIST_HEAD(&file_queue);
    INIT_LIST_HEAD(&read_queue);
    INIT_LIST_HEAD(&dir_queue);

    file_on_run = 0;
    sem_init(&finish_sem, 0, 0);
    sem_init(&file_sem, 0, 0);
    sem_init(&read_sem, 0, 0);
    sem_init(&dir_sem, 0, 0);

    DCS_ENTER("__dcs_create_thread enter. \n");
    DCS_MSG("__dcs_create_thread thread num: %d \n", DCS_CLIENT_THREAD_NUM);
    for(i=0; i < DCS_CLIENT_THREAD_NUM; i++){
        threadp = &client_threads[i];
        threadp->seqno = i;
        threadp->is_up = 0;
        threadp->to_shutdown = 0;
        sem_init(&threadp->startsem, 0, 0);
        sem_init(&threadp->stopsem, 0, 0);
        rc = pthread_create(&threadp->thread,
                            NULL,
                            __dcs_clt_thread,
                            (void *)threadp);
        if(rc){
            DCS_ERROR("__dcs_create_thread create thread error. \n");
            goto EXIT;
        }

        sem_wait(&threadp->startsem);
        
        threadp = &read_threads[i];
        threadp->seqno = i;
        threadp->is_up = 0;
        threadp->to_shutdown = 0;
        sem_init(&threadp->startsem, 0, 0);
        sem_init(&threadp->stopsem, 0, 0);
        rc = pthread_create(&threadp->thread,
                            NULL,
                            __dcs_read_thread,
                            (void *)threadp);
        if(rc){
            DCS_ERROR("__dcs_create_thread create thread error. \n");
            goto EXIT;
        }

        sem_wait(&threadp->startsem);

        threadp = &dir_threads[i];
        threadp->seqno = i;
        threadp->is_up = 0;
        threadp->to_shutdown = 0;
        sem_init(&threadp->startsem, 0, 0);
        sem_init(&threadp->stopsem, 0, 0);
        rc = pthread_create(&threadp->thread,
                            NULL,
                            __dcs_dir_thread,
                            (void *)threadp);
        if(rc){
            DCS_ERROR("__dcs_create_thread create thread error. \n");
            goto EXIT;
        }

        sem_wait(&threadp->startsem);
    }

EXIT:
    DCS_LEAVE("__dcs_create_thread leave. \n");
    return rc;
}

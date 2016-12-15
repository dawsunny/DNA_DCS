#ifndef __DC_THREAD_H_
#define __DC_THREAD_H_

#include <pthread.h>

#include "dc_type.h"
#include "dc_const.h"

//task struct
struct dc_task
{
    dc_s8_t input_path[FILE_PATH_LEN];
    dc_s8_t output_name[FILE_PATH_LEN];

    struct dc_task *next;
};
typedef struct dc_task dc_task_t;

//thread pool struct
struct dc_thread_pool
{
    pthread_mutex_t task_queue_lock;
    pthread_cond_t  task_queue_ready;

    dc_task_t *task_queue_head;
    dc_s32_t   task_queue_size;

    pthread_t *thread_id;

    dc_s32_t shutdown;  //whether destroy the thread pool
};
typedef struct dc_thread_pool dc_thread_pool_t;


extern dc_s32_t
thread_pool_init();

void *
thread_routine(void *unused_arg);

extern dc_s32_t
pool_add_task(dc_s8_t *input_path);

extern void
thread_pool_destroy();

#endif

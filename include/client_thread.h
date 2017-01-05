/*client thread header*/

#ifndef __CLIENT_THREAD_H__
#define __CLIENT_THREAD_H__


#include "dcs_list.h"
#include "amp.h"
#include <semaphore.h>

struct dcs_clt_file{
    dcs_u32_t target_server;
    dcs_u64_t filesize;
    dcs_u32_t filetype;
    dcs_s8_t filename[256];
    struct list_head file_list;
};
typedef struct dcs_clt_file dcs_clt_file_t;

struct dcs_dir_path{
    dcs_s8_t pathname[PATH_LEN];
    struct list_head file_list;
};
typedef struct dcs_dir_path dcs_dir_t;

extern dcs_u32_t file_on_run;

extern pthread_mutex_t clt_file_lock;
extern pthread_mutex_t clt_read_lock;
extern pthread_mutex_t file_num_lock;

extern pthread_mutex_t clt_dir_lock;
extern pthread_mutex_t dir_num_lock;

extern sem_t file_sem;
extern sem_t read_sem;
extern sem_t dir_sem;
extern sem_t finish_sem;
extern struct list_head file_queue;
extern struct list_head read_queue;
extern struct list_head dir_queue;

/*create write threads*/
dcs_s32_t __dcs_create_thread(void);
/*work thread*/
void *__dcs_clt_thread(void *argv);
/*dir thread*/
void *__dcs_dir_thread(void *argv);
void *__dcs_read_thread(void *argv);
#endif

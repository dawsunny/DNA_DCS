/*master thread head file*/

#ifndef __MASTER_THREAD_H_
#define __MASTER_THREAD_H_


/*create master thread*/
dcs_s32_t __dcs_master_create_thread(void);
/*master server thread*/
void *__dcs_master_thread(void *argv);

void *__dcs_master_bloom_sync_thread(void *argv);
#endif

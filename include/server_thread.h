/*server thread header*/

#ifndef __SERVER_THREAD_H__
#define __SERVER_THREAD_H__


#include "dcs_list.h"
#include "amp.h"
#include <semaphore.h>

/*create server thread*/
dcs_s32_t __dcs_create_server_thread(void);
/*server thread*/
void *__dcs_server_thread(void *argv);
/*server collect compressor disk info thread*/
void *__dcs_server_collect_thread(void *argv);
void * __dcs_server_upload_thread(void *argv);
#endif

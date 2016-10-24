/*compressor thread header*/

#ifndef __COMPRESSOR_THREAD_H__
#define __COMPRESSOR_THREAD_H__


#include "dcs_list.h"
#include "amp.h"
#include <semaphore.h>

/*create compressor thread*/
dcs_s32_t __dcs_create_compressor_thread(void);
/*compressor thread*/
void *__dcs_compressor_thread(void *argv);
/*compressor data container write back thread*/
void *__dcs_datawb_thread(void *argv);
/*compressor fp container write back thread*/
void *__dcs_fpwb_thread(void *argv);

void *__dcs_data_monitor_thread(void *argv);
#endif

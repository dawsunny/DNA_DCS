/*master cache manager*/

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

master_cache_t sha_cache[MAX_CACHE_NUM];

/*init master cache*/
dcs_s32_t __dcs_master_cache_init()
{
    dcs_s32_t i;
    dcs_s32_t rc = 0;

    DCS_ENTER("__dcs_master_cache_init enter \n");

    for(i=0; i<MAX_CACHE_NUM; i++){
        sha_cache[i].sha = NULL;
        sha_cache[i].sha_num = 0;
        pthread_mutex_init(&(sha_cache[i].cache_lock), NULL);
    }
    
    DCS_LEAVE("__dcs_master_cache_init leave \n");
    return rc;
}

/*insert FP to cache table*/
dcs_s32_t __dcs_master_cache_insert(dcs_u8_t *sha, dcs_u32_t sha_num)
{
    dcs_s32_t i;
    dcs_s32_t rc = 0;

    DCS_ENTER("__dcs_master_cache_insert enter \n");

    for(i=0; i<MAX_CACHE_NUM; i++){
        pthread_mutex_lock(&(sha_cache[i].cache_lock));
        if(sha_cache[i].sha == NULL){
            sha_cache[i].sha = sha;
            sha_cache[i].sha_num = sha_num;
            rc = i;
            pthread_mutex_unlock(&(sha_cache[i].cache_lock));
            break;
        }
        pthread_mutex_unlock(&(sha_cache[i].cache_lock));
    }

    if(i == MAX_CACHE_NUM){
        rc = -1;
        DCS_ERROR("__dcs_master_cache_insert no space to cache err:%d \n", rc);
        goto EXIT;
    }

EXIT:
    DCS_LEAVE("__dcs_master_cache_insert leave \n");
    return rc;
}



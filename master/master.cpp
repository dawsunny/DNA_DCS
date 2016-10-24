/*master main function*/

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

dcs_s32_t main(dcs_s32_t argc, dcs_s8_t **argv)
{
    dcs_s32_t rc = 0;

    DCS_ENTER("main enter \n");

    rc = __dcs_master_parse_paramatter(argc, argv);
    if(rc != 0){
        DCS_ERROR("main __dcs_master_parse_paramatter err:%d \n", rc);
        goto EXIT;
    }

    rc = __dcs_master_init_com();
    if(rc != 0){
        DCS_ERROR("main __dcs_master_init_com err:%d \n", rc);
        goto EXIT;
    }

    rc = __dcs_master_tier_bloom_init();
    //rc = __dcs_bloom_init();
    if(rc != 0){
        DCS_ERROR("main __dcs_tier_bloom_init() err:%d \n", rc);
        goto EXIT;
    }

    usage();

    rc = __dcs_master_create_thread();
    if(rc != 0){
        DCS_ERROR("main __dcs_create_master_thread err:%d \n", rc);
        goto EXIT;
    }

    while(1){
        sleep(30);
    }

EXIT:
    DCS_LEAVE("main leave \n");
    return rc;
}

/*compressor main funtion*/

#include "amp.h"
#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"

#include "compressor_cache.h"
#include "compressor_cnd.h"
#include "compressor_con.h"
#include "compressor_thread.h"
#include "compressor_index.h"
#include "bloom.h"
#include "hash.h"
#include "compressor_op.h"

//#include <stropts.h>
#include <stdint.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <string.h>
#include <semaphore.h>

dcs_s32_t main(dcs_s32_t argc, dcs_s8_t **argv)
{
    dcs_s32_t rc = 0;

    DCS_ENTER("compressor enter \n");

    /*parse paramatter*/
    rc = __dcs_compressor_parse_paramatter(argc, argv);
    if(rc != 0){
        DCS_ERROR("main __dcs_compressor_parse_paramatter err \n");
        goto EXIT;
    }

    rc = __compressor_com_init();
    if(rc != 0){
        DCS_ERROR("main __dcs_compressor_init_com err \n");
        goto EXIT;
    }

    //rc = __dcs_compressor_bloom_init();
#ifdef __DCS_TIER_BLOOM__
    //rc = __dcs_tier_bloom_init();
    if(rc != 0){
        DCS_ERROR("main __dcs_tier_bloom_init\n");
        goto EXIT;
    }
#endif

    rc = container_list_init();
    if(rc != 0){
        DCS_ERROR("main container_list_init \n");
        goto EXIT;
    }

    rc = cache_table_init();
    if(rc != 0){
        DCS_ERROR("main cache_table_init \n");
        goto EXIT;
    }

    rc = data_cache_init();
    if(rc != 0){
        DCS_ERROR("main data_cache_init err \n");
        goto  EXIT;
    }

    rc = container_table_init();
    if(rc != 0){
        DCS_ERROR("main container_table_init \n");
        goto EXIT;
    }

    rc = __dcs_compressor_index_init();
    if(rc != 0){
        DCS_ERROR("main __dcs_compressor_index_init");
        goto EXIT;
    }

    rc = __dcs_create_compressor_thread();
    if(rc != 0){
        DCS_ERROR("main __dcs_create_compressor_thread \n");
        goto EXIT;
    }

    while(1){
        sleep(30);
    }

EXIT:
    DCS_LEAVE("main leave \n");
    return rc;
}

/*server main funtion*/

#include "amp.h"
#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"

#include "server_cnd.h"
#include "server_thread.h"
#include "server_op.h"
#include "server_map.h"
#include "chunker.h"

#include <openssl/sha.h>
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

/*main funtion*/
dcs_s32_t main(dcs_s32_t argc, dcs_s8_t **argv)
{
    dcs_s32_t rc = 0;

    DCS_ENTER("main enter \n");

    rc = __dcs_server_parse_paramatter(argc, argv);
    if(rc != 0){
        DCS_ERROR("main parase_paramatter error \n");
        goto EXIT;
    }

    rc = __dcs_server_init_com();
    if(rc != 0){
        DCS_ERROR("main init com error \n");
        goto EXIT;
    }

    rc = __dcs_server_maptable_init();
    if(rc != 0){
        DCS_ERROR("main init maptable err \n");
        goto EXIT;
    }

    rc = __dcs_server_mapbuf_init();
    if(rc != 0){
        DCS_ERROR("main init mapbuf err \n");
        goto EXIT;
    }
    
    rc = __dcs_server_init_diskinfo();

    rc = __dcs_create_server_thread();
    if(rc != 0){
        DCS_ERROR("main create thread error \n");
        goto EXIT;
    }

    while(1){
        sleep(30);
    }

EXIT:
    DCS_LEAVE("main leave \n");
    return rc;
}

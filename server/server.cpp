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
#include <map>
#include <string>
#include <iostream>
using namespace std;

/*main funtion*/
dcs_s32_t main(dcs_s32_t argc, dcs_s8_t **argv)
{
    dcs_s32_t rc = 0;

    DCS_ENTER("main enter \n");
    
    //map<string, server_hash_t>::iterator it;

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

    /*
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
*/
    printf("Read server map info, please wait...\n");
    if (access(SERVER_MAP_PATH, 0) != 0) {
        printf("no server map info.\n");
    } else {
        pthread_mutex_lock(&server_table_lock);
        rc = __dcs_server_read_mapinfo();
        pthread_mutex_unlock(&server_table_lock);
        if (rc != 0) {
            DCS_ERROR("read server map info error\n");
            goto EXIT;
        } else {
            printf("done.\n");
        }
    }
    
    /*
    for (it = server_table.begin(); it != server_table.end(); ++it) {
        cout << it->first << endl;
        printf("\t%u\n", it->second.filetype);
        printf("\t%u\n", it->second.compressor_id);
        printf("\t%lu\n", it->second.filesize);
        printf("\t%lu\n", it->second.inode);
        printf("\t%lu\n", it->second.timestamp);
        printf("\t%s\n", it->second.md5);
    }
     */
    
    rc = __dcs_create_server_thread();
    if(rc != 0){
        DCS_ERROR("main create thread error \n");
        goto EXIT;
    }

    printf("Ready.\n");
    while(1){
        sleep(30);
    }

EXIT:
    DCS_LEAVE("main leave \n");
    return rc;
}

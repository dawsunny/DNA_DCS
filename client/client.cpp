/* client.c */

#include "amp.h"
#include "dcs_type.h"
#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"

#include "client_thread.h"
#include "client_cnd.h"
#include "client_op.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

/*main function*/
dcs_s32_t main(dcs_s32_t argc, dcs_s8_t **argv)
{
    dcs_s32_t rc;

    rc = __dcs_clt_parse_parameter(argc, argv) ;
    if(rc < 0){
        DCS_ERROR("main parse parameter error\n");
        goto EXIT;
    }

    rc = __dcs_clt_init_com();
    if(rc < 0){
        DCS_ERROR("main communication init error. \n");
        goto EXIT;
    }

    rc = __dcs_create_thread();
    if(rc < 0){
        DCS_ERROR("main create thread err \n");
        goto EXIT;
    }

    rc = __dcs_clt_op();
    if(rc < 0){
        DCS_ERROR("main client read or write err. \n");
        goto EXIT;
    }
    
    /*wait for the file on run finish*/
    if(clt_optype == DCS_WRITE || clt_optype == DCS_READ){
        sleep(1);
        while(file_on_run){
            sem_wait(&finish_sem);
            pthread_mutex_lock(&file_num_lock);
            file_on_run --;
            DCS_MSG("file_on_run %d \n", file_on_run);
            pthread_mutex_unlock(&file_num_lock);
        }
    }

EXIT:

    //sleep(300);
    DCS_LEAVE("main leave \n");
    return rc;
}

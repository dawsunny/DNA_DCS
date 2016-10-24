/*compressor configuration*/

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

amp_comp_context_t  *compressor_comp_context;
dcs_u32_t         compressor_this_id;
pthread_mutex_t     request_queue_lock = PTHREAD_MUTEX_INITIALIZER;
sem_t               request_sem;
dcs_u32_t         power;
dcs_u32_t         daemonlize = 0;
dcs_u64_t         container = 1;
pthread_mutex_t     container_lock = PTHREAD_MUTEX_INITIALIZER;
//add by bxz
struct list_head request_queue;

/*parse paramatter of server*/
dcs_s32_t __dcs_compressor_parse_paramatter(dcs_s32_t argc, dcs_s8_t **argv)
{
    dcs_s8_t c;
    dcs_s32_t rc = 0;

    daemonlize = 1;

    DCS_ENTER("__dcs_server_parse_paramatter enter \n");
    
    if(argc < 3){
__error_paramatter:
        printf("Usage:\t %s [-b | -f] id \n", argv[0]);
        printf("\t -b ---- running on the background \n");
        printf("\t -f ---- running on the foreground \n");
        printf("\t id ---- the id of this server \n");
        rc = -1;
        goto EXIT;
    }

    compressor_this_id = atoi(argv[argc - 1]);

    while((c = getopt(argc, argv, "bf")) != EOF){
        switch(c){
            case '?':
                goto __error_paramatter;
                break;
            case 'b':
                daemonlize = 1;
                break;
            case 'f':
                daemonlize = 0;
                break;
        }
    }

    if(daemonlize){
        __dcs_daemonlize();
    }

    power = get_sample_power();

EXIT:
    DCS_LEAVE("__dcs_compressor_parse_paramatter leave \n");
    return rc;
}

/*init the connection between server and compressor*/
dcs_s32_t __compressor_com_init()
{
    dcs_s32_t rc = 0;

    DCS_ENTER("__compressor_com_init enter \n");

    INIT_LIST_HEAD(&request_queue);
    sem_init(&request_sem, 0, 0);

    compressor_comp_context = amp_sys_init(DCS_NODE, compressor_this_id);
    if(!compressor_comp_context){
        DCS_ERROR("__compressor_com_init , create context err \n");
        rc = -1;
        goto EXIT;
    }

    rc = amp_create_connection(compressor_comp_context,
                                0,
                                0,
                                INADDR_ANY,
                                COMPRESSOR_PORT,
                                AMP_CONN_TYPE_TCP,
                                AMP_CONN_DIRECTION_LISTEN,
                                __queue_req,
                                __compressor_allocbuf,
                                __compressor_freebuf);
    if(rc < 0){
        DCS_ERROR("__compressor_com_init, amp create connection err. \n");
        amp_sys_finalize(compressor_comp_context);
        rc = -1;
        goto EXIT;
    }


EXIT:
    DCS_LEAVE("__compressor_com_init leave \n");
    return rc;

}

dcs_s32_t __compressor_allocbuf(void *msgp, 
                amp_u32_t *niov, amp_kiov_t **iov)
{
    dcs_s32_t err = 0;
    dcs_u32_t bufsize;
    dcs_s8_t *buf = NULL;
    amp_kiov_t *kiov = NULL;
    dcs_msg_t *recvmsgp = NULL;
    
    DCS_ENTER("__compressor_allocbuf enter. \n");

    recvmsgp = (dcs_msg_t *)msgp;
    if(recvmsgp->optype == DCS_DELETE){
        bufsize = (recvmsgp->u.m2d_del_req.sha_num) * sizeof(dcs_datamap_t);
    }else if(recvmsgp->optype == DCS_UPLOAD){
        bufsize = (recvmsgp->u.s2d_upload_req.sha_num) * sizeof(conid_sha_block_t);
    }else{
        bufsize = (recvmsgp->u.s2d_req.chunk_num) * sizeof(sha_array_t) + recvmsgp->u.s2d_req.scsize;
    }

    //chunk_num = recvmsgp->u.s2d_req.chunk_num;
    //bufsize = chunk_num * sizeof(sha_array_t) + recvmsgp->u.s2d_req.scsize;
    DCS_MSG("__compressor_allocbuf, bufsize is %d \n", bufsize);
    buf = (dcs_s8_t *)malloc(bufsize);
    if(buf == NULL){
        err = errno;
        DCS_ERROR("__compressor_allocbuf, fail to malloc for buf,err:%d\n", errno);
        goto EXIT;
    }

    kiov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if(!kiov){
        DCS_ERROR("__compressor_allocbuf, fail to malloc for kiov. \n");
        err = -ENOMEM;
        goto EXIT;
    }

    kiov->ak_addr = buf;
    kiov->ak_len = bufsize;
    kiov->ak_offset = 0;
    kiov->ak_flag = 0;

    *iov = kiov;
    *niov = 1;

EXIT:
    DCS_LEAVE("__compressor_allocbuf leave. \n");
    return err;
}

void __compressor_freebuf(amp_u32_t niov, amp_kiov_t *iov)
{
    dcs_s32_t i;
    amp_kiov_t *kiovp = NULL;
    kiovp = iov;
    
    DCS_ENTER("__compressor_freebuf enter. \n");
    for(i=0; i<niov; i++){
        if(kiovp->ak_addr)
            free(kiovp->ak_addr);
        kiovp ++;
    }
    DCS_LEAVE("__compressor_freebuf leave. \n");
}

dcs_s32_t __queue_req(amp_request_t *req)
{
    DCS_ENTER("__queue_req enter. \n");

    pthread_mutex_lock(&request_queue_lock);
    list_add_tail(&req->req_list, &request_queue);
    pthread_mutex_unlock(&request_queue_lock);
    sem_post(&request_sem);

    DCS_LEAVE("__queue_req leave. \n");
    return 1;
}

/*
 * init background process
 */
inline int  __dcs_daemonlize()
{
	int	i, fd;

    DCS_ENTER("__dcs_daemonlize enter .\n");
	if (fork() != 0)
		exit(0);

	if ((fd = open("/dev/tty", O_RDWR)) >= 0) {	// disassociate
							 					//contolling tty 
		ioctl(fd, TIOCNOTTY, (char *) NULL);
		close(fd);
	}

	setpgrp();

	for (fd = 0; fd < 3; fd++)
		close(fd);

	i = open("/dev/null", O_RDONLY, 0);
	if (i != 0) {
		abort();
	}
	i = open("/dev/null", O_WRONLY, 0);
	if (i != 1) {
		abort();
	}
	
    {
		char *p;
		char prg_name[256];
		char log_name[1024];
		char hostname[256];

		strcpy (prg_name, getenv("_"));
		i = strlen (prg_name);
		while (*(prg_name + i) != '/') i --;
		i ++;
		p = &(prg_name [i]);

		gethostname(hostname, 256);
	
		sprintf (log_name, "%s%s.%s.log", DCS_LOG_FILE_PATH, p, hostname); 
		unlink (log_name);

		i = open(log_name, (O_WRONLY | O_CREAT), 0666);
		if (i < 0) {
            DCS_MSG("open log file err,errno:%d \n", errno);
			abort();
		}

        DCS_ERROR("log file :%s\n", log_name);
	}
	setpriority (PRIO_PGRP, getpgrp(), -20);
	
    DCS_LEAVE("__dcs_daemonlize leave. \n");
	return 0;
}

dcs_u32_t get_sample_power()
{
    dcs_s32_t rc = 0;
    dcs_u32_t count = 0;
    dcs_u32_t a;

    a = SAMPLE_RATE;
    while(a){
        a = a/2;
        count++;
    }

    rc = count - 1;

    return rc; 
}

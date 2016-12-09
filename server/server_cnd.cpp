/*server configuration*/

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

amp_comp_context_t  *server_comp_context;
dcs_u32_t         server_this_id;
dcs_u32_t         server_chunk_type;
dcs_u32_t         server_chunk_size;
dcs_u32_t         server_rout_type;
pthread_mutex_t     request_queue_lock = PTHREAD_MUTEX_INITIALIZER;
sem_t               request_sem;
//add by bxz
struct list_head request_queue;

/*parse paramatter of server*/
dcs_s32_t __dcs_server_parse_paramatter(dcs_s32_t argc, dcs_s8_t **argv)
{
    dcs_u32_t daemonlize;
    dcs_s8_t c;
    dcs_s32_t rc = 0;

    daemonlize = 1;

    DCS_ENTER("__dcs_server_parse_paramatter enter \n");

    if(argc < 9){
    __error_paramatter:
        printf("Usage:\t %s [-c] chunktype [-s] chunksize [-t] routingtype [-d | -f] id \n", argv[0]);
        printf("\t -c ---- chunktype 0 means fixed chunk 1 means var chunk \n");
        printf("\t -s ---- chunksize unit is byte \n");
        printf("\t -t ---- data routing type 0 means stateless 1 means stateful \n");
        sem_init(&request_sem, 0, 0);
        printf("\t -b ---- running on the background \n");
        printf("\t -f ---- running on the foreground \n");
        printf("\t id ---- the id of this server \n");
        rc = -1;
        goto EXIT;
    }

    server_this_id = atoi(argv[argc -1]);

    while ((c = getopt(argc, argv, "c:s:t:bf")) != EOF){
        switch(c){
            case '?':
                goto __error_paramatter;
                break;
            case 'c':
                server_chunk_type = atoi(optarg);
                if(server_chunk_type == 0){
                    DCS_MSG("__dcs_server_parse_paramatter server chunk type is fixed \n");
                }
                else if(server_chunk_type == 1){
                    DCS_MSG("__dcs_server_parse_paramatter server chunk type is var \n");
                }
                else{
                    DCS_MSG("__dcs_server_parse_paramatter server chunk type error \n");
                    goto __error_paramatter;
                }
                break;
            case 's':
                server_chunk_size = atoi(optarg);
                if(server_chunk_size <= 0){
                    DCS_ERROR("__dcs_server_parse_paramatter set server_chunk_size error \n");
                    goto __error_paramatter;
                }
                break;
            case 't':
                server_rout_type = atoi(optarg);
                if(server_rout_type == 0){
                    DCS_MSG("__dcs_server_parse_paramatter server routing type is stateless \n");
                }
                else if(server_rout_type == 1){
                    DCS_MSG("__dcs_server_parse_paramatter server routing type is stateful \n");
                }
                else{
                    goto __error_paramatter;
                }
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


EXIT:
    DCS_LEAVE("__dcs_server_parse_paramatter leave \n");
    return rc;
}

/*init the connection
 * 1. create connection between client server
 * //2. create connection between server master
 * 3. create connection between server compressor
 */
dcs_s32_t __dcs_server_init_com()
{
    dcs_s32_t rc = 0;
    dcs_u32_t addr;
    dcs_u32_t i = 0;
    dcs_s8_t  con_addr[ADDR_LEN]; 
    struct in_addr inaddr;
    
    DCS_ENTER("__dcs_server_init_com enter \n");

    FILE *fp;
    /* //bxz
    if((fp = fopen(MASTER_ADDR_CONF, "r")) == NULL){
        DCS_ERROR("__dcs_server_init_com open master_addr configuration err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
     */

    INIT_LIST_HEAD(&request_queue);
    sem_init(&request_sem, 0, 0);

    server_comp_context = amp_sys_init(DCS_SERVER, server_this_id);
    if(!server_comp_context){
        DCS_ERROR("__dcs_server_init_com init com context err \n");
        rc = -1;
        goto EXIT;
    }

    //connect to client
    rc = amp_create_connection(server_comp_context,
                                0,
                                0,
                                INADDR_ANY,
                                SERVER_PORT,
                                AMP_CONN_TYPE_TCP,
                                AMP_CONN_DIRECTION_LISTEN,
                                __queue_req,
                                __server_allocbuf,
                                __server_freebuf);

    if(rc < 0){
        DCS_ERROR("__dcs_server_init_com create connection between client and server err: %d \n", errno);
        amp_sys_finalize(server_comp_context);
        rc = errno;
        goto EXIT;
    }

    /* //bxz
    for(i=0; i<DCS_MASTER_NUM; i++){
        memset(con_addr, 0, ADDR_LEN);
        if(fgets(con_addr, ADDR_LEN, fp)){
            DCS_MSG("get addr string and master addr is %s", con_addr);
        }
        
        if (!inet_aton(con_addr, &inaddr)){
            DCS_ERROR("__dcs_server_init_com master inet error. \n");
            DCS_MSG("the master addr is %s \n", con_addr);
            rc = -1;
            goto EXIT;
        }
        addr = inaddr.s_addr;
        addr = ntohl(addr);
    
        rc = amp_create_connection(server_comp_context,
                DCS_MASTER,
                (i+1),
                addr,
                MASTER_PORT,
                AMP_CONN_TYPE_TCP,
                AMP_CONN_DIRECTION_CONNECT,
                NULL, 
                __server_allocbuf1,
                __server_freebuf);

        if(rc != 0){
            DCS_ERROR("__dcs_server_init_com create connection between server master err :%d \n", errno);
            amp_sys_finalize(server_comp_context);
            rc = errno;
            goto EXIT;
        }
    }
    fclose(fp);
     */

    if((fp = fopen(COMPRESSOR_ADDR_CONF, "r")) == NULL){
        DCS_ERROR("__dcs_server_init_com open compressor_addr configuration err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }

    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        memset(con_addr, 0, ADDR_LEN);
        if(fgets(con_addr, ADDR_LEN, fp)){
            DCS_MSG("get addr string and compressor addr is %s", con_addr);
        }
        
        if (!inet_aton(con_addr, &inaddr)){
            DCS_ERROR("__dcs_server_init_com compressor inet error. \n");
            DCS_MSG("the compressor addr is %s \n", con_addr);
            rc = -1;
            goto EXIT;
        }
        addr = inaddr.s_addr;
        addr = ntohl(addr);
    
        rc = amp_create_connection(server_comp_context,
                DCS_NODE,
                (i+1),
                addr,
                COMPRESSOR_PORT,
                AMP_CONN_TYPE_TCP,
                AMP_CONN_DIRECTION_CONNECT,
                NULL, 
                __server_allocbuf2,
                __server_freebuf);

        if(rc != 0){
            DCS_ERROR("__dcs_server_init_com create connection between server master err :%d \n", errno);
            amp_sys_finalize(server_comp_context);
            rc = errno;
            goto EXIT;
        }
    }
    fclose(fp);

EXIT:
   
    DCS_LEAVE("__dcs_server_init_com leave \n");
    return rc;
}

dcs_s32_t __queue_req(amp_request_t *req)
{
    DCS_ENTER("__queue_req enter. \n");
    pthread_mutex_lock(&request_queue_lock);
    //DCS_MSG("1 \n");
    list_add_tail(&req->req_list, &request_queue);
    //DCS_MSG("2 \n");
    pthread_mutex_unlock(&request_queue_lock);
    sem_post(&request_sem);
    //DCS_MSG("3 \n");
    DCS_LEAVE("__queue_req leave. \n");
    return 1;
}

dcs_s32_t __server_allocbuf(void *msgp, 
                amp_u32_t *niov, amp_kiov_t **iov)
{
    dcs_s32_t rc = 0;
    dcs_u32_t bufsize;
    dcs_s8_t *buf = NULL;
    amp_kiov_t *kiov = NULL;
    dcs_msg_t *recvmsgp = NULL;

    DCS_ENTER("__server_allocbuf enter. \n");

    recvmsgp = (dcs_msg_t *)msgp;
    bufsize = recvmsgp->u.c2s_req.size;
    DCS_MSG("__server_allocbuf, bufsize is %d \n", bufsize);
    buf = (dcs_s8_t *)malloc(bufsize);
    if(buf == NULL){
        rc = errno;
        DCS_ERROR("__server_allocbuf, fail to malloc for buf,err:%d\n", errno);
        goto EXIT;
    }

    kiov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if(!kiov){
        DCS_ERROR("__server_allocbuf, fail to malloc for kiov. \n");
        rc = -ENOMEM;
        goto EXIT;
    }

    kiov->ak_addr = buf;
    kiov->ak_len = bufsize;
    kiov->ak_offset = 0;
    kiov->ak_flag = 0;

    *iov = kiov;
    *niov = 1;

EXIT:
    DCS_LEAVE("__server_allocbuf leave. \n");
    return rc;
}

/*alloc buf for master reply message*/
dcs_s32_t __server_allocbuf1(void *msgp, 
                amp_u32_t *niov, amp_kiov_t **iov)
{
    dcs_s32_t rc = 0;
    dcs_u32_t bufsize;
    dcs_s8_t *buf = NULL;
    amp_kiov_t *kiov = NULL;
    dcs_msg_t *recvmsgp = NULL;

    DCS_ENTER("__server_allocbuf1 enter. \n");

    recvmsgp = (dcs_msg_t *)msgp;
    bufsize = sizeof(dcs_u32_t)*DCS_COMPRESSOR_NUM;
    DCS_MSG("__server_allocbuf1, bufsize is %d \n", bufsize);
    buf = (dcs_s8_t *)malloc(bufsize);
    if(buf == NULL){
        rc = errno;
        DCS_ERROR("__server_allocbuf1, fail to malloc for buf,err:%d\n", errno);
        goto EXIT;
    }

    kiov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if(!kiov){
        DCS_ERROR("__server_allocbuf1, fail to malloc for kiov. \n");
        rc = -ENOMEM;
        goto EXIT;
    }

    kiov->ak_addr = buf;
    kiov->ak_len = bufsize;
    kiov->ak_offset = 0;
    kiov->ak_flag = 0;

    *iov = kiov;
    *niov = 1;

EXIT:
    DCS_LEAVE("__server_allocbuf1 leave. \n");
    return rc;
}


/*server allocbuf for compressor reply info*/
dcs_s32_t __server_allocbuf2(void *msgp, 
                amp_u32_t *niov, amp_kiov_t **iov)
{
    dcs_s32_t rc = 0;
    dcs_u32_t bufsize;
    dcs_s8_t *buf = NULL;
    amp_kiov_t *kiov = NULL;
    dcs_msg_t *recvmsgp = NULL;

    DCS_ENTER("__server_allocbuf2 enter. \n");

    recvmsgp = (dcs_msg_t *)msgp;
    bufsize = recvmsgp->u.d2s_reply.bufsize;
    DCS_MSG("__server_allocbuf2, bufsize is %d \n", bufsize);
    buf = (dcs_s8_t *)malloc(bufsize);
    if(buf == NULL){
        rc = errno;
        DCS_ERROR("__server_allocbuf2, fail to malloc for buf,err:%d\n", errno);
        goto EXIT;
    }

    kiov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if(!kiov){
        DCS_ERROR("__server_allocbuf2, fail to malloc for kiov. \n");
        rc = -ENOMEM;
        goto EXIT;
    }

    kiov->ak_addr = buf;
    kiov->ak_len = bufsize;
    kiov->ak_offset = 0;
    kiov->ak_flag = 0;

    *iov = kiov;
    *niov = 1;

EXIT:
    DCS_LEAVE("__server_allocbuf2 leave. \n");
    return rc;
}


/*server freebuf*/
void __server_freebuf(amp_u32_t niov, amp_kiov_t *iov)
{
    dcs_s32_t i;
    amp_kiov_t *kiovp = NULL;
    kiovp = iov;
    
    DCS_ENTER("__server_freebuf enter. \n");
    for(i=0; i<niov; i++){
        if(kiovp->ak_addr)
            free(kiovp->ak_addr);
        kiovp ++;
    }
    DCS_LEAVE("__server_freebuf leave. \n");
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


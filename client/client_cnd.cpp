/*client configuration*/
#include "amp.h"
#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"

#include "client_thread.h"
#include "client_cnd.h"
#include "client_op.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <unistd.h>
//#include <stropts.h>
#include <sys/time.h>
#include <sys/resource.h>

amp_comp_context_t  *clt_comp_context;
dcs_u32_t         clt_this_id;
dcs_u32_t         clt_num;
dcs_u32_t         server_num = DCS_SERVER_NUM;
dcs_u32_t         clt_optype;
//dcs_u32_t         server_id;
dcs_s8_t          *clt_pathname = NULL;

//add by bxz
dcs_s32_t       clt_filetype;

/* client parse paramatter */
dcs_s32_t __dcs_clt_parse_parameter(dcs_s32_t argc, dcs_s8_t **argv)
{
    dcs_u32_t daemonlize;
    dcs_s8_t  c;
    dcs_s32_t rc = 0;

    daemonlize = 1;

    DCS_ENTER("__dcs_clt_parse_paramatter enter \n");
    
    if(argc < 7){
    __error_paramatter:
        printf("Usage:\t %s  [-w | -r | -d | -l | -D] path [-t] type [-b | -f]  id\n", argv[0]);
        printf("\t-l path ---- list files of pathname\n");
        printf("\t-d path ---- delete file\n");
        printf("\t-D path ---- delele dir\n");
        printf("\t-w path ---- write file|dir\n");
        printf("\t-r path ---- read file|dir\n");
        //add by bxz
        printf("\t-t type ---- a: FASTA file; q: FASTQ file\n");
        
        printf("\t-b ---- running on background, it's default option\n");
        printf("\t-f ---- running on foreground\n");
        printf("\tid ---- the id of this mos\n");
        rc = -1;
        goto EXIT;
        //exit(1);
    }

    clt_this_id = atoi(argv[argc-1]);

    while((c = getopt(argc, argv, "w:r:l:d:D:t:bf")) != EOF){
        switch(c){
            case '?':
                goto __error_paramatter;
                break;
            case 'w':
                clt_optype = DCS_WRITE;
                clt_pathname = optarg;
                DCS_MSG("__dcs_clt_parse_paramatter optype is DCS_WRITE \n");
                break;
            case 'r':
                DCS_MSG("__dcs_clt_parse_paramatter optype is DCS_READ \n");
                clt_optype = DCS_READ;
                clt_pathname = optarg;
                break;
            case 'd':
                clt_optype = DCS_DELETE;
                clt_pathname = optarg;
                break;
            case 'l':
                clt_optype = DCS_LIST;
                clt_pathname = optarg;
                break;
            case 'D':
                clt_optype = DCS_RMDIR;
                clt_pathname = optarg;
                break;
            case 't':
                if (strcmp(optarg, "a") == 0) {
                    clt_filetype = DCS_FILETYPE_FASTA;
                } else if (strcmp(optarg, "q") == 0) {
                    clt_filetype = DCS_FILETYPE_FASTQ;
                } else {
                    DCS_ERROR("__dcs_clt_parse_paramatter wrong file type!\n");
                    goto __error_paramatter;
                }
                break;
            case 'b':
                daemonlize = 1;
                break;
            case 'f':
                daemonlize = 0;
                break;
            default:
                printf("wrong option \n");
                rc = -1;
                goto EXIT;
                //exit(-1);
        }
    }

    if(daemonlize){
        __dcs_daemonlize();
    }

    /*
    if(clt_optype == DCS_READ){
        rc = __dcs_read_file(pathname);
    }
    */

EXIT:
    DCS_LEAVE("__dcs_clt_parse_paramatter leave \n");
    return rc;
}

/* create connection bewteen client and server */
dcs_s32_t __dcs_clt_init_com(void)
{
    dcs_u32_t addr;
    dcs_s32_t rc = 0;
    //dcs_s8_t  *server_addr = NULL;
    dcs_s32_t i = 0;
    dcs_s8_t  con_addr[ADDR_LEN];
    struct in_addr inaddr;

    FILE *fp;

    //server_id = (clt_this_id % DCS_SERVER_NUM);
    //server_id ++;
    DCS_ENTER("__dcs_clt_init_com enter. \n");

    clt_comp_context = amp_sys_init(DCS_CLIENT, clt_this_id);
    if(clt_comp_context == NULL){
        DCS_ERROR("__dcs_clt_init_com init clt_comp_context error. \n");
        rc = -1;
        goto EXIT;
    }

    if((fp = fopen(SERVER_ADDR_CONF, "r")) == NULL){
        DCS_ERROR("__dcs_client_init_com open server_addr configuration err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }

    for(i=0; i<DCS_SERVER_NUM; i++){
        memset(con_addr, 0, ADDR_LEN);
        if(fgets(con_addr, ADDR_LEN, fp)){
            DCS_MSG("get addr string and server addr is %s", con_addr);
        }
        
        if (!inet_aton(con_addr, &inaddr)){
            DCS_ERROR("__dcs_client_init_com server inet error. \n");
            DCS_MSG("the server addr is %s \n", con_addr);
            rc = -1;
            goto EXIT;
        }
        addr = inaddr.s_addr;
        addr = ntohl(addr);
    
        rc = amp_create_connection(clt_comp_context,
                DCS_SERVER,
                (i+1),
                addr,
                SERVER_PORT,
                AMP_CONN_TYPE_TCP,
                AMP_CONN_DIRECTION_CONNECT,
                NULL, 
                __client_allocbuf,
                __client_freebuf);

        if(rc != 0){
            DCS_ERROR("__dcs_client_init_com create connection between client server err :%d \n", errno);
            amp_sys_finalize(clt_comp_context);
            rc = errno;
            goto EXIT;
        }
    }
    fclose(fp);

    /*
    DCS_MSG("1  clt_this is: %d server_num: %d \n", clt_this_id, server_num);
    server_id = clt_this_id % server_num;
    server_addr = (dcs_s8_t *)malloc(20);
    server_addr = getaddr(server_id);
    */

    /*
    if (!inet_aton("10.10.108.60", &inaddr)){
        DCS_ERROR("__dcs_clt_init_com client inet error. \n");
        rc = -1;
        goto EXIT;
    }
    addr = inaddr.s_addr;
    addr = ntohl(addr);

    DCS_MSG("server_id is %d \n", server_id);
    rc = amp_create_connection(clt_comp_context,
            DCS_SERVER,
            server_id,
            addr,
            SERVER_PORT,
            AMP_CONN_TYPE_TCP,
            AMP_CONN_DIRECTION_CONNECT,
            NULL, 
             __client_allocbuf,
             __client_freebuf);

    DCS_MSG("__dcs_clt_init_com init com rc: %d \n", rc);
    if(rc != 0){
        DCS_ERROR("__dcs_clt_init_com create connection err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }
    */

EXIT:
    /*
    if(clt_comp_context){
        amp_sys_finalize(clt_comp_context);
    }
    */

    /*
    if(con_addr != NULL){
        free(con_addr);
        con_addr = NULL;
    }
    */

    DCS_LEAVE("__dcs_clt_init_com leave. \n");
    return rc;
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


/*
*/

/*dcs_s8_t *getaddr(dcs_u32_t server_id)
{
    //dcs_s8_t *server_addr = NULL;

    //server_addr = (dcs_s8_t *)malloc(128);
    if(server_addr == NULL){
        DCS_ERROR("getaddr err: %d \n", errno);
        exit(0);
    }

    return server_addr;
}*/

void __client_freebuf(amp_u32_t niov, amp_kiov_t *iov)
{
    dcs_s32_t i;
    amp_kiov_t *kiovp = NULL;
    kiovp = iov;
    
    DCS_ENTER("__client_freebuf enter. \n");
    for(i=0; i<niov; i++){
        if(kiovp->ak_addr)
            free(kiovp->ak_addr);
        kiovp ++;
    }
    DCS_LEAVE("__client_freebuf leave. \n");
}

dcs_s32_t __client_allocbuf(void *msgp, 
                amp_u32_t *niov, amp_kiov_t **iov)
{
    dcs_s32_t err = 0;
    dcs_u32_t bufsize;
    dcs_s8_t *buf = NULL;
    amp_kiov_t *kiov = NULL;
    dcs_msg_t *recvmsgp = NULL;

    DCS_ENTER("__client_allocbuf enter. \n");

    recvmsgp = (dcs_msg_t *)msgp;
    bufsize = recvmsgp->u.s2c_reply.size;
    DCS_MSG("__client_allocbuf, bufsize is %d \n", bufsize);
    buf = (dcs_s8_t *)malloc(bufsize);
    if(buf == NULL){
        err = errno;
        DCS_ERROR("__client_allocbuf, fail to malloc for buf,err:%d\n", errno);
        goto EXIT;
    }

    kiov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if(!kiov){
        DCS_ERROR("__client_allocbuf, fail to malloc for kiov. \n");
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
    DCS_LEAVE("__client_allocbuf leave. \n");
    return err;
}

/*
dcs_s32_t __client_init_sendbuf(dcs_u32_t size, 
                                amp_u32_t *niov, 
                                amp_kiov_t **iov, 
                                dcs_s8_t *buf)
{
    dcs_s32_t rc = 0;
    dcs_u32_t bufsize;
    amp_kiov_t *kiov = NULL;
    
    DCS_ENTER("__client_init_sendbuf enter \n");
    bufsize = size;
    
    kiov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if(!kiov){
        DCS_ERROR("__client_init_sendbuf, alloc for kiov err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    kiov->ak_addr = buf;
    kiov->ak_len = bufsize;
    kiov->ak_offset = 0;
    kiov->ak_flag = 0;

    *iov = kiov;
    *niov = 1;

EXIT:
    DCS_LEAVE("__client_init_sendbuf leave \n");

    return rc;
}   
*/

dcs_s32_t dcs_clt_init_conf()
{
    return 1;    
}



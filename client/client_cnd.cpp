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

amp_comp_context_t  *clt_comp_context;  //inited in clt_init_com
dcs_u32_t         clt_this_id;
dcs_u32_t         clt_num;
dcs_u32_t         server_num = DCS_SERVER_NUM;
dcs_u32_t         clt_optype;
//dcs_u32_t         server_id;
dcs_s8_t          *clt_pathname = NULL;
dcs_s8_t           *file_tobe_stored = NULL;

//add by bxz
dcs_u32_t       clt_filetype;

void print_usage(char op, char *proc) {
    printf("++++++++++++++++++++++++++++++++++++++++++++++++\n");
    switch (op) {
        case 'a':
            printf("Usage:\n\t%s [w | r | l | d] ...\n", proc);
            printf("\tw: compress file\n");
            printf("\tr: decompress file\n");
            printf("\tl: list the compressed files\n");
            printf("\td: delete the compressed file\n");
            break;
            
        case 'w':
            printf("Usage:\n\t%s w file -t type -f id\n", proc);
            printf("\t-t type\t----\ta: FASTA file; q: FASTQ file\n");
            printf("\t-f id\t----\tthe id of this mos\n");
            break;
            
        case 'r':
            printf("Usage:\n\t%s r file path -f id\n", proc);
            printf("\tpath\t----\tthe location of the file to be written\n");
            printf("\t-f id\t----\tthe id of this mos\n");
            break;
            
        case 'l':
            printf("Usage:\n\t%s l -f id\n", proc);
            printf("\t-f id\t----\tthe id of this mos\n");
            break;
            
        case 'd':
            printf("Usage:\n\t%s d file -f id\n", proc);
            printf("\t-f id\t----\tthe id of this mos\n");
            break;
            
        default:
            break;
    }
    printf("++++++++++++++++++++++++++++++++++++++++++++++++\n");
}

/* client parse paramatter */
dcs_s32_t __dcs_clt_parse_parameter(dcs_s32_t argc, dcs_s8_t **argv)
{
    dcs_u32_t daemonlize;
    dcs_s8_t  c;
    dcs_s32_t rc = 0;

    daemonlize = 0;
    
    char op = 0;

    DCS_ENTER("__dcs_clt_parse_paramatter enter \n");
    
    if (argc < 2) {
        print_usage('a', argv[0]);
        rc = -1;
        goto EXIT;
    }
    
    if (strlen(argv[1]) != 1) {
        print_usage('a', argv[0]);
        rc = -1;
        goto EXIT;
    }
    
    op = argv[1][0];
    switch (op) {
        case 'w':
            if (argc != 7 || strcmp(argv[3], "-t") != 0 || strcmp(argv[argc - 2], "-f") != 0 ||
                (strcmp(argv[4], "a") != 0 && strcmp(argv[4], "q") != 0)) {
                print_usage('w', argv[0]);
                rc = -1;
                goto EXIT;
            }
            clt_optype = DCS_WRITE;
            clt_pathname = argv[2];
            if (strcmp(argv[4], "a") == 0) {
                clt_filetype = DCS_FILETYPE_FASTA;
            } else {
                clt_filetype = DCS_FILETYPE_FASTQ;
            }
            break;
            
        case 'r':
            if (argc != 6 || strcmp(argv[argc - 2], "-f") != 0) {
                print_usage('r', argv[0]);
                rc = -1;
                goto EXIT;
            }
            clt_optype = DCS_READ;
            clt_pathname = argv[2];
            file_tobe_stored = argv[3];
            break;
        case 'l':
            if (argc != 4 || strcmp(argv[argc - 2], "-f") != 0) {
                print_usage('l', argv[0]);
                rc = -1;
                goto EXIT;
            }
            clt_optype = DCS_LIST;
            break;
        case 'd':
            if (argc != 5 || strcmp(argv[argc - 2], "-f") != 0) {
                print_usage('d', argv[0]);
                rc = -1;
                goto EXIT;
            }
            clt_optype = DCS_DELETE;
            clt_pathname = argv[2];
            break;
            
        default:
            print_usage('a', argv[0]);
            rc = -1;
            goto EXIT;
            break;
    }

    clt_this_id = atoi(argv[argc-1]);
    //printf("file: %s\npath: %s\noptype: %d\nfiletype: %d\nid: %d\n", clt_pathname, file_tobe_stored, clt_optype, clt_filetype, clt_this_id);

    /*
    while((c = getopt(argc, argv, "w:r:l:d:D:t:bf")) != EOF){
        switch(c){
            case '?':
                print_usage('a', argv[0]);
                rc = -1;
                goto EXIT;
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
                    print_usage('a', argv[0]);
                    rc = -1;
                    goto EXIT;
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
        //__dcs_daemonlize();
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



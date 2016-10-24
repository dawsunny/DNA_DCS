/*master configuration*/

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


//BLOOM               *bf[DCS_COMPRESSOR_NUM];
BLOOM               **bf;
pthread_mutex_t     bloom_lock[DCS_COMPRESSOR_NUM];

tier_bloom          **tbf;

amp_comp_context_t  *master_comp_context;
dcs_u32_t         master_this_id;
dcs_u32_t         master_chunk_type;
dcs_u32_t         master_chunk_size;
dcs_u32_t         master_rout_type;
pthread_mutex_t     request_queue_lock;
sem_t               request_sem;
struct list_head    request_queue;

/*parse the paramatter*/
dcs_s32_t __dcs_master_parse_paramatter(dcs_s32_t argc, dcs_s8_t **argv)
{
    dcs_u32_t daemonlize;
    dcs_s8_t c;
    dcs_s32_t rc = 0;

    daemonlize = 1;

    DCS_ENTER("__dcs_master_parse_paramatter enter \n");
    if(argc < 3){
__error_paramatter:
        printf("Usage:\t %s [-d | -f] id \n", argv[0]);
        printf("\t -b ---- running on the background \n");
        printf("\t -f ---- running on the foreground \n");
        printf("\t id ---- the id of this master \n");
        rc = -1;
        goto EXIT;
    }

    master_this_id = atoi(argv[argc -1]);

    while((c = getopt(argc, argv, "bf")) != EOF){
        switch(c) {
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

EXIT:
    DCS_LEAVE("__dcs_master_parse_paramatter leave \n");
    return rc;
}

/*init the connection*/
dcs_s32_t __dcs_master_init_com()
{
    dcs_s32_t rc = 0;
    FILE * fp = NULL;
    dcs_u32_t addr;
    dcs_u32_t i = 0;
    dcs_s8_t  con_addr[ADDR_LEN];
    //dcs_u32_t master_id = 1;
    //dcs_u32_t compressor_id = 1;
    //dcs_s8_t  *server_addr[DCS_SERVER_NUM];
    
    struct in_addr inaddr;

    DCS_ENTER("__dcs_master_init_com enter \n");

    master_comp_context = amp_sys_init(DCS_MASTER, master_this_id);
    if(!master_comp_context){
        DCS_ERROR("__dcs_master_init_com init com context err \n");
        rc = -1;
        goto EXIT;
    }
    
    rc = amp_create_connection(master_comp_context,
                                0,
                                0,
                                INADDR_ANY,
                                MASTER_PORT,
                                AMP_CONN_TYPE_TCP,
                                AMP_CONN_DIRECTION_LISTEN,
                                __queue_req,
                                __master_allocbuf,
                                __master_freebuf);

    if(rc < 0){
        DCS_ERROR("__dcs_master_init_com create connection between client and master err: %d \n", errno);
        rc = errno;
        amp_sys_finalize(master_comp_context);
        goto EXIT;
    }

    INIT_LIST_HEAD(&request_queue);
    pthread_mutex_init(&request_queue_lock, NULL);
    sem_init(&request_sem, 0, 0);


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

        rc = amp_create_connection(master_comp_context,
                DCS_NODE,
                (i+1),
                addr,
                COMPRESSOR_PORT,
                AMP_CONN_TYPE_TCP,
                AMP_CONN_DIRECTION_CONNECT,
                NULL,
                __master_allocbuf,
                __master_freebuf);

        if(rc != 0){
            DCS_ERROR("__dcs_server_init_com create connection between server master err :%d \n", errno);
            amp_sys_finalize(master_comp_context);
            rc = errno;
            goto EXIT;
        }
    }
    fclose(fp);

EXIT:

    DCS_LEAVE("__dcs_master_init_com leave \n");
    return rc;
}

/*request queue manager*/
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

/*malloc buf for request*/
dcs_s32_t __master_allocbuf(void *msgp, 
                amp_u32_t *niov, amp_kiov_t **iov)
{
    dcs_s32_t rc = 0;
    dcs_u32_t bufsize;
    dcs_s8_t *buf = NULL;
    amp_kiov_t *kiov = NULL;
    dcs_msg_t *recvmsgp = NULL;

    DCS_ENTER("__master_allocbuf enter. \n");

    recvmsgp = (dcs_msg_t *)msgp;
    if(recvmsgp->optype == DCS_DELETE){
        bufsize = (recvmsgp->u.s2m_del_req.sha_num) * sizeof(dcs_datamap_t);
    }else if(recvmsgp->optype == DCS_UPLOAD){
        bufsize = (recvmsgp->u.s2m_upload_req.sha_num) * sizeof(sha_bf_t);
    }else{
        bufsize = (recvmsgp->u.s2m_req.sha_num) * SHA_LEN;
    }
    DCS_MSG("__master_allocbuf sha_num is %d \n", recvmsgp->u.s2m_req.sha_num);
    DCS_MSG("__master_allocbuf, bufsize is %d \n", bufsize);
    buf = (dcs_s8_t *)malloc(bufsize);
    if(buf == NULL){
        rc = errno;
        DCS_ERROR("__master_allocbuf, fail to malloc for buf,err:%d\n", errno);
        goto EXIT;
    }

    kiov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if(!kiov){
        DCS_ERROR("__master_allocbuf, fail to malloc for kiov. \n");
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
    DCS_LEAVE("__master_allocbuf leave. \n");
    return rc;
}

/*master freebuf*/
void __master_freebuf(amp_u32_t niov, amp_kiov_t *iov)
{
    dcs_s32_t i;
    amp_kiov_t *kiovp = NULL;
    kiovp = iov;
    
    DCS_ENTER("__master_freebuf enter. \n");
    for(i=0; i<niov; i++){
        if(kiovp->ak_addr)
            free(kiovp->ak_addr);
        kiovp ++;
    }
    DCS_LEAVE("__master_freebuf leave. \n");
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

dcs_s32_t __dcs_master_tier_bloom_init(){
	dcs_s32_t rc = 0;
	dcs_s32_t i = 0;
	
	tbf = (tier_bloom **)malloc(sizeof(tier_bloom*) * DCS_COMPRESSOR_NUM);
	if(NULL == tbf){
		DCS_ERROR("__dcs_tier_bloom_init malloc for tier_bloom failed, err:%d\n", errno);
		rc = errno;
		goto EXIT;
	}

	for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        	tbf[i] = NULL;
    	}	


	for(i=0; i<DCS_COMPRESSOR_NUM; i++){
                tbf[i] = bloom_create1(MASTER_BLOOM_SIZE);
    	}

        //__tier_bloom_local_reload();

EXIT:
	return rc;
}

dcs_s32_t __tier_bloom_local_reload(){
	dcs_s32_t rc = 0;
	dcs_s32_t i = 0;
	dcs_u64_t tmp_u64;
	dcs_u32_t j = 0;
	dcs_s8_t ** bloom_name = NULL;
    	dcs_s32_t read_fd = 0;
	dcs_s32_t read_size = 0;

	bloom_name = (dcs_s8_t **)malloc(sizeof(dcs_s8_t *)*DCS_COMPRESSOR_NUM);
	if(NULL == bloom_name){
		DCS_ERROR("__tier_bloom_local_reload malloc for bloom name err:%d \n", errno);
		rc = errno;
		goto EXIT;	
	}

	for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        	bloom_name[i] = NULL;
    	}	

    	get_bloom_filename(bloom_name);
    	for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        	if(bloom_name[i] == NULL){
            		DCS_ERROR("__tier_bloom_local_reload get bloom name err\n");
            		rc = -1;
            		goto EXIT;
        	}
    	}

	for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        	read_fd = open(bloom_name[i], O_RDONLY, 0666);
        	if(read_fd < 0){
            		if(ENOENT == errno){
               			DCS_MSG("__tier_bloom_local_reload the %dth bloom filter is not exist\n", i);
                		continue;
            		}
            		DCS_ERROR("__tier_bloom_local_reload open %dth bloom file err:%d \n",i ,errno);
            		rc = errno;
            		goto EXIT;
        	}

        	read_size = read(read_fd, &tmp_u64, sizeof(dcs_u64_t));
        	if(read_size != sizeof(dcs_u64_t)){
			DCS_ERROR("__tier_bloom_local_reload read blocks_num from file failed\n");
			rc = -1;
			goto EXIT1;
		}

		read_size = read(read_fd, tbf[i]->bmap, tmp_u64/CHAR_BIT);
		if(read_size * CHAR_BIT != tmp_u64){
			DCS_ERROR("__tier_bloom_local_reload read bmap failed\n");
			rc = -1;
			goto EXIT1;
		}

		for(j = 0; j < tbf[i]->blocks_num; j++){
			dcs_u8_t tmp_level_num;
			dcs_u8_t tmp_real_level_num;
			dcs_s32_t k;
			read_size = read(read_fd, &tmp_level_num, sizeof(dcs_u8_t));
			if(read_size != sizeof(dcs_u8_t)){
				DCS_ERROR("__tier_bloom_local_reload read %d tier_bloom's %d blocks' level_num failed\n", i, j);
				rc = -1;
				goto EXIT1;
			}
			
			if(tmp_level_num == 0){
				DCS_ERROR("__tier_bloom_local_reload tier_bloom[%d]'s %d is empty\n", i, j);
				continue;
			}
			
			read_size = read(read_fd, &tmp_real_level_num, sizeof(dcs_u8_t));
			if(read_size != sizeof(dcs_u8_t)){
				DCS_ERROR("__tier_bloom_local_reload read %d tier_bloom's %d blocks' real_level_num failed\n", i, j);
				rc = -1;
				goto EXIT1;
			}
			
			bloom_block_init1(tbf[i], j, 2, tmp_level_num, tmp_real_level_num);
			
			read_size = read(read_fd, tbf[i]->blocks[j]->bmap, BLOOM_BLOCK_SIZE/CHAR_BIT);
			
			if(read_size != BLOOM_BLOCK_SIZE/CHAR_BIT){
				DCS_ERROR("__tier_bloom_local_reload read %d tier_bloom's %d blocks' bmap failed\n", i, j);
				rc = -1;
				goto EXIT1;
			}
			
			for(k = 0; k < tbf[i]->blocks[j]->real_level_num; k++){
				read_size = read(read_fd, tbf[i]->blocks[j]->a[k],sizeof(char) * BLOOM_BLOCK_SIZE);
				if(read_size != BLOOM_BLOCK_SIZE){
					DCS_ERROR("__tier_bloom_local_reload read %d tier_bloom's %d blocks' %d level bloom failed\n", i, j, k);
					rc = -1;
					goto EXIT1;
				}
			}
		}	

        	close(read_fd);
        	read_fd = 0;
    	}

EXIT:
	if(read_fd){
        	close(read_fd);
    	}
	for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        	if(bloom_name[i]){
            		free(bloom_name[i]);
            		bloom_name[i] = NULL;
        	}
    	}
EXIT1:

	return rc;
}

/*init bloom filter*/
dcs_s32_t __dcs_bloom_init()
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_u64_t filesize = 0;
    dcs_u64_t read_size = 0;
    dcs_s32_t read_fd = 0;
    dcs_s8_t  **bloom_name = NULL;

    struct stat f_state;

    DCS_ENTER("__dcs_bloom_init enter \n");

    bf = (BLOOM **)malloc(sizeof(BLOOM *)*DCS_COMPRESSOR_NUM);
    if(bf == NULL){
        DCS_ERROR("__dcs_bloom_init malloc for bloom pointer err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    bloom_name = (dcs_s8_t **)malloc(sizeof(dcs_s8_t *)*DCS_COMPRESSOR_NUM);
    if(bloom_name == NULL){
        DCS_ERROR("__dcs_bloom_init malloc for bloom name err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        bloom_name[i] = NULL;
        bf[i] = NULL;
        pthread_mutex_init(&bloom_lock[i], NULL);
    }

    get_bloom_filename(bloom_name);
    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        if(bloom_name[i] == NULL){
            DCS_ERROR("__dcs_bloom_init get bloom name err\n");
            rc = -1;
            goto EXIT;;
        }
    }
    
    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        if(stat(bloom_name[i], &f_state) == -1){
            if(errno == 2){
                DCS_MSG("__dcs_bloom_init get %dth bloom filter err because not exist \n", i);
                bf[i] = bloom_create(MASTER_BLOOM_SIZE);
                DCS_MSG("__dcs_bloom_init finish create the bloom filter \n");
                continue;
            }
            DCS_ERROR("__dcs_bloom_init get %dth bloom file state err:%d \n",i,errno);
            rc = errno;
            goto EXIT;
        }
        filesize = f_state.st_size;
        
        read_fd = open(bloom_name[i], O_RDONLY, 0666);
        if(read_fd < 0){
            if(errno == 2){
                DCS_MSG("__dcs_bloom_init the %dth bloom filter is not exist\n", i);
                bf[i] = bloom_create(MASTER_BLOOM_SIZE);
                continue;
            }
            DCS_ERROR("__dcs_bloom_init open %dth bloom file err:%d \n",i ,errno);
            rc = errno;
            goto EXIT;
        }
        

        read_size = read(read_fd, bf[i]->a, filesize);
        if(read_size != filesize){
            DCS_ERROR("__dcs_bloom_init read %dth bloom file err:%d \n", i, errno);
            rc = -1;
            goto EXIT;
        }
        bf[i]->asize = read_size * CHAR_BIT;
        
        close(read_fd);
        read_fd = 0;
    }

EXIT:
    if(read_fd){
        close(read_fd);
    }

    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        if(bloom_name[i]){
            free(bloom_name[i]);
            bloom_name[i] = NULL;
        }
    }
    
    DCS_LEAVE("__dcs_bloom_init leave \n");
    return rc;
}

/*get bloom fileter name*/
void get_bloom_filename(dcs_s8_t **bloom_name)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s8_t  *bf_name = NULL;

    DCS_ENTER("get_bloom_filename enter \n");

    bf_name = (dcs_s8_t *)malloc(16);
    if(bf_name == NULL){
        DCS_ERROR("get_bloom_filename malloc for bf_name err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    for(i=0; i<DCS_COMPRESSOR_NUM; i++){
        bloom_name[i] = (dcs_s8_t *)malloc(256);
        if(!bloom_name[i]){
            DCS_ERROR("get_bloom_filename malloc for %dth bloom name err:%d \n", i, errno);
            rc = errno;
            goto EXIT;
        }
        memset(bloom_name[i], 0, 256);
        memcpy(bloom_name[i], MASTER_BLOOM_PATH, strlen(MASTER_BLOOM_PATH));
        memset(bf_name, 0, 16);
        sprintf(bf_name, "%d", i);
        memcpy(bloom_name[i] + strlen(bloom_name[i]), bf_name, 16);
    }

EXIT:
    if(bf_name){
        free(bf_name);
        bf_name = NULL;
    }

    DCS_LEAVE("get_bloom_filename leave \n");
}

/*display hash function name*/
void usage()
{
	DCS_MSG("hash_func_name:\n");
	DCS_MSG("\tsimple_hash\n");
	DCS_MSG("\tRS_hash\n");
	DCS_MSG("\tJS_hash\n");
	DCS_MSG("\tPJW_hash\n");
	DCS_MSG("\tELF_hash\n");
	DCS_MSG("\tBKDR_hash\n");
	DCS_MSG("\tSDBM_hash\n");
	DCS_MSG("\tDJB_hash\n");
	DCS_MSG("\tAP_hash\n");
	DCS_MSG("\tCRC_hash\n");
}

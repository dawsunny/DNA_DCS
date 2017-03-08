/*client operations*/

#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"

#include "amp.h"

#include "client_thread.h"
#include "client_cnd.h"
#include "client_op.h"
#include "md5.h"

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

#include <iostream>
#include <fstream>
#include <string>
#include <map>
using namespace std;

//dcs_file_set *fileset = NULL;
map<string, client_hash_t> client_table;
pthread_mutex_t client_table_lock = PTHREAD_MUTEX_INITIALIZER;

/*client operation*/
dcs_s32_t __dcs_clt_op()
{
    dcs_s32_t rc = 0;
    
    DCS_ENTER("__dcs_clt_op enter \n");
    if(clt_optype == DCS_READ){
        rc = __dcs_clt_get_read_filename(clt_pathname, file_tobe_stored);
        //rc = __dcs_clt_read_file(clt_pathname);
        if(rc < 0){
            DCS_ERROR("__dcs_clt_op read file err %d \n", rc);
            goto EXIT;
        }
    }
    else if(clt_optype == DCS_WRITE){
        rc = __dcs_clt_get_filename(clt_pathname);
        if(rc < 0){
            DCS_ERROR("__dcs_clt_op write file err %d \n", rc);
            goto EXIT;
        }
    }else if(clt_optype == DCS_LIST){
        rc = __dcs_clt_list();
        if(rc < 0){
            DCS_ERROR("__dcs_clt_op list files err %d \n", rc);
        }
    }else if(clt_optype == DCS_DELETE){
    	rc = __dcs_clt_delete(clt_pathname);
        if(rc < 0){
            DCS_ERROR("__dcs_clt_op list remove file err %d \n", rc);
        }
    }else if(clt_optype == DCS_RMDIR){
        rc = __dcs_clt_rmdir(clt_pathname);
        if(rc){
            DCS_ERROR("__dcs_clt_op rmdir error %d \n", rc);
        }
    }

EXIT:
    DCS_LEAVE("__dcs_clt_op leave \n");
    return rc;
}

/*get dir name from configuration*/
/* 1. open the config file
 * 2. read the dir name 
 * 3. get file from the dir name */
dcs_s32_t __dcs_clt_get_dirname()
{
    dcs_s32_t rc = 0;
    FILE *fp;
    dcs_s8_t dirname[PATH_LEN];
    

    DCS_ENTER("__dcs_clt_get_dirname enter. \n");
    fp = fopen("conf", "r");
    
    if(fp == NULL){
        DCS_ERROR("__dcs_clt_get_dirname open configuration fail err: %d\n", errno);
        rc = errno;
        goto EXIT;
    }

    while(!feof(fp)){
        memset(dirname, 0, PATH_LEN);
        fscanf(fp,"%s", dirname);
        __dcs_clt_get_filename(dirname);

    }

    fclose(fp);

EXIT:
    DCS_LEAVE("__dcs_clt_get_dirname leave \n");
    return rc;
}

dcs_s32_t mode2string (mode_t mode,
                         char * perms) {
	dcs_s32_t rc = 0;
        if(NULL == perms){
		DCS_MSG("mode2string error, perms is null\n");
		rc = -1;
	}
	memset(perms, '-', 10);
        if (mode & S_IRUSR) perms[1] = 'r';
        if (mode & S_IWUSR) perms[2] = 'w';
        if (mode & S_IXUSR) perms[3] = 'x';
        if (mode & S_IRGRP) perms[4] = 'r';
        if (mode & S_IWGRP) perms[5] = 'w';
        if (mode & S_IXGRP) perms[6] = 'x';
        if (mode & S_IROTH) perms[7] = 'r';
        if (mode & S_IWOTH) perms[8] = 'w';
        if (mode & S_IXOTH) perms[9] = 'x';

        if ( S_ISREG(mode) )
                perms[0] = '-';
        else if ( S_ISDIR(mode) )
                perms[0] = 'd';
        else if ( S_ISLNK(mode) )
                perms[0] = 'l';
        else if ( S_ISSOCK(mode) )
                perms[0] = 's';
        else if ( S_ISFIFO(mode) )
                perms[0] = 'n';
        else if ( S_ISCHR(mode) )
                perms[0] = 'c';
        else if ( S_ISBLK(mode) )
                perms[0] = 'b';

        return rc;
}

dcs_s32_t __dcs_clt_rmdir(dcs_s8_t *path){
    dcs_s32_t rc = 0;
    dcs_s8_t tmppath[PATH_LEN];
    dcs_s8_t dirpath[PATH_LEN];
    dcs_s8_t filepath[PATH_LEN];
    struct dirent *entry = NULL;
    struct stat f_type;
    DIR *dp;

    memset(dirpath, 0, PATH_LEN);
    memcpy(dirpath, CLIENT_MD_PATH, strlen(CLIENT_MD_PATH));
    if(dirpath[strlen(dirpath)-1] != '/' && path[0] != '/')
	dirpath[strlen(dirpath)] = '/';
    if(NULL != path && strlen(path) > 0){
        memcpy(dirpath + strlen(dirpath), path, strlen(path));
    }

    dp = opendir(dirpath);
    if(dp == NULL){
        DCS_ERROR("__dcs_clt_rmdir dir:%s open failed, error: %d \n", dirpath, errno);
        rc = errno;
        goto EXIT;
    }
    while((entry = readdir(dp)) != NULL){
        if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0){
            continue;
        }
	
	memset(tmppath, 0, PATH_LEN);
	sprintf(tmppath, "%s/%s", path, entry->d_name);

	memset(filepath, 0, PATH_LEN);
	sprintf(filepath, "%s/%s", dirpath, entry->d_name);
        
	rc = stat(filepath, &f_type);
        if(rc != 0){
            DCS_ERROR("__dcs_clt_rmdir get file: %s stat failed,  err :%d \n", filepath, errno);
            goto EXIT;
        }

	if(!S_ISDIR(f_type.st_mode)){
		rc = __dcs_clt_delete(tmppath);
		if(rc){
			DCS_ERROR("__dcs_clt_rmdir delete file %s failed\n", tmppath);
			goto EXIT;
		}
	}else{
		rc = __dcs_clt_rmdir(tmppath);
		if(rc){
			DCS_ERROR("__dcs_clt_rmdir delete dir %s failed\n", tmppath);
		}
	}
    }

    closedir(dp);
    if(dirpath[strlen(CLIENT_MD_PATH)] == '/' && strlen(dirpath)-strlen(CLIENT_MD_PATH) == 1){
        goto EXIT;
    }
    rc = remove(dirpath);//remove = unlink + rmdir
    if(rc){
        DCS_ERROR("__dcs_clt_rmdir delete dir :%s failed, errno: %d\n", dirpath, errno);
    }

EXIT:
	return rc;
}

dcs_u64_t str2val(char *msg) {
    dcs_u64_t res = 0;
    dcs_u32_t i = 0, len = strlen(msg);
    for (i = 0; i < len; ++i) {
        res = res * 10 + msg[i] - '0';
    }
    return res;
}

//bxz
string transferFilesize(dcs_u64_t filesize) {
    string res = "";
    char data[10];
    memset(data, 0, 10);
    if (filesize < KB_SIZE) {
        sprintf(data, "%luB", filesize);
    } else if (filesize < MB_SIZE) {
        sprintf(data, "%.1fKB", filesize * 1.0 / KB_SIZE);
    } else if (filesize < GB_SIZE) {
        sprintf(data, "%.1fMB", filesize * 1.0 / MB_SIZE);
    } else {
        sprintf(data, "%.1fGB", filesize * 1.0 / GB_SIZE);
    }
    res = data;
    return res;
}

//bxz
string transferTime(dcs_u64_t timestamp) {
    string res = "";
    time_t tick = (time_t)timestamp;
    struct tm tm = *localtime(&tick);
    char s[100];
    memset(s, 0, sizeof(s));
    strftime(s, sizeof(s), "%Y-%m-%d %H:%M", &tm);
    res = s;
    return res;
}

//bxz
void transfer2cltHash(dcs_s8_t *msg, dcs_u32_t len) {
    dcs_u32_t i = 0, cnt = 0;
    string filename = "";
    char *p;
    char *buf = msg;
    char *token_out = NULL;
    char *token_in = NULL;
    
    while ((p = strtok_r(buf, "\n", &token_out)) != NULL) {
        buf = p;
        //printf("out: %s\n", p);
        while ((p = strtok_r(buf, "\t", &token_in)) != NULL) {
            //printf("in: %s\n", p);
            switch (cnt) {
                case 0:
                    filename = p;
                    cnt++;
                    break;
                    
                case 1:
                    client_table[filename].filesize = transferFilesize(str2val(p));
                    cnt++;
                    break;
                    
                case 2:
                    if (strcmp(p, "100") == 0) {
                        client_table[filename].filetype = "fasta";
                    } else if (strcmp(p, "200") == 0) {
                        client_table[filename].filetype = "fastq";
                    } else {
                        client_table[filename].filetype = "unknown";
                    }
                    cnt++;
                    break;
                    
                case 3:
                    client_table[filename].timestamp = transferTime(str2val(p));
                    cnt = 0;
                    filename = "";
                    break;
                    
                default:
                    
                    break;
            }
            buf = NULL;
        }
        buf = NULL;
    }
}

dcs_s32_t __dcs_clt_list()    //bxz
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_u32_t size = 0;
    dcs_s32_t target_server = -1;
    dcs_c2s_req_t c2s_datainfo;
    
    amp_request_t *req = NULL;
    amp_message_t *reqmsgp = NULL;
    amp_message_t *repmsgp = NULL;
    dcs_msg_t *msgp = NULL;
    dcs_msg_t *msgp_ack = NULL;
    map<string, client_hash_t>::iterator it;
    
    DCS_ENTER("__dcs_clt_list enter\n");
    
    rc = __amp_alloc_request(&req);
    if (rc < 0) {
        DCS_ERROR("__dcs_clt_list alloc request error[%d]\n", errno);
        rc = errno;
        goto EXIT;
    }
    c2s_datainfo.size = 1;
    
    pthread_mutex_lock(&client_table_lock);
    client_table.clear();
    pthread_mutex_unlock(&client_table_lock);
    
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    reqmsgp = (amp_message_t *)malloc(size);
    if (reqmsgp == NULL) {
        DCS_ERROR("__dcs_clt_list malloc for reqmsgp error[%d]\n", errno);
        rc = errno;
        goto EXIT;
    }
    
    memset(reqmsgp, 0, size);
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->size = size;
    msgp->seqno = 0;
    msgp->msg_type = is_req;
    msgp->fromid = clt_this_id;
    msgp->fromtype = DCS_CLIENT;
    msgp->optype = DCS_LIST;
    msgp->ack = 0;
    msgp->u.c2s_req = c2s_datainfo;
    
    req->req_iov = NULL;
    req->req_niov = 0;
    req->req_msg = reqmsgp;
    req->req_msglen = size;
    req->req_need_ack = 1;
    req->req_resent = 1;
    req->req_type = AMP_REQUEST | AMP_MSG;
    
    for (i = 0; i < DCS_SERVER_NUM; ++i) {
    SEND_AGAIN:
        rc = amp_send_sync(clt_comp_context,
                           req,
                           DCS_SERVER,
                           (i + 1),
                           0);
        if (rc != 0) {
            DCS_ERROR("__dcs_clt_list amp send to server error[%d]\n", rc);
            goto EXIT;
        }
        
        repmsgp = req->req_reply;
        if (!repmsgp) {
            DCS_ERROR("__dcs_clt_list cannot recieve msg from server[%d]\n", errno);
            rc = errno;
            goto SEND_AGAIN;
        }
        
        msgp_ack = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
        if (msgp_ack->ack == 1) {
            //read the server hash map
            if (req->req_iov == NULL) {
                DCS_ERROR("__dcs_clt_list get no reply data from compressor\n");
                rc = -1;
                goto EXIT;
            }
            //printf("||||get data:\n%s[%d]\n[%d]\n", (char *)req->req_iov->ak_addr, req->req_iov->ak_len, strlen((char *)req->req_iov->ak_addr));
            pthread_mutex_lock(&client_table_lock);
            transfer2cltHash((char *)req->req_iov->ak_addr, req->req_iov->ak_len);
            pthread_mutex_unlock(&client_table_lock);
        } else {
            continue;   //no data in corresponding server
        }
    }
    
    printf("\n------------------------------------\n\n");
    if (client_table.size() > 0) {
        printf("filename\tsize\ttype\tmtime\n");
        for (it = client_table.begin(); it != client_table.end(); ++it) {
            cout << it->first << "\t" << it->second.filesize << "\t" << it->second.filetype << "\t" << it->second.timestamp << endl;
        }
    } else {
        printf("NO DATA.\n");
    }
    printf("\n------------------------------------\n\n");
    
EXIT:
    if (reqmsgp) {
        free(reqmsgp);
        reqmsgp = NULL;
    }
    
    if(repmsgp){
        free(repmsgp);
        repmsgp = NULL;
    }
    if(req){
        if(req->req_iov){
            if (req->req_iov->ak_addr) {
                req->req_iov->ak_addr = NULL;
            }
            free(req->req_iov);
            req->req_iov = NULL;
            req->req_niov = 0;
        }
        __amp_free_request(req);
    }
    
    DCS_LEAVE("__dcs_clt_list leave\n");
    return rc;
    /*
    dcs_s32_t fd = 0;
    dcs_s8_t pathinfo[PATH_LEN];
    dcs_s8_t dirpath[PATH_LEN];
    dcs_s8_t filepath[PATH_LEN];
    struct dirent *entry = NULL;
    struct stat f_type;
    DIR *dp;

    memset(dirpath, 0, PATH_LEN);
    memcpy(dirpath, CLIENT_MD_PATH, strlen(CLIENT_MD_PATH));
    if(dirpath[strlen(dirpath)-1] != '/')
	dirpath[strlen(dirpath)] = '/';
    if(NULL != path && strlen(path) > 0){
        memcpy(dirpath + strlen(dirpath), path, strlen(path));
    }

    dp = opendir(dirpath);
    if(dp == NULL){
        DCS_ERROR("__dcs_clt_list dir:%s open failed, error: %d \n", dirpath, errno);
        rc = errno;
        goto EXIT;
    }

    while((entry = readdir(dp)) != NULL){
        if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0){
            continue;
        }
	memset(filepath, 0, PATH_LEN);
	sprintf(filepath, "%s/%s", dirpath, entry->d_name);
        rc = stat(filepath, &f_type);
        if(rc != 0){
            DCS_ERROR("__dcs_clt_list get file: %s stat failed,  err :%d \n", filepath, errno);
            goto EXIT;
        }
	if(!S_ISDIR(f_type.st_mode)){
		fd = open(filepath, O_RDONLY, 0666);
		if(fd <= 0){
			DCS_MSG("dcs_clt_list meta file :%s open failed, errno:%d\n",filepath, errno);
			rc = errno;
			goto EXIT;
		}
		rc = read(fd, (char *)&f_type, sizeof(f_type));
		if(rc < sizeof(f_type)){
			DCS_MSG("dcs_clt_list read meta file: %s, error:%d\n",filepath, rc);
			goto EXIT;
		}
		close(fd);
	}
	memset(pathinfo, 0, PATH_LEN);
        mode2string(f_type.st_mode, pathinfo);
        sprintf(pathinfo + strlen(pathinfo), "\t%ld", f_type.st_nlink);
        sprintf(pathinfo + strlen(pathinfo), "\t%d\t%d", f_type.st_uid, f_type.st_gid);
	sprintf(pathinfo + strlen(pathinfo), "\t%ld", f_type.st_size);
        sprintf(pathinfo + strlen(pathinfo), "\t%s",strstr(filepath, CLIENT_MD_PATH) + strlen(CLIENT_MD_PATH));
        printf("%s\n", pathinfo);

    }

    closedir(dp);

EXIT:
    return rc;
     */
}

/*get filename from a dir
 * 1.scan the dir
 * 2.if a common file
 * 3.insert the file to queue;
 * 3.write the file to the server
 */
dcs_s32_t __dcs_clt_get_filename(dcs_s8_t *path)
{
    dcs_s32_t rc = 0;
    dcs_s8_t *filename = NULL;
    dcs_u32_t pathsize = 0;
    dcs_s8_t dirpath[PATH_LEN];
    struct dirent *entry = NULL;
    struct stat f_type;
    DIR *dp;
    
    DCS_ENTER("__dcs_clt_get_filename enter \n");

    DCS_MSG("__dcs_clt_get_filename path is %s \n", path);
    rc = stat(path, &f_type);
    if(rc != 0){
        DCS_ERROR("__dcs_clt_get_filename get file %s stat err :%d \n", path, errno);
        goto EXIT;
    }

    if(!S_ISDIR(f_type.st_mode)){
        dcs_clt_file_t *tmp = NULL;
        tmp = (dcs_clt_file_t *)malloc(sizeof(dcs_clt_file_t));
        if(tmp == NULL ){
            DCS_ERROR("__dcs_clt_get_filename malloc for tmp erron \n");
            rc = errno;
            goto EXIT;
        }

        /*add tail to the file queue*/
        memset(tmp->filename, 0, PATH_LEN);
        memcpy(tmp->filename, path, strlen(path));

        pthread_mutex_lock(&clt_file_lock);
        list_add_tail(&tmp->file_list, &file_queue);
        pthread_mutex_unlock(&clt_file_lock);
        
        pthread_mutex_lock(&file_num_lock);
        file_on_run++;
        DCS_MSG("file on run %d \n", file_on_run);
        pthread_mutex_unlock(&file_num_lock);

        sem_post(&file_sem);
        DCS_MSG("__dcs_clt_get_filename one resource is been relesed \n");
        goto EXIT;
    } else {
        DCS_ERROR("__dcs_clt_get_filename not a reguler file or file not exists \n");
        rc = -1;
        goto EXIT;
    }

    //is dir
    filename = (dcs_s8_t *)malloc(PATH_LEN);
    if(filename == NULL){
        DCS_ERROR("__dcs_clt_get_filename malloc filename error %d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(filename, 0, PATH_LEN);
    pathsize = strlen(path);
    rc = clt_create_dir(path);
    if(rc != 0 && rc != 17){
        DCS_ERROR("__dcs_clt_get_filename create dir err: %d\n", errno);
        goto EXIT;
    }

    //DCS_MSG("1 \n");
    dp = opendir(path);
    //DCS_MSG("2 \n");
    if(dp == NULL){
        DCS_ERROR("__dcs_clt_get_filename open the dir error: %d \n", errno);
        rc = errno;
        goto EXIT;
        //exit(0);
    }

    //DCS_MSG("3 \n");
    while((entry = readdir(dp)) != NULL){
        //DCS_MSG("3 \n");
        memcpy(filename, path, pathsize);
        if(filename[pathsize - 1] != '/'){
            filename[pathsize] = '/';
        }
        if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0){
            DCS_MSG("the file is %s \n", entry->d_name);
            continue;
        }

        memset(dirpath, 0, PATH_LEN);
        if(path[strlen(path) - 1] == '/')
            sprintf(dirpath, "%s%s", path, entry->d_name);
        else
            sprintf(dirpath, "%s/%s", path, entry->d_name);
        DCS_MSG("__dcs_clt_get_filename path is %s \n", path);
        rc = stat(dirpath, &f_type);
        if(rc != 0){
            DCS_ERROR("__dcs_clt_get_filename get file %s stat err :%d \n", dirpath, errno);
            goto EXIT;
        }

        if(S_ISDIR(f_type.st_mode)){    //dir in the current path
            DCS_MSG("the dir is %s \n", entry->d_name);
            rc = add_to_dirqueue(dirpath);
            if(rc != 0){
                DCS_ERROR("add dir name to queue err %d \n", rc);
                goto EXIT;
            }
            continue;
        }

        //file in the current path
        memcpy(filename + strlen(filename), entry->d_name, strlen(entry->d_name));
        DCS_MSG("__dcs_clt_get_filename it is a common file: %s\n", filename);

        dcs_clt_file_t *tmp = NULL;
        tmp = (dcs_clt_file_t *)malloc(sizeof(dcs_clt_file_t));
        if(tmp == NULL ){
            DCS_ERROR("__dcs_clt_get_filename malloc for tmp erron \n");
            rc = errno;
            goto EXIT;
        }

        /*add tail to the file queue*/
        memcpy(tmp->filename, filename, PATH_LEN);

        pthread_mutex_lock(&clt_file_lock);
        list_add_tail(&tmp->file_list, &file_queue);
        pthread_mutex_unlock(&clt_file_lock);

        pthread_mutex_lock(&file_num_lock);
        file_on_run++;
        DCS_MSG("file on run %d \n", file_on_run);
        pthread_mutex_unlock(&file_num_lock);

        sem_post(&file_sem);
        memset(filename, 0, PATH_LEN); 
    }

    closedir(dp);

EXIT:
    if(filename != NULL){
        free(filename);
        filename = NULL;
    }

    DCS_LEAVE("__dcs_clt_get_filename leave \n");
    return rc;
}

/*add the dirname to queue*/
dcs_s32_t add_to_dirqueue(dcs_s8_t *dirpath)
{
    dcs_s32_t rc = 0;
    dcs_u32_t len = 0;

    dcs_s8_t *pathname = NULL;
    dcs_dir_t *tmp = NULL;

    pathname = (dcs_s8_t *)malloc(PATH_LEN);
    if(pathname == NULL){
        DCS_ERROR("add_to_dirqueue err %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    memcpy(pathname, dirpath, PATH_LEN);
    len = strlen(pathname);
    pathname[len] = '/';

    
    tmp = (dcs_dir_t *)malloc(sizeof(dcs_dir_t));
    if(tmp == NULL ){
        DCS_ERROR("add_to_dirpath  malloc for tmp erron \n");
        rc = errno;
        goto EXIT;
    }

    /*add tail to the file queue*/
    memcpy(tmp->pathname, pathname, PATH_LEN);

    pthread_mutex_lock(&clt_dir_lock);
    list_add_tail(&tmp->file_list, &dir_queue);
    pthread_mutex_unlock(&clt_dir_lock);

    sem_post(&dir_sem);

EXIT:
    if(pathname != NULL){
        free(pathname);
        pathname = NULL;
    }
    
    return rc;
}

/* write the file to server
 * 1. read file meta from the file and create a file to store it
 * 2. read file data based the superchunk size
 * 3. send the data to the server 
 */
dcs_s32_t __dcs_clt_write_file(dcs_s8_t *filename, dcs_thread_t *threadp)
{
    dcs_s32_t rc = 0;
    dcs_s32_t meta_fd = 0;
    dcs_s32_t write_fd = 0;
    dcs_u64_t fileoffset = 0;
    dcs_u64_t filesize = 0;
    //dcs_s8_t  *meta_path = NULL;
    dcs_s8_t *buf = NULL;
    dcs_u32_t len;
    dcs_c2s_req_t c2s_datainfo;

    struct stat f_state;
    struct stat fm_state;
    
    FILE *filep = NULL;
    dcs_s8_t *line_buf = NULL;
    dcs_u32_t cnt_rec = 0;
    fstream filest;
    string strstr, strtmp;
    double processed_rate = 0.1;
    char *file_md5 = NULL;
    dcs_u32_t target_server = 0;
    
    dcs_s8_t filename1[PATH_LEN];
    dcs_s32_t i = 0;

    DCS_ENTER("__dcs_clt_write_file enter \n");

    DCS_MSG("__dcs_clt_write_file the filename is %s \n", filename);
    if(stat(filename, &f_state) == -1){
        DCS_ERROR("__dcs_clt_write_file get meta data err: %d. \n", errno);
        goto EXIT;
    }
    
    //bxz
    for (i = strlen(filename) - 1; i>=0; --i) {
        if (filename[i] == '/') {
            break;
        }
    }
    if (i < 0) {
        DCS_ERROR("__dcs_clt_write_file got the read filename error\n");
        rc = -1;
        goto EXIT;
    }
    memcpy(filename1, filename + i + 1, strlen(filename) - i - 1);
    
    /* //by bxz
    meta_path = get_meta_path(filename);
    
    if(0 == stat(meta_path, &fm_state)){    //the meta file corresponding to filename exists.
        dcs_s32_t meta_fd = open(meta_path, O_RDONLY, 0666);
        if(meta_fd < 0 || meta_fd == 0){
            printf("no such file or open meta file err %d \n",errno);
            rc = errno;
            goto EXIT;
        }
        rc = read(meta_fd, &fm_state, sizeof(struct stat));
        close(meta_fd);
        if(rc != (sizeof(struct stat))){
            DCS_ERROR("__dcs_clt_read_file read meta data err: %d \n", errno);
            rc = errno;
            goto EXIT;
        }

        if(fm_state.st_mtime == f_state.st_mtime){  //the mtime stored in metafile equals to the mtime of filename, so already processed
            DCS_ERROR("__dcs_clt_write_file file %s have writed into the ddss system already, thank you!\n", filename);
            sem_post(&finish_sem);
            rc = 0;
            goto EXIT;
        }else{  //meta file exists but not the latest one, rename the old one and create a new
            char tmp[PATH_LEN];
            memset(tmp, 0, PATH_LEN);
            sprintf(tmp, "%s.v%ld", meta_path, fm_state.st_mtime);
            if(0 != (rc = rename(meta_path, tmp))){
                DCS_ERROR("__dcs_clt_write_file old meta file %s rename %s failed, errno: %d\n", meta_path, tmp, errno);
                goto EXIT;
            }
        }
    }
     */

    /* //by bxz
    rc = check_dir(filename);
    if(rc != 0){
        DCS_ERROR("__dcs_clt_write_file check_dir failed, rc:%d\n",rc);
        goto EXIT;
    }
    DCS_MSG("__dcs_clt_write_file meta filename: %s \n", meta_path);
    meta_fd = open(meta_path, O_WRONLY | O_CREAT, 0666);
    if(meta_fd < 0){
        DCS_ERROR("__dcs_clt_write_file open meta data file err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    len = sizeof(struct stat);
    rc = write(meta_fd, &f_state, len);
    if(rc != len){
        DCS_ERROR("__dcs_clt_write_file store meta err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }
     */
    file_md5 = (char *)malloc(MD5_STR_LEN + 1);
    if (!file_md5) {
        DCS_ERROR("__dcs_clt_write_file malloc for file_md5 error\n");
        rc = -1;
        goto EXIT;
    }
    memset(file_md5, 0, MD5_STR_LEN + 1);

    filesize = (dcs_u64_t)(f_state.st_size);
    //bxz
    //close(write_fd);
    //filep = fopen(filename, "r");
    printf("Computing the MD5 of the file, please wait...\n");
    rc = MD5_file(filename, file_md5);
    if (rc != 0) {
        DCS_ERROR("__dcs_clt_write_file compute md5 error\n");
        rc = -1;
        goto EXIT;
    }
    
    printf("The MD5 of file %s is:\n%s\n", filename, file_md5);
    target_server = (file_md5[0] * 256 + file_md5[1]) % DCS_SERVER_NUM + 1;
    printf("\nProcessing, please wait...\n");
    
    if (clt_filetype == DCS_FILETYPE_FASTA) {
        write_fd = open(filename, O_RDONLY, 0666);
        if(write_fd < 0){
            DCS_ERROR("__dcs_clt_write_file open meta file err:%d \n", errno);
            rc = errno;
            goto EXIT;
        }
        
        buf = (dcs_s8_t *)malloc(FA_CHUNK_SIZE);
        if(buf == NULL){
            DCS_ERROR("__dcs_clt_write_file malloc for read buf err: %d \n", errno);
            rc = errno;
            goto EXIT;
        }
        memset(buf, 0, FA_CHUNK_SIZE);
        
        while((rc = read(write_fd, buf, FA_CHUNK_SIZE)) != 0){
            c2s_datainfo.size = rc;
            c2s_datainfo.offset = fileoffset;
            c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
            c2s_datainfo.target_server = target_server;
            c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
            c2s_datainfo.finish = 0;
            memcpy(c2s_datainfo.filename, filename1, strlen(filename1));
            fileoffset = fileoffset + rc;
            rc = clt_send_data(filesize, file_md5, c2s_datainfo, buf, threadp);
            if(rc != 0){
                DCS_ERROR("__dcs_clt_write_file send data to server err:%d \n", rc);
                goto EXIT;
            }
            memset(buf, 0, FA_CHUNK_SIZE);
        }
        
        if(fileoffset == filesize && filesize != 0){
            DCS_MSG("__dcs_clt_write_file finish write file %s  to server \n",
                    filename);
            c2s_datainfo.finish = 1;
            c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
            c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
            memcpy(c2s_datainfo.filename, filename1, strlen(filename1));
            c2s_datainfo.offset = filesize;
            c2s_datainfo.target_server = target_server;
            rc = __dcs_clt_finish_msg(filesize, file_md5, c2s_datainfo, threadp);
            if(rc != 0){
                DCS_ERROR("send finish massage error: %d \n", rc);
                goto EXIT;
            }
            DCS_MSG("__dcs_clt_write_file sem post finish_sem\n");
            sem_post(&finish_sem);
        } else if(fileoffset == filesize && filesize == 0){
            rc = 0;
            DCS_MSG("__dcs_clt_write_file sem post finish_sem\n");
            sem_post(&finish_sem);
            goto EXIT;
        } else if(fileoffset != filesize){
            DCS_ERROR("__dcs_clt_write_file fail to finish store file %s to server \n",
                      filename);
            rc = -1;
            goto EXIT;
        }
        /*
        //bxz
        fgets(buf, FA_CHUNK_SIZE, filep);    //read the first line(ID part)
        c2s_datainfo.size = strlen(buf);
        c2s_datainfo.offset = fileoffset;
        c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
        c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
        c2s_datainfo.finish = 0;
        fileoffset = fileoffset + strlen(buf);
        rc = clt_send_data(c2s_datainfo, buf, threadp);
        if(rc != 0){
            DCS_ERROR("__dcs_clt_write_file send data to server err:%d \n", rc);
            goto EXIT;
        }
        memset(buf, 0, FA_CHUNK_SIZE);
        
        //while((rc = read(write_fd, buf, FA_CHUNK_SIZE)) != 0){
        while((rc = fread(buf, 1, FA_CHUNK_SIZE, filep)) != 0){
            c2s_datainfo.size = rc;
            c2s_datainfo.offset = fileoffset;
            c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
            c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
            c2s_datainfo.finish = 0;
            fileoffset = fileoffset + rc;
            rc = clt_send_data(c2s_datainfo, buf, threadp);
            if(rc != 0){
                DCS_ERROR("__dcs_clt_write_file send data to server err:%d \n", rc);
                goto EXIT;
            }
            memset(buf, 0, FA_CHUNK_SIZE);
        }
        
        if(fileoffset == filesize && filesize != 0){
            DCS_MSG("__dcs_clt_write_file finish write file %s  to server \n",
                    filename);
            c2s_datainfo.finish = 1;
            c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
            c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
            c2s_datainfo.offset = filesize;
            rc = __dcs_clt_finish_msg(c2s_datainfo, threadp);
            if(rc != 0){
                DCS_ERROR("send finish massage error: %d \n", rc);
                goto EXIT;
            }
            DCS_MSG("__dcs_clt_write_file sem post finish_sem\n");
            sem_post(&finish_sem);
        } else if(fileoffset == filesize && filesize == 0){
            rc = 0;
            DCS_MSG("__dcs_clt_write_file sem post finish_sem\n");
            sem_post(&finish_sem);
            goto EXIT;
        } else if(fileoffset != filesize){
            DCS_ERROR("__dcs_clt_write_file fail to finish store file %s to server \n",
                      filename);
            rc = -1;
            goto EXIT;
        }
         */
    } else if (clt_filetype == DCS_FILETYPE_FASTQ) {
        buf = (dcs_s8_t *)malloc(FQ_CHUNK_SIZE);
        if(buf == NULL){
            DCS_ERROR("__dcs_clt_write_file malloc for read buf err: %d \n", errno);
            rc = errno;
            goto EXIT;
        }
        memset(buf, 0, FQ_CHUNK_SIZE);
        
        line_buf = (dcs_s8_t *)malloc(FQ_LINE_SIZE);
        if (line_buf == NULL) {
            DCS_ERROR("__dcs_clt_write_file malloc for line buf err: %d \n", errno);
            rc = errno;
            goto EXIT;
        }
        memset(line_buf, 0, FQ_LINE_SIZE);
        
        filest.open(filename, ios::in);
        
        cnt_rec = FQ_CNT_REC * 4;
        while (!filest.eof()) {
            filest.getline(line_buf, FQ_LINE_SIZE, '\n');
            if (cnt_rec == FQ_CNT_REC * 4) {
                strstr = line_buf;
            } else {
                strtmp = line_buf;
                strstr += "\n" + strtmp;    //NOTICE: the last line of each block has no '\n' [bxz]
            }
            cnt_rec--;
            if (cnt_rec <= 0) {
                memcpy(buf, strstr.c_str(), strstr.size());
                c2s_datainfo.size = strlen(buf);
                c2s_datainfo.offset = fileoffset;
                c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
                c2s_datainfo.target_server = target_server;
                c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
                memcpy(c2s_datainfo.filename, filename1, strlen(filename1));
                c2s_datainfo.finish = 0;
                fileoffset = fileoffset + strlen(buf);
                if (fileoffset < filesize) {
                    fileoffset++;       //plus the last return character
                }
                printf("wq1\n");
                rc = clt_send_data(filesize, file_md5, c2s_datainfo, buf, threadp);
                if(rc != 0){
                    DCS_ERROR("__dcs_clt_write_file send data to server err:%d \n", rc);
                    goto EXIT;
                }
                if (((double)fileoffset) / filesize >= processed_rate) {
                    printf("%.2f%% done.\n", ((double)fileoffset) / filesize * 100);
                    processed_rate += 0.1;
                }
                memset(buf, 0, FQ_CHUNK_SIZE);
                strstr = "";
                memset(buf, 0, FQ_CHUNK_SIZE);
                cnt_rec = FQ_CNT_REC * 4;
            }
        }
        filest.close();
        if (strstr.size() > 0) {
            memcpy(buf, strstr.c_str(), strstr.size());
            c2s_datainfo.size = strlen(buf);
            c2s_datainfo.offset = fileoffset;
            c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
            c2s_datainfo.target_server = target_server;
            c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
            memcpy(c2s_datainfo.filename, filename1, strlen(filename1));
            c2s_datainfo.finish = 0;
            fileoffset = fileoffset + strlen(buf);
            rc = clt_send_data(filesize, file_md5, c2s_datainfo, buf, threadp);
            if(rc != 0){
                DCS_ERROR("__dcs_clt_write_file send data to server err:%d \n", rc);
                goto EXIT;
            }
            memset(buf, 0, FQ_CHUNK_SIZE);
        }
        printf("100.00%% done.\n");
        /*
        cnt_rec = FQ_CNT_REC;
        filep = fopen(filename, "r");
        
        if (filesize <= FQ_CHUNK_SIZE) {
            rc = fread(buf, 1, FQ_CHUNK_SIZE, filep);
            c2s_datainfo.size = rc;
            c2s_datainfo.offset = fileoffset;
            c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
            c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
            c2s_datainfo.finish = 0;
            fileoffset = fileoffset + rc;
            rc = clt_send_data(c2s_datainfo, buf, threadp);
            if(rc != 0){
                DCS_ERROR("__dcs_clt_write_file send data to server err:%d \n", rc);
                goto EXIT;
            }
            if(fileoffset == filesize && filesize != 0){
                DCS_MSG("__dcs_clt_write_file finish write file %s  to server \n",
                        filename);
                c2s_datainfo.finish = 1;
                c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
                c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
                c2s_datainfo.offset = filesize;
                rc = __dcs_clt_finish_msg(c2s_datainfo, threadp);
                if(rc != 0){
                    DCS_ERROR("send finish massage error: %d \n", rc);
                    goto EXIT;
                }
                DCS_MSG("__dcs_clt_write_file sem post finish_sem\n");
                sem_post(&finish_sem);
            } else if(fileoffset == filesize && filesize == 0){
                rc = 0;
                DCS_MSG("__dcs_clt_write_file sem post finish_sem\n");
                sem_post(&finish_sem);
                goto EXIT;
            } else if(fileoffset != filesize){
                DCS_ERROR("__dcs_clt_write_file fail to finish store file %s to server \n",
                          filename);
                rc = -1;
                goto EXIT;
            }
            goto EXIT;
        }
        
        line_buf = (dcs_s8_t *)malloc(FQ_LINE_SIZE);
        if (line_buf == NULL) {
            DCS_ERROR("__dcs_clt_write_file malloc for line buf err: %d \n", errno);
            rc = errno;
            goto EXIT;
        }
        memset(line_buf, 0, FQ_LINE_SIZE);
    
        while (fgets(line_buf, FQ_LINE_SIZE, filep) != NULL) {
            memcpy(buf + strlen(buf), line_buf, strlen(line_buf));
            memset(line_buf, 0, FQ_LINE_SIZE);
            
            if (fgets(line_buf, FQ_LINE_SIZE, filep) != NULL) {
                memcpy(buf + strlen(buf), line_buf, strlen(line_buf));
                memset(line_buf, 0, FQ_LINE_SIZE);
            } else {
                DCS_ERROR("__dcs_clt_write_file fastq filetype error\n");
                rc = -1;
                goto EXIT;
            }
            
            if (fgets(line_buf, FQ_LINE_SIZE, filep) != NULL) {
                memcpy(buf + strlen(buf), line_buf, strlen(line_buf));
                memset(line_buf, 0, FQ_LINE_SIZE);
            } else {
                DCS_ERROR("__dcs_clt_write_file fastq filetype error\n");
                rc = -1;
                goto EXIT;
            }
            
            if (fgets(line_buf, FQ_LINE_SIZE, filep) != NULL) {
                memcpy(buf + strlen(buf), line_buf, strlen(line_buf));
                memset(line_buf, 0, FQ_LINE_SIZE);
            } else {
                DCS_ERROR("__dcs_clt_write_file fastq filetype error\n");
                rc = -1;
                goto EXIT;
            }
            cnt_rec--;
            if (cnt_rec % 100 == 0 || cnt_rec < 10) {
                printf("%d\n", cnt_rec);
            }
            if (cnt_rec <= 0) {
                c2s_datainfo.size = strlen(buf);
                c2s_datainfo.offset = fileoffset;
                c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
                c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
                c2s_datainfo.finish = 0;
                fileoffset = fileoffset + strlen(buf);
                rc = clt_send_data(c2s_datainfo, buf, threadp);
                if(rc != 0){
                    DCS_ERROR("__dcs_clt_write_file send data to server err:%d \n", rc);
                    goto EXIT;
                }
                memset(buf, 0, FQ_CHUNK_SIZE);
                cnt_rec = FQ_CNT_REC;
            }
        }
        printf("filesize: %lu, FQ_CHUNK_SIZE: %d\n", filesize, FQ_CHUNK_SIZE);
        
        if (cnt_rec > 0) {
            c2s_datainfo.size = strlen(buf);
            c2s_datainfo.offset = fileoffset;
            c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
            c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
            c2s_datainfo.finish = 0;
            fileoffset = fileoffset + strlen(buf);
            rc = clt_send_data(c2s_datainfo, buf, threadp);
            if(rc != 0){
                DCS_ERROR("__dcs_clt_write_file send data to server err:%d \n", rc);
                goto EXIT;
            }
        }
         */
        
        if(fileoffset == filesize && filesize != 0){
            DCS_MSG("__dcs_clt_write_file finish write file %s  to server \n",
                    filename);
            c2s_datainfo.finish = 1;
            c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
            c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
            memcpy(c2s_datainfo.filename, filename1, strlen(filename1));
            c2s_datainfo.offset = filesize;
            c2s_datainfo.target_server = target_server;
            rc = __dcs_clt_finish_msg(filesize, file_md5, c2s_datainfo, threadp);
            if(rc != 0){
                DCS_ERROR("send finish massage error: %d \n", rc);
                goto EXIT;
            }
            DCS_MSG("__dcs_clt_write_file sem post finish_sem\n");
            sem_post(&finish_sem);
        } else if(fileoffset == filesize && filesize == 0){
            rc = 0;
            DCS_MSG("__dcs_clt_write_file sem post finish_sem\n");
            sem_post(&finish_sem);
            goto EXIT;
        } else if(fileoffset != filesize){
            DCS_ERROR("__dcs_clt_write_file fail to finish store file %s to server \n",
                      filename);
            rc = -1;
            goto EXIT;
        }
        
        goto EXIT;
    } else {
        DCS_ERROR("__dcs_clt_write_file file type error \n");
        goto EXIT;
    }
    goto EXIT;

    while((rc = read(write_fd, buf, SUPER_CHUNK_SIZE)) != 0){
        c2s_datainfo.size = rc;
        c2s_datainfo.offset = fileoffset;
        c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
        c2s_datainfo.target_server = target_server;
        c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
        c2s_datainfo.finish = 0;
        fileoffset = fileoffset + rc;
        rc = clt_send_data(filesize, file_md5, c2s_datainfo, buf, threadp);
        if(rc != 0){
            DCS_ERROR("__dcs_clt_write_file send data to server err:%d \n", rc);
            goto EXIT;
        }
        memset(buf, 0, SUPER_CHUNK_SIZE);
    }

    if(fileoffset == filesize && filesize != 0){
        DCS_MSG("__dcs_clt_write_file finish write file %s  to server \n",
                  filename);
        c2s_datainfo.finish = 1;
        c2s_datainfo.inode = (dcs_u64_t)f_state.st_ino;
        c2s_datainfo.timestamp = (dcs_u64_t)f_state.st_mtime;
        c2s_datainfo.offset = filesize;
        rc = __dcs_clt_finish_msg(filesize, file_md5, c2s_datainfo, threadp);
        if(rc != 0){
            DCS_ERROR("send finish massage error: %d \n", rc);
            goto EXIT;
        }
        DCS_MSG("__dcs_clt_write_file sem post finish_sem\n");
        sem_post(&finish_sem);
    } else if(fileoffset == filesize && filesize == 0){
        rc = 0;
        DCS_MSG("__dcs_clt_write_file sem post finish_sem\n");
        sem_post(&finish_sem);
        goto EXIT;
    } else if(fileoffset != filesize){
        DCS_ERROR("__dcs_clt_write_file fail to finish store file %s to server \n",
                    filename);
        rc = -1;
        goto EXIT;
    }

EXIT:

    if(write_fd > 0){
        close(write_fd);
    }

    if(meta_fd > 0){
        close(meta_fd);
    }

    if(buf != NULL){
        free(buf);
        buf = NULL;
    }
    
    //bxz
    if (filep) {
        fclose(filep);
        filep = NULL;
    }

    DCS_LEAVE("__dcs_clt_write_file leave \n");
    return rc;
}

/* send write data from client to server
 * 1.init the msg type
 * 2.init iov
 * 3.send data
 * 4.wait for reply
 */
dcs_s32_t clt_send_data(dcs_u64_t filesize,
                        dcs_s8_t *file_md5,
                        dcs_c2s_req_t c2s_datainfo,
                        dcs_s8_t *buf,
                        dcs_thread_t *threadp)
{
    dcs_s32_t rc = 0;
    dcs_u32_t size;     //total length
    dcs_u32_t server_id = 0;
    dcs_s8_t  *sendbuf = NULL;
    //int size;
    amp_request_t *req = NULL;
    amp_message_t *reqmsgp = NULL;
    amp_message_t *repmsgp = NULL;
    dcs_msg_t   *msgp = NULL;

    DCS_ENTER("clt_send_data enter \n");

    rc = __amp_alloc_request(&req);     //req is the last real sending type
    if(rc < 0){
        DCS_ERROR("clt_send_data alloc request err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    sendbuf = (dcs_s8_t *)malloc(c2s_datainfo.size);  //equals to strlen(buf) + 1 ??
    if(sendbuf == NULL){
        DCS_ERROR("clt_send_data malloc sendbuf err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(sendbuf, 0, c2s_datainfo.size);
    memcpy(sendbuf, buf, c2s_datainfo.size);

    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);  //total length
    //size = DCS_MSGHEAD_SIZE + sizeof(dcs_c2s_req_t);
    reqmsgp = (amp_message_t *)malloc(size);
    if(!reqmsgp){
        DCS_ERROR("clt_send_data alloc for reqmsgp err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    memset(reqmsgp, 0, size);
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->size = size;
    msgp->seqno = threadp->seqno;
    msgp->msg_type = is_req;
    msgp->fromid = clt_this_id;
    msgp->fromtype = DCS_CLIENT;
    msgp->optype = clt_optype;
    msgp->filetype = clt_filetype;  //by bxz
    msgp->filesize = filesize;      //by bxz
    memcpy(msgp->md5, file_md5, MD5_STR_LEN);    //bxz
    msgp->u.c2s_req = c2s_datainfo;

    
    req->req_iov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if(req->req_iov == NULL){
        DCS_ERROR("clt_send_data malloc req_iov err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    DCS_MSG("clt_send_data init iov to send buf \n");
    DCS_MSG("clt_send_data buf size :%d \n", c2s_datainfo.size);
    req->req_iov->ak_addr = sendbuf;    //ak_addr is the addr of the sendbuf
    //DCS_MSG("1 \n");
    req->req_iov->ak_len = c2s_datainfo.size;   //ak_len is the length of sendbuf
    //DCS_MSG("2 \n");
    req->req_iov->ak_offset = 0;
    //DCS_MSG("3 \n");
    req->req_iov->ak_flag = 0;
    //DCS_MSG("4 \n");
    req->req_niov = 1;
    
    /*
    rc = __client_init_sendbuf(c2s_datainfo.size, req->req_niov, req->req_iov, buf);
    if(rc != 0){
        DCS_ERROR("clt_send_data init req->req_iov err:%d \n", rc);
        goto EXIT;
    }
    */

    req->req_msg = reqmsgp;
    req->req_msglen = size;
    req->req_need_ack = 1;
    req->req_resent = 1;
    req->req_type = AMP_REQUEST | AMP_DATA;

SEND_AGAIN:
    //server_id = c2s_datainfo.inode % DCS_SERVER_NUM;
    server_id = c2s_datainfo.target_server;
    DCS_MSG("server_id is %d , inode is %ld \n", server_id, c2s_datainfo.inode);
    //if(server_id == 0)
    //    server_id = DCS_SERVER_NUM;
    rc = amp_send_sync(clt_comp_context, 
                        req, 
                        DCS_SERVER, 
                        server_id, 
                        0);
    printf("server_id is %d, client_id is %d, inode is %ld \n"
            ,server_id, clt_this_id, c2s_datainfo.inode);
    if(rc < 0){
        DCS_ERROR("clt_send_data amp send err: %d \n", rc);
        goto EXIT;
    }
    repmsgp = req->req_reply;
    if(!repmsgp){
        DCS_ERROR("clt_send_data cannot recieve the reply msg \n");
        goto SEND_AGAIN;
    }
    else{
        msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
        if(msgp->ack == 0){
            DCS_ERROR("clt_send_data server fail to store the data \n");
            rc = -1;
            goto EXIT;
        }

        if(req->req_iov){
            __client_freebuf(req->req_niov, req->req_iov);
            free(req->req_iov);
            req->req_iov = NULL;
            req->req_niov = 0;
        }

        if(repmsgp){
            amp_free(repmsgp, req->req_replylen);
        }
    }
    
EXIT:
    if(reqmsgp != NULL){
        free(reqmsgp);
        reqmsgp = NULL;
    }
    
    if(req->req_iov != NULL){
        __client_freebuf(req->req_niov, req->req_iov);
        free(req->req_iov);
        req->req_iov = NULL;
        req->req_niov = 0;
    }

    /*
    if(sendbuf != NULL){
        free(sendbuf);
        sendbuf = NULL;
    }
    */

    if(req != NULL){
        DCS_MSG("clt_send_data free request \n");
        __amp_free_request(req); 
    }
    
    DCS_LEAVE("clt_send_data leave \n");
    return rc;
}

/*send a finish msg to tell server a file is sended
 * 1.init finish msg
 * 2.send finish msg
 * 3.recieve reply
 */

dcs_s32_t __dcs_clt_finish_msg(dcs_u64_t filesize,
                               dcs_s8_t *file_md5,
                               dcs_c2s_req_t c2s_datainfo,
                            dcs_thread_t *threadp)
{
    dcs_s32_t rc = 0;
    dcs_u32_t size;
    dcs_u32_t server_id = 0;
    //int size;
    amp_request_t *req = NULL;
    amp_message_t *reqmsgp = NULL;
    amp_message_t *repmsgp = NULL;
    dcs_msg_t   *msgp = NULL;
    
    DCS_ENTER("__dcs_clt_finish_msg enter \n");

    rc = __amp_alloc_request(&req);
    if(rc < 0){
        DCS_ERROR("__dcs_clt_finish_msg alloc for request err \n");
        goto EXIT;
    }
    
    size = sizeof(dcs_msg_t) + AMP_MESSAGE_HEADER_LEN;
    reqmsgp = (amp_message_t *)malloc(size);
    if(!reqmsgp){
        DCS_ERROR("__dcs_clt_finish_msg malloc for reqmsgp err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }
    
    memset(reqmsgp, 0, size);
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->size = size;
    msgp->seqno = threadp->seqno;
    msgp->optype = DCS_WRITE;
    msgp->fromid = clt_this_id;
    msgp->fromtype = DCS_CLIENT;
    msgp->optype = clt_optype;
    msgp->filetype = clt_filetype;  //bxz
    msgp->filesize = filesize;      //bxz
    memcpy(msgp->md5, file_md5, MD5_STR_LEN);    //bxz
    msgp->u.c2s_req = c2s_datainfo;
    
    req->req_iov = NULL;
    req->req_niov = 0;

    req->req_msg = reqmsgp;
    req->req_msglen = size;
    req->req_need_ack = 1;
    req->req_resent = 1;
    req->req_type = AMP_REQUEST | AMP_MSG;

//SEND_AGAIN:
    //server_id = c2s_datainfo.inode % DCS_SERVER_NUM;
    //if(server_id == 0)
    //    server_id = DCS_SERVER_NUM;
    server_id = c2s_datainfo.target_server;

    rc = amp_send_sync(clt_comp_context, 
                        req, 
                        DCS_SERVER, 
                        server_id, 
                        0);

    if(rc < 0){
        DCS_ERROR("__dcs_clt_finish_msg amp send err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    repmsgp = req->req_reply;
    if(!repmsgp){
        DCS_ERROR("__dcs_clt_finish_msg cannot recieve the reply msg \n");
        rc = -1;
        goto EXIT;
    }
    else{
        msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
        if(msgp->ack == 0){
            DCS_ERROR("__dcs_clt_finish_msg server fail to store the data \n");
            rc = -1;
            goto EXIT;
        }

        if(req->req_iov){
            free(req->req_iov);
            req->req_iov = NULL;
            req->req_niov = 0;
        }
        amp_free(repmsgp, req->req_replylen);
    }

EXIT:
    if(reqmsgp != NULL){
        free(reqmsgp);
        reqmsgp = NULL;
    }
    
    if(req->req_iov){
        free(req->req_iov);
        req->req_iov = NULL;
        req->req_niov = 0;
    }

    if(req){
        DCS_MSG("__dcs_clt_finish_msg free request \n");
        __amp_free_request(req); 
    }
    
    DCS_MSG("__dcs_clt_finish_msg leave \n");
    return rc;
}

//by bxz
dcs_s32_t __dcs_clt_delete(dcs_s8_t *filename) {
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_u32_t size = 0;
    dcs_s32_t target_server = -1;
    
    amp_request_t *req = NULL;
    amp_message_t *reqmsgp = NULL;
    amp_message_t *repmsgp = NULL;
    dcs_msg_t *msgp = NULL;
    
    dcs_c2s_req_t c2s_datainfo;
    
    DCS_ENTER("__dcs_clt_delete enter\n");
    
    rc = __amp_alloc_request(&req);
    if (rc < 0) {
        DCS_ERROR("__dcs_clt_delete alloc request error\n");
        goto EXIT;
    }
    
    c2s_datainfo.size = strlen(filename);
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    reqmsgp = (amp_message_t *)malloc(size);
    if (!reqmsgp) {
        DCS_ERROR("__dcs_clt_delete malloc for reqmsgp error\n");
        rc = -1;
        goto EXIT;
    }
    memset(reqmsgp, 0, size);
    
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->seqno = 0;
    msgp->msg_type = is_req;
    msgp->fromid = clt_this_id;
    msgp->fromtype = DCS_CLIENT;
    msgp->optype = DCS_DELETE;
    msgp->u.c2s_req = c2s_datainfo;
    req->req_iov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if (req->req_iov == NULL) {
        DCS_ERROR("__dcs_clt_delete malloc for req_iov error\n");
        rc = -1;
        goto EXIT;
    }
    memset(req->req_iov, 0, sizeof(amp_kiov_t));
    req->req_iov->ak_addr = filename;
    req->req_iov->ak_len = strlen(filename);
    req->req_iov->ak_offset = 0;
    req->req_iov->ak_flag = 0;
    req->req_niov = 1;
    req->req_msg = reqmsgp;
    req->req_msglen = size;
    req->req_need_ack = 1;
    req->req_resent = 1;
    req->req_type = AMP_REQUEST | AMP_DATA;
    
    for (i = 0; i < DCS_SERVER_NUM; ++i) {
    SEND_AGAIN:
        rc = amp_send_sync(clt_comp_context, req, DCS_SERVER, (i + 1), 0);
        if (rc != 0) {
            DCS_ERROR("__dcs_clt_delete send to server error\n");
            goto EXIT;
        }
        repmsgp = req->req_reply;
        if (!repmsgp) {
            DCS_ERROR("__dcs_clt_delete cannot recieve the reply msg\n");
            goto SEND_AGAIN;
        } else {
            msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
            if (msgp->ack == 0) {
                continue;
            }
            target_server = msgp->fromid;
            break;
        }
    }
    
    if (target_server <= 0) {
        printf("\nDelete file %s failed, please check your filename.\n\n", filename);
        DCS_ERROR("__dcs_clt_delete cannot delete file: %s\n", filename);
        rc = -1;
        goto EXIT;
    }
    
EXIT:
    if (req) {
        if (req->req_iov) {
            if (req->req_iov->ak_addr) {
                req->req_iov->ak_addr = NULL;
            }
            free(req->req_iov);
            req->req_iov = NULL;
            req->req_niov = 0;
        }
        
        if (reqmsgp) {
            free(reqmsgp);
            reqmsgp = NULL;
        }
        
        if (repmsgp) {
            free(repmsgp);
            repmsgp = NULL;
        }
        
        __amp_free_request(req);
    }
    
    DCS_LEAVE("__dcs_clt_delete leave\n");
    return rc;
}

dcs_s32_t __dcs_clt_delete1(dcs_s8_t *filename)
{
    dcs_s32_t rc;
    dcs_u32_t server_id = 0;
    dcs_s32_t meta_fd = 0;
    dcs_s32_t size;
    dcs_u64_t inode;
    dcs_u64_t filesize;
    dcs_s8_t  *meta_file;
    amp_request_t *req = NULL;
    amp_message_t *reqmsgp = NULL;
    amp_message_t *repmsgp = NULL;
    dcs_msg_t   *msgp = NULL;
    dcs_msg_t   *msgp_ack = NULL;
    dcs_c2s_del_req_t c2s_del_req;
    struct stat *tmpbuf;
    
    meta_file = get_meta_path(filename);
    if(meta_file == NULL){
        DCS_ERROR("__dcs_clt_delete get meta file name err\n");
        rc = -1;
        goto EXIT;
    }

    meta_fd = open(meta_file, O_RDONLY, 0666);
    if(meta_fd < 0 || meta_fd == 0){
        printf("no such file or open meta file err %d \n",errno);
        rc = errno;
        goto EXIT;
    }
    
    tmpbuf = (struct stat *)malloc(sizeof(struct stat));
    rc = read(meta_fd, tmpbuf, sizeof(struct stat));
    if(rc != (sizeof(struct stat))){
        DCS_ERROR("__dcs_clt_delete read meta data err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    close(meta_fd);

    inode = tmpbuf->st_ino;
    filesize  = tmpbuf->st_size;

    rc = __amp_alloc_request(&req);
    if(rc < 0){
        DCS_ERROR("clt_send_data alloc request err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    c2s_del_req.size = filesize;
    c2s_del_req.inode = inode;
    c2s_del_req.timestamp = tmpbuf->st_mtime;

    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    reqmsgp = (amp_message_t *)malloc(size);
    if(!reqmsgp){
        DCS_ERROR("__dcs_clt_delete malloc for reqmsgp err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    memset(reqmsgp, 0, size);
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->size = size;
    msgp->seqno = 0;
    msgp->fromid = clt_this_id;
    msgp->fromtype = DCS_CLIENT;
    msgp->optype = DCS_DELETE;
    msgp->ack = 0;

    msgp->u.c2s_del_req = c2s_del_req;

    req->req_iov = NULL;
    req->req_niov = 0;
    req->req_msg = reqmsgp;
    req->req_msglen = size;
    req->req_need_ack = 1;
    req->req_resent = 1;
    req->req_type = AMP_REQUEST | AMP_MSG;

    server_id = c2s_del_req.inode % DCS_SERVER_NUM;
    if(server_id == 0)
        server_id = DCS_SERVER_NUM;

        rc = amp_send_sync(clt_comp_context, 
                        req, 
                        DCS_SERVER, 
                        server_id, 
                        0);

        if(rc < 0){
            DCS_ERROR("__dcs_clt_delete send request err: %d \n", errno);
            goto EXIT;
        }

        repmsgp = req->req_reply;
        if(!repmsgp){
            DCS_ERROR("__dcs_clt_delete fail to recieve replymsg \n");
        }
	msgp_ack = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
	if(msgp_ack->ack){
		rc = remove(meta_file);
		if(!rc){
			DCS_MSG("file:%s delete success!\n", filename);
			goto EXIT;
		}
	}
	rc = -1;
	DCS_ERROR("__dcs_clt_delete delete file %s failed\n", filename);
	
EXIT:

    if(reqmsgp != NULL){
        free(reqmsgp);
        reqmsgp = NULL;
    }

    if(repmsgp != NULL){
        free(repmsgp);
        repmsgp = NULL;
    }

    if(req != NULL){
        __amp_free_request(req);      
    }
    if(rc !=  0){
        DCS_LEAVE("__dcs_clt_read_file leave \n");
        return rc;
        //exit(0);
    }
    else{
        DCS_LEAVE("__dcs_clt_read_file leave \n");
        return rc;
    }
}

dcs_s32_t __dcs_clt_get_read_filename(dcs_s8_t *path, dcs_s8_t *file_tobe_stored)
{
    dcs_s32_t rc = 0;
    //dcs_s8_t filename[PATH_LEN];
    dcs_u32_t pathsize = 0;
    dcs_s8_t dirpath[PATH_LEN];
    struct dirent *entry = NULL;
    struct stat f_type;
    DIR *dp;
    dcs_s8_t *meta_path = NULL;
    dcs_s8_t *meta_dir_path = NULL;
    
    DCS_ENTER("__dcs_clt_get_read_filename enter \n");

    DCS_MSG("__dcs_clt_get_read_filename path is %s \n", path);
    
    //by bxz
    dcs_s32_t i = 0;
    dcs_u32_t size = 0;
    dcs_s32_t target_server = -1;
    dcs_clt_file_t *tmp = NULL;
    dcs_u32_t filetype;
    dcs_u64_t filesize;
    
    dcs_c2s_req_t c2s_datainfo;
    amp_request_t *req = NULL;
    amp_message_t *reqmsgp = NULL;
    amp_message_t *repmsgp = NULL;
    dcs_msg_t *msgp = NULL;
    dcs_u64_t inode = 0;
    dcs_u64_t timestamp = 0;
    
    /*
    for (i = strlen(path) - 1; i >= 0; --i) {
        if (path[i] == '/') {
            break;
        }
    }
    if (i < 0) {
        DCS_ERROR("__dcs_clt_get_read_filename get the real filename error\n");
        rc = -1;
        goto EXIT;
    }
    memcpy(filename, path + i + 1, strlen(path) - i - 1);
    */
    
    rc = __amp_alloc_request(&req);
    if(rc < 0){
        DCS_ERROR("__dcs_clt_get_read_filename alloc request err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    c2s_datainfo.size = strlen(path);
    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    reqmsgp = (amp_message_t *)malloc(size);
    if(!reqmsgp){
        DCS_ERROR("__dcs_clt_get_read_filename alloc for reqmsgp err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(reqmsgp, 0, size);
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp + AMP_MESSAGE_HEADER_LEN);
    //msgp->size = size;
    msgp->seqno = 0;
    msgp->msg_type = is_req;
    msgp->fromid = clt_this_id;
    msgp->fromtype = DCS_CLIENT;
    msgp->optype = DCS_READ_QUERY;
    //memcpy(msgp->u.c2s_req.filename, path, strlen(path));
    msgp->u.c2s_req = c2s_datainfo;
    req->req_iov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
    if(req->req_iov == NULL){
        DCS_ERROR("__dcs_clt_get_read_filename malloc req_iov err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }
    //memset(req->req_iov, 0, sizeof(amp_kiov_t));
    req->req_iov->ak_addr = path;
    req->req_iov->ak_len = strlen(path);
    req->req_iov->ak_offset = 0;
    req->req_iov->ak_flag = 0;
    req->req_niov = 1;
    req->req_msg = reqmsgp;
    req->req_msglen = size;
    req->req_need_ack = 1;
    req->req_resent = 1;
    req->req_type = AMP_REQUEST | AMP_DATA;
    
    printf("path: %s[%d]\n", path, strlen(path));
    for (i = 0; i < DCS_SERVER_NUM; ++i) {
    SEND_AGAIN:
        printf("rq0\n");
        rc = amp_send_sync(clt_comp_context, req, DCS_SERVER, (i + 1), 0);
        if(rc != 0){
            DCS_ERROR("__dcs_clt_get_read_filename amp send err: %d \n", rc);
            goto EXIT;
        }
        printf("rq1\n");
        repmsgp = req->req_reply;
        if(!repmsgp){
            printf("rq2\n");
            DCS_ERROR("__dcs_clt_get_read_filename cannot recieve the reply msg \n");
            goto SEND_AGAIN;
        }
        else{
            printf("rq3\n");
            msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
            if(msgp->ack == 0){
                continue;
            }
            printf("rq4\n");
            target_server = msgp->fromid;
            filesize = msgp->filesize;
            filetype = msgp->filetype;
            inode = msgp->inode;
            timestamp = msgp->timestamp;
            
            /*
            if(req->req_iov){
                __client_freebuf(req->req_niov, req->req_iov);
                free(req->req_iov);
                req->req_iov = NULL;
                req->req_niov = 0;
            }
            
            if(repmsgp){
                amp_free(repmsgp, req->req_replylen);
            }
             */
             
            break;
        }
    }
    
    if (target_server <= 0) {
        printf("\nRead file %s failed, please check your filename.\n\n", path);
        DCS_ERROR("__dcs_clt_get_read_filename cannot fetch the corresponding file: %s\n", path);
        rc = -1;
        goto EXIT;
    } else {
        printf("__dcs_clt_get_filename got from server:\ntarget_server\t%d\nfilesize\t%lu\nfiletype\t%d\n", target_server, filesize, filetype);
    }
    
    tmp = (dcs_clt_file_t *)malloc(sizeof(dcs_clt_file_t));
    if(tmp == NULL ){
        DCS_ERROR("__dcs_clt_get_filename malloc for tmp erron \n");
        rc = errno;
        goto EXIT;
    }
    memset(tmp->filename, 0, PATH_LEN);
    memcpy(tmp->filename, path, strlen(path));
    tmp->target_server = target_server;
    tmp->filetype = filetype;
    tmp->filesize = filesize;
    tmp->inode = inode;
    tmp->timestamp = timestamp;
    memcpy(tmp->file_tobe_stored, file_tobe_stored, strlen(file_tobe_stored));
    pthread_mutex_lock(&clt_read_lock);
    list_add_tail(&tmp->file_list, &read_queue);
    pthread_mutex_unlock(&clt_read_lock);
    
    pthread_mutex_lock(&file_num_lock);
    file_on_run++;
    DCS_MSG("file on run %d \n", file_on_run);
    pthread_mutex_unlock(&file_num_lock);
    
    sem_post(&read_sem);
EXIT:
    //if(filename != NULL){
    //  free(filename);
    //filename = NULL;
    //}
    
    if(NULL != req){
        if(req->req_iov != NULL){
            if (req->req_iov->ak_addr) {
                //free(req->req_iov->ak_addr);
                req->req_iov->ak_addr = NULL;
            }
            free(req->req_iov);
            req->req_iov = NULL;
            req->req_niov = 0;
        }
        
        if(reqmsgp != NULL){
            free(reqmsgp);
            reqmsgp = NULL;
        }
        
        if(repmsgp != NULL){
            free(repmsgp);
            repmsgp = NULL;
        }
        
        __amp_free_request(req);
    }
    
     
    DCS_LEAVE("__dcs_clt_get_filename leave \n");
    return rc;
    
    //by bxz end
    /*
    meta_path = get_meta_path(path);
    if(meta_path == NULL){
        DCS_ERROR("__dcs_clt_get_read_filename get meta_path %s failed\n", path);
        rc = -1;
        goto EXIT;
    }
    rc = stat(meta_path, &f_type);
    if(rc != 0){
        DCS_ERROR("__dcs_clt_get_filename get file %s stat err :%d \n", path, errno);
        goto EXIT;
    }

    if(!S_ISDIR(f_type.st_mode)){
        dcs_clt_file_t *tmp = NULL;
        tmp = (dcs_clt_file_t *)malloc(sizeof(dcs_clt_file_t));
        if(tmp == NULL ){
            DCS_ERROR("__dcs_clt_get_filename malloc for tmp erron \n");
            rc = errno;
            goto EXIT;
        }

        //add tail to the file queue
        memset(tmp->filename, 0, PATH_LEN);
        memcpy(tmp->filename, path, strlen(path));

        pthread_mutex_lock(&clt_read_lock);
        list_add_tail(&tmp->file_list, &read_queue);
        pthread_mutex_unlock(&clt_read_lock);
        
        pthread_mutex_lock(&file_num_lock);
        file_on_run++;
        DCS_MSG("file on run %d \n", file_on_run);
        pthread_mutex_unlock(&file_num_lock);

        sem_post(&read_sem);
        DCS_MSG("__dcs_clt_get_filename one resource is been relesed \n");
        goto EXIT;
    }

    filename = (dcs_s8_t *)malloc(PATH_LEN);
    if(filename == NULL){
        DCS_ERROR("__dcs_clt_get_filename malloc filename error %d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(filename, 0, PATH_LEN);
    pathsize = strlen(path);

    if(access(path, 0) && mkdir(path, 0755)){
        DCS_ERROR("__dcs_clt_get_read_filename cannot init dir %s, errno: %d\n", path, errno);
        rc = errno;
        goto EXIT;
    }

    dp = opendir(meta_path);
    //DCS_MSG("2 \n");
    if(dp == NULL){
        DCS_ERROR("__dcs_clt_get_filename open the dir error: %d \n", errno);
        rc = errno;
        goto EXIT;
        //exit(0);
    }

    //DCS_MSG("3 \n");
    while((entry = readdir(dp)) != NULL){
        //DCS_MSG("3 \n");
        memcpy(filename, path, pathsize);
        if(filename[pathsize - 1] != '/'){
            filename[pathsize] = '/';
        }
        if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0){
            DCS_MSG("the file is %s \n", entry->d_name);
            continue;
        }

        memset(dirpath, 0, PATH_LEN);
        if(path[strlen(path) - 1] == '/')
            sprintf(dirpath, "%s%s", path, entry->d_name);
        else
            sprintf(dirpath, "%s/%s", path, entry->d_name);
        DCS_MSG("__dcs_clt_get_filename path is %s \n", path);

        meta_dir_path = get_meta_path(dirpath); 
        if(NULL == meta_dir_path){
            DCS_ERROR("__dcs_clt_get_read_filename get meta_dir_path %s failed\n", meta_dir_path);
            rc = -1;
            goto EXIT;
        }
        rc = stat(meta_dir_path, &f_type);
        if(rc != 0){
            DCS_ERROR("__dcs_clt_get_filename get file %s stat err :%d \n", dirpath, errno);
            goto EXIT;
        }

        free(meta_dir_path);

        if(S_ISDIR(f_type.st_mode)){ 
            DCS_MSG("the dir is %s \n", entry->d_name);
            rc = __dcs_clt_get_read_filename(dirpath);
            if(rc != 0){
                DCS_ERROR("add dir name to queue err %d \n", rc);
                goto EXIT;
            }
            continue;
        }

        memcpy(filename + strlen(filename), entry->d_name, strlen(entry->d_name));
        DCS_MSG("__dcs_clt_get_filename it is a common file: %s\n", filename);

        dcs_clt_file_t *tmp = NULL;
        tmp = (dcs_clt_file_t *)malloc(sizeof(dcs_clt_file_t));
        if(tmp == NULL ){
            DCS_ERROR("__dcs_clt_get_filename malloc for tmp erron \n");
            rc = errno;
            goto EXIT;
        }

        //add tail to the file queue
        memcpy(tmp->filename, filename, PATH_LEN);

        pthread_mutex_lock(&clt_read_lock);
        list_add_tail(&tmp->file_list, &read_queue);
        pthread_mutex_unlock(&clt_read_lock);

        pthread_mutex_lock(&file_num_lock);
        file_on_run++;
        DCS_MSG("file on run %d \n", file_on_run);
        pthread_mutex_unlock(&file_num_lock);

        sem_post(&read_sem);
        memset(filename, 0, PATH_LEN); 
    }

    closedir(dp);
    free(meta_path);
    

EXIT:
    //if(filename != NULL){
      //  free(filename);
        //filename = NULL;
    //}
    if(NULL != req){
        if(req->req_iov != NULL){
            free(req->req_iov->ak_addr);
            free(req->req_iov);
            req->req_iov = NULL;
            req->req_niov = 0;
        }
        
        if(reqmsgp != NULL){
            free(reqmsgp);
            reqmsgp = NULL;
        }
        
        if(repmsgp != NULL){
            free(repmsgp);
            repmsgp = NULL;
        }
        
        __amp_free_request(req);
    }

    DCS_LEAVE("__dcs_clt_get_filename leave \n");
    return rc;
     */
}

/* read file from server
 * 1.read inode and filesize from the meta file
 * 2.send read request to server
 * 3.get data until finish
 */
dcs_s32_t __dcs_clt_read_file(dcs_clt_file_t *clt_file, dcs_thread_t *threadp) 
{
    dcs_s32_t rc = 0;
    dcs_u32_t server_id = 0;
    //dcs_s32_t meta_fd = 0;
    dcs_s32_t size;
    //dcs_u64_t inode;
    //dcs_u64_t timestamp;
    dcs_u64_t filesize;
    dcs_u32_t bufsize;
    dcs_u64_t fileoffset = 0;
    //dcs_s8_t  *meta_file;
    amp_request_t *req = NULL;
    amp_message_t *reqmsgp = NULL;
    amp_message_t *repmsgp = NULL;
    dcs_msg_t   *msgp = NULL;
    dcs_c2s_req_t c2s_req;
    dcs_s32_t read_fd;

    //struct stat *tmpbuf;
    
    DCS_ENTER("__dcs_clt_read_file enter \n");

    DCS_MSG("filename is %s \n", filename);
    
    if (clt_file->filetype == DCS_FILETYPE_FASTA) {
        bufsize = FA_CHUNK_SIZE;
    } else if (clt_file->filetype == DCS_FILETYPE_FASTQ) {
        bufsize = FQ_CHUNK_SIZE;
    } else {
        rc = -1;
        DCS_ERROR("__dcs_clt_read_file filetype[%d] error\n", clt_file->filetype);
        goto EXIT;
    }
    
    read_fd = open(clt_file->file_tobe_stored, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (read_fd <= 0) {
        DCS_ERROR("__dcs_clt_read_file open/create the file %s error: %d\n", clt_file->file_tobe_stored, read_fd);
        rc = -1;
        goto EXIT;
    }
    /*
    bufsize = SUPER_CHUNK_SIZE;
    read_fd = open(clt_file, O_RDWR | O_CREAT | O_TRUNC, 0666);
    meta_file = get_meta_path(clt_file);
    if(meta_file == NULL){
        DCS_ERROR("__dcs_clt_read_file get meta file name err\n");
        rc = -1;
        goto EXIT;
    }

    DCS_MSG("meta_file name is %s \n", meta_file);
    meta_fd = open(meta_file, O_RDONLY, 0666);
    if(meta_fd < 0 || meta_fd == 0){
        printf("no such file or open meta file err %d \n",errno);
        rc = errno;
        goto EXIT;
    }
    
    tmpbuf = (struct stat *)malloc(sizeof(struct stat));
    rc = read(meta_fd, tmpbuf, sizeof(struct stat));
    close(meta_fd);
    if(rc != (sizeof(struct stat))){
        DCS_ERROR("__dcs_clt_read_file read meta data err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    inode = tmpbuf->st_ino;
    filesize  = tmpbuf->st_size;
    timestamp = tmpbuf->st_mtime;
     */

    filesize = clt_file->filesize;
    printf("|||||target server: %d\n", clt_file->target_server);
    printf("|||||filesize: %lu\n", clt_file->filesize);
    printf("|||||filetype: %d\n", clt_file->filetype);
    printf("|||||filename: %s\n", clt_file->filename);
    printf("|||||file tobe stored: %s\n", clt_file->file_tobe_stored);
    rc = __amp_alloc_request(&req);
    if(rc < 0){
        DCS_ERROR("__dcs_clt_read_file alloc request err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    memset(c2s_req.filename, 0, PATH_LEN);
    memcpy(c2s_req.filename, clt_file->filename, strlen(clt_file->filename));
    c2s_req.filename_len = strlen(clt_file->filename);
    printf("clt_file->filename: %s[%d]\n", clt_file->filename, strlen(clt_file->filename));
    while(fileoffset < filesize){
        c2s_req.size = bufsize;
        if(fileoffset + bufsize > filesize) {
            c2s_req.size = filesize - fileoffset;
            if (clt_file->filetype == DCS_FILETYPE_FASTQ && fileoffset > 0) {
                c2s_req.size++;
            }
        }
        printf("bufsize is %d , fileoffset is %ld , filesize is %ld, c2s.reqsize is %d \n", bufsize, fileoffset, filesize, c2s_req.size);
        //c2s_req.inode = inode;
        //c2s_req.timestamp = timestamp;
        //memcpy(c2s_req.filename, clt_file->filename, strlen(clt_file->filename));
        c2s_req.offset = fileoffset;
        c2s_req.finish = 0;

        size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
        //size = DCS_MSGHEAD_SIZE + sizeof(dcs_c2s_req_t);
        reqmsgp = (amp_message_t *)malloc(size);
        if(!reqmsgp){
            DCS_ERROR("__dcs_clt_read_file malloc for reqmsgp err: %d \n", errno);
            rc = errno;
            goto EXIT;
        }

        memset(reqmsgp, 0, size);
        msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp + AMP_MESSAGE_HEADER_LEN);
        msgp->size = size;
        msgp->seqno = threadp->seqno;
        msgp->fromid = clt_this_id;
        msgp->fromtype = DCS_CLIENT;
        msgp->optype = DCS_READ;
        msgp->ack = 0;

        msgp->u.c2s_req = c2s_req;

        req->req_iov = NULL;
        req->req_niov = 0;

        req->req_msg = reqmsgp;
        req->req_msglen = size;
        req->req_need_ack = 1;
        req->req_resent = 1;
        req->req_type = AMP_REQUEST | AMP_MSG;
        printf("r1 \n");

//SEND_AGAIN:
        /*
    server_id = c2s_req.inode % DCS_SERVER_NUM;
    if(server_id == 0)
        server_id = DCS_SERVER_NUM;
         */
        server_id = clt_file->target_server;

        rc = amp_send_sync(clt_comp_context, 
                        req, 
                        DCS_SERVER, 
                        server_id, 
                        0);

    printf("server_id is %d, client_id is %d, inode is %ld \n"
            ,server_id, clt_this_id, c2s_req.inode);

        //printf("r2 \n");
        if(rc < 0){
            DCS_ERROR("__dcs_clt_read_file send request err: %d \n", errno);
            goto EXIT;
        }

        //printf("r3 \n");
        //get the data from server
        repmsgp = req->req_reply;
        if(!repmsgp){
            DCS_ERROR("__dcs_clt_read_file fail to recieve replymsg \n");
            //goto SEND_AGAIN;
        }
        printf("ack: %d\n", ((dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN))->ack);
        if(!((dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN))->ack){
            DCS_ERROR("__dcs_clt_read_file failed, maybe the mapfile is not in the server\n");
            goto EXIT;
        }

        printf("r4 \n");
        if(req->req_iov == NULL){
            DCS_MSG("__dcs_clt_read_file no data in reply msg \n");
            goto CONTINUE;
        }
        else{
        //printf("r5 \n");
            msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
            if(msgp->u.s2c_reply.offset != fileoffset){
                DCS_ERROR("__dcs_clt_read_file recieve wrong data\n");
                goto CONTINUE;
            }

            lseek(read_fd, fileoffset, SEEK_SET);
//printf("data:\n%s[%zu]\n", req->req_iov->ak_addr, strlen((dcs_s8_t *)req->req_iov->ak_addr));
            rc = write(read_fd, req->req_iov->ak_addr, msgp->u.s2c_reply.size);//req->req_iov->ak_len);
            if(rc != msgp->u.s2c_reply.size) {// req->req_iov->ak_len){
                printf("r5.1 \n");
                printf("rc: %d, ak_len: %d\n", rc, req->req_iov->ak_len);
                DCS_ERROR("__dcs_clt_read_file write data err: %d \n", errno);
                goto CONTINUE;
            }
            else{
                printf("r5.2 \n");
                printf("rc: %d, ak_len: %d\n", rc, req->req_iov->ak_len);
                printf("fileoffset: %lu\n", fileoffset);

                fileoffset = fileoffset + rc;
                if (clt_file->filetype == DCS_FILETYPE_FASTQ) {
                    if (fileoffset + 1 < filesize) {
                        //lseek(read_fd, fileoffset, SEEK_SET);
                        //write(read_fd, "\n", 1);
                        //fileoffset++;
                    }
                }
                rc = 0;
            }
        printf("r6 \n");
            printf("fileoffset: %lu\n", fileoffset);
        }

CONTINUE:
        printf("r7 \n");
        
        if(req->req_iov != NULL){
            free(req->req_iov->ak_addr);
            free(req->req_iov);
            req->req_iov = NULL;
            req->req_niov = 0;
        }

        if(reqmsgp != NULL){
            free(reqmsgp);
            reqmsgp = NULL;
        }

        if(repmsgp != NULL){
            free(repmsgp);
            repmsgp = NULL;
        }
        
    }

    if(fileoffset == filesize){
        //rc = __client_send_finish_msg(inode, timestamp, threadp);
        rc = __client_send_finish_msg(clt_file->filename, clt_file->target_server, threadp);
        if(rc != 0){
            DCS_ERROR("__dcs_clt_read_file send finish message err: %d \n", rc);
            goto EXIT;
        }
    }
    sem_post(&finish_sem);

EXIT:
    close(read_fd);
    if(NULL != req){
        if(req->req_iov != NULL){
            free(req->req_iov->ak_addr);
            free(req->req_iov);
            req->req_iov = NULL;
            req->req_niov = 0;
        }

        if(reqmsgp != NULL){
            free(reqmsgp);
            reqmsgp = NULL;
        }

        if(repmsgp != NULL){
            free(repmsgp);
            repmsgp = NULL;
        }

        __amp_free_request(req);      
    }
    if(rc !=  0){
        DCS_LEAVE("__dcs_clt_read_file leave \n");
        return rc;
        //exit(0);
    }
    else{
        DCS_LEAVE("__dcs_clt_read_file leave \n");
        return rc;
    }
}

dcs_s32_t __client_send_finish_msg(dcs_s8_t *read_filename, dcs_u32_t target_server, dcs_thread_t *threadp)
{
    dcs_s32_t rc = 0;
    //dcs_u32_t server_id = 0;
    dcs_u32_t size = 0;
    
    dcs_msg_t *msgp = NULL;
    amp_request_t *req = NULL;
    amp_message_t *reqmsgp = NULL;
    amp_message_t *repmsgp = NULL;
    dcs_c2s_req_t c2s_req;

    DCS_ENTER("__client_send_finish_msg enter \n");

    rc = __amp_alloc_request(&req);
    if(rc < 0){
        DCS_ERROR("__client_send_finish_msg alloc request err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    c2s_req.size = 0;
    //c2s_req.inode = inode;
    //c2s_req.timestamp = timestamp;
    c2s_req.offset = 0;
    c2s_req.finish = 1;
    memcpy(c2s_req.filename, read_filename, strlen(read_filename));

    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
    //size = DCS_MSGHEAD_SIZE + sizeof(dcs_c2s_req_t);
    reqmsgp = (amp_message_t *)malloc(size);
    if(!reqmsgp){
        DCS_ERROR("__client_send_finish_msg malloc for reqmsgp err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(reqmsgp, 0, size);
    
    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp + AMP_MESSAGE_HEADER_LEN);
    msgp->size = size;
    msgp->seqno = threadp->seqno;
    msgp->fromid = clt_this_id;
    msgp->fromtype = DCS_CLIENT;
    msgp->optype = DCS_READ;
    msgp->ack = 0;
    

    msgp->u.c2s_req = c2s_req;

    req->req_iov = NULL;
    req->req_niov = 0;

    req->req_msg = reqmsgp;
    req->req_msglen = size;
    req->req_need_ack = 1;
    req->req_resent = 1;
    req->req_type = AMP_REQUEST | AMP_MSG;

SEND_AGAIN:
    //server_id = c2s_req.inode % DCS_SERVER_NUM;
    //if(server_id == 0)
    //    server_id = DCS_SERVER_NUM;

    rc = amp_send_sync(clt_comp_context, 
                    req, 
                    DCS_SERVER, 
                    target_server,
                    0);
    if(rc < 0){
        DCS_ERROR("__client_send_finish_msg send request err: %d \n", errno);
        goto EXIT;
    }

    repmsgp = req->req_reply;
    if(!repmsgp){
        DCS_ERROR("__client_send_finish_msg fail to recieve replymsg \n");
        goto SEND_AGAIN;
    }
    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp + AMP_MESSAGE_HEADER_LEN);
    if(msgp->ack == 1){
        DCS_MSG("__client_send_finish_msg finish read file successed \n");
    }
    else{
        DCS_ERROR("__client_send_finish_msg fail to finish read file ack is %d  \n", msgp->ack);
        rc = -1;
        goto EXIT;
    }

EXIT:
    if(repmsgp){
        free(repmsgp);
        repmsgp = NULL;
    }

    if(reqmsgp){
        free(reqmsgp);
        reqmsgp = NULL;
    }

    if(req){
        __amp_free_request(req);
        req = NULL;
    }

    DCS_LEAVE("__client_send_finish_msg leave \n");
    return rc;
}

/*create corresponding dir to store the meta file*/
dcs_s32_t clt_create_dir(dcs_s8_t *path)
{
    //dcs_s8_t pathname[256];
    //dcs_s8_t shell[256];
    dcs_s32_t rc = 0;
    dcs_s8_t *pathname = NULL;
    dcs_s8_t *shell = NULL;
    dcs_u32_t len = 0;

    DCS_ENTER("clt_create_dir enter \n");

    pathname = (dcs_s8_t *)malloc(256);
    if(pathname == NULL){
        DCS_ERROR("clt_create_dir malloc for pathname err: %d \n", errno);
        rc = errno;
        goto EXIT;
        //exit(0);
    }
    memset(pathname, 0 ,256);

    shell = (dcs_s8_t *)malloc(256);
    if(shell == NULL){
        DCS_ERROR("clt_create_dir malloc for shell err: %d \n", errno);
        rc = errno;
        goto EXIT;
        //exit(0);
    }
    //DCS_MSG("1 \n");
    memset(shell, 0, 256);
    //DCS_MSG("2 \n");

    len = strlen(CLIENT_MD_PATH);

    memcpy(pathname, CLIENT_MD_PATH, len);
    memcpy(pathname + len, path, strlen(path));

    len = strlen(shell_ins);
    memcpy(shell, shell_ins, len);
    memcpy(shell + len, pathname, strlen(pathname));
    
    //DCS_MSG("3 \n");
    DCS_MSG("clt_create_dir shell to call is %s \n", shell);
    system(shell);
    //DCS_MSG("4 \n");

EXIT:

    if(pathname != NULL){
        free(pathname);
        pathname = NULL;
    }

    if(shell != NULL){
        free(shell);
        shell = NULL;
    }

    DCS_MSG("clt_create_dir rc: %d \n", rc);
    DCS_LEAVE("clt_create_dir leave \n");
    return rc;
}

//meta_path: CLIET_MD_PATH + pathname
//meta_path is the path client created to store meta data of the files
dcs_s8_t *get_meta_path(dcs_s8_t *pathname)
{
    dcs_s8_t *meta_path;
    dcs_u32_t len = 0;

    meta_path = (dcs_s8_t *)malloc(256);
    memset(meta_path, 0, 256);

    len = strlen(CLIENT_MD_PATH);
    memcpy(meta_path, CLIENT_MD_PATH, len);
    if(meta_path[len-1] != '/' && pathname[0] != '/')
        meta_path[len] = '/';
    memcpy(meta_path + strlen(meta_path), pathname, strlen(pathname));

    return meta_path;
}

dcs_s32_t check_dir(dcs_s8_t * path){
    dcs_s8_t * p = NULL;
    dcs_s8_t * q = NULL;
    dcs_s32_t len = strlen(CLIENT_MD_PATH);
    dcs_s8_t * tmp = (dcs_s8_t *)malloc(256);
    struct stat tmpstat;
    dcs_s32_t rc = 0;
    p = path;
    memset(tmp, 0, 256);
    memcpy(tmp, CLIENT_MD_PATH, len);

    while(NULL != (q = strchr(p, '/'))){
        p++;
        if(q == path){
            continue;
        }
        memcpy(tmp + len, path, q-path);
        if(stat(tmp, &tmpstat) < 0){
            if(errno == ENOENT){
                if(mkdir(tmp, 0755)){
                    DCS_ERROR("check_dir make dir %s failed, errno: %d\n", tmp, errno);
                    rc = errno;
                    break;
                }
            }else{
                DCS_ERROR("check_dir dir: %s is not exist, but fail to create, stat errno:%d\n", tmp, errno);
            }
        }
    }
    return rc;

}

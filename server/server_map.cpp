/*chunk file mapping info manager*/

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

#include <limits.h>
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

/*write map buf related*/
map_table_t *map_table[MAP_TABLE_NUM];
pthread_mutex_t map_table_lock[MAP_TABLE_NUM];

/*read map buf related*/
dcs_mapbuf_t *map_buf[MAX_BUF_NUM];
map_name_t      map_name_table[MAX_BUF_NUM];
pthread_mutex_t map_buf_lock[MAX_BUF_NUM];

/*init a array for store the chunk file mapping info
 * to improve write request performance*/
dcs_s32_t __dcs_server_maptable_init()
{
    dcs_s32_t rc = 0;
    dcs_u32_t i = 0;

    DCS_ENTER("__dcs_server_maptable_init enter \n");
    
    for(i=0; i<MAP_TABLE_NUM; i++){
        map_table[i] = NULL;
        pthread_mutex_init(&map_table_lock[i], NULL);
    }

    DCS_LEAVE("__dcs_server_maptable_init leave \n");
    return rc;
}

/*init a array to cache the map info
 * to improve read request performance*/
dcs_s32_t __dcs_server_mapbuf_init()
{
    dcs_s32_t rc = 0;
    dcs_u32_t i = 0;

    DCS_ENTER("__dcs_server_mapbuf_init enter \n");

    for(i=0; i<MAX_BUF_NUM; i++){
        map_buf[i] = NULL;
        map_name_table[i].map_name = NULL;
        pthread_mutex_init(&map_buf_lock[i], NULL);
    }
    
    DCS_LEAVE("__dcs_server_mapbuf_init leave \n");
    return rc;
}

/*insert the chunk file mapping info to table
 * 1. check if the map file has exited in table
 * 2. if exit inset the map buf to the same position 
 * 3. if not find an empty one to insert
 * */

dcs_u32_t __dcs_server_insert_mapinfo(dcs_datamap_t *datamap,
                                          dcs_u64_t fileinode,
                                          dcs_u64_t timestamp,
                                          dcs_u32_t clientid,
                                          dcs_u32_t new_chunk_num)
{
    dcs_u32_t rc = 0;
    dcs_u32_t i = 0;
    dcs_u32_t chunk_num = 0;
    dcs_u32_t size1 = 0;
    dcs_u32_t size2 = 0;
    dcs_s8_t  *map_file_name = NULL;
    //dcs_datamap_t *tmpmap = NULL;
    //dcs_u64_t tmpoffset = 0;
    dcs_datamap_t *mergemap = NULL;

    DCS_ENTER("__dcs_server_insert_mapinfo enter \n");
    
    map_file_name = get_map_name(fileinode, clientid, timestamp);
    DCS_MSG("map file name is %s \n", map_file_name);
    if(map_file_name == NULL){
        DCS_ERROR("__dcs_server_insert_mapinfo get file name err:-1 \n");
        rc = -1;
        goto EXIT;
    }
    
    for(i=0; i<MAP_TABLE_NUM; i++){
        pthread_mutex_lock(&map_table_lock[i]);//add bt weizheng,20151023
	if(map_table[i] == NULL){
            pthread_mutex_unlock(&map_table_lock[i]);
            continue;
        }

        //DCS_MSG("table %d is not NULL \n", i);
        if(strcmp(map_file_name, map_table[i]->inode_cid) == 0){
            chunk_num = map_table[i]->chunk_num;
            size1 = sizeof(dcs_datamap_t)*chunk_num;
            size2 = sizeof(dcs_datamap_t)*new_chunk_num;
            mergemap = (dcs_datamap_t *)malloc(size1 + size2);
            if(mergemap == NULL){
                DCS_ERROR("__dcs_server_insert_mapinfo malloc mergemap err:%d \n",errno);
                pthread_mutex_unlock(&map_table_lock[i]);
                goto EXIT;
            }
            memcpy(mergemap, map_table[i]->datamap, size1);
            memcpy((dcs_s8_t *)mergemap + size1, datamap, size2);

            if(map_table[i]->datamap != NULL){
                free(map_table[i]->datamap);
                map_table[i]->datamap = NULL;
            }

            if(datamap != NULL){
                free(datamap);
                datamap = NULL;
            }
            map_table[i]->datamap = mergemap;
            map_table[i]->chunk_num = chunk_num + new_chunk_num;
            pthread_mutex_unlock(&map_table_lock[i]);
            goto EXIT;
        }

        pthread_mutex_unlock(&map_table_lock[i]);
    }

    for(i=0; i<MAP_TABLE_NUM; i++){
        pthread_mutex_lock(&map_table_lock[i]);

        if(map_table[i] == NULL){
            map_table[i] = (map_table_t *)malloc(sizeof(map_table_t));
            if(map_table[i] == NULL){
                DCS_ERROR("__dcs_server_insert_mapinfo malloc for map_table %d err:%d\n",i ,errno);
                rc = errno;
                pthread_mutex_unlock(&map_table_lock[i]);
                goto EXIT;
            }
            map_table[i]->datamap = datamap;
            map_table[i]->next = NULL;
            map_table[i]->chunk_num = new_chunk_num;
            map_table[i]->inode_cid = (dcs_s8_t *)malloc(INODE_LEN + CLIENTID_LEN);
            if(map_table[i]->inode_cid == NULL){
                DCS_ERROR("__dcs_server_insert_mapinfo malloc for inode_cid err:%d \n", errno);
                rc = errno;
                pthread_mutex_unlock(&map_table_lock[i]);
                goto EXIT;
            }
            memset(map_table[i]->inode_cid, 0, INODE_LEN + CLIENTID_LEN);
            memcpy(map_table[i]->inode_cid, map_file_name, INODE_LEN + CLIENTID_LEN);
            DCS_MSG("the new file is comming and the mapfile name is %s and i is %d\n", 
                                                            map_table[i]->inode_cid, i);
            pthread_mutex_unlock(&map_table_lock[i]);
            goto EXIT;
        }
        pthread_mutex_unlock(&map_table_lock[i]);
    } 

EXIT:
    if(map_file_name != NULL){
        free(map_file_name);
        map_file_name = NULL;
    }
    
    DCS_LEAVE("__dcs_server_insert_mapinfo leave \n");

    return rc;
}

/*
dcs_u32_t __dcs_server_insert_mapinfo(dcs_datamap_t *datamap,
                                          dcs_u64_t fileinode,
                                          dcs_u32_t clientid,
                                          dcs_u32_t new_chunk_num)
{
    dcs_u32_t rc = 0;
    dcs_u32_t i = 0;
    dcs_u32_t chunk_num = 0;
    dcs_u32_t size1 = 0;
    dcs_u32_t size2 = 0;
    dcs_s8_t  *map_file_name = NULL;
    dcs_datamap_t *tmpmap = NULL;
    dcs_u64_t tmpoffset = 0;
    dcs_datamap_t *mergemap = NULL;

    map_table_t *tmptable = NULL;
    map_table_t *head = NULL;
    map_table_t *newone = NULL;

    DCS_ENTER("__dcs_server_insert_mapinfo enter \n");
    
    map_file_name = get_map_name(fileinode, clientid);
    //DCS_MSG("map file name is %s \n", map_file_name);
    if(map_file_name == NULL){
        DCS_ERROR("__dcs_server_insert_mapinfo get file name err:-1 \n");
        rc = -1;
        goto EXIT;
    }

    for(i=0; i<MAP_TABLE_NUM; i++){
        //DCS_MSG("1 \n");
        pthread_mutex_lock(&map_table_lock[i]);
        if(map_table[i] == NULL){
            //DCS_MSG("map table %d is NULL \n", i);
            pthread_mutex_unlock(&map_table_lock[i]);
            continue;
        }

        //DCS_MSG("the i is %d ,the inode_cid is %s \n", i,  map_table[i]->inode_cid);
        //DCS_MSG("the map_file_name is %s \n", map_file_name);
        //DCS_MSG("100 \n");
        if(strcmp(map_file_name, map_table[i]->inode_cid) == 0){
            //chunk_num = strlen((dcs_s8_t *)map_table[i]->datamap)/(sizeof(dcs_datamap_t));
            //DCS_MSG("101 \n");
            chunk_num = map_table[i]->chunk_num;
            //tmpmap = (dcs_datamap_t *)((dcs_s8_t *)map_table[i]->datamap + sizeof(dcs_datamap_t)*(chunk_num-1));
            tmpmap = map_table[i]->datamap;

            //DCS_MSG("102 \n");
            tmpoffset = (dcs_u64_t)(tmpmap[chunk_num - 1].chunksize + tmpmap[chunk_num - 1].offset) ;
            DCS_MSG("the begining offset: %ld \n", tmpmap[0].offset);
            DCS_MSG("tmpoffset is %ld, datamap offset is%ld \n", tmpoffset, datamap[0].offset);

            if((dcs_u64_t)(tmpmap[chunk_num - 1].chunksize + tmpmap[chunk_num - 1].offset) 
                             == datamap[0].offset){
                //size1 = strlen((dcs_s8_t *)map_table[i]->datamap);
                //size2 = strlen((dcs_s8_t *)datamap);

                size1 = sizeof(dcs_datamap_t)*chunk_num;
                size2 = sizeof(dcs_datamap_t)*new_chunk_num;
                mergemap = (dcs_datamap_t *)malloc(size1 + size2);
                if(mergemap == NULL){
                    DCS_ERROR("__dcs_server_insert_mapinfo malloc for mergemap err:%d \n", errno);
                    rc = errno;
                    pthread_mutex_unlock(&map_table_lock[i]);
                    goto EXIT;
                }
                memcpy(mergemap, map_table[i]->datamap, size1);
                memcpy((dcs_s8_t *)mergemap + size1, datamap, size2);

                free(map_table[i]->datamap);
                map_table[i]->datamap = mergemap;
                map_table[i]->chunk_num = chunk_num + new_chunk_num;
                free(datamap);
                datamap = NULL;
                pthread_mutex_unlock(&map_table_lock[i]);
                goto EXIT;
            }
            else{
                //DCS_MSG("105 \n");
                head = map_table[i];
                while(head->next != NULL){
                    tmptable = head->next;
                    if(tmptable->datamap[0].offset > datamap->offset){
                        break;
                    }
                    else{
                        head = head->next;
                    }
                }

                newone = (map_table_t *)malloc(sizeof(map_table_t));
                if(newone == NULL){
                    DCS_ERROR("__dcs_server_insert_mapinfo malloc for new one err:%d \n", errno);
                    rc = errno;
                    pthread_mutex_unlock(&map_table_lock[i]);
                    goto EXIT;
                }
                newone->datamap = datamap;
                newone->chunk_num = new_chunk_num;
                newone->inode_cid = NULL;
                
                newone->next = head->next;
                head->next = newone;
                pthread_mutex_unlock(&map_table_lock[i]);
                goto EXIT;
            }
        }

        //DCS_MSG("200 \n");
        pthread_mutex_unlock(&map_table_lock[i]);
    }
    //DCS_MSG("2 \n");

    for(i=0; i<MAP_TABLE_NUM; i++){
        pthread_mutex_lock(&map_table_lock[i]);

        if(map_table[i] == NULL){
            map_table[i] = (map_table_t *)malloc(sizeof(map_table_t));
            if(map_table[i] == NULL){
                DCS_ERROR("__dcs_server_insert_mapinfo malloc for map_table %d err:%d\n",i ,errno);
                rc = errno;
                pthread_mutex_unlock(&map_table_lock[i]);
                goto EXIT;
            }
            map_table[i]->datamap = datamap;
            map_table[i]->next = NULL;
            map_table[i]->chunk_num = new_chunk_num;
            map_table[i]->inode_cid = (dcs_s8_t *)malloc(INODE_LEN + CLIENTID_LEN);
            if(map_table[i]->inode_cid == NULL){
                DCS_ERROR("__dcs_server_insert_mapinfo malloc for inode_cid err:%d \n", errno);
                rc = errno;
                pthread_mutex_unlock(&map_table_lock[i]);
                goto EXIT;
            }
            memset(map_table[i]->inode_cid, 0, INODE_LEN + CLIENTID_LEN);
            memcpy(map_table[i]->inode_cid, map_file_name, INODE_LEN + CLIENTID_LEN);
            DCS_MSG("the new file is comming and the mapfile name is %s and i is %d\n", 
                                                            map_table[i]->inode_cid, i);
            pthread_mutex_unlock(&map_table_lock[i]);
            goto EXIT;
        }
        pthread_mutex_unlock(&map_table_lock[i]);
    } 

EXIT:
    //DCS_MSG("3 \n");
    if(map_file_name != NULL){
        free(map_file_name);
        map_file_name = NULL;
    }
    
    DCS_LEAVE("__dcs_server_insert_mapinfo leave \n");

    return rc;
}
*/

/*write the chunk-file mapping info to disk*/
dcs_u32_t __dcs_server_mapinfo_wb(dcs_u64_t inode,
                                      dcs_u64_t timestamp,
                                      dcs_u32_t clientid,
                                      dcs_u64_t filesize)
{
    dcs_u32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s32_t fd = 0;
    dcs_u32_t chunk_num = 0;
    dcs_s8_t  *map_name = NULL;
    dcs_s8_t  *map_path = NULL;

    DCS_ENTER("__dcs_server_mapinfo_wb enter \n");
    
    map_name = get_map_name(inode, clientid, timestamp);
    map_path = get_map_path(map_name);
    /*find the bucket use the key name inode_cid*/ 
    for(i=0; i<MAP_TABLE_NUM; i++){
        if(map_table[i] != NULL){
            if(strcmp(map_table[i]->inode_cid, map_name) == 0){
                DCS_MSG("find the file and i is %d \n", i);
                break;
            }
        }
    }
    
    if(i == MAP_TABLE_NUM){
        DCS_ERROR("__dcs_server_mapinfo_wb can not find the map table entry \n");
        rc = -1;
        goto EXIT;
    }

    /*open or create the map file*/
    fd = open(map_path, O_WRONLY | O_CREAT, 0666);
    if(fd <= 0){
        DCS_ERROR("__dcs_server_mapinfo_wb open mapfile %s failed, err:%d \n", map_path, errno);
        rc = errno;
        goto EXIT;
    }

    pthread_mutex_lock(&map_table_lock[i]);

    chunk_num = map_table[i]->chunk_num;
    DCS_MSG("__dcs_server_mapinfo_wb write chunk_num %d to mapfile %s\n", chunk_num, map_path);
    rc = write(fd, map_table[i]->datamap, chunk_num*sizeof(dcs_datamap_t));
    if(rc != chunk_num*sizeof(dcs_datamap_t)){
        DCS_ERROR("__dcs_server_mapinfo_wb err:%d \n", errno);
        pthread_mutex_unlock(&map_table_lock[i]);
        rc = errno;
        goto EXIT;
    }
    rc = 0;

    DCS_MSG("__dcs_server_mapinfo_wb write chunk_num %d to mapfile %s done\n", chunk_num, map_path);
    if(map_table[i]->datamap != NULL){
        free(map_table[i]->datamap);
        map_table[i]->datamap = NULL;
    }

    if(map_table[i] != NULL){
        free(map_table[i]);
        map_table[i] = NULL;
    }

    pthread_mutex_unlock(&map_table_lock[i]);

EXIT:

    if(map_name){
        free(map_name);
        map_name = NULL;
    }

    if(map_path){
        free(map_path);
        map_path = NULL;
    }

    if(fd > 0)
        close(fd);

    DCS_LEAVE("__dcs_server_mapinfo_wb leave \n");
    return rc;
}

/*write the chunk-file mapping info to disk*/
/*
dcs_u32_t __dcs_server_mapinfo_wb(dcs_u64_t inode,
                                      dcs_u32_t clientid,
                                      dcs_u64_t filesize)
{
    dcs_u32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s32_t fd = 0;
    dcs_u32_t chunk_num = 0;
    dcs_u64_t offset = 0;
    dcs_s8_t  *map_name = NULL;
    dcs_s8_t  *map_path = NULL;

    map_table_t *head = NULL;
    map_table_t *tmp = NULL;
    DCS_ENTER("__dcs_server_mapinfo_wb enter \n");
    
    map_name = get_map_name(inode, clientid);
    map_path = get_map_path(map_name);
    fd = open(map_path, O_WRONLY | O_CREAT, 0666);
    if(fd <= 0){
        DCS_ERROR("__dcs_server_mapinfo_wb open mapfile err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    for(i=0; i<MAP_TABLE_NUM; i++){
        if(map_table[i] != NULL){
            if(strcmp(map_table[i]->inode_cid, map_name) == 0){
                DCS_MSG("find the file and i is %d \n", i);
                break;
            }
        }
    }
    
CHECK_AGAIN:
    pthread_mutex_lock(&map_table_lock[i]);
    //DCS_MSG("100 \n");

    head = map_table[i];
    while(head != NULL){
        if(head->datamap[0].offset != offset)
        {
            DCS_MSG("i is %d, datamap offset is %ld the offset is %ld \n", i, head->datamap[0].offset, offset);
            DCS_MSG("the chunk num is %d \n", head->chunk_num);
            break;
        }
        else{
           // DCS_MSG("200 \n");
            chunk_num = head->chunk_num;
            offset = head->datamap[chunk_num - 1].offset + head->datamap[chunk_num - 1].chunksize;
            DCS_MSG("the chunk offset is %ld, and chunksize is %d \n",
                                            head->datamap[chunk_num - 1].offset,
                                            head->datamap[chunk_num - 1].chunksize);
        }
        head = head->next;
    }

    if(offset != filesize){
        pthread_mutex_unlock(&map_table_lock[i]);
        DCS_MSG("file size is %ld, offset is %ld \n", filesize, offset);
        DCS_MSG("mapinfo is not ready \n");
        sleep(1);
        goto CHECK_AGAIN;
    }

    DCS_MSG("finish chech out file size is %ld, offset is %ld \n", filesize, offset);
    //DCS_MSG("2 \n");
    head = map_table[i];
    while(head){
        rc = write(fd, head->datamap, (head->chunk_num)*sizeof(dcs_datamap_t));
        if(rc != head->chunk_num*sizeof(dcs_datamap_t)){
            DCS_ERROR("__dcs_server_mapinfo_wb err:%d \n", errno);
            pthread_mutex_unlock(&map_table_lock[i]);
            rc = errno;
            goto EXIT;
        }
        rc = 0;
        head = head->next;
    }

    //DCS_MSG("3 \n");
    head = map_table[i];
    while(head){
        free(head->datamap);
        head->datamap = NULL;
        if(head->inode_cid){
            free(head->inode_cid);
            head->inode_cid = NULL;
            head->chunk_num = 0;
        }
        tmp = head->next;
        free(head);
        head = NULL;
        head = tmp;
    }

    map_table[i] = NULL;
    pthread_mutex_unlock(&map_table_lock[i]);
    //DCS_MSG("5 \n");

EXIT:

    if(map_name){
        free(map_name);
        map_name = NULL;
    }

    if(map_path){
        free(map_path);
        map_path = NULL;
    }

    if(fd > 0)
        close(fd);

    DCS_LEAVE("__dcs_server_mapinfo_wb leave \n");
    return rc;
}
*/

/*get map file path
 * 1. get the filepath 
 * 2. according the fist bit if 1 store in dir 1 or 0 store in dir 2
 */
dcs_s8_t *get_map_path(dcs_s8_t *filename)
{
    dcs_u32_t rc = 0;
    dcs_s8_t  *filepath = NULL;

    filepath = (dcs_s8_t *)malloc(256);
    if(filepath == NULL){
        DCS_ERROR("get_map_path malloc for filepath err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
    memset(filepath, 0, 256);
    
    /*according the first bit of the map_file_name to store them into diff dir*/
    if(GETBIT(filename, 1)){
        memcpy(filepath, MAP_FILE_PATH1, strlen(MAP_FILE_PATH1));
    }
    else{  
        memcpy(filepath, MAP_FILE_PATH2, strlen(MAP_FILE_PATH2));
    }

    if('/' != filepath[strlen(filepath) - 1])
	filepath[strlen(filepath)] = '/';

    memcpy(filepath + strlen(filepath), filename, strlen(filename));
EXIT:
    if(rc)
        return NULL;
    else
        return filepath;
}

/*turn the inode number to char*/
dcs_s8_t *get_map_name(dcs_u64_t inode, dcs_u32_t client_id, dcs_u64_t timestamp)
{
    dcs_s8_t *map_name = NULL;
    dcs_u32_t rc = 0;

    map_name = (dcs_s8_t *)malloc(PATH_LEN);
    if(map_name == NULL){
        DCS_ERROR("get_map_name malloc for map_name err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(map_name, 0, PATH_LEN);
    sprintf(map_name, "%ld_%d_%ld", inode, client_id, timestamp);
    return map_name;
 
EXIT:
        DCS_ERROR("get_map_name get map_name error \n");
        return NULL;
}

/*read mapinfo into buf*/
dcs_mapbuf_t *read_map_buf(dcs_s8_t *map_name, 
                              dcs_u64_t fileoffset,
                              dcs_u32_t reqsize)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s32_t read_fd = 0;
    dcs_s32_t buf_id = 0;
    dcs_s32_t mapsize = 0;
    dcs_u32_t chunk_num = 0;
    dcs_u64_t begin = 0;
    dcs_u64_t end = 0;
    dcs_u64_t filesize = 0;
    dcs_s8_t  *map_path = NULL;
    
    dcs_mapbuf_t *mapbuf = NULL;
    dcs_mapbuf_t *tmpbuf = NULL;
    struct stat f_state;

    DCS_ENTER("read_map_buf enter\n");
    
    map_path = get_map_path(map_name);
    read_fd = open(map_path, O_RDONLY | O_APPEND, 0666);
    if(read_fd < 0){
        DCS_ERROR("read_map_buf open map info file:%s err:%d \n",map_path, errno);
        rc = errno;
        goto EXIT;
    }

    //DCS_MSG("1 \n");
    if(stat(map_path, &f_state)){
        DCS_ERROR("read_map_buf get map file err:%d  \n", errno);
        rc = errno;
        goto EXIT;
    }
    filesize = (dcs_u64_t)(f_state.st_size);
    chunk_num = filesize/(sizeof(dcs_datamap_t));
    DCS_MSG("filesize is %ld, sizeof dcs_datamap_t is %ld, chunk_num is %d \n",
            filesize, sizeof(dcs_datamap_t), chunk_num);

    //DCS_MSG("2 \n");
    //DCS_MSG("the size of dcs_mapbuf_t is %ld \n", sizeof(dcs_mapbuf_t));
    mapbuf = (dcs_mapbuf_t *)malloc(sizeof(dcs_mapbuf_t));
    if(mapbuf == NULL){
        DCS_ERROR("read_map_buf malloc for mapbuf err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    //DCS_MSG("3 \n");
    mapbuf->chunk_num = chunk_num;
    mapbuf->datamap = (dcs_datamap_t *)malloc(filesize);
    if(mapbuf->datamap == NULL){
        DCS_ERROR("read_map_buf malloc for datamap err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    rc = read(read_fd, mapbuf->datamap, filesize);
    if(rc < 0 || rc != filesize){
        DCS_ERROR("read_map_buf read mapinfo file err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    /*reset the rc*/
    rc = 0;

    /*add the entry to the map file buf*/
    for(i=0; i<MAX_BUF_NUM; i++){
        pthread_mutex_lock(&map_buf_lock[i]);
        if(map_buf[i] == NULL){
            DCS_MSG("the %dth mapbuf, map_name:%s\n", i, map_name);
            //DCS_MSG("6 \n");

            map_buf[i] = (dcs_mapbuf_t *)malloc(sizeof(dcs_mapbuf_t));
            if(map_buf[i] == NULL){
                DCS_ERROR("malloc for map buf err:%d \n", errno);
                rc = errno;
                goto EXIT;
            }
            map_buf[i]->datamap = mapbuf->datamap;
            map_buf[i]->chunk_num = chunk_num;

            //DCS_MSG("66 \n");
            DCS_MSG("chunk num is  %d \n", chunk_num);
            dcs_u32_t len = strlen(map_name);
            map_name_table[i].map_name = (dcs_s8_t *)malloc(len + 1);
            if(map_name_table[i].map_name == NULL){
                DCS_ERROR("read_map_buf malloc for map_name err:%d \n", errno);
                rc = errno;
                goto EXIT;
            }
            memcpy(map_name_table[i].map_name, map_name, len);
            map_name_table[i].map_name[len] = '\0';

            //map_name_table[i] = map_name;
            //DCS_MSG("66 \n");
            pthread_mutex_unlock(&map_buf_lock[i]);
            break;
        }
    }

    DCS_MSG("7 \n");
    buf_id = i;
    pthread_mutex_lock(&map_buf_lock[buf_id]);

    /*as the request data may not from the beginning of the file,
     * so we cut the map info for it*/
    /*define the begin chunk*/
    for(i=0; i<chunk_num; i++){
        if(mapbuf->datamap[i].offset + mapbuf->datamap[i].chunksize > fileoffset){
            begin = i;
            //DCS_MSG("datamap offset is %ld, chunksize is %d, fileoffset is %ld, begin is %ld \n",
             //       mapbuf->datamap[i].offset, mapbuf->datamap[i].chunksize, fileoffset, begin);
            break;
        }
    }

    /*define the last chunk*/
    DCS_MSG("chunk_num is %d \n", chunk_num);
    for(i=begin; i<chunk_num; i++){
        if(mapbuf->datamap[i].offset + mapbuf->datamap[i].chunksize >= fileoffset + reqsize){
            end = i;
            //DCS_MSG("datamap offset is %ld, chunksize is %d, fileoffset is %ld, reqsize is %d, end is %ld \n",
            //        mapbuf->datamap[i].offset, mapbuf->datamap[i].chunksize, fileoffset, reqsize, end);
            break;
        }
    }

    if(i < chunk_num - 1){
            DCS_MSG("datamap offset is %ld, chunksize is %d, fileoffset is %ld, reqsize is %d, end is %ld and i is %d \n",
                    mapbuf->datamap[i].offset, mapbuf->datamap[i].chunksize, fileoffset, reqsize, end, i);
        /*
        for(i = end + 1; i<chunk_num; i++){
            DCS_MSG("datamap offset is %ld, chunksize is %d, fileoffset is %ld, reqsize is %d, end is %ld \n",
                    mapbuf->datamap[i].offset, mapbuf->datamap[i].chunksize, fileoffset, reqsize, end);
        }
        */
    }

    DCS_MSG("end is %ld and begin is %ld \n", end, begin);
    /*return the needed mapinfo*/
    mapsize = sizeof(dcs_datamap_t)*(end - begin + 1);
    tmpbuf = (dcs_mapbuf_t *)malloc(sizeof(dcs_mapbuf_t));
    if(!tmpbuf){
        DCS_ERROR("read_map_buf malloc for tmpbuf err:%d \n", errno);
        rc = errno;
        pthread_mutex_unlock(&map_buf_lock[buf_id]);
        goto EXIT;
    }
    tmpbuf->chunk_num = end - begin + 1;

    DCS_MSG("mapsize is %d \n", mapsize);
    tmpbuf->datamap = (dcs_datamap_t *)malloc(mapsize);
    if(tmpbuf->datamap == NULL){
        DCS_ERROR("read_map_buf malloc for tmpbuf datamap err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    DCS_MSG("read_map_buf read chunknum is %ld \n", (end - begin + 1));
    memcpy(tmpbuf->datamap, (dcs_s8_t *)mapbuf->datamap + (sizeof(dcs_datamap_t)* begin), mapsize);
    pthread_mutex_unlock(&map_buf_lock[buf_id]);

EXIT:
    if(map_path){
        free(map_path);
        map_path = NULL;
    }

    if(read_fd){
        close(read_fd);
    }

    DCS_LEAVE("read_map_buf leave \n");

    if(rc != 0){
        return NULL;
    }
    else
        return tmpbuf;
}

/*get needed map info from whole file mapping info*/
dcs_mapbuf_t *get_map_buf(dcs_s8_t *map_name,
                             dcs_u64_t fileoffset,
                             dcs_u32_t reqsize,
                             dcs_u32_t pos)
{
    dcs_s32_t rc = 0;
    dcs_s32_t i = 0;
    dcs_s32_t mapsize = 0;
    dcs_u32_t begin = 0;
    dcs_u32_t end = 0;
    dcs_u32_t chunk_num;
    
    dcs_mapbuf_t *mapbuf = NULL;
    dcs_mapbuf_t *tmpbuf = NULL;

    DCS_ENTER("get_map_buf enter \n");

    mapbuf = map_buf[pos];
    if(!mapbuf){
        DCS_ERROR("get_map_buf mapbuf is not exist \n");
        rc = -1;
        goto EXIT;
    }
    pthread_mutex_lock(&map_buf_lock[pos]);
    
    chunk_num = mapbuf->chunk_num;
    DCS_MSG("get map buf, buf chunk_num is %d \n",  chunk_num);

    //chunk_num = strlen((dcs_s8_t *)mapbuf)/sizeof(dcs_datamap_t);
    /*define the begin chunk*/
    for(i=0; i<chunk_num; i++){
        if(mapbuf->datamap[i].offset + mapbuf->datamap[i].chunksize > fileoffset){
            begin = i;
            break;
        }
    }

    DCS_MSG("begin is %d chunk num is %d \n", begin, chunk_num);

    /*define the last chunk*/
    for(i=begin; i<chunk_num; i++){
        if(mapbuf->datamap[i].offset + mapbuf->datamap[i].chunksize >= fileoffset + reqsize){
            end = i;
            break;
        }
    }

    DCS_MSG("end is %d chunk num is %d \n", end, chunk_num);

    /*return the needed mapinfo*/
    mapsize = sizeof(dcs_datamap_t)*(end - begin + 1);
    tmpbuf = (dcs_mapbuf_t *)malloc(sizeof(dcs_mapbuf_t));
    if(!tmpbuf){
        DCS_ERROR("get_map_buf malloc for tmpbuf err:%d \n", errno);
        rc = errno;
        pthread_mutex_unlock(&map_buf_lock[pos]);
        goto EXIT;
    }
    tmpbuf->chunk_num = end - begin + 1;
    DCS_MSG("get map buf chunk_num is %d end is %d begin is %d \n",tmpbuf->chunk_num, end, begin);

    tmpbuf->datamap = (dcs_datamap_t *)malloc(mapsize);
    if(tmpbuf->datamap == NULL){
        DCS_ERROR("get_map_buf malloc for tmpbuf datamap err:%d \n", errno);
        rc = errno;
        pthread_mutex_unlock(&map_buf_lock[pos]);
        goto EXIT;
    }

    //DCS_MSG("get_map_buf read chunknum is %d \n", (end - begin + 1));
    DCS_MSG("mapsize is %d \n", mapsize);
    memcpy(tmpbuf->datamap, (dcs_s8_t *)mapbuf->datamap+(sizeof(dcs_datamap_t)* begin), mapsize);
    //DCS_MSG("get map buf chunk_num is %d end is %d begin is %d \n",tmpbuf->chunk_num, end, begin);
    pthread_mutex_unlock(&map_buf_lock[pos]);

    //tmpbuf->chunk_num = end - begin + 1;
    /*
    for(i=0; i<end-begin + 1; i++){
        DCS_MSG("compressor id in tmpbuf is %d, in mapbuf is %d \n", 
                tmpbuf->datamap[i].compressor_id, mapbuf->datamap[i+begin].compressor_id);
    }
    */

EXIT:
    DCS_MSG("get_map_buf leave \n");
    DCS_MSG("get map buf chunk_num is %d end is %d begin is %d \n",tmpbuf->chunk_num, end, begin);

    if(rc == 0){
        return tmpbuf;
    }
    else{
        return NULL;
    }
}

/*check the if the mapinfo had been cached*/
dcs_s32_t check_map_buf(dcs_s8_t *map_name)
{
    dcs_s32_t rc = -1;
    dcs_s32_t i = 0;

    for(i=0; i<MAX_BUF_NUM; i++){
        if(map_name_table[i].map_name != NULL && strcmp(map_name_table[i].map_name, map_name) == 0){
            rc = i;
            break;
        } 
    }

    return rc;
}

/*free the map buf according the map_name */
dcs_s32_t free_map_buf(dcs_s8_t *map_name)
{
    dcs_s32_t rc = 0;
    dcs_u32_t i;

    DCS_ENTER("free_map_buf enter \n");

    for(i=0; i<MAX_BUF_NUM; i++){
        if(map_name_table[i].map_name != NULL && (strcmp(map_name_table[i].map_name, map_name)) == 0){
            pthread_mutex_lock(&map_buf_lock[i]);
            DCS_MSG("free_map_buf free the %dth mapbuf, map_name:%s\n", i, map_name);
            free(map_buf[i]->datamap);
            map_buf[i]->datamap = NULL;
            free(map_buf[i]);
            map_buf[i] = NULL;
            free(map_name_table[i].map_name);
            map_name_table[i].map_name = NULL;
            pthread_mutex_unlock(&map_buf_lock[i]);
            break;
        }
    }

    if(i == MAX_BUF_NUM){
        rc = -1;
        goto EXIT;
    }

EXIT:
    DCS_LEAVE("free_map_buf leave \n");
    
    return rc;
}

dcs_s32_t free_map_table(dcs_s8_t *map_name){
    
    dcs_s32_t i = 0, rc = -1;

    for(i=0; i<MAP_TABLE_NUM; i++){
        if(map_table[i] != NULL){
            if(strcmp(map_table[i]->inode_cid, map_name) == 0){
                DCS_MSG("find the file and i is %d \n", i);
                pthread_mutex_lock(&map_table_lock[i]);

                if(map_table[i]->datamap != NULL){
                    free(map_table[i]->datamap);
                    map_table[i]->datamap = NULL;
                }

                if(map_table[i] != NULL){
                    free(map_table[i]);
                    map_table[i] = NULL;
                }

                pthread_mutex_unlock(&map_table_lock[i]);
                rc = 0;
                break;
            }
        }
    }
    return rc;
}

dcs_s32_t sha_bf_upload_to_master(){
    dcs_s32_t rc = 0;
    dcs_s8_t * filepath = NULL;

    filepath = (dcs_s8_t *)malloc(PATH_LEN);
    if(NULL == filepath){
        DCS_ERROR("sha_bf_upload_to_master malloc for filepath failed, errno: %d\n", errno);
        rc = -1;
        goto EXIT;
    }
    memset(filepath, 0, PATH_LEN);
    memcpy(filepath, MAP_FILE_PATH1, strlen(MAP_FILE_PATH1));
    
    dir_read_sha_bf_upload(filepath);
    
    memset(filepath, 0, PATH_LEN);
    memcpy(filepath, MAP_FILE_PATH2, strlen(MAP_FILE_PATH2));
    
    dir_read_sha_bf_upload(filepath);


EXIT:
    if(NULL != filepath){
        free(filepath);
        filepath = NULL;
    }
    return rc;
}

dcs_s32_t dir_read_sha_bf_upload(dcs_s8_t * filepath){
    dcs_s32_t rc = 0;
    dcs_s32_t fd = 0;
    dcs_s32_t total_num;
    dcs_s32_t size;
    dcs_s8_t * filename = NULL;
    DIR *dp = NULL;
    sha_bf_t * sha_bf = NULL;
    dcs_datamap_t * datamap = NULL;
    struct dirent *entry;
    struct stat f_state;
    time_t now = time(NULL);
    sha_bf_t * tmp_sha_v[DCS_MASTER_NUM];
    //sha_bf_t * sha_v[DCS_MASTER_NUM];
    dcs_s32_t sha_num[DCS_MASTER_NUM];
    amp_request_t * req2m;
    amp_message_t * reqmsgp2m;
    amp_message_t * repmsgp2m;
    dcs_msg_t * msgp = NULL;

    conid_sha_block_t * tmp_csb_v[DCS_COMPRESSOR_NUM];
    //conid_sha_block_t * csb_v[DCS_COMPRESSOR_NUM];
    dcs_s32_t csb_num[DCS_COMPRESSOR_NUM];
    amp_request_t * req2d;
    amp_message_t * reqmsgp2d;
    amp_message_t * repmsgp2d;

    dcs_s32_t power = get_master_power();

    dp = opendir(filepath);
    if(dp == NULL){
        DCS_ERROR("dir_read_sha_bf_upload open the dir error: %d \n", errno);
        rc = errno;
        goto EXIT;
    }
    
    filename = (dcs_s8_t *)malloc(PATH_LEN);
    if(NULL == filename){
        DCS_ERROR("dir_read_sha_bf_upload malloc for filename failed, errno:%d\n", errno);
        rc = errno;
        goto EXIT;
    }

    memset(filename, 0, PATH_LEN);

    while((entry = readdir(dp)) != NULL){
        if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0){
            continue;
        }

        memset(filename, 0, PATH_LEN);
        sprintf(filename, "%s/%s", filepath, entry->d_name);
        
        rc = stat(filename, &f_state);
        if(rc){
            DCS_ERROR("dir_read_sha_bf_upload stat file %s failed, errno: %d\n", filename, errno);
            rc = errno;
            goto EXIT;
        }
        if(f_state.st_atime > now){
            DCS_ERROR("dir_read_sha_bf_upload file %s have accessed after reboot of system, now: %ld, create_time: %ld, modify_time:%ld, access_time:%ld\n", filename, now, f_state.st_ctime, f_state.st_mtime, f_state.st_atime);
            continue;
        }
      
        if(S_ISDIR(f_state.st_mode)){
            rc = dir_read_sha_bf_upload(filename);
            if(rc){
                DCS_ERROR("dir_read_sha_bf_upload call subdir %s failed, rc: %d\n", filename, rc);
                goto EXIT;
            }
        }else{
            dcs_s32_t i = 0;
            dcs_s32_t rsize = 0;

            total_num = f_state.st_size / sizeof(dcs_datamap_t);
            if(!total_num){
                continue;
            }
            datamap = (dcs_datamap_t *)malloc(total_num * sizeof(dcs_datamap_t));    
            if(NULL == datamap){
                DCS_ERROR("dir_read_sha_bf_upload malloc for datamap failed, errno: %d\n", errno);
                goto EXIT;
            }
            memset(datamap, 0, total_num * sizeof(dcs_datamap_t));
            sha_bf = (sha_bf_t *)malloc(total_num * sizeof(sha_bf_t));            
            if(NULL == sha_bf){
                DCS_ERROR("dir_read_sha_bf_upload malloc for sha_bf failed, errno: %d\n", errno);
                goto EXIT;
            }
            memset(sha_bf, 0, total_num * sizeof(sha_bf_t));
            
            fd = open(filename, O_RDONLY, 0666);
            if(fd < 0){
                DCS_ERROR("dir_read_sha_bf_upload open file %s failed, errno:%d\n", filename, errno);
                rc = errno;
                goto EXIT;
            }

            rsize = read(fd, datamap, total_num * sizeof(dcs_datamap_t));
            close(fd);

            for(i=0; i<DCS_MASTER_NUM; i++){
                tmp_sha_v[i] = NULL;
                sha_num[i] = 0;
            }

            for(i=0; i<DCS_COMPRESSOR_NUM; i++){
                tmp_csb_v[i] = NULL;
                csb_num[i] = 0;
            }

            for(i=0; i<DCS_MASTER_NUM; i++){
                tmp_sha_v[i] = (sha_bf_t *)malloc(sizeof(sha_bf_t)*total_num);
                if(tmp_sha_v[i] == NULL){
                    DCS_ERROR("dir_read_sha_bf_upload malloc for tmp_sha_v array err:%d \n", errno);
                    rc = errno;
                    goto EXIT;
                }
                memset(tmp_sha_v[i], 0, (sizeof(sha_bf_t)*total_num));
            }

            for(i=0; i<DCS_COMPRESSOR_NUM; i++){
                tmp_csb_v[i] = (conid_sha_block_t *)malloc(sizeof(conid_sha_block_t)*total_num);
                if(tmp_csb_v[i] == NULL){
                    DCS_ERROR("dir_read_sha_bf_upload malloc for tmp_csb_v array err:%d \n", errno);
                    rc = errno;
                    goto EXIT;
                }
                memset(tmp_csb_v[i], 0, (sizeof(conid_sha_block_t)*total_num));
            }

            for(i = 0; i< total_num; i++){
                dcs_s32_t pos = 0;
                dcs_s32_t sign = 0;
                sign = datamap[i].sha[0] & ((1 << power) - 1);
                pos = sha_num[sign];
                memcpy(tmp_sha_v[sign][pos].sha, datamap[i].sha, SHA_LEN);
                tmp_sha_v[sign][pos].bf_id = datamap[i].compressor_id - 1;
                sha_num[sign]++;

                sign = datamap[i].compressor_id - 1;
                pos = csb_num[sign];
                memcpy(tmp_csb_v[sign][pos].sha, datamap[i].sha, SHA_LEN);
                tmp_csb_v[sign][pos].con_id = datamap[i].container_id;
                csb_num[sign]++;
            }

            for(i=0; i<DCS_MASTER_NUM; i++){
                dcs_s32_t block_num;
                dcs_s32_t blocks_num;
                dcs_s32_t j = 0;
                sha_bf_t * tmp = NULL;
                if(!sha_num[i]){
                    continue;
                }

                block_num = SUPER_CHUNK_SIZE / sizeof(sha_bf_t);
                blocks_num = sha_num[i] / block_num + ((sha_num[i] % block_num > 0) ? 1 : 0);
                for(j = 0; j < blocks_num; j++){
                    dcs_s32_t start = j *block_num;
                    dcs_s32_t end = (j + 1)* block_num;
                    dcs_s32_t blocks = 0;
                    if(end > sha_num[i]){
                        end = sha_num[i];
                    }
                    blocks = end - start;

                    rc = __amp_alloc_request(&req2m);
                    if(rc < 0){
                        DCS_ERROR("dir_read_sha_bf_upload alloc for %dth req err:%d \n", i, errno);
                        rc = errno;
                        goto EXIT;
                    }

                    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
                    reqmsgp2m = (amp_message_t *)malloc(size);
                    if(!reqmsgp2m){
                        DCS_ERROR("dir_read_sha_bf_upload alloc for %dth reqmsgp err:%d \n", i, errno);
                        rc = errno;
                        goto EXIT;
                    }
                    memset(reqmsgp2m, 0, size);
                    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp2m + AMP_MESSAGE_HEADER_LEN);
                    msgp->size = size;
                    msgp->msg_type = is_req;
                    msgp->fromid = server_this_id;
                    msgp->fromtype = DCS_SERVER;
                    msgp->optype = DCS_UPLOAD;
                    msgp->u.s2m_upload_req.sha_num = blocks;
                    msgp->u.s2m_upload_req.scsize = blocks*sizeof(sha_bf_t);

                    req2m->req_iov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
                    if(req2m->req_iov == NULL){
                        DCS_ERROR("__dcs_write_server malloc for %dth err:%d \n", i, errno);
                        rc = errno;
                        goto EXIT;
                    }

                    DCS_MSG("__dcs_write_server init iov to send sha value \n");
                    tmp = (sha_bf_t *)malloc(blocks * sizeof(sha_bf_t));
                    if(tmp == NULL){
                        DCS_ERROR("dir_read_sha_bf_upload malloc for tmp err:%d \n",  errno);
                        rc = errno;
                        goto EXIT;
                    }
                    memset(tmp, 0, blocks * sizeof(sha_bf_t));
                    memcpy(tmp, (dcs_u8_t *)tmp_sha_v[i] + start * sizeof(sha_bf_t), blocks * sizeof(sha_bf_t));
                    req2m->req_iov->ak_addr = tmp;
                    if(req2m->req_iov->ak_addr == NULL){
                        DCS_ERROR("__dcs_write_server ak_addr is null \n");
                    }

                    req2m->req_iov->ak_len  = blocks * sizeof(sha_bf_t);
                    if(req2m->req_iov->ak_len){
                        DCS_MSG("__dcs_write_server req2m[i]->req_iov->ak_len is %d \n", req2m->req_iov->ak_len);
                    }

                    req2m->req_iov->ak_offset = 0;
                    req2m->req_iov->ak_flag = 0;
                    req2m->req_niov = 1;
                    req2m->req_msg = reqmsgp2m;
                    req2m->req_msglen = size;
                    req2m->req_need_ack = 1;
                    req2m->req_resent = 1;
                    req2m->req_type = AMP_REQUEST | AMP_DATA;
                    DCS_MSG("send to %dth master \n", i+1);
                    rc = amp_send_sync(server_comp_context, 
                                       req2m, 
                                       DCS_MASTER, 
                                       (i+1), 
                                       0);
                    DCS_MSG("after send %dth master msg\n",i+1);
                    if(rc < 0){
                        DCS_ERROR("__dcs_write_server send req to master err:%d \n",rc);
                        goto EXIT;
                    }
                    DCS_MSG("before get reply msg \n");
                    repmsgp2m = req2m->req_reply;
                    if(!repmsgp2m){
                        DCS_ERROR("__dcs_write_server recieve reply %dth  msg err:%d \n", i, errno );
                        rc = -1;
                        goto EXIT;
                    }

                    DCS_MSG("after get reply msg \n");
                    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp2m+ AMP_MESSAGE_HEADER_LEN);
                    if(msgp->ack == 0){
                        DCS_ERROR("__dcs_write_server server fail to query the %dth master \n",(i+1));
                        rc = -1;
                        goto EXIT;
                    }
                    if(NULL != req2m){
                        if(NULL != req2m->req_iov){
                            free(req2m->req_iov);
                            req2m->req_iov = NULL;
                        }
                        __amp_free_request(req2m);
                        req2m = NULL;
                    }
                    if(NULL != reqmsgp2m){
                        free(reqmsgp2m);
                        reqmsgp2m = NULL;
                    }
                    if(NULL != repmsgp2m){
                        free(repmsgp2m);
                        repmsgp2m = NULL;
                    }
                }
            }

           
            for(i=0; i<DCS_COMPRESSOR_NUM; i++){
                dcs_s32_t block_num;
                dcs_s32_t blocks_num;
                dcs_s32_t j = 0;
                conid_sha_block_t * tmp = NULL;
                if(!csb_num[i]){
                    continue;
                }
                
                block_num = SUPER_CHUNK_SIZE / sizeof(conid_sha_block_t);
                blocks_num = csb_num[i] / block_num + ((csb_num[i] % block_num > 0) ? 1 : 0);
                for(j = 0; j < blocks_num; j++){
                    dcs_s32_t start = j *block_num;
                    dcs_s32_t end = (j + 1)* block_num;
                    dcs_s32_t blocks = 0;
                    if(end > csb_num[i]){
                        end = csb_num[i];
                    }
                    blocks = end - start;
                    
                    rc = __amp_alloc_request(&req2d);
                    if(rc < 0){
                        DCS_ERROR("dir_read_sha_bf_upload alloc for %dth req err:%d \n", i, errno);
                        rc = errno;
                        goto EXIT;
                    }

                    size = AMP_MESSAGE_HEADER_LEN + sizeof(dcs_msg_t);
                    reqmsgp2d = (amp_message_t *)malloc(size);
                    if(!reqmsgp2d){
                        DCS_ERROR("dir_read_sha_bf_upload alloc for %dth reqmsgp err:%d \n", i, errno);
                        rc = errno;
                        goto EXIT;
                    }

                    memset(reqmsgp2d, 0, size);
                    msgp = (dcs_msg_t *)((dcs_s8_t *)reqmsgp2d + AMP_MESSAGE_HEADER_LEN);
                    msgp->size = size;
                    msgp->msg_type = is_req;
                    msgp->fromid = server_this_id;
                    msgp->fromtype = DCS_SERVER;
                    msgp->optype = DCS_UPLOAD;

                    msgp->u.s2d_upload_req.sha_num = blocks;
                    msgp->u.s2d_upload_req.scsize = blocks*sizeof(conid_sha_block_t);

                    tmp = (conid_sha_block_t *)malloc(blocks * sizeof(conid_sha_block_t));
                    if(tmp == NULL){
                        DCS_ERROR("__dcs_write_server malloc for send buf err:%d \n", errno);
                        rc = errno;
                        goto EXIT;
                    }

                    memset(tmp, 0, blocks * sizeof(conid_sha_block_t));
                    memcpy(tmp, (dcs_u8_t *)tmp_csb_v[i] + start * sizeof(conid_sha_block_t) , blocks * sizeof(conid_sha_block_t));

                    req2d->req_iov = (amp_kiov_t *)malloc(sizeof(amp_kiov_t));
                    if(req2d->req_iov == NULL){
                        DCS_ERROR("__dcs_write_server malloc for %dth err:%d \n", i, errno);
                        rc = errno;
                        goto EXIT;
                    }

                    DCS_MSG("__dcs_write_server init iov to send sha value \n");

                    req2d->req_iov->ak_addr = tmp;
                    if(req2d->req_iov->ak_addr == NULL){
                        DCS_ERROR("__dcs_write_server ak_addr is null \n");
                    }

                    req2d->req_iov->ak_len  = blocks*sizeof(conid_sha_block_t);
                    if(req2d->req_iov->ak_len){
                        DCS_MSG("__dcs_write_server req2d->req_iov->ak_len is %d \n", req2d->req_iov->ak_len);
                    }

                    req2d->req_iov->ak_offset = 0;
                    req2d->req_iov->ak_flag = 0;
                    req2d->req_niov = 1;
                    req2d->req_msg = reqmsgp2d;
                    req2d->req_msglen = size;
                    req2d->req_need_ack = 1;
                    req2d->req_resent = 1;
                    req2d->req_type = AMP_REQUEST | AMP_DATA;
                
                    DCS_MSG("send to %dth compressor \n", i+1);
                    rc = amp_send_sync(server_comp_context, 
                                       req2d, 
                                       DCS_NODE, 
                                       (i+1), 
                                       0);
                    DCS_MSG("after send %dth compressor msg\n",i+1);
                    if(rc < 0){
                        DCS_ERROR("__dcs_write_server send req to compressor err:%d \n",rc);
                        goto EXIT;
                    }
                    DCS_MSG("before get reply msg \n");
                    repmsgp2d = req2d->req_reply;
                    if(!repmsgp2d){
                        DCS_ERROR("__dcs_write_server recieve reply %dth  msg err:%d \n", i, errno );
                        rc = -1;
                        goto EXIT;
                    }

                    DCS_MSG("after get reply msg \n");
                    msgp = (dcs_msg_t *)((dcs_s8_t *)repmsgp2d+ AMP_MESSAGE_HEADER_LEN);
                    if(msgp->ack == 0){
                        DCS_ERROR("__dcs_write_server server fail to query the %dth master \n",(i+1));
                        rc = -1;
                        goto EXIT;
                    }
                    if(tmp != NULL){
                        free(tmp);
                        tmp = NULL;
                    }
                    if(NULL != req2d){
                        if(NULL != req2d->req_iov){
                            free(req2d->req_iov);
                            req2d->req_iov = NULL;
                        }
                        __amp_free_request(req2d);
                        req2d = NULL;
                    }
                    if(NULL != reqmsgp2d){
                        free(reqmsgp2d);
                        reqmsgp2d = NULL;
                    }
                    if(NULL != repmsgp2d){
                        free(repmsgp2d);
                        repmsgp2d = NULL;
                    }
                }
            }


            for(i = 0; i < DCS_MASTER_NUM; i++){
                if(NULL != tmp_sha_v[i]){
                    free(tmp_sha_v[i]);
                    tmp_sha_v[i] = NULL;
                }
            }

            for(i = 0; i < DCS_COMPRESSOR_NUM; i++){
                if(NULL != tmp_csb_v[i]){
                    free(tmp_csb_v[i]);
                    tmp_csb_v[i] = NULL;
                }
            }


            if(NULL != datamap){
                free(datamap);
                datamap = NULL;
            }
            if(NULL != sha_bf){
                free(sha_bf);
                sha_bf = NULL;
            }
        }
    }

EXIT:
    closedir(dp);

    if(NULL != filename){
        free(filename);
        filename = NULL;
    }
    return rc;
}

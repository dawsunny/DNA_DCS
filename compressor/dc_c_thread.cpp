#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <pthread.h>
#include <unistd.h>     //access, sleep
#include <sys/time.h>   //gettimeofday
#include <sys/stat.h>   //struct stat

#include "dc_type.h"
#include "dc_const.h"
#include "dc_debug.h"

#include "dc_c_global.h"   
#include "dc_c_alignment.h"   //call compress_input_file
#include "dc_c_thread.h"

//global variable
dc_thread_pool_t pool;


dc_s32_t
thread_pool_init()
{
    dc_s32_t rc = 0, i;

    pthread_mutex_init(&(pool.task_queue_lock),  NULL);
    pthread_cond_init( &(pool.task_queue_ready), NULL);

    pool.task_queue_head = NULL;
    pool.task_queue_size = 0;

    pool.thread_id = (pthread_t *) malloc(sizeof(pthread_t) * THREAD_NUM);
    if(pool.thread_id == NULL)
    {
        DC_ERROR("ERROR: thread_pool_init: threads create error\n");
        rc = -1;
        goto EXIT;
    }
    for(i = 0; i < THREAD_NUM; ++i) 
    {
        pthread_create(&(pool.thread_id[i]), NULL, thread_routine, NULL);  //create threads
    }

    pool.shutdown = 0;

EXIT:
	return rc;
}

//loop: get a task from task list, work on it, and free memory
void *
thread_routine(void *unused_arg)
{
    DC_PRINT("starting thread 0x%x\n", (dc_s32_t)pthread_self());

    struct timeval start_time, end_time;
    float  taken_time;

    while(1) 
    {
        pthread_mutex_lock(&(pool.task_queue_lock));

        //wait for a task
        while(pool.task_queue_size == 0 && !pool.shutdown) 
        {
            DC_PRINT("thread 0x%x is waiting\n", (dc_s32_t)pthread_self());
            pthread_cond_wait(&(pool.task_queue_ready), &(pool.task_queue_lock));
        }

        //check the switch, thread exit
        if(pool.shutdown) 
        {
            pthread_mutex_unlock(&(pool.task_queue_lock));
            DC_PRINT("thread 0x%x will exit\n", (dc_s32_t)pthread_self());
            pthread_exit(NULL);
        }

        DC_PRINT("thread 0x%x is starting to work\n", (dc_s32_t)pthread_self());

        //get the first task in the task list
        dc_task_t *head_task = pool.task_queue_head;
        pool.task_queue_head = head_task->next;
        --pool.task_queue_size;

        pthread_mutex_unlock (&(pool.task_queue_lock));


        gettimeofday(&start_time, NULL);

        //work on the task
        //compress_input_file(head_task->input_path, head_task->output_name);

        gettimeofday(&end_time, NULL);
        taken_time = end_time.tv_sec - start_time.tv_sec + 1e-6 * (end_time.tv_usec - start_time.tv_usec);

        printf("compress file %s takes time: %.2fs\n", head_task->input_path, taken_time);
        fflush(stdout);

        head_task->next = NULL;
        free(head_task);
        head_task = NULL;
    }
    pthread_exit(NULL);
}

dc_s32_t
pool_add_task(dc_s8_t *input_path)
{
	dc_s32_t rc = 0;
	DC_PRINT("pool_add_task enter:\n");

	struct stat file_stat;
	stat(input_path, &file_stat);

	if( S_ISREG(file_stat.st_mode) )    //input a file path
	{
		dc_task_t *new_task = (dc_task_t *) malloc( sizeof(dc_task_t) );
		if( new_task == NULL )
		{
			DC_ERROR("ERROR!: pool_add_task: malloc for new_task error\n");
			rc = -1;
			goto EXIT;
		}
		new_task->next = NULL;

		dc_s32_t i, path_len = strlen(input_path), file_name_len;
		for(i = path_len - 1; i >= 0 && input_path[i] != '/'; --i)
			;
		file_name_len = path_len - 1 - i;

		strncpy( new_task->input_path,  input_path, path_len + 1);
		strncpy( new_task->output_name, input_path + i + 1, file_name_len );
		strncpy( new_task->output_name + file_name_len, "_out", 5);


		pthread_mutex_lock( &(pool.task_queue_lock) );

        //add task to list
		pool.task_queue_head = new_task;
		++pool.task_queue_size;

		pthread_mutex_unlock( &(pool.task_queue_lock) );
		pthread_cond_signal( &(pool.task_queue_ready) );
	}
	else if( S_ISDIR(file_stat.st_mode) )   //input a directory path
	{
		struct dirent *dirp = NULL; //local
		DIR           *dp   = NULL;

		if( (dp = opendir(input_path)) == NULL ) 
		{
			DC_ERROR("ERROR!: pool_add_task: open dir error\n");
			rc = -1;
			goto EXIT;
		}

		dc_s32_t path_len = strlen(input_path), file_name_len;

		while( (dirp = readdir(dp)) != NULL ) 
		{
			if( strcmp(dirp->d_name, ".") == 0 || strcmp(dirp->d_name, "..") == 0 ) 
			{
				continue;
			}

            dc_s32_t   i, output_name_len = 0;

			dc_task_t *new_task = (dc_task_t *) malloc( sizeof(dc_task_t) );
			if( new_task == NULL )
			{
				DC_ERROR("ERROR!: pool_add_task: malloc for new_task error\n");
				rc = -1;
				goto EXIT;
			}
			new_task->next = NULL;

			file_name_len = strlen(dirp->d_name);   //input file name
			strncpy( new_task->input_path, input_path, path_len );
			strncpy( new_task->input_path + path_len, dirp->d_name, file_name_len + 1 );  //input file full path


            strncpy( new_task->output_name + output_name_len, "/tmp/dna_compress/out/", 22); 
            output_name_len += 22;
			strncpy( new_task->output_name + output_name_len, dirp->d_name, file_name_len);  //output file name
            output_name_len += file_name_len;

            for(i = path_len - 2; i >= 0 && input_path[i] != '/'; --i)
                ;
            strncpy( new_task->output_name + output_name_len, input_path + i, path_len - 1 - i );
            output_name_len += path_len - 1 - i;
            new_task->output_name[output_name_len] = '\0';


			pthread_mutex_lock( &(pool.task_queue_lock) );

			dc_task_t *traverse = pool.task_queue_head;
			if( traverse != NULL )  //list not empty, add to list tail
			{
				while( traverse->next != NULL ) 
				{
					traverse = traverse->next;
				}
				traverse->next = new_task;
			}
			else 
			{
				pool.task_queue_head = new_task;
			}
			++pool.task_queue_size;

			pthread_mutex_unlock( &(pool.task_queue_lock) );
			pthread_cond_signal( &(pool.task_queue_ready) );
		}
		closedir(dp);
		dp = NULL;
	}
	else
	{
		DC_ERROR("ERROR!: input_path is illegal\n");
		rc = -1;
		goto EXIT;
	}

EXIT:
	DC_PRINT("pool_add_task enter:\n");
	return rc;
}

void
thread_pool_destroy()
{
    if(pool.thread_id == NULL) //not init
        return;

    while(1)    //wait, ensure all task be worked
    {
        pthread_mutex_lock( &(pool.task_queue_lock) );
        if( pool.task_queue_size == 0 ) 
        {
            pthread_mutex_unlock( &(pool.task_queue_lock) );
            break;
        }
        pthread_mutex_unlock( &(pool.task_queue_lock) );

        sleep(2);
    }

    //thread switch off, inform all threads to exit
    pool.shutdown = 1;
    pthread_cond_broadcast( &(pool.task_queue_ready) );

    dc_u32_t i;
    for(i = 0; i < THREAD_NUM; ++i) 
    {
        pthread_join(pool.thread_id[i], NULL);
    }

    //assert
    if( pool.task_queue_size != 0 )
    {
        DC_ERROR("ERROR!: task_list not empty\n");
    }

    free(pool.thread_id);
    pool.thread_id = NULL;
    pthread_mutex_destroy(&(pool.task_queue_lock));
    pthread_cond_destroy(&(pool.task_queue_ready));
}

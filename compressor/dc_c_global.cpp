/*
 * usage:
 *		./dc_compress ref_file_path input_file_path/input_dir_path
 */

#include <stdio.h>
#include <sys/time.h>

#include "dc_type.h"
#include "dc_debug.h"

#include "dc_c_io.h"
#include "dc_c_thread.h"

#include <dc_c_global.h>

void
print_time(char *explain, struct timeval start_time, struct timeval end_time)
{
    float taken_time;

    taken_time = end_time.tv_sec - start_time.tv_sec +
        1e-6 * (end_time.tv_usec - start_time.tv_usec);

    printf("%s take time: %.2fs\n", explain, taken_time);
    fflush(stdout);
}

dc_s32_t
dc_c_main( dc_s32_t argc, dc_s8_t *argv[] )
{
	dc_s32_t rc = 0;
    struct timeval start_time, end_time;
    struct timeval glo_start_time, glo_end_time;

    printf("Compression begin, please wait...\n");
    fflush(stdout);

    gettimeofday( &glo_start_time, NULL );

	rc = dc_c_check_arg( argc, argv );   //check whether file/dir exist
	if( rc )
	{
		DC_ERROR("error: dc_c_check_arg return error\n");
		goto EXIT;
	}

    gettimeofday( &start_time, NULL );
	rc = dc_c_read_ref_file( argv[argc - 2] );   //read sequences to string array
	if( rc )
	{
		DC_ERROR("error: dc_c_read_ref_file return error\n");
		goto EXIT;
	}
    gettimeofday( &end_time, NULL );
    print_time("dc_c_read_ref_file", start_time, end_time);

    gettimeofday( &start_time, NULL );
	rc = save_seed_loc();    //save 10-mer locations to struct array
	if( rc )
	{
		DC_ERROR("error: save_seed_loc return error\n");
		goto EXIT;
	}
    gettimeofday( &end_time, NULL );
    print_time("save_seed_loc", start_time, end_time);

	rc = thread_pool_init();   //create threads and wait for task
	if( rc )
	{
		DC_ERROR("error: thread_pool_init return error\n");
		goto EXIT;
	}

	rc = pool_add_task(argv[argc - 1]);   //add tasks to task list
	if( rc )
	{
		DC_ERROR("error: pool_add_task return error\n");
		goto EXIT;
	}

EXIT:
	thread_pool_destroy();  //destroy threads
	dc_c_free_memory();      //free the allocated memory

//    tar_compressed_files();  //tar -cjf /tmp/dna_compress/out/...

    gettimeofday( &glo_end_time, NULL );
    print_time("main", glo_start_time, glo_end_time);

    printf("Compression end.\n");
    fflush(stdout);
	return rc;
}

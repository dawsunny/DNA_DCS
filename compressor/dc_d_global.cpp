/*
 * usage:
 *		./dc_compress ref_file_path input_file_path/input_dir_path
 */

#include <stdio.h>
#include <sys/time.h>

#include "dc_type.h"
#include "dc_debug.h"

#include "dc_d_io.h"
#include "dc_d_decompress.h"

void
print_time(const char *explain, struct timeval start_time, struct timeval end_time)
{
    float taken_time;

    taken_time = end_time.tv_sec - start_time.tv_sec +
        1e-6 * (end_time.tv_usec - start_time.tv_usec);

    printf("%s take time: %.2fs\n", explain, taken_time);
    fflush(stdout);
}

dc_s32_t
dc_d_main( dc_s32_t argc, dc_s8_t *argv[] )
{
	dc_s32_t rc = 0;
    struct timeval start_time, end_time;
    struct timeval glo_start_time, glo_end_time;

    gettimeofday( &glo_start_time, NULL );

    printf("Decompression begin, please wait...\n");
    fflush(stdout);

	rc = dc_d_check_arg( argc, argv );
	if( rc )
	{
		DC_ERROR("error: dc_d_check_arg return error\n");
		goto EXIT;
	}

    gettimeofday( &start_time, NULL );
	rc = dc_d_read_ref_file( argv[argc - 2] );     //read reference sequences from file
	if( rc )
	{
		DC_ERROR("error: dc_d_read_ref_file return error\n");
		goto EXIT;
	}
    gettimeofday( &end_time, NULL );
    print_time("dc_d_read_ref_file", start_time, end_time);


    gettimeofday( &start_time, NULL );
	rc = analyze_path( argv[argc - 1] );   //analyze input path: dir or path
	if( rc )
	{
		DC_ERROR("error: analyze_path return error\n");
		goto EXIT;
	}
    gettimeofday( &end_time, NULL );
    print_time("analyze_path", start_time, end_time);

EXIT:
	dc_d_free_memory();  //free the allocated memory

    gettimeofday( &glo_end_time, NULL );
    print_time("main", glo_start_time, glo_end_time);

    printf("Decompression end.\n");
    fflush(stdout);
	return rc;
}

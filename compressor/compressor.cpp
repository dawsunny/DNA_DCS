/*compressor main funtion*/

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

//#include <stropts.h>
#include <stdint.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <string.h>
#include <semaphore.h>

#include "dc_global.h"
#include "dc_io.h"

#include <map>
#include <string>
#include <iostream>
using namespace std;

dcs_s32_t main(dcs_s32_t argc, dcs_s8_t **argv)
{
    dcs_s32_t rc = 0;
    
    struct timeval start_time, end_time;
    struct timeval glo_start_time, glo_end_time;
    
    map<string, compressor_hash_t>::iterator it;
    map<dcs_u64_t, string>::iterator it1;

    DCS_ENTER("compressor enter \n");

    /*parse paramatter*/
    rc = __dcs_compressor_parse_paramatter(argc, argv);
    if(rc != 0){
        DCS_ERROR("main __dcs_compressor_parse_paramatter err \n");
        goto EXIT;
    }

    rc = __compressor_com_init();
    if(rc != 0){
        DCS_ERROR("main __dcs_compressor_init_com err \n");
        goto EXIT;
    }

    //rc = __dcs_compressor_bloom_init();
#ifdef __DCS_TIER_BLOOM__
    //rc = __dcs_tier_bloom_init();
    if(rc != 0){
        DCS_ERROR("main __dcs_tier_bloom_init\n");
        goto EXIT;
    }
#endif

    rc = container_list_init();
    if(rc != 0){
        DCS_ERROR("main container_list_init \n");
        goto EXIT;
    }

    rc = cache_table_init();
    if(rc != 0){
        DCS_ERROR("main cache_table_init \n");
        goto EXIT;
    }

    rc = data_cache_init();
    if(rc != 0){
        DCS_ERROR("main data_cache_init err \n");
        goto  EXIT;
    }

    rc = container_table_init();
    if(rc != 0){
        DCS_ERROR("main container_table_init \n");
        goto EXIT;
    }

    rc = __dcs_compressor_index_init();
    if(rc != 0){
        DCS_ERROR("main __dcs_compressor_index_init");
        goto EXIT;
    }
    
    
    printf("Read ref begin, please wait...\n");
    fflush(stdout);
    gettimeofday( &start_time, NULL );
    rc = dc_c_read_ref_file( FASTA_REF_PATH );   //read sequences to string array
    if( rc )
    {
        DCS_ERROR("error: dc_c_read_ref_file return error\n");
        goto EXIT;
    }
    gettimeofday( &end_time, NULL );
    print_time("dc_c_read_ref_file", start_time, end_time);
    
    gettimeofday( &start_time, NULL );
    rc = save_seed_loc();    //save 10-mer locations to struct array
    if( rc )
    {
        DCS_ERROR("error: save_seed_loc return error\n");
        goto EXIT;
    }
    gettimeofday( &end_time, NULL );
    print_time("save_seed_loc", start_time, end_time);
    
    printf("Read FASTA map info, please wait...\n");
    if (access(FASTA_MAP_PATH, 0) != 0) {
        printf("no FASTA map info.\n");
    } else {
        pthread_mutex_lock(&compressor_location_fa_lock);
        rc = do_read_map(compressor_location_fa, DCS_FILETYPE_FASTA);
        //compressor_location_fa_cnt = compressor_location_fa.size();
        pthread_mutex_unlock(&compressor_location_fa_lock);
        if (rc != 0) {
            DCS_ERROR("read fasta map info error\n");
            goto EXIT;
        } else {
            printf("done.\n");
        }
    }
    printf("Read FASTQ map info, please wait...\n");
    if (access(FASTQ_MAP_PATH, 0) != 0) {
        printf("no FASTQ map info.\n");
    } else {
        pthread_mutex_lock(&compressor_location_fq_lock);
        rc = do_read_map(compressor_location_fq, DCS_FILETYPE_FASTQ);
        //compressor_location_fq_cnt = compressor_location_fq.size();
       // compressor_location_fq_cnt_local = 0;
        pthread_mutex_unlock(&compressor_location_fq_lock);
        if (rc != 0) {
            DCS_ERROR("read fastq map info error\n");
            goto EXIT;
        } else {
            printf("done.\n");
        }
    }
    
    printf("----------------------\n");
    printf("fasta map info\n");
    for (it = compressor_location_fa.begin(); it != compressor_location_fa.end(); ++it) {
        cout << it->first << endl;
        cout << "\t" << it->second.chunk_num << endl;
        cout << "\t" << it->second.location << endl;
        for (it1 = it->second.off_loc.begin(); it1 != it->second.off_loc.end(); ++it1) {
            cout << "\t\t" << it1->first << "\t" << it1->second << endl;
        }
    }
    
    printf("----------------------\n");
    printf("fastq map info\n");
    for (it = compressor_location_fq.begin(); it != compressor_location_fq.end(); ++it) {
        cout << it->first << endl;
        cout << "\t" << it->second.chunk_num << endl;
        cout << "\t" << it->second.location << endl;
        for (it1 = it->second.off_loc.begin(); it1 != it->second.off_loc.end(); ++it1) {
            cout << "\t\t" << it1->first << "\t" << it1->second << endl;
        }
    }
    
    rc = __dcs_create_compressor_thread();
    if(rc != 0){
        DCS_ERROR("main __dcs_create_compressor_thread \n");
        goto EXIT;
    }
    printf("Ready.\n");
    
    while(1){
        sleep(30);
    }

EXIT:
    DCS_LEAVE("main leave \n");
    dc_c_free_memory();
    return rc;
}

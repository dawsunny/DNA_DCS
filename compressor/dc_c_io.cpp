#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>    //access
#include <dirent.h>    //DIR, struct dirent
#include <sys/stat.h>

#include "dc_type.h"
#include "dc_const.h"
#include "dc_debug.h"

#include "dc_c_encode.h"
#include "dc_c_io.h"

//global variables
dc_s8_t  **dc_c_ref_seqs_g;
dc_s32_t  *dc_c_ref_seqs_len_g;

dc_seed_loc_t *seed_locs_g[MEGA];
dc_s32_t       seed_locs_freq_g[MEGA];

dc_s32_t   dc_c_ref_seq_total_no_g;
dc_s32_t   max_seed_freq_g;

int dc_c_DIF_RATE;
int dc_c_OVERLAP;
int MAX_DP_LEN;
int THREAD_NUM;

void
dc_c_print_usage()
{
    printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
    printf("Usage:\n");
    printf("./dc_compress [-m mutation_rate] [-n thread_num] ref_file_path input_file|dir_path/ \n");
    printf("\n");
    printf("Arguments:\n");
    printf("1 - mutation_rate, 0.01 default\n");
    printf("2 - thread_num, 1 default\n");
    printf("3 - ref_file_path\n");
    printf("4 - input_file|dir_path/ \n");
    printf("\n");
    printf("Example usage:\n");
    printf("./dc_compress ref_file_path input_file\n");
    printf("./dc_compress -n 3 ref_file_path input_dir/ \n");
    printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
}

dc_s32_t 
dc_c_check_arg( dc_s32_t argc, dc_s8_t *argv[] )
{
    dc_s32_t rc = 0;
    DC_PRINT("dc_c_check_arg enter:\n");

    THREAD_NUM = 1;
    dc_c_DIF_RATE = 0.01;

    if( argc != 3 && argc != 5 && argc != 7 ) 
    {   
        DC_ERROR("ERROR!: dc_c_check_arg: input arg error\n");
        dc_c_print_usage();
        rc = -1; 
        goto EXIT;
    }   

    if( access(argv[argc - 2], 0) != 0 ) 
    {   
        DC_ERROR("ERROR!: dc_c_check_arg: ref file not exsits\n");
        rc = -1; 
        goto EXIT;
    }   

    if( access(argv[argc - 1], 0) != 0 ) 
    {   
        DC_ERROR("ERROR!: dc_c_check_arg: input file/dir not exsits\n");
        rc = -1; 
        goto EXIT;
    }   

    char opt;
    while( (opt = getopt(argc, argv, "m:n:")) != EOF )
    {
        switch(opt)
        {
            case '?':
                DC_ERROR("ERROR!: dc_c_check_arg: unrecognized option: -%c\n", optopt);
                dc_c_print_usage();
                rc = -1;
                goto EXIT;
            case 'm':
                dc_c_DIF_RATE = atof(optarg);
                break;
            case 'n':
                THREAD_NUM = atoi(optarg);
                break;
        }
    }

    dc_c_OVERLAP = INPUT_CHUNK + (int)(INPUT_CHUNK * dc_c_DIF_RATE);
    MAX_DP_LEN = 12 + (int)(30 * dc_c_DIF_RATE);
    MAX_DP_LEN = MIN(MAX_DP_LEN, 25);

EXIT:
    DC_PRINT("dc_c_check_arg leave\n");
    return rc; 
}

//read reference file, allocate a string array, and fill it 
dc_s32_t 
dc_c_read_ref_file(dc_s8_t *ref_file_path)
{
	dc_s32_t  rc = 0;
	DC_PRINT("dc_c_read_ref_file enter:\n");

	dc_s32_t  i, j, tmp_len, ref_seq_no = 0;
	dc_s8_t line_buf[LINE_BUF_LEN];

    dc_s64_t ref_file_total_base;  

	//file total size
	struct stat file_stat;
	stat( ref_file_path, &file_stat );

	FILE *fin_ref = fopen(ref_file_path, "r");
	if( fin_ref == NULL ) 
    {
		DC_PRINT("error: dc_c_read_ref_file: open file error\n");
		rc = -1;
		goto EXIT;
	}

    //global varibles assignment
	dc_c_ref_seq_total_no_g = (file_stat.st_size + REF_CHUNK - 1) / REF_CHUNK;

	dc_c_ref_seqs_g = (dc_s8_t **) malloc( sizeof(dc_s8_t *) * dc_c_ref_seq_total_no_g );
	if( dc_c_ref_seqs_g == NULL )
	{
		DC_ERROR("ERROR!: dc_c_read_ref_file: malloc for dc_c_ref_seqs_g error\n");
		rc = -1;
		goto EXIT;
	}
	dc_c_ref_seqs_len_g = (dc_s32_t *) malloc( sizeof(dc_s32_t) * dc_c_ref_seq_total_no_g );
	if( dc_c_ref_seqs_len_g == NULL )
	{
		DC_ERROR("ERROR!: dc_c_read_ref_file: malloc for dc_c_ref_seqs_len_g error\n");
		rc = -1;
		goto EXIT;
	}

	//initialize
	for(i = 0; i < dc_c_ref_seq_total_no_g; ++i) 
    {
		dc_c_ref_seqs_g[i] = NULL;
		dc_c_ref_seqs_len_g[i]  = 0;
	}

	//allocate            
	for(i = 0; i < dc_c_ref_seq_total_no_g; ++i) 
    {
		dc_c_ref_seqs_g[i] = (dc_s8_t *) malloc( sizeof(dc_s8_t) * (REF_CHUNK + dc_c_OVERLAP) );
		if( dc_c_ref_seqs_g[i] == NULL ) 
        {
			DC_ERROR("ERROR!: dc_c_read_ref_file: seq %d: malloc error\n", i);
			rc = -1;
			goto EXIT;
		}
	}

	//fgets(line_buf, LINE_BUF_LEN, fin_ref);   //head line, by zhj,multi-mark

	while( fgets(line_buf, LINE_BUF_LEN, fin_ref) != NULL ) 
    {
        if(line_buf[0] == '>')  //******** multi head
        {
            continue;
        }

        for(j = 0; line_buf[j] != '\n' && line_buf[j] != 'N'; ++j)  //find if have N base
            ;
        if(line_buf[j] == 'N')  //meet N
        {
            i = j++;
            while(line_buf[j] != '\n')
            {
                if(line_buf[j] != 'N')
                    line_buf[i++] = line_buf[j];
                ++j;
            }
            tmp_len = i;
        }
        else //no N
        {
            tmp_len = j;
        }

        if(tmp_len == 0)
            continue;

		strncpy(dc_c_ref_seqs_g[ref_seq_no] + dc_c_ref_seqs_len_g[ref_seq_no], line_buf, tmp_len);
		dc_c_ref_seqs_len_g[ref_seq_no] += tmp_len;

		if( dc_c_ref_seqs_len_g[ref_seq_no] > REF_CHUNK )  //each fragment has REF_CHUNK elements
        {
			tmp_len = dc_c_ref_seqs_len_g[ref_seq_no] - REF_CHUNK;
			strncpy(dc_c_ref_seqs_g[ref_seq_no + 1], dc_c_ref_seqs_g[ref_seq_no] + REF_CHUNK, tmp_len);
			dc_c_ref_seqs_len_g[ref_seq_no]      = REF_CHUNK;
			dc_c_ref_seqs_len_g[ref_seq_no + 1] += tmp_len;

			++ref_seq_no;
		}
	}
	fclose(fin_ref);

    for(i = ref_seq_no + 1; i < dc_c_ref_seq_total_no_g; ++i)
    {
        if(dc_c_ref_seqs_g[i] != NULL)
        {
            free(dc_c_ref_seqs_g[i]);
            dc_c_ref_seqs_g[i] = NULL;
        }
    }

    dc_c_ref_seq_total_no_g  = ref_seq_no + 1;
	ref_file_total_base = (dc_s64_t)ref_seq_no * REF_CHUNK + dc_c_ref_seqs_len_g[ref_seq_no];  //cast, or will exceed 
	max_seed_freq_g     = MIN( (ref_file_total_base >> (2 * SEED_LEN)) + 2, MAX_SEED_FREQ );//tag by weizheng

    //fill the overlap
    for(i = 0; i <= dc_c_ref_seq_total_no_g - 3; ++i) 
    {
        strncpy(dc_c_ref_seqs_g[i] + REF_CHUNK, dc_c_ref_seqs_g[i + 1], dc_c_OVERLAP);
        dc_c_ref_seqs_len_g[i] += dc_c_OVERLAP;
    }
    strncpy(dc_c_ref_seqs_g[i] + dc_c_ref_seqs_len_g[i], dc_c_ref_seqs_g[i + 1], MIN(dc_c_OVERLAP, dc_c_ref_seqs_len_g[i + 1]));  //i = refFragTotNo - 2
    dc_c_ref_seqs_len_g[i] += MIN(dc_c_OVERLAP, dc_c_ref_seqs_len_g[i + 1]);

EXIT:
	DC_PRINT("dc_c_read_ref_file leave\n");
	return rc;
}

//save the locations of 10-mer in the reference sequences
dc_s32_t 
save_seed_loc()
{
    dc_s32_t rc = 0;
    DC_PRINT("save_seed_loc enter:\n");

    dc_s32_t seq_i = 0; //for reference seqs
    dc_u32_t base_code = 0; 
    dc_u32_t seed = 0;  //for seed loc array
    dc_u32_t seed_mask = MEGA - 1;

	dc_s8_t  *seq = NULL;
    dc_s32_t i, j, tmp, len; 
    dc_s32_t seed_loc_size;

    for(i = 0; i < MEGA; ++i)
    {
        seed_loc_size = max_seed_freq_g * sizeof(dc_seed_loc_t);
        seed_locs_g[i] = (dc_seed_loc_t *) malloc(seed_loc_size);
        if(seed_locs_g[i] == NULL)
        {
            DC_ERROR("error: save_seed_loc: malloc for seed array [%d] error, malloc size %d\n", i, seed_loc_size);
            rc = -1;
            goto EXIT;
        }
    }

    for(seq_i = 0; seq_i < dc_c_ref_seq_total_no_g; ++seq_i) 
    {
        seq = dc_c_ref_seqs_g[seq_i];
        len = ( seq_i == dc_c_ref_seq_total_no_g - 1 ? dc_c_ref_seqs_len_g[seq_i] : REF_CHUNK );

        if( len < SEED_LEN )  //last line len
        {
            goto EXIT;
        }

#if SAMPLE_RATE >= SEED_LEN    
//no overlap between two adjacent seed
        for(i = 0; i <= len - SEED_LEN; i += SAMPLE_RATE) 
        {
            //converte 10-mer into int, for array index
			for(j = 0; j < SEED_LEN; ++j)
			{
                base_code = base_to_uint( seq[i + j] );
				seed <<= 2;
				seed  |= base_code;
			}
			seed &= seed_mask;

            //only save max_seed_freq_g locations for each seed 
            if( seed_locs_freq_g[seed] < max_seed_freq_g ) 
            {
                tmp = seed_locs_freq_g[seed];
                seed_locs_g[seed][tmp].ref_seq_no = seq_i;
                seed_locs_g[seed][tmp].seed_stop  = i + SEED_LEN - 1;
                ++seed_locs_freq_g[seed];
            } 
        }
#else
//overlap in adjacent 10-mer, avoid double counting
        for(i = 0; i <= len - SEED_LEN; i += SAMPLE_RATE) 
        {
            for(j = 0; j < SEED_LEN - SAMPLE_RATE; ++j)
            {
                base_code = base_to_uint( seq[i + j] );
                seed <<= 2;
                seed  |= base_code;
            }

            for( ; i <= len - SEED_LEN; i += SAMPLE_RATE) 
            {
                for(j = SEED_LEN - SAMPLE_RATE; j < SEED_LEN; ++j)
                {
                    base_code = base_to_uint( seq[i + j] );
                    seed <<= 2;
                    seed  |= base_code;
                }
                seed &= seed_mask;

                if( seed_locs_freq_g[seed] < max_seed_freq_g ) 
                {
                    tmp = seed_locs_freq_g[seed];
                    seed_locs_g[seed][tmp].ref_seq_no = seq_i;
                    seed_locs_g[seed][tmp].seed_stop  = i + SEED_LEN - 1;
                    ++seed_locs_freq_g[seed];
                }
            }
        }
#endif
    }//for seq_i 

EXIT:
    DC_PRINT("save_seed_loc leave\n");
    return rc;
}

void
dc_c_free_memory()
{
	DC_PRINT("dc_c_free_memory enter:\n");

	dc_s32_t i;

	if(dc_c_ref_seqs_g != NULL)
	{
        for(i = 0; i < dc_c_ref_seq_total_no_g; ++i)
        {
            if(dc_c_ref_seqs_g[i] != NULL)
            {
                free(dc_c_ref_seqs_g[i]);
                dc_c_ref_seqs_g[i] = NULL;
            }
        }

		free(dc_c_ref_seqs_g);
		dc_c_ref_seqs_g = NULL;
	}

	if(dc_c_ref_seqs_len_g != NULL)
	{
		free(dc_c_ref_seqs_len_g);
		dc_c_ref_seqs_len_g = NULL;
	}

	for(i = 0; i < MEGA; ++i)
	{
		if(seed_locs_g[i] != NULL)
		{
			free(seed_locs_g[i]);
			seed_locs_g[i] = NULL;
		}
	}

	DC_PRINT("dc_c_free_memory leave\n");
}

void
write_link(dc_s32_t ref_seq_no, dc_s32_t ref_start, dc_s32_t ref_len,
           dc_s32_t inp_seq_no, dc_s32_t inp_len,   FILE *fout)   
{
    dc_link_t link_info;

    link_info.ref_seq_no = ref_seq_no;
    link_info.ref_start  = ref_start;
    link_info.ref_len    = ref_len;

    link_info.inp_seq_no = inp_seq_no;
    link_info.inp_len    = inp_len;

    fwrite(&link_info, sizeof(dc_link_t), 1, fout);
}

//encode difference string and write to file
void
write_dstr(dc_s8_t *dstr, dc_s32_t dstr_len, dc_u8_t *dstr_encode, FILE *fout)   
{
    DC_PRINT("save_dstr enter:\n");
    dc_s32_t i, encode_len;

    if( dstr_len % 2 )
    {
        dstr[dstr_len++] = 'N';
        dstr[dstr_len] = '\0';
    }
    encode_len = dstr_len / 2;

    for( i = 0; i < encode_len; ++i )
    {
        dstr_encode[i] = ( char_to_halfByte(dstr[2 * i]) << 4 ) |
                           char_to_halfByte(dstr[2 * i + 1]);
    }
    
    fwrite(&encode_len, sizeof(dc_s32_t), 1, fout);
    fwrite(dstr_encode, sizeof(dc_u8_t),  encode_len, fout);

    DC_PRINT("save_dstr leave\n");
}

void 
tar_compressed_files()
{
    DIR           *dp   = NULL;
    struct dirent *dirp = NULL;

    char out_dir_name[FILE_PATH_LEN] = "/tmp/dna_compress/out/";

    char out_tar_name[FILE_PATH_LEN];
    char cmd[2 * FILE_PATH_LEN];

    if( (dp = opendir("/tmp/dna_compress/out/")) == NULL )
    {
        DC_ERROR("Error: tar_compressed_files: open dir error.\n");
        return;
    }

    while( (dirp = readdir(dp)) != NULL )
    {
        if( strcmp(dirp->d_name, ".") == 0 || strcmp(dirp->d_name, "..") == 0 )
            continue;

        strcpy(out_dir_name + 22, dirp->d_name);

        strcpy(out_tar_name, out_dir_name);
        strcat(out_tar_name, "_out.tar.bz2 ");

        strcpy(cmd, "tar cjfP ");
        strcat(cmd, out_tar_name);
        strcat(cmd, out_dir_name);

        system(cmd);
    }

    closedir(dp);
}

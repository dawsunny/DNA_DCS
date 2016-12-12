#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>    //access
#include <sys/stat.h>

#include "dc_type.h"
#include "dc_const.h"
#include "dc_debug.h"

#include "dc_d_io.h"

#include <vector> 
#include <string>
using namespace std;

dc_s32_t   ref_seq_total_no_g;
dc_s8_t	 **ref_seqs_g;
dc_s32_t  *ref_seqs_len_g;

int DIF_RATE;
int OVERLAP;
int LINE_LEN;

void
print_usage()
{
    printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
    printf("Usage:\n");
    printf("./dc_decompress [-m mutation_rate] [-l line_length] ref_file_path compressed_file|dir_path/ \n");
    printf("\n");
    printf("Arguments:\n");
    printf("1 - optional, 0.01 default, equal to the compress mutation_rate\n");
    printf("2 - optional, 60 default, equal to the length of the line in original file\n");
    printf("3 - ref_file_path\n");
    printf("4 - compressed_file|dir_path/ \n");
    printf("\n");
    printf("Example usage:\n");
    printf("./dc_decompress ref_file_path compressed_file\n");
    printf("./dc_decompress -m 0.02 ref_file_path compressed_dir_path/ \n");
    printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
}

//check whether the path exsits
dc_s32_t 
check_arg( dc_s32_t argc, dc_s8_t *argv[] )
{
    dc_s32_t rc = 0;
    DC_PRINT("check_arg enter:\n");

    DIF_RATE = 0.01;
    LINE_LEN = 60;

    if( argc != 3 && argc != 5 && argc != 7) 
    {   
        DC_ERROR("ERROR!: check_arg: input arg error\n");
        print_usage();
        rc = -1; 
        goto EXIT;
    }   

    if( access(argv[argc - 2], 0) != 0 ) 
    {   
        DC_ERROR("ERROR!: check_arg: ref file not exsits\n");
        rc = -1; 
        goto EXIT;
    }   

    if( access(argv[argc - 1], 0) != 0 ) 
    {   
        DC_ERROR("ERROR!: check_arg: compressed file/dir not exsits\n");
        rc = -1; 
        goto EXIT;
    }   

    char opt;
    while( (opt = getopt(argc, argv, "m:l:")) != EOF )
    {
        switch(opt)
        {
            case '?':
                DC_ERROR("ERROR!: check_arg: unrecognized option: -%c", optopt);
                print_usage();
                rc = -1;
                goto EXIT;
            case 'm':
                DIF_RATE = atof(optarg);
                break;
            case 'l':
                LINE_LEN = atoi(optarg);
                break;
        }
    }

    OVERLAP = REF_CHUNK + (int)(INPUT_CHUNK * DIF_RATE);

EXIT:
    DC_PRINT("check_arg leave\n");
    return rc; 
}


//read reference file, allocate and fill the reference buf
dc_s32_t 
read_ref_file(dc_s8_t *ref_file_path)
{
	dc_s32_t  rc = 0;
	DC_PRINT("read_ref_file enter:\n");

	dc_s32_t i, j, tmp_len, ref_seq_no = 0;
	dc_s8_t  line_buf[LINE_BUF_LEN];

	//file total size
	struct stat file_stat;
	stat( ref_file_path, &file_stat );

	FILE *fin_ref = fopen(ref_file_path, "r");
	if( fin_ref == NULL ) 
    {
		DC_PRINT("error: read_ref_file: open file error\n");
		rc = -1;
		goto EXIT;
	}

    //global varibles assignment
	ref_seq_total_no_g = (file_stat.st_size + REF_CHUNK - 1) / REF_CHUNK;

	ref_seqs_g = (dc_s8_t **) malloc( sizeof(dc_s8_t *) * ref_seq_total_no_g );
	if( ref_seqs_g == NULL )
	{
		DC_ERROR("ERROR!: read_ref_file: malloc for ref_seqs_g error\n");
		rc = -1;
		goto EXIT;
	}
	ref_seqs_len_g = (dc_s32_t *) malloc(ref_seq_total_no_g * sizeof(dc_s32_t));
	if( ref_seqs_len_g == NULL )
	{
		DC_ERROR("ERROR!: read_ref_file: malloc for ref_seqs_len_g error\n");
		rc = -1;
		goto EXIT;
	}

	//initialize
	for(i = 0; i < ref_seq_total_no_g; ++i) 
    {
		ref_seqs_g[i] = NULL;
		ref_seqs_len_g[i]  = 0;
	}

	//allocate            
	for(i = 0; i < ref_seq_total_no_g; ++i) 
    {
		ref_seqs_g[i] = (dc_s8_t *) malloc( sizeof(dc_s8_t) * (REF_CHUNK + OVERLAP) );
		if( ref_seqs_g[i] == NULL ) 
        {
			DC_ERROR("ERROR!: read_ref_file: seq %d: malloc error\n", i);
			rc = -1;
			goto EXIT;
		}
	}

    fgets(line_buf, LINE_BUF_LEN, fin_ref);   //head line

	while( fgets(line_buf, LINE_BUF_LEN, fin_ref) != NULL ) 
    {
        for(j = 0; line_buf[j] != '\n' && line_buf[j] != 'N'; ++j)
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
        else
        {
            tmp_len = j;
        }

        if(tmp_len == 0)
            continue;

		strncpy(ref_seqs_g[ref_seq_no] + ref_seqs_len_g[ref_seq_no], line_buf, tmp_len);
		ref_seqs_len_g[ref_seq_no] += tmp_len;

		if( ref_seqs_len_g[ref_seq_no] > REF_CHUNK )  //each fragment has REF_CHUNK elements
        {
			tmp_len = ref_seqs_len_g[ref_seq_no] - REF_CHUNK;
			strncpy(ref_seqs_g[ref_seq_no + 1], ref_seqs_g[ref_seq_no] + REF_CHUNK, tmp_len);
			ref_seqs_len_g[ref_seq_no]      = REF_CHUNK;
			ref_seqs_len_g[ref_seq_no + 1] += tmp_len;

			++ref_seq_no;
		}
	}
	fclose(fin_ref);

    for(i = ref_seq_no + 1; i < ref_seq_total_no_g; ++i)
    {
        if(ref_seqs_g[i] != NULL)
        {
            free(ref_seqs_g[i]);
            ref_seqs_g[i] = NULL;
        }
    }

    ref_seq_total_no_g = ref_seq_no + 1;

    //fill the overlap
    for(i = 0; i <= ref_seq_total_no_g - 3; ++i) 
    {
        strncpy(ref_seqs_g[i] + REF_CHUNK, ref_seqs_g[i + 1], OVERLAP);
        ref_seqs_len_g[i] += OVERLAP;
    }
    strncpy(ref_seqs_g[i] + ref_seqs_len_g[i], ref_seqs_g[i + 1], MIN(OVERLAP, ref_seqs_len_g[i + 1]));  //i = refFragTotNo - 2
    ref_seqs_len_g[i] += MIN(OVERLAP, ref_seqs_len_g[i + 1]);

EXIT:
	DC_PRINT("read_ref_file leave\n");
	return rc;
}

void
write_to_file(vector<string> &inp_seqs, FILE *fout)
{
    dc_s8_t write_buf[LINE_LEN + 2];
    write_buf[LINE_LEN] = '\n';
    write_buf[LINE_LEN + 1] = '\0';

    dc_s32_t idx, i;

    for(i = 0; i < (dc_s32_t)inp_seqs.size() - 1; ++i)
    {
        idx = 0;

        while( idx + LINE_LEN <= (dc_s32_t)inp_seqs[i].size() )
        {
            strncpy(write_buf, inp_seqs[i].c_str() + idx, LINE_LEN);
            fputs(write_buf, fout);
            idx += LINE_LEN;
        }

        inp_seqs[i+1] = inp_seqs[i].substr(idx) + inp_seqs[i+1];
    }

    idx = 0;

    while( idx + LINE_LEN <= (dc_s32_t)inp_seqs[i].size() )
    {
        strncpy(write_buf, inp_seqs[i].c_str() + idx, LINE_LEN);
        fputs(write_buf, fout);
        idx += LINE_LEN;
    }

    int tmp_len = inp_seqs[i].size() - idx;
    if(idx < (dc_s32_t)inp_seqs[i].size())
    {
        strncpy(write_buf, inp_seqs[i].c_str() + idx, tmp_len);
        write_buf[tmp_len] = '\n';
        write_buf[tmp_len + 1] = '\0';
        fputs(write_buf, fout);
    }
}

void
free_memory()
{
	DC_PRINT("free_memory enter:\n");

	dc_s32_t i;

	if(ref_seqs_g != NULL)
	{
        for(i = 0; i < ref_seq_total_no_g; ++i)
        {
            if(ref_seqs_g[i] != NULL)
            {
                free(ref_seqs_g[i]);
                ref_seqs_g[i] = NULL;
            }
        }

		free(ref_seqs_g);
		ref_seqs_g = NULL;
	}

	if(ref_seqs_len_g != NULL)
	{
		free(ref_seqs_len_g);
		ref_seqs_len_g = NULL;
	}

	DC_PRINT("free_memory leave\n");
}


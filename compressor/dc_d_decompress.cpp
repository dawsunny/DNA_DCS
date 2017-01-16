/*
 * usage:
 *
 * ./dc_decompress reference_file_path compressed_file_dir_path 
 * 
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

#include "dc_type.h"
#include "dc_const.h"
#include "dc_debug.h"

#include "dc_global.h"
#include "dc_d_decode.h"
#include "dc_io.h"
#include "dc_d_decompress.h"

#include <string>
#include <vector>
using namespace std;

//variables defined in dc_io.c
extern dc_s8_t **dc_d_ref_seqs_g;


//make orignal sequence according to the reference sequences and difference strings
void
make_orig_seq(const dc_s8_t *s0, dc_s32_t len0, dc_s8_t *dstr, dc_s32_t dstr_len, dc_s8_t *s1, dc_s32_t len1)
{
    dc_s8_t  opt;
    dc_s32_t dstr_i = 0, idx0 = 0, idx1 = 0, next_edit = 0, offset;
    DC_PRINT("make_orig_seq enter:\n");

    while(dstr_i < dstr_len)   //read the d-string
    {
        opt = dstr[dstr_i++];  //1st, the option char, 's' or 'i'

        offset = dstr[dstr_i++] - '0';   //2nd, the delta-distance between two variation
        while( '0' <= dstr[dstr_i] && dstr[dstr_i] <= '7' ) 
        {
            offset *= 8;
            offset += dstr[dstr_i] - '0';
            ++dstr_i;
        }
        next_edit += offset;   //the next variation position

        while( idx1 < next_edit )  //filled with the bases between the two edit string
        {
            s1[idx1++] = s0[idx0++];
        }

        if( opt == 'i' )   //insert, filled  base by base
        {
            while( dstr_i < dstr_len && dstr[dstr_i] != 's' && dstr[dstr_i] != 'i' ) 
            {
                s1[idx1++] = dstr[dstr_i++];
            }
        }
        else 
        { 
            while( dstr_i < dstr_len && dstr[dstr_i] != 's' && dstr[dstr_i] != 'i' ) 
            {
                if( dstr[dstr_i] != '-' )  //substitution
                {
                    s1[idx1++] = dstr[dstr_i++];
                    ++idx0;
                }
                else   //deletion
                {
                    ++dstr_i;
                    ++idx0;
                    --next_edit;
                }
            }
        } //if-else
    }

    while( idx1 < len1 )   //filled with the more bases, which are matched
    {
        s1[idx1++] = s0[idx0++];
    }

	//assert
	if( idx0 != len0 || idx1 != len1 )
	{
		DC_ERROR("ERROR! make_orig_seq: idx point error\n");
	}

    DC_PRINT("make_orig_seq leave\n");
}

//decompress from cfile_name into output_name
dc_s32_t
decompress_file(const dc_s8_t *cfile_name, string &output)
{
    dc_s32_t rc = 0;
    DC_PRINT("decompress_file enter:\n");
    printf("input: %s\noutput: %s\n", cfile_name, output);

    FILE *fin = NULL, *fin_n = NULL, *fin_r = NULL;// *fout = NULL;
    dc_link_t link_info;

    dc_s8_t *read_buf = NULL, write_buf[LINE_LEN + 2], head_line[LINE_BUF_LEN];
    dc_s32_t i;
    dc_u8_t  mask = 0xf;   
    dc_u16_t head_line_len = 0;   

	dc_s32_t inp_seq_len, ref_seq_len, dstr_len, encode_len;
	dc_s8_t *dstr = NULL;
    dc_u8_t *dstr_encode = NULL;
    dc_u32_t output_offset = 0;

    vector<string> inp_seqs;
    vector<dc_u32_t> n_locs;

    string n_file_name(string(cfile_name) + "_n");
    string r_file_name(n_file_name);
    r_file_name[r_file_name.size() - 1] = 'r';

    //malloc
    read_buf    = (dc_s8_t *) malloc( sizeof(dc_s8_t) * (INPUT_CHUNK + LINE_BUF_LEN) );
    dstr        = (dc_s8_t *) malloc( sizeof(dc_s8_t) * (INPUT_CHUNK + LINE_BUF_LEN) );
    dstr_encode = (dc_u8_t *) malloc( sizeof(dc_u8_t) *  INPUT_CHUNK );
    if( read_buf == NULL || dstr == NULL || dstr_encode == NULL )
    {
        DC_ERROR("ERROR!: decompress_file: malloc for str error\n");
        rc = -1;
        goto EXIT;
    }

    //open
    /*
    if( (fout = fopen(output, "w")) == NULL ) 
	{
        DC_ERROR("error: decompress_file: open output file error\n");
        rc = -1;
        goto EXIT;
    }
     */
    if( (fin = fopen(cfile_name, "rb")) == NULL ) 
    {
        DC_ERROR("error: decompress_file: open compressed_file error\n");
        rc = -1;
        goto EXIT;
    }
    if( (fin_n = fopen(n_file_name.c_str(), "rb")) == NULL ) 
    {
        DC_ERROR("error: decompress_file: open compressed_file error\n");
        rc = -1;
        goto EXIT;
    }
    if( access(r_file_name.c_str(), 0) == 0 && (fin_r = fopen(r_file_name.c_str(), "rb")) == NULL ) //maybe not exsit
    {
        DC_ERROR("error: decompress_file: open compressed_file error\n");
        rc = -1;
        goto EXIT;
    }

    //head line
    fread(&head_line_len, sizeof(dc_u16_t), 1, fin);
    fread(head_line, sizeof(dc_s8_t), head_line_len, fin);
    head_line[head_line_len]     = '\n';
    head_line[head_line_len + 1] = '\0';
    //fputs(head_line, fout);
    output = head_line;
    //memcpy(output + output_offset, head_line, strlen(head_line));
    //output_offset += strlen(head_line);

    write_buf[LINE_LEN] = '\n', write_buf[LINE_LEN + 1] = '\0';
    while( fread(&link_info, sizeof(dc_link_t), 1, fin) == 1 )   //1st, read a link struct
    {
		ref_seq_len = link_info.ref_len;  
        inp_seq_len = link_info.inp_len;  
        string inp_seq(inp_seq_len + 2, 0);

        fread(&encode_len, sizeof(dc_s32_t), 1, fin);   //2nd, read the length of encoding d-string 
        fread(dstr_encode, sizeof(dc_u8_t), encode_len, fin);   //3rd, read the encoding d-string
        dstr_len = 2 * encode_len;

        if( encode_len == 0 )   //exact match
        {
            //assert
            if( inp_seq_len != ref_seq_len )
            {
                DC_ERROR("ERROR!: decompress_file: lengths assert error\n");
                rc = -1;
                goto EXIT;
            }

            strncpy((dc_s8_t *)inp_seq.c_str(), dc_d_ref_seqs_g[link_info.ref_seq_no] + link_info.ref_start, inp_seq_len);
        }
		else if( ref_seq_len != -1 )     //unexact match
		{
			for(i = 0; i < encode_len; ++i)
			{
				dstr[2 * i] = halfByte_to_char( dstr_encode[i] >> 4 & mask );    
				dstr[2 * i + 1] = halfByte_to_char( dstr_encode[i]      & mask );    
			}
			if(dstr[dstr_len - 1] == 'N') 
			{
				--dstr_len;
			}

			make_orig_seq(dc_d_ref_seqs_g[link_info.ref_seq_no] + link_info.ref_start, ref_seq_len, dstr, dstr_len, (dc_s8_t *)inp_seq.c_str(), inp_seq_len);
		}
		else//by weizheng, for what 
		{
            dstr_len -= 2;
			for(i = 1; i < encode_len; ++i)
			{
				inp_seq[2 * i - 2] = halfByte_to_char( dstr_encode[i] >> 4 & mask );    
				inp_seq[2 * i - 1] = halfByte_to_char( dstr_encode[i]	   & mask );    
			}
			if(inp_seq[dstr_len - 1] == 'N') 
			{
				--dstr_len;
			}

			//assert
			if( inp_seq_len != dstr_len )
			{
				DC_ERROR("ERROR!: decompress_file: inp_seq_len error\n");
                rc = -1;
                goto EXIT;
			}
		}

        inp_seq.resize(inp_seq_len);
        inp_seqs.push_back(inp_seq);
    }

    dc_u32_t nLoc, nCnt;
    while( fread(&nLoc, sizeof(dc_u32_t), 1, fin_n) == 1 )
    {
        fread(&nCnt, sizeof(dc_u32_t), 1, fin_n);

        n_locs.push_back(nLoc);
        n_locs.push_back(nCnt);
    }

    for(int i = n_locs.size() - 1; i >= 0; i -= 2)
    {
        nLoc = n_locs[i-1];
        nCnt = n_locs[i];
        dc_u32_t seq_no  = nLoc / INPUT_CHUNK;
        dc_u32_t base_no = nLoc % INPUT_CHUNK;

        if(seq_no >= inp_seqs.size())
            inp_seqs.push_back(string());

        inp_seqs[seq_no].insert(inp_seqs[seq_no].begin() + base_no, nCnt, 'N');
    }

    //write_to_file(inp_seqs, output, output_offset);
    write_to_file(inp_seqs, output);

    if( fin_r != NULL )
    {
        int  base_pos;
        char c;
        
        while( fread(&base_pos, sizeof(int), 1, fin_r) == 1 )
        {
            fread(&c, sizeof(char), 1, fin_r);

            output = output.substr(0, base_pos) + "N" + output.substr(base_pos);
            //fseek(fout, base_pos, SEEK_SET);
            //fputc(c, fout);
        }
    }

EXIT:
    //if( fout != NULL )
    //{
    //    fclose(fout);
   //     fout = NULL;
   // }
    if( fin != NULL )
    {
        fclose(fin);
        fin = NULL;
    }
    if( fin_n != NULL )
    {
        fclose(fin_n);
        fin_n = NULL;
    }
    if( fin_r != NULL )
    {
        fclose(fin_r);
        fin_r = NULL;
    }

    if( dstr != NULL )
    {
        free(dstr);
        dstr = NULL;
    }
    if( dstr_encode != NULL )
    {
        free(dstr_encode);
        dstr_encode = NULL;
    }

    DC_PRINT("decompress_file leave\n");
    return rc;
}

//check whether the path is a file or dir
dc_s32_t
analyze_path(dc_s8_t *input_path, string& output)
{
	dc_s32_t rc = 0;
	DC_PRINT("analyze_path enter:\n");
    printf("get input_path: %s\n", input_path);

	struct stat file_stat;
	stat(input_path, &file_stat);

	if( S_ISREG(file_stat.st_mode) )
	{
        /*string cmd("tar -xjf " + string(input_path));*/
        /*system(cmd.c_str());*/

        /*
        dc_s32_t i, path_len, name_len;

		path_len = strlen(input_path);
		for(i = path_len - 1; i >= 0 && input_path[i] != '/'; --i)
			;
		name_len = path_len - 1 - i;
         */
        
		/*strncpy( output_name, input_path + i + 1, name_len - 4); //4 -> _out*/
		/*strncpy( output_name + name_len - 4, "_res", 5);*/

        //string output_name(input_path + i + 1, input_path + path_len - 3); //3 -> out
        //output_name += "res";

        rc = decompress_file(input_path, output);
        if( rc )
        {
            DC_ERROR("ERROR!: analyze_path: decompress_file return error\n");
        }
	}
	else if( S_ISDIR(file_stat.st_mode) )
	{
		struct dirent *dirp = NULL; //local
		DIR           *dp   = NULL;

        string file_path(input_path);

        dc_s32_t i;
        for(i = file_path.size() - 2; i >= 0 && file_path[i] != '/'; --i)
            ;
        string file_no(input_path + i + 1);
        file_no.resize(file_no.size() - 1);     //desert the last '/'

		if( (dp = opendir(input_path)) == NULL ) 
		{
			DC_ERROR("ERROR!: analyze_path: open dir error\n");
			rc = -1;
			goto EXIT;
		}

		while( (dirp = readdir(dp)) != NULL ) 
		{
			if( strcmp(dirp->d_name, ".") == 0 || strcmp(dirp->d_name, "..") == 0 || dirp->d_name[strlen(dirp->d_name) - 1] == 'n' || dirp->d_name[strlen(dirp->d_name) - 1] == 'r') 
			{
				continue;
			}

            string file_name(dirp->d_name);
            string output_name("/tmp/dna_decompress/out/");
            output_name += file_name + '/' + file_no;

            //rc = decompress_file((file_path + file_name).c_str(), output_name.c_str());
            if( rc )
            {
                DC_ERROR("ERROR!: analyze_path: decompress_file return error\n");
            }
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
	DC_PRINT("analyze_path enter:\n");
	return rc;
}

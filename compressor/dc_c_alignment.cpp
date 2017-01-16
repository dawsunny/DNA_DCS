/*
 *  dc_alignment.c
 *  find match, encode it, and write to file
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "dc_type.h"
#include "dc_const.h"
#include "dc_debug.h"

#include "dc_global.h"
#include "dc_c_encode.h"
#include "dc_io.h"
#include "dc_c_alignment.h"

//defined in dc_io.c
//ref seqs and seeds
extern dc_s8_t	    **dc_c_ref_seqs_g;
extern dc_s32_t      *dc_c_ref_seqs_len_g;

extern dc_seed_loc_t *seed_locs_g[];
extern dc_s32_t       seed_locs_freq_g[];

dc_s32_t
make_dstr(dc_s8_t *dstr, dc_s8_t *aligned_seqs[])
{
    dc_s32_t  i, dstr_len = 0, oct_len;
    dc_s8_t  *ref_seq  = aligned_seqs[0];
    dc_s8_t  *inp_seq  = aligned_seqs[1];
    dc_s32_t  ref_seq_len = strlen(ref_seq);

    dc_s8_t   insert_open = 0, subdel_open = 0;
    dc_s32_t  last_edit = 0;

    DC_PRINT("make_dstr enter:\n");

    //assert
    if( strlen(inp_seq) != ref_seq_len )
    {
        DC_ERROR("ERROR!: make_dstr: seq length not equal\n");
    }

    for( i = 0; i < ref_seq_len; ++i ) 
    {
        if( inp_seq[i] == ref_seq[i] ) 
        {
            insert_open = 0;
            subdel_open = 0;
        }
        else if( ref_seq[i] == '-' )  // insertion in orig relative to ref (i.e., gap in ref)
        {
            subdel_open = 0;
            if( !insert_open )  // indicate start of insertion
            {
                insert_open = 1;
                dstr[ dstr_len++ ] = 'i';
                oct_len = sprintf(dstr + dstr_len, "%o", i - last_edit);
                dstr_len += oct_len;
                last_edit = i;
            }
            dstr[ dstr_len++ ] = inp_seq[i];
        }
        else  // substitution or deletion in str (represented in script by '-') relative to ref
        {
            insert_open = 0;
            if( !subdel_open )  // indicate start of subdel
            {
                subdel_open = 1;
                dstr[ dstr_len++ ] = 's';
                oct_len = sprintf(dstr + dstr_len, "%o", i - last_edit);
                dstr_len += oct_len;
                last_edit = i;
            }
            dstr[ dstr_len++ ] = inp_seq[i];
        }
    } //for
    dstr[ dstr_len ] = '\0';
    
    DC_PRINT("make_dstr leave\n");
    return dstr_len;
}

inline void
reverse(dc_s8_t *str)
{
	dc_s32_t i, len = strlen(str), half_len = (len - 1)/2;
	dc_s8_t  ch_tmp;

    if( len == 0 || len == 1 )
    {
        return;
    }

	for( i = 0; i <= half_len; ++i ) 
    {
		ch_tmp = str[i];
		str[i] = str[len - 1 - i];
		str[len - 1 -i] = ch_tmp;
	}
}

//return the max DP length, quota or the remaining base number
inline dc_s32_t 
max_usable_len(dc_s32_t i, dc_s32_t dir, dc_s32_t len, dc_s32_t quota)
{
    return (dir == 1) ?
        ( quota > (len - i) ? (len - i) : quota ) :
        ( quota > (i + 1)   ? (i + 1)   : quota ) ;
}

//attempt ungapped and gapped extend, return two aligned sequences
//idxp0, idxp1 and aligned_s are return values
void 
attempt_ext(dc_s32_t *idxp0, dc_s32_t dir0, const dc_s8_t *s0,  dc_s32_t len0,
            dc_s32_t *idxp1, dc_s32_t dir1, const dc_s8_t *s1,  dc_s32_t len1,
            dc_s8_t  *aligned_s[])
{
    dc_s32_t i, j, k;  //for loop
    dc_s32_t check_dist, mismatch_num;
    dc_s32_t aligned_s_len[2] = {0}; 

    dc_s32_t dp_len0; 
    dc_s32_t dp_len1; 

    dc_s32_t max, max_i, max_j;   //dp_score max
    dc_s32_t tmp_max, tmp_max_i, tmp_max_j;   
    dc_s32_t dp_score[MAX_DP_LEN + 1][MAX_DP_LEN + 1];
    dc_s32_t matrix_max[MAX_DP_LEN + 1][MAX_DP_LEN + 1], matrix_max_i[MAX_DP_LEN + 1][MAX_DP_LEN + 1], matrix_max_j[MAX_DP_LEN + 1][MAX_DP_LEN + 1];

    dc_s8_t  subs0_dp[2 * MAX_DP_LEN], subs1_dp[2 * MAX_DP_LEN];
    dc_s32_t subs0_dp_len, subs1_dp_len;

    *idxp0 += dir0; //move 1 base forward
    *idxp1 += dir1;

    DC_PRINT("attempt_ext enter: %s\n", dir1 == 1 ? "pos" : "neg");

    while( 0 <= *idxp0 && *idxp0 < len0 &&
           0 <= *idxp1 && *idxp1 < len1)
    {
        //loop, find the first mismatched base
        if( s0[*idxp0] == s1[*idxp1] )
        {
            aligned_s[0][ aligned_s_len[0]++ ] = s0[*idxp0];
            aligned_s[1][ aligned_s_len[1]++ ] = s1[*idxp1];

            *idxp0 += dir0;
            *idxp1 += dir1;
            continue;
        }

        //meet a mismatched base, check whether SNP or insert/delete
        check_dist = MIN(max_usable_len(*idxp0 + dir0, dir0, len0, MAX_CHECK_DIST),
                         max_usable_len(*idxp1 + dir1, dir1, len1, MAX_CHECK_DIST)); 

        mismatch_num = 0;

        for(i = 1; i <= check_dist; ++i)
        {
            if( s0[*idxp0 + dir0 * i] != s1[*idxp1 + dir1 * i] )
            {
                ++mismatch_num;
            }
        }

        if( mismatch_num < check_dist * CHECK_IDENT_THRESH )   //SNP
        {
            for( i = 0; i < 1 + check_dist; ++i )
            {
                aligned_s[0][ aligned_s_len[0]++ ] = s0[*idxp0];
                aligned_s[1][ aligned_s_len[1]++ ] = s1[*idxp1];

                *idxp0 += dir0;
                *idxp1 += dir1;
            }
            continue;
        }


        //insert or delete, begin dp
        dp_len0 = max_usable_len(*idxp0, dir0, len0, MAX_DP_LEN);
        dp_len1 = max_usable_len(*idxp1, dir1, len1, MAX_DP_LEN);
        if( dp_len0 <= MIN_DP_LEN || dp_len1 <= MIN_DP_LEN )  //exit, no need to DP
        {
            break;
        }

        max = 0, max_i = 0, max_j = 0;
        //fill the DP matrix
        for( j = 0; j <= dp_len0; ++j ) 
        {
            dp_score[0][j] = 0;
        }
        for( i = 1; i <= dp_len1; ++i) 
        {
            dp_score[i][0] = 0;

            for( j = 1; j <= dp_len1; ++j ) 
            {
                if( s0[*idxp0 + dir0 * (j - 1)] == s1[*idxp1 + dir1 * (i - 1)] ) 
                {
                    dp_score[i][j] = dp_score[i-1][j-1] + 1;

                    if(dp_score[i][j] > max) 
                    {
                        max = dp_score[i][j];
                        max_i = i;
                        max_j = j;
                    }
                }
                else 
                {
                    dp_score[i][j] = 0;
                }
            } 
        } 

        //no match find in dp (max_i and max_j will >0 or ==0 at the same time)
        if( max_i == 0 || max_j == 0 )
        {
            //assert
            if( max_i != 0 || max_j != 0 )
            {
                DC_ERROR("ERROR!: attempt_ext: dp max loc error2\n");
            }

            for( k = 0; k < MIN(dp_len0, dp_len1); ++k )
            {
                aligned_s[0][ aligned_s_len[0]++ ] = s0[*idxp0];
                aligned_s[1][ aligned_s_len[1]++ ] = s1[*idxp1];

                *idxp0 += dir0;
                *idxp1 += dir1;
            }
            continue;
        }

        //assert
        if( max_i <= 0 || max_j <= 0 )
        {
            DC_ERROR("ERROR!: attempt_ext: dp max loc error1\n");
        }

        //find the max values and their locations in every sub-matrix
        for(j = 0; j <= max_j; ++j) 
        {
            matrix_max[0][j] = 0;
            matrix_max_i[0][j] = 0;
            matrix_max_j[0][j] = 0;
        }
        for(i = 1; i <= max_i; ++i) 
        {
            matrix_max[i][0] = 0;
            matrix_max_i[i][0] = 0;
            matrix_max_j[i][0] = 0;

            for(j = 1; j <= max_j; ++j) 
            {
                if(dp_score[i][j] >= matrix_max[i][j-1] && dp_score[i][j] >= matrix_max[i-1][j]) 
                {
                    matrix_max[i][j] = dp_score[i][j];
                    matrix_max_i[i][j] = i;
                    matrix_max_j[i][j] = j;
                }   
                else if(matrix_max[i][j-1] >= matrix_max[i-1][j]) 
                {
                    matrix_max[i][j] = matrix_max[i][j-1];
                    matrix_max_i[i][j] = matrix_max_i[i][j-1];
                    matrix_max_j[i][j] = matrix_max_j[i][j-1];
                }   
                else 
                {
                    matrix_max[i][j] = matrix_max[i-1][j];
                    matrix_max_i[i][j] = matrix_max_i[i-1][j];
                    matrix_max_j[i][j] = matrix_max_j[i-1][j];
                }   
            }
        }

        subs0_dp_len = 0, subs1_dp_len = 0;
        tmp_max = max, tmp_max_i = max_i, tmp_max_j = max_j;

        while(i > 0 && j > 0 && tmp_max > 0) 
        {
            for(k = 0; k < tmp_max; ++k) 
            {
                subs0_dp[subs0_dp_len++] = s0[*idxp0 + dir0 * (tmp_max_j - 1 - k)];            
                subs1_dp[subs1_dp_len++] = s1[*idxp1 + dir1 * (tmp_max_i - 1 - k)];            
            }
            i = tmp_max_i - tmp_max;
            j = tmp_max_j - tmp_max;
            tmp_max_i = matrix_max_i[i][j];
            tmp_max_j = matrix_max_j[i][j];
            tmp_max   = matrix_max[i][j];

            while( i > tmp_max_i && j > tmp_max_j ) 
            { 
                subs0_dp[subs0_dp_len++] = s0[*idxp0 + dir0 * --j];            
                subs1_dp[subs1_dp_len++] = s1[*idxp1 + dir1 * --i];            
            }   
            while( i > tmp_max_i ) 
            {
                //assert
                if( j != tmp_max_j )
                {
                    DC_ERROR("ERROR!: attempt_ext: extend j error\n");
                }
                subs0_dp[subs0_dp_len++] = '-';        
                subs1_dp[subs1_dp_len++] = s1[*idxp1 + dir1 * --i];        
            }
            while( j > tmp_max_j ) 
            {
                //assert
                if( i != tmp_max_i )
                {
                    DC_ERROR("ERROR!: attempt_ext: extend i error\n");
                }
                subs0_dp[subs0_dp_len++] = s0[*idxp0 + dir0 * --j];        
                subs1_dp[subs1_dp_len++] = '-';        
            }
        }

        if(i == 0 || j == 0) 
        {
            while( i > 0 ) 
            { 
                //assert
                if( j != 0 )
                {
                    DC_ERROR("ERROR!: attempt_ext: dp boundary j error\n");
                }
                subs0_dp[subs0_dp_len++] = '-';            
                subs1_dp[subs1_dp_len++] = s1[*idxp1 + dir1 * --i];            
            }   
            while( j > 0 ) 
            { 
                //assert
                if( i != 0 )
                {
                    DC_ERROR("ERROR!: attempt_ext: dp boundary i error\n");
                }
                subs0_dp[subs0_dp_len++] = s0[*idxp0 + dir0 * --j];            
                subs1_dp[subs1_dp_len++] = '-';            
            }   
        }  
        else if( i >= j ) 
        { 
            while( j > 0 ) 
            { 
                subs0_dp[subs0_dp_len++] = s0[*idxp0 + dir0 * --j];            
                subs1_dp[subs1_dp_len++] = s1[*idxp1 + dir1 * --i];            
            }   
            while( i > 0 ) 
            { 
                subs0_dp[subs0_dp_len++] = '-';          
                subs1_dp[subs1_dp_len++] = s1[*idxp1 + dir1 * --i];            
            }
        }
        else 
        {
            while( i > 0 ) 
            { 
                subs0_dp[subs0_dp_len++] = s0[*idxp0 + dir0 * --j];            
                subs1_dp[subs1_dp_len++] = s1[*idxp1 + dir1 * --i];            
            }   
            while( j > 0 ) 
            { 
                subs0_dp[subs0_dp_len++] = s0[*idxp0 + dir0 * --j];            
                subs1_dp[subs1_dp_len++] = '-';            
            }   
        }  
        
        //assert
        if(subs0_dp_len != subs1_dp_len)
        {
            DC_ERROR("ERROR!: attempt_ext: dp error\n");
        }

        // update alignment seq with dp region
        subs0_dp[subs0_dp_len] = '\0';
        subs1_dp[subs1_dp_len] = '\0';
        reverse(subs0_dp); //record the backtrack, need reverse
        reverse(subs1_dp);

        strncpy(aligned_s[0] + aligned_s_len[0], subs0_dp,  subs0_dp_len); 
        strncpy(aligned_s[1] + aligned_s_len[1], subs1_dp,  subs1_dp_len); 
        aligned_s_len[0] += subs0_dp_len; 
        aligned_s_len[1] += subs1_dp_len; 

        *idxp0 += dir0 * max_j;
        *idxp1 += dir1 * max_i; // move positions to the 1st base after the match
    } //while(1)

    *idxp0 -= dir0; 
    *idxp1 -= dir1; // interval is inclusive

    if(dir1 == 1 && *idxp1 < len1 - 1)   //filled with the mismatched bases
    {
        dc_s32_t moreLen = len1 - 1 - *idxp1;
        for(i = 0; i < moreLen; ++i) 
        {
            aligned_s[0][ aligned_s_len[0] + i ] = '-';
            aligned_s[1][ aligned_s_len[1] + i ] = s1[*idxp1 + 1 + i];
        }
        aligned_s_len[0] += moreLen;
        aligned_s_len[1] += moreLen;
    }
    else if(dir1 == -1 && *idxp1 > 0) 
    {
        dc_s32_t moreLen = *idxp1;
        for(i = 0; i < moreLen; ++i) 
        {
            aligned_s[0][ aligned_s_len[0] + i ] = '-';
            aligned_s[1][ aligned_s_len[1] + i ] = s1[*idxp1 - 1 - i];
        }
        aligned_s_len[0] += moreLen;
        aligned_s_len[1] += moreLen;
    }

    /* the return values */
    aligned_s[0][ aligned_s_len[0] ]  = '\0';
    aligned_s[1][ aligned_s_len[1] ]  = '\0';

    if( dir1 == -1 )  // choose direction so reference (s1) is always going forward
    {
        reverse(aligned_s[0]);
        reverse(aligned_s[1]);
    }

    DC_PRINT("attempt_ext leave\n");
}

dc_s32_t 
test_ext(dc_s32_t idx0, dc_s32_t dir0, dc_s8_t *s0, dc_s32_t len0,
         dc_s32_t idx1, dc_s32_t dir1, dc_s8_t *s1, dc_s32_t len1)
{
    dc_s32_t progress = 0, consec_mismatch = 0, total_mismatch = 0; 
    idx0 += dir0;
    idx1 += dir1;

    while( consec_mismatch < MAX_CONSEC_MISMATCH &&
           progress < MIN_PROGRESS   &&
           0 <= idx0 && idx0 < len0  &&
           0 <= idx1 && idx1 < len1 )
    {    
        if( s0[idx0] != s1[idx1] ) 
        {
            ++consec_mismatch;
            ++total_mismatch;
        }    
        else 
        {    
            consec_mismatch = 0; 
        }    
        idx0 += dir0;
        idx1 += dir1;
        ++progress;
    }    
    
    if( total_mismatch > CHECK_IDENT_THRESH * progress )
    {
        return 0;
    }
    return progress;
}

dc_s32_t
test_seed(dc_s32_t s0_no, dc_s8_t *s0, dc_s32_t len0, dc_s32_t start0,
          dc_s32_t s1_no, dc_s8_t *s1, dc_s32_t len1, dc_s32_t start1, dc_s32_t ref_start_g)
{
    dc_s32_t global_loc0 = s0_no * REF_CHUNK + start0 + 1;
/*
    if( global_loc0 < ref_start_g - INPUT_CHUNK || global_loc0 > ref_start_g + 2 * INPUT_CHUNK ) //by zhj
    {
        return 1;  //1: not fit 
    }
*/
    if( test_ext(start0, -1, s0, len0,
                 start1, -1, s1, len1) +
        test_ext(start0 + SEED_LEN - 1, 1, s0, len0,
                 start1 + SEED_LEN - 1, 1, s1, len1)
        < MIN_PROGRESS )
    {
        return 1;
    }

    return 0;   //0: seed fit
}

//this func is called in a loop, so the arrays in this func are put into the arg list
void 
find_match(dc_s32_t inp_seq_no, dc_s8_t *inp_seq, dc_s32_t inp_seq_len, dc_s32_t *ref_start_gp,
           dc_s8_t *aligned_prev[], dc_s8_t *aligned_next[], dc_s8_t *aligned_seqs[], dc_s8_t *dstr, dc_u8_t *dstr_encode, FILE *fout)
{
    DC_PRINT("find_match enter:\n");

    dc_s32_t i, j, k; //just for loop
    dc_s32_t seed_freq_tmp;
    dc_u32_t seed_i = 0, seed_mask = MEGA - 1;

    dc_s32_t inp_start, inp_stop;
    dc_s32_t ref_start, ref_stop;
    dc_s8_t *ref_seq = NULL;
    dc_s32_t ref_seq_len;
    dc_s32_t ref_seq_no;

    dc_s32_t len_prev, len_next;
    dc_s8_t  aligned_seed[2][SEED_LEN]; 
/*    
    dc_s8_t aligned_prev[2][2 * INPUT_CHUNK];  //aligment before seed
    dc_s8_t aligned_next[2][2 * INPUT_CHUNK]; 
    dc_s8_t	aligned_seqs[2][2 * INPUT_CHUNK];  //aligned_prev + aligned_seed + aligned_next

    dc_s8_t dstr[2 * INPUT_CHUNK];  //difference string, initialized empty
*/  
    dc_s32_t dstr_len = -1;

    if( inp_seq_len < SEED_LEN )
    {
        goto EXIT;
    }

    for(i = 0; i < SEED_LEN - 1; ++i)
    {
        seed_i <<= 2;
        seed_i  |= base_to_uint( inp_seq[i] );
    }
    for( ; i < inp_seq_len; ++i) 
    {
        seed_i <<= 2;
        seed_i  |= base_to_uint( inp_seq[i] );
        seed_i  &= seed_mask;

        seed_freq_tmp = seed_locs_freq_g[seed_i];

        for(j = 0; j < seed_freq_tmp; ++j) 
        {
            ref_seq_no  = seed_locs_g[seed_i][j].ref_seq_no;
            ref_seq	    = dc_c_ref_seqs_g[ ref_seq_no ];
            ref_seq_len = dc_c_ref_seqs_len_g[ ref_seq_no ];

            ref_stop   = seed_locs_g[seed_i][j].seed_stop;
            ref_start  = ref_stop - SEED_LEN + 1;

            inp_stop  = i;  //seed position
            inp_start = inp_stop - SEED_LEN + 1;

            if( test_seed(ref_seq_no, ref_seq, ref_seq_len, ref_start,
                          inp_seq_no, inp_seq, inp_seq_len, inp_start, *ref_start_gp) )
            {
                continue;   //seed don't fit
            }

            //seed fit
            strncpy(aligned_seed[0], ref_seq + ref_start, SEED_LEN);
            strncpy(aligned_seed[1], ref_seq + ref_start, SEED_LEN);

            attempt_ext(&ref_start, -1, ref_seq, ref_seq_len,
                        &inp_start, -1, inp_seq, inp_seq_len,
                        aligned_prev);

            attempt_ext(&ref_stop, 1, ref_seq, ref_seq_len,
                        &inp_stop, 1, inp_seq, inp_seq_len,
                        aligned_next);

            for(k = 0; k < 2; ++k) 
            {
                len_prev = strlen(aligned_prev[k]);
                len_next = strlen(aligned_next[k]);
                strncpy(aligned_seqs[k],                       aligned_prev[k], len_prev);
                strncpy(aligned_seqs[k] + len_prev,            aligned_seed[k], SEED_LEN);
                strncpy(aligned_seqs[k] + len_prev + SEED_LEN, aligned_next[k], len_next);

                aligned_seqs[k][len_prev + SEED_LEN + len_next] = '\0';
            }

            //fill the dstr lists
            dstr_len = make_dstr(dstr, aligned_seqs);

            goto EXIT;
        } //for all seeds ...
    } // for(i ... 

EXIT:
    if( dstr_len > inp_seq_len + 2 || dstr_len == -1 )
    {
        DC_PRINT("inp seq %d: no match find\n", inp_seq_no);

        write_link(-1, -1, -1, inp_seq_no, inp_seq_len, fout);

        dstr[0] = 's';
        dstr[1] = '0';
        strncpy(dstr + 2, inp_seq, inp_seq_len);
        dstr_len = inp_seq_len + 2;
        dstr[dstr_len] = '\0';
    }
    else
    {
        DC_PRINT("find match, ref: seq[%d] %d-%d %d, inp: seq[%d] %d-%d %d\n",
                 ref_seq_no, ref_start, ref_stop, ref_stop - ref_start + 1,
                 inp_seq_no, inp_start, inp_stop, inp_stop - inp_start + 1);

        DC_PRINT("dstr len is %d\n", dstr_len);

        write_link(ref_seq_no, ref_start, ref_stop - ref_start + 1,
                   inp_seq_no, inp_seq_len, fout);

        *ref_start_gp += INPUT_CHUNK;
    }
    write_dstr(dstr, dstr_len, dstr_encode, fout);

    DC_PRINT("find_match leave\n");
}

//compress the input file
dc_s32_t 
compress_input_file(dc_s8_t *data, dc_u32_t datasize, dc_s8_t *output_name)
{
    //bxz
    if (datasize == 0) {
        return -1;
    }
    dc_s32_t rc = 0;
    DC_PRINT("compress_input_file [%s] enter:\n", input_path);

    //global position
    dc_s32_t ref_start_g = 0;

    //by zhj
    dc_s8_t n_output_fname[FILE_PATH_LEN];
    dc_s32_t output_name_len = strlen(output_name);
    strncpy(n_output_fname, output_name, output_name_len + 1);
    strncpy(n_output_fname + output_name_len, "_n", 3);
    FILE *fout_n = NULL;
    dc_u32_t nCnt = 0, nLoc = 0;

/*
    //by zhj
    char tar_fname[FILE_PATH_LEN]; 
    strncpy(tar_fname, output_name, output_name_len + 1);
    strncpy(tar_fname + output_name_len, ".tar.bz2", 9);

    int cmd_len;
    char cmd1[FILE_PATH_LEN] = "tar -cjf "; 
    cmd_len = 9;
    strncpy(cmd1 + cmd_len, tar_fname, output_name_len + 8);
    cmd_len += output_name_len + 8;
    cmd1[cmd_len++] = ' ';
    strncpy(cmd1 + cmd_len, output_name, output_name_len + 1);
    cmd_len += output_name_len;
    cmd1[cmd_len++] = ' ';
    strncpy(cmd1 + cmd_len, n_output_fname, output_name_len + 3);

    char cmd2[FILE_PATH_LEN] = "rm -f ";       
    cmd_len = 6;
    strncpy(cmd2 + cmd_len, output_name, output_name_len + 1);
    cmd_len += output_name_len;
    cmd2[cmd_len++] = ' ';
    strncpy(cmd2 + cmd_len, n_output_fname, output_name_len + 3);
    //by zhj end
*/
	FILE *fin_inp = NULL, *fout = NULL;  //fout is global
	dc_s8_t *input_seq = NULL, *dstr = NULL, line_buf[LINE_BUF_LEN], tmp_buf[LINE_BUF_LEN];
	dc_s32_t input_seq_len, line_buf_len, tmp_buf_len; 
	dc_s32_t input_seq_no;
    dc_u8_t *dstr_encode = NULL;

    //just as args
    dc_s8_t *aligned_prev[2] = {NULL, NULL}, *aligned_next[2] = {NULL, NULL}, *aligned_seqs[2] = {NULL, NULL};
    dc_s32_t i, j, k, h;
    dc_u16_t head_line_len = 0;

    //malloc 
    input_seq   = (dc_s8_t *) malloc( sizeof(dc_s8_t) * (INPUT_CHUNK + LINE_BUF_LEN) );
    dstr        = (dc_s8_t *) malloc( sizeof(dc_s8_t) * (INPUT_CHUNK * 2 + 2) );
    dstr_encode = (dc_u8_t *) malloc( sizeof(dc_u8_t) * (INPUT_CHUNK     + 2) );
    if( input_seq == NULL || dstr == NULL || dstr_encode == NULL )
    {
        DC_ERROR("ERROR!: compress_input_file: malloc for input_seq error\n");
        rc = -1;
        goto EXIT;
    }
    for(i = 0; i < 2; ++i)
    {
        aligned_prev[i] = (dc_s8_t *) malloc( sizeof(dc_s8_t) * (INPUT_CHUNK * 2) );
        aligned_next[i] = (dc_s8_t *) malloc( sizeof(dc_s8_t) * (INPUT_CHUNK * 2) );
        aligned_seqs[i] = (dc_s8_t *) malloc( sizeof(dc_s8_t) * (INPUT_CHUNK * 2) );

        if( aligned_prev[i] == NULL || aligned_next[i] == NULL || aligned_seqs[i] == NULL )
        {
            DC_ERROR("ERROR!: compress_input_file: malloc for aligned_* error\n");
            rc = -1;
            goto EXIT;
        }
    }

    //open
    /*
    if( (fin_inp = fopen(data, "r")) == NULL ) 
    {
        DC_ERROR("ERROR!: compress_input_file: file %s open error\n", data);
        rc = -1;
        goto EXIT;
    }
     */

    if( (fout = fopen(output_name, "wb")) == NULL ) 
    {
        DC_ERROR("error: compress_input_file: output file open error\n");
        rc = -1;
        goto EXIT;
    }

    if( (fout_n = fopen(n_output_fname, "wb")) == NULL )  //by zhj
    {
        DC_ERROR("ERROR: compress_input_file: n output file open error\n");
        rc = -1;
        goto EXIT;
    }

    input_seq_no  = 0;  //fragment number: 0, 1, 2, ...
    input_seq_len = 0;

    
    //bxz
    if (data[0] == '>') {
        head_line_len = 0;
        memset(line_buf, 0, LINE_BUF_LEN);
        for (k = 0; k < datasize && data[k] != '\n'; ++k) {
            line_buf[k] = data[k];
            head_line_len++;
        }
        if (k < datasize) {
            //line_buf[k] = data[k];
            ++k;
        }
        fwrite(&head_line_len, sizeof(dc_u16_t), 1, fout);
        fwrite(line_buf, sizeof(dc_s8_t), head_line_len, fout);
        //goto EXIT;
    }
     
    /*
    fgets(line_buf, LINE_BUF_LEN, fin_inp);   //head line
    head_line_len = strlen(line_buf) - 1;
    fwrite(&head_line_len, sizeof(dc_u16_t), 1, fout);
    fwrite(line_buf, sizeof(dc_s8_t), head_line_len, fout);
    */

    while( k < datasize ){
        h = k;
        memset(line_buf, 0, LINE_BUF_LEN);
        for (; k < datasize && data[k] != '\n'; ++k) {
            ;
        }
        k++;
        memcpy(line_buf, data + h, k - h);  //copy an extra return
        //if (k < datasize) {
        //    memcpy(line_buf, data + h, k + 1 - h);  //copy an extra return
        //} else {
        //    memcpy(line_buf, data + h, k - h);
        //}
        
        for(j = 0; j < k - h && line_buf[j] != '\n' && line_buf[j] != 'N'; ++j, ++nLoc)
            ;
        if(j > 0 && nCnt > 0)//tag by weizheng
        {
            nLoc -= j;
            fwrite(&nLoc, sizeof(dc_u32_t), 1, fout_n);
            fwrite(&nCnt, sizeof(dc_u32_t), 1, fout_n);
            nLoc += j;
            nCnt = 0;
        }
        if(line_buf[j] == 'N')
        {
            i = j++;
            ++nCnt;
            
            while(line_buf[j] != '\n')
            {
                if(line_buf[j] == 'N')
                    ++nCnt;
                else
                {
                    if(nCnt > 0)
                    {
                        fwrite(&nLoc, sizeof(dc_u32_t), 1, fout_n);
                        fwrite(&nCnt, sizeof(dc_u32_t), 1, fout_n);
                        nCnt = 0;
                    }
                    ++nLoc;
                    line_buf[i++] = line_buf[j];
                }
                ++j;
            }
            
            line_buf_len = i;
        }
        else
        {
            if(nCnt > 0)
            {
                nLoc -= j;
                fwrite(&nLoc, sizeof(dc_u32_t), 1, fout_n);
                fwrite(&nCnt, sizeof(dc_u32_t), 1, fout_n);
                nLoc += j;
                nCnt = 0;
            }
            line_buf_len = j;
        }
        
        if(line_buf_len == 0)
            continue;
        
        strncpy(input_seq + input_seq_len, line_buf, line_buf_len);
        input_seq_len += line_buf_len;
        
        if( input_seq_len >= INPUT_CHUNK )
        {
            tmp_buf_len = input_seq_len - INPUT_CHUNK;  //copy the more bases into the tmp_buf
            strncpy(tmp_buf, input_seq + INPUT_CHUNK, tmp_buf_len);
            input_seq_len = INPUT_CHUNK;
            
            //printf("||||input_seq_no: %d\ninput_seq: %s[%d]\n", input_seq_no, input_seq, input_seq_len);
            //align
            find_match(input_seq_no, input_seq, input_seq_len, &ref_start_g,
                       aligned_prev, aligned_next, aligned_seqs, dstr, dstr_encode, fout);
            
            ++input_seq_no;
            
            strncpy(input_seq, tmp_buf, tmp_buf_len);  //copy back
            input_seq_len = tmp_buf_len;
        }
    }
    
    /*
    while( fgets(line_buf, LINE_BUF_LEN, fin_inp) != NULL ) 
    {
        for(j = 0; line_buf[j] != '\n' && line_buf[j] != 'N'; ++j, ++nLoc)
            ;
        if(j > 0 && nCnt > 0)//tag by weizheng
        {
            nLoc -= j;
            fwrite(&nLoc, sizeof(dc_u32_t), 1, fout_n);
            fwrite(&nCnt, sizeof(dc_u32_t), 1, fout_n);
            nLoc += j;
            nCnt = 0;
        }
        if(line_buf[j] == 'N')
        {
            i = j++;
            ++nCnt;

            while(line_buf[j] != '\n')
            {
                if(line_buf[j] == 'N')
                    ++nCnt;
                else
                {
                    if(nCnt > 0)
                    {
                        fwrite(&nLoc, sizeof(dc_u32_t), 1, fout_n);
                        fwrite(&nCnt, sizeof(dc_u32_t), 1, fout_n);
                        nCnt = 0;
                    }
                    ++nLoc;
                    line_buf[i++] = line_buf[j];
                }
                ++j;
            }

            line_buf_len = i;
        }
        else
        {
            if(nCnt > 0)
            {
                nLoc -= j;
                fwrite(&nLoc, sizeof(dc_u32_t), 1, fout_n);
                fwrite(&nCnt, sizeof(dc_u32_t), 1, fout_n);
                nLoc += j;
                nCnt = 0;
            }
            line_buf_len = j;
        }

        if(line_buf_len == 0)
            continue;

        strncpy(input_seq + input_seq_len, line_buf, line_buf_len);
        input_seq_len += line_buf_len;

        if( input_seq_len >= INPUT_CHUNK ) 
        {
            tmp_buf_len = input_seq_len - INPUT_CHUNK;  //copy the more bases into the tmp_buf
            strncpy(tmp_buf, input_seq + INPUT_CHUNK, tmp_buf_len);
            input_seq_len = INPUT_CHUNK;

            //align
            find_match(input_seq_no, input_seq, input_seq_len, &ref_start_g,
                       aligned_prev, aligned_next, aligned_seqs, dstr, dstr_encode, fout);

            ++input_seq_no;

            strncpy(input_seq, tmp_buf, tmp_buf_len);  //copy back
            input_seq_len = tmp_buf_len;
        }
    }
     */
    if(nCnt > 0)
    {
        fwrite(&nLoc, sizeof(dc_u32_t), 1, fout_n);
        fwrite(&nCnt, sizeof(dc_u32_t), 1, fout_n);
    }
    if( input_seq_len > 0 )
	{
        //printf("~~~~input_seq_no: %d\ninput_seq: %s[%d]\n", input_seq_no, input_seq, input_seq_len);
        //align
        find_match(input_seq_no, input_seq, input_seq_len, &ref_start_g,
                   aligned_prev, aligned_next, aligned_seqs, dstr, dstr_encode, fout);
	}

EXIT:
    //close
    if( fout != NULL )
    {
        fclose(fout);
        fout = NULL;
    }
    if( fin_inp != NULL )
    {
        fclose(fin_inp);
        fin_inp = NULL;
    }
    if( fout_n != NULL )
    {
        fclose(fout_n);
        fout_n = NULL;
    }
/*    
    system(cmd1);
    system(cmd2);
*/
    //free
    if( input_seq != NULL )
    {
        free(input_seq);
        input_seq = NULL;
    }
    for(i = 0; i < 2; ++i)
    {
        if( aligned_prev[i] != NULL )
        {
            free(aligned_prev[i]); 
            aligned_prev[i] = NULL;
        }
        if( aligned_next[i] != NULL )
        {
            free(aligned_next[i]); 
            aligned_next[i] = NULL;
        }
        if( aligned_seqs[i] != NULL )
        {
            free(aligned_seqs[i]); 
            aligned_seqs[i] = NULL;
        }
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

    DC_PRINT("compress_input_file leave\n");
    return rc;
}

#ifndef __DC_EXT_H_
#define __DC_EXT_H_

#include "dc_type.h"

#include "dc_c_io.h"

dc_s32_t
make_dstr(dc_s8_t *dstr, dc_s8_t *aligned_seqs[]);

void
attempt_ext(dc_s32_t *idxp0, dc_s32_t dir0, const dc_s8_t *s0, dc_s32_t len0,
            dc_s32_t *idxp1, dc_s32_t dir1, const dc_s8_t *s1, dc_s32_t len1,
            dc_s8_t  *aligned_s[]);

dc_s32_t
test_ext(dc_s32_t idx0, dc_s32_t dir0, dc_s8_t *s0, dc_s32_t len0,
         dc_s32_t idx1, dc_s32_t dir1, dc_s8_t *s1, dc_s32_t len1);

dc_s32_t
test_seed(dc_s32_t s0_no, dc_s8_t *s0, dc_s32_t len0, dc_s32_t start0,
          dc_s32_t s1_no, dc_s8_t *s1, dc_s32_t len1, dc_s32_t start1, dc_s32_t ref_start);

void 
find_match(dc_s32_t inp_seq_no, dc_s8_t *inp_seq, dc_s32_t inp_seq_len, dc_s32_t *ref_start_gp,
           dc_s8_t *aligned_prev[], dc_s8_t *aligned_next[], dc_s8_t *aligned_seqs[], 
           dc_s8_t *dstr, dc_u8_t *dstr_encode, FILE *fout);

//extern dc_s32_t compress_input_file(dc_s8_t *input_path, dc_s8_t *output_name);
extern dc_s32_t compress_input_file(dc_s8_t *data, dc_u32_t datasize, dc_s8_t *output_name);

#endif

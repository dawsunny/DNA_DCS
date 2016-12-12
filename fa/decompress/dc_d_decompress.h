#ifndef __DC_DECOMPRESS_H_
#define __DC_DECOMPRESS_H_

struct dc_link 
{
    dc_s32_t ref_seq_no;
    dc_s32_t ref_start;
    dc_s32_t ref_len;

    dc_s32_t inp_seq_no;
    dc_s32_t inp_len;
};
typedef struct dc_link dc_link_t;

void
make_orig_seq(const dc_s8_t *s0, dc_s32_t len0, dc_s8_t *dstr, dc_s32_t dstr_len, dc_s8_t *s1, dc_s32_t len1);

dc_s32_t
decompress_file(const dc_s8_t *file_path, const dc_s8_t *output_name);

extern dc_s32_t
analyze_path(dc_s8_t *input_path);

#endif

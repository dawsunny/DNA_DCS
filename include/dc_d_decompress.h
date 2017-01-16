#ifndef __DC_DECOMPRESS_H_
#define __DC_DECOMPRESS_H_
#include <string>
using namespace std;

void
make_orig_seq(const dc_s8_t *s0, dc_s32_t len0, dc_s8_t *dstr, dc_s32_t dstr_len, dc_s8_t *s1, dc_s32_t len1);

dc_s32_t
decompress_file(const dc_s8_t *file_path, string &output_name);

extern dc_s32_t
analyze_path(dc_s8_t *input_path, string &);

#endif

#ifndef __DC_IO_H_
#define __DC_IO_H_

#include "dc_type.h"

struct dc_seed_loc
{
    dc_s32_t ref_seq_no;
    dc_s32_t seed_stop;
};
typedef struct dc_seed_loc dc_seed_loc_t;

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
print_usage();

extern dc_s32_t
check_arg( dc_s32_t argc, dc_s8_t *argv[] );

extern dc_s32_t
read_ref_file( dc_s8_t *ref_file_path );

extern dc_s32_t
save_seed_loc();

extern void
free_memory();

extern void
write_link( dc_s32_t ref_seq_no, dc_s32_t ref_start, dc_s32_t ref_len, 
            dc_s32_t inp_seq_no, dc_s32_t inp_len, FILE *fout );

extern void
write_dstr( dc_s8_t *dstr, dc_s32_t dstr_len, dc_u8_t *dstr_encode, FILE *fout );

extern void
tar_compressed_files();

#endif

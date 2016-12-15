#ifndef __DC_IO_H_
#define __DC_IO_H_

#include "dc_type.h"
#include <vector> 
#include <string>
using namespace std;

void
dc_d_print_usage();

extern dc_s32_t
dc_d_check_arg( dc_s32_t argc, dc_s8_t *argv[] );

extern dc_s32_t
dc_d_read_ref_file( dc_s8_t *ref_file_path );

extern void 
write_to_file(vector<string> &inp_seqs, FILE *fout);

extern void
dc_d_free_memory();

#endif

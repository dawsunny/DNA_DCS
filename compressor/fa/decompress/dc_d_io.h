#ifndef __DC_IO_H_
#define __DC_IO_H_

#include "dc_type.h"
#include <vector> 
#include <string>
using namespace std;

void
print_usage();

extern dc_s32_t
check_arg( dc_s32_t argc, dc_s8_t *argv[] );

extern dc_s32_t
read_ref_file( dc_s8_t *ref_file_path );

extern void 
write_to_file(vector<string> &inp_seqs, FILE *fout);

extern void
free_memory();

#endif

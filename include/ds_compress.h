/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#ifndef _COMPRESS_H
#define _COMPRESS_H

#include <set>
#include <vector>
#include "ds_io.h"
#include "ds_DSRCFile.h"

using namespace std;

bool compress(char *in_file_name, char *out_file_name, bool try_lz, uint32 max_lz_memory);
bool decompress(char *in_file_name, char *out_file_name);
bool extract_record(char *in_file_name, char *out_file_name, uint64 rec_id);

#endif

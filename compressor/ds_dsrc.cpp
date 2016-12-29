/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#include "ds_config.h"
#include "ds_defs.h"
#include "ds_io.h"
#include "ds_dsrc.h"
#include "ds_compress.h"
#include "ds_huffman.h"
#include "ds_lz.h"

#include "dcs_const.h"
#include "dcs_type.h"

#include <iostream>
#include <time.h>
using namespace std;

// ********************************************************************************************
void usage()
{
	cout << "*** DNA Sequence Reads Compressor ***\n";
	cout << "Usage:\n";
	cout << "  dsrc <mode> [options] <input_file_name> <output_file_name>\n";
	cout << "Modes:\n";
	cout << "  e - compression\n";
	cout << "  d - decompression\n";
	cout << "Options:\n";
	cout << "  -l    - try LZ matches (about twice slower compression, but a bit stronger)\n";
	cout << "  -m<n> - use at most <n> MB for finding LZ matches (default: 2048)\n";
	cout << "          <n> must be at least 128; about the same amount of memory will be\n";
	cout << "          necessary to decompress the file\n";
	cout << "  -r<n> - decompress only record number n\n";
	cout << "Examples:\n";
	cout << "  dsrc e -l SRR001471 SRR001471.dsrc\n";
	cout << "  dsrc d SRR001471.dsrc SRR001471.out\n";
	cout << "  dsrc d -r532 SRR001471.dsrc SRR001471.out\n";
}


// ********************************************************************************************
//
// ********************************************************************************************
int dsrc_main(char *data, uint32 datasize,  dcs_u32_t op_type, char *input_file_name, char *output_file_name)
{
    /*
	if(argc < 4 || strcmp(argv[argc-1], argv[argc-2]) == 0)
	{
		usage();
		return 0;
	}
     */
	bool try_lz = false;
	int32 max_lz_memory = 2048;
	bool one_rec = false;
	uint32 rec_request;

	clock_t t1 = clock();

    /*
	for(int32 i = 2; i < argc-2; ++i)
	{
		if(strcmp(argv[i], "-l") == 0)
			try_lz = true;
		if(strncmp(argv[i], "-m", 2) == 0)
			max_lz_memory = atoi(argv[i]+2);
		if(strncmp(argv[i], "-r", 2) == 0)
		{
			rec_request = atoi(argv[i]+2);
			one_rec = true;
		}
	}
	if(max_lz_memory < 128)
		max_lz_memory = 128;
	if(max_lz_memory > (1 << 16))
		max_lz_memory = 1 << 16;
     */

    /*
	if(!strcmp(argv[1], "e"))
		compress(argv[argc-2], argv[argc-1], try_lz, max_lz_memory);
	if(!strcmp(argv[1], "d"))
	{
		if(one_rec)
			extract_record(argv[argc-2], argv[argc-1], rec_request);
		else
			decompress(argv[argc-2], argv[argc-1]);
	}
     */
    
    //bxz
    if (op_type == DCS_WRITE) {
        compress(data, datasize, output_file_name, 0, 0);
    } else if (op_type == DCS_READ) {
        decompress(input_file_name, output_file_name);
    }

	//cout << "Completed!\n";
	//cout << "Processing time: " << (double) (clock() - t1) / CLOCKS_PER_SEC << "s \n";

	return 0;
}


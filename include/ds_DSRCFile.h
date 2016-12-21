/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#ifndef _DSRCFILE_H
#define _DSRCFILE_H
#include "ds_lz.h"
#include "ds_superblock.h"
#include "ds_Trie.h"

class CDSRCFile {
	CBitStream bit_stream;
	CLZMatcher lz_matcher;

	vector<uint64> sb_file_pos;
	vector<uint32> block_file_pos;
	CSuperBlock superblock;
	CSuperBlock superblock_lz;		// superblock only for extracting LZ-matches of single records
	uint64 rec_count;
	uint64 sb_count;
	uint64 header_pos;
	uint64 header_sb_count;
	uint64 header_b_count;
	uint32 block_offset_bit_len;
	bool opened;
	t_mode mode;

	bool try_lz;
	int32 max_lz_memory;
    
    //by bxz
    CTrie tr;

public:
	CDSRCFile();
	~CDSRCFile();

	bool Create(char *file_name, bool _try_lz, uint32 _max_lz_memory);
	bool Open(char *file_name);
	bool OpenRA(char *file_name);
	bool Close();

	bool InsertRecord(const CFastqRecord &rec);
	bool ReadRecord(CFastqRecord &rec);
	bool ExtractRecord(CFastqRecord &rec, uint64 rec_id);
	int64 GetFilePos() {return bit_stream.file_pos;}
	int64 GetFileSize() {return bit_stream.file_size;}
};

#endif

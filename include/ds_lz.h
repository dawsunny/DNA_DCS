/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#ifndef _LZ_H
#define _LZ_H

#include "ds_defs.h"
#include <vector>

using namespace std;

struct CLZMatch {
	uint32 rec_no;
	uint32 rec_offset;
	uint32 length;
};

class CLZMatcher {
	typedef struct {
		uint64 buffer_pos;
		uint32 rec_no;
		uint32 rec_offset;
	} t_ht_entry;

	vector<unsigned char*> buffer;
	int32 cur_part;
	uint32 part_pos;

	t_ht_entry *hash_table;
	uint32 ht_size;
	uint32 ht_size_mask;
	uint32 ht_elems;
	uint32 ht_max_elems;
	double ht_max_fill_factor;

	void PrepareNextPart();
	void ResizeHT_Encode();
	bool InsertHT_Encode(unsigned char *str, uint64 buffer_pos, uint64 rec_no, uint32 rec_offset, 
		bool force_insert = false);

	void ResizeHT_Decode();
	void InsertHT_Decode(uint64 buffer_pos, uint32 rec_no);

	inline uint32 Hash1(unsigned char *str);
	inline uint32 Hash1_fast(unsigned char *str);
	inline uint32 Hash2(unsigned char *str);

	double log_phred[256];
	bool frozen;
	uint32 min_factor;
	uint32 hash_value;

	uint64 ht_memory;
	uint64 buf_memory;

	void PreparePhred();

public:
	CLZMatcher();
	~CLZMatcher();

	static uint32 MIN_MATCH_LEN;
	static uint32 LZ_STEP;

	static uint64 PART_SIZE;
	static uint64 PART_SIZE_EXP;
	static uint64 PART_SIZE_MASK;
	static uint32 MAX_ONE_ENTRY_COL;
	static double LOG_PROB_THR;

	uint64 max_lz_memory;

	bool InsertEncoding(uint64 rec_no, unsigned char *seq, uint32 seq_len, unsigned char *quality, uint32 quality_len);
	bool FindMatch(unsigned char *seq, uint32 seq_len, unsigned char *quality, uint32 quality_len, 
		uint32 &rec_no, uint32 &rec_offset, uint32 &match_len);

	bool InsertDecoding(uint64 rec_no, unsigned char *seq, uint32 seq_len, unsigned char *quality, uint32 quality_len);
	bool DecodeMatch(unsigned char *seq, uint32 rec_no, uint32 rec_offset, uint32 match_len, 
		uint32 seq_len);
};

// ********************************************************************************************
uint32 CLZMatcher::Hash1(unsigned char *str)
{
	// Bernstein
	hash_value = 0;
	for(uint32 i = 0; i < MIN_MATCH_LEN; ++i)
		hash_value = (hash_value << 5) + hash_value + str[i];

	return hash_value & ht_size_mask;
}

// ********************************************************************************************
// Berstein hash for 1-byte shift of window (faster than Hash1)
uint32 CLZMatcher::Hash1_fast(unsigned char *str)
{
	// Bernstein
	hash_value -= str[-1] * min_factor;
	hash_value = (hash_value << 5) + hash_value + str[MIN_MATCH_LEN-1];

	return hash_value & ht_size_mask;
}

// ********************************************************************************************
uint32 CLZMatcher::Hash2(unsigned char *str)
{
	return 1;
}

#endif

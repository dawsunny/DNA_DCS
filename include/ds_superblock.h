/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#ifndef _SUPERBLOCK_H
#define _SUPERBLOCK_H
#include "ds_block.h"
#include "ds_huffman.h"
#include "ds_lz.h"
#include "ds_Trie.h"
#include <vector>

using namespace std;

struct CSuperBlock {
	static const uint32 MAX_SIZE;
	static const uint32 MAX_FIELD_STAT_LEN;
	static const uint32 MAX_NUM_VAL_HUF;

	uint32 b_count;
	uint32 b_no;
	uint32 rec_count;
	uint32 rec_no;
	uint64 sb_start_rec_no;
	uint32 try_lz;

	vector<CBlock*> blocks;
	vector<CField> fields;
	uint32 n_field;
	bool is_num_fields_constant;

	uint32 dna_stats_mode;
	uint32 quality_stats_mode;
	bool is_length_variable;
	uint32 max_seq_length;
	uint32 global_max_seq_length;
	uint32 rec_length;				// only if constant
	bool is_delta;
	bool is_delta_constant;
	unsigned char seq_start;
	unsigned char qua_start;
	bool is_plus_only;
	bool is_dna_plain;

	uint32 no_of_symbols;
	uint32 no_of_qualities;

	vector<unsigned char> symbols;
	vector<unsigned char> qualities;
	uint32 max_run_len;

	unsigned char sym_code[256], qua_code[256];
	CHuffman::t_code **qua_huf_codes;
	CHuffman::t_code *raw_qua_huf_codes;
	vector<CHuffman*> Huffman_qua;
	CHuffman::t_code **run_huf_codes;
	CHuffman::t_code *raw_run_huf_codes;
	vector<CHuffman*> Huffman_run;

	uint32 **quality_stats;
	uint32 *raw_quality_stats;

	vector<uint32> dna_occ;
	vector<uint32> quality_occ;
	uint32 min_quality_len, max_quality_len;

	uint32 **run_stats;
	uint32 *raw_run_stats;

	vector<uint32> file_pos;
	uint64 sb_file_pos;

	bool InsertRecord(const CFastqRecord &rec);
	bool ReadRecord(CFastqRecord &rec);
	bool ExtractRecord(CFastqRecord &rec, uint64 rec_id, uint64 &lz_rec_id, uint32 &lz_rec_offset, uint32 &lz_match_len);
	bool Process(CBitStream &bit_stream, CLZMatcher &lz_matcher, bool _try_lz);
	bool Read(CBitStream &bit_stream, CLZMatcher &lz_matcher, int32 block_id = -1, uint64 file_pos = 0);
	void Reset(uint64 _sb_start_rec_no = 0, uint32 _global_max_seq_length = 0, int32 single_block = -1);

	void AnalyzeTitles(CBitStream &bit_stream);

	CSuperBlock(uint64 _sb_start_rec_no = 0, uint32 _global_max_seq_length = 0);
	~CSuperBlock();
    
    
    //by bxz
    bool Process1(CBitStream &bit_stream, CTrie &tr, bool _try_lz);
    bool Process2(CBitStream &bit_stream, CLZMatcher &lz_matcher, bool _try_lz);
	
private:
	void AnalyzeDelta();
	void MakeUndelta();
	void MakeDelta(int32 block_id = -1);

	void ComputeStatsDNAandPlus();
	void ComputeStatsQualityPlain();
	void ComputeStatsQualityRLE();
	void AllocateStats();
	void ComputeQualityPlainHuffman();
	void ComputeQualityRLEHuffman();

	uint32 ChooseQualityMethod();
	inline uint32 TruncHashLength(unsigned char *str, uint32 len);
	inline uint32 RLELength(unsigned char *str, uint32 len);

	void StoreDNA(CBitStream &bit_stream);
	void StoreQualityPlain(CBitStream &bit_stream);
	void StoreQualityRLE(CBitStream &bit_stream);
	void StoreTitle(CBitStream &bit_stream);
	void StoreStats(CBitStream &bit_stream, uint32 *stat, uint32 n_stat, bool use_bits = true, bool code_max = false);

	bool ReadDNA(CBitStream &bit_stream);
	bool ReadQualityPlain(CBitStream &bit_stream);
	bool ReadQualityRLE(CBitStream &bit_stream);
	bool ReadTitle(CBitStream &bit_stream);
	void ReadStats(CBitStream &bit_stream, uint32 *stat, uint32 n_stat, bool use_bits = true, bool code_max = false);
};

// ********************************************************************************************
uint32 CSuperBlock::TruncHashLength(unsigned char *str, uint32 len)
{
	uint32 r = 0;
	
	for(uint32 i = 0; i < len; ++i)
		if(str[i] != '#')
			r = i;

	return r;
}

// ********************************************************************************************
uint32 CSuperBlock::RLELength(unsigned char *str, uint32 len)
{
	uint32 r = 0;
	unsigned char prev = 0;

	for(uint32 i = 0; i < len; ++i)
	{
		if(str[i] != prev)
			r++;
		prev = str[i];
	}
	if(prev == '#')
		r--;

	return r;
}

#endif

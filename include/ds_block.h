/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#ifndef _BLOCK_H
#define _BLOCK_H

#include <set>
#include <vector>

using namespace std;

#include "ds_io.h"
#include "ds_huffman.h"
#include "ds_field.h"
#include "ds_lz.h"
#include "ds_Trie.h"

struct CBlock {
	static const uint32 MAX_SIZE;

	uint64 b_start_rec_no;
	uint32 rec_count;
	uint32 global_max_seq_length;
	vector<unsigned char> lz_match_occ;
	bool is_dna_plain;
	vector<uint32> rec_lengths;		// only if variable length of records
	vector<bool> is_plus_only;		// only if not only plus
	unsigned char *quality_data;
	unsigned char *dna_data;
	vector<CLZMatch> lz_matches;
	vector<CFastqRecord> records;
	vector<uint32> no_of_N;
	vector<int32> prev_value;
	uint32 quality_stats_mode;
	uint32 try_lz;

	unsigned char *qua_stream;
	unsigned char *run_stream;
	uint32 qua_stream_len;
	uint32 run_stream_len;

	bool Process(CBitStream &bit_stream, CLZMatcher &lz_matcher, vector<CField> &fields, uint32 n_field,
		bool _is_plus_only, bool _is_dna_plain, bool _is_length_variable,
		unsigned char *sym_code, unsigned char *qua_code, CHuffman::t_code **qua_huf_codes, 
		uint32 max_run_len, CHuffman::t_code **run_huf_codes, uint32 n_qualities, uint32 block_no, 
		uint32 _quality_stats_mode, uint32 _try_lz);
	bool Read(CBitStream &bit_stream, CLZMatcher &lz_matcher, vector<CField> &fields, uint32 n_field,
		bool _is_plus_only, bool _is_dna_plain, bool _is_length_variable,
		vector<unsigned char> &symbols, vector<unsigned char> &qualities, vector<CHuffman*> &Huffman_qua, 
		uint32 max_run_len, vector<CHuffman*> &Huffman_run, uint32 n_qualities, uint32 block_no, 
		uint32 _quality_stats_mode, uint32 _try_lz, bool extracting = false);

	void FindLZMatches(CLZMatcher &lz_matcher);
	void DecodeLZMatches(CLZMatcher &lz_matcher, uint32 rec_no);
	void DecodeLZMatches_Insert(CLZMatcher &lz_matcher, uint32 rec_no);

	CBlock(uint64 _b_start_rec_no, uint32 _rec_count = 0);
	~CBlock();
	void Reset(uint64 _b_start_rec_no, uint32 _global_max_seq_length, uint32 _rec_count = 0);
	void InsertRecord(const CFastqRecord &rec, vector<uint32> &dna_occ, bool &is_plus_only, vector<uint32> &quality_occ);

	bool MakeRLE();
	bool MakeUnRLE();
    
    // by bxz
    vector<CTrieNode> trie_matches;
    bool Process1(CBitStream &bit_stream, CTrie &tr, vector<CField> &fields, uint32 n_field,
                 bool _is_plus_only, bool _is_dna_plain, bool _is_length_variable,
                 unsigned char *sym_code, unsigned char *qua_code, CHuffman::t_code **qua_huf_codes,
                 uint32 max_run_len, CHuffman::t_code **run_huf_codes, uint32 n_qualities, uint32 block_no,
                 uint32 _quality_stats_mode, uint32 _try_lz);
    bool Process2(CBitStream &bit_stream, CLZMatcher &lz_matcher, vector<CField> &fields, uint32 n_field,
                 bool _is_plus_only, bool _is_dna_plain, bool _is_length_variable,
                 unsigned char *sym_code, uint32 block_no,
                 uint32 _quality_stats_mode, uint32 _try_lz);
    void FindLZMatches1(CTrie &tr);

private:
	bool StoreTitle(CBitStream &bit_stream, vector<CField> &fields, int32 block_no);
	bool StoreQualityPlain(CBitStream &bit_stream, unsigned char *qua_code, CHuffman::t_code **qua_huf_codes, int32 n_qualities);
	bool StoreQualityRLE(CBitStream &bit_stream, unsigned char *qua_code, CHuffman::t_code **qua_huf_codes, CHuffman::t_code **run_huf_codes, 
		int32 n_qualities);
	bool StoreDNAPlain(CBitStream &bit_stream, CLZMatcher &lz_matcher, bool _is_dna_plain, unsigned char *sym_code);

	bool ReadTitle(CBitStream &bit_stream, vector<CField> &fields, uint32 n_fields, int32 block_no);
	bool ReadQualityPlain(CBitStream &bit_stream, vector<unsigned char> &qualities, 
		vector<CHuffman*> &Huffman_qua, int32 n_qualities);
	bool ReadQualityRLE(CBitStream &bit_stream, vector<unsigned char> &qualities,
		vector<CHuffman*> &Huffman_qua, int32 n_qualities, vector<CHuffman*> &Huffman_run, int32 max_run_len);
	bool ReadDNAPlain(CBitStream &bit_stream, CLZMatcher &lz_matcher, bool _is_dna_plain, 
		vector<unsigned char> &symbols, bool extracting = false);
    
    //by bxz
    bool StoreDNAPlain1(CBitStream &bit_stream, CTrie &tr, bool _is_dna_plain, unsigned char *sym_code);
    void StoreQuality2(CBitStream &bit_stream);
    void BinaryHighEncode(CBitStream &bit_stream, uint32 value, uint32 high);
    void BinaryLowHighEncode(CBitStream &bit_stream, uint32 value, uint32 low, uint32 high);
    void UnaryEncode(CBitStream &bit_stream, uint32 value);
    void GammaEncode(CBitStream &bit_stream, uint32 value);
    void InterpEncode(CBitStream &bit_stream, vector<uint32> &v);
    void RecursiveInterpEncode(CBitStream &bit_stream, vector<uint32> &list, uint32 list_size,
                               uint32 left, uint32 right, uint32 left_value, uint32 right_value);
    uint32 FloorLog(uint32 value);
    uint32 CeilLog(uint32 value);

};
#endif

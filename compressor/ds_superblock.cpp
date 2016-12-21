/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#include "ds_superblock.h"
#include <algorithm>
#include <time.h>
#include <assert.h>

using namespace std;

const uint32 CSuperBlock::MAX_SIZE			 = (1 << 14);
const uint32 CSuperBlock::MAX_FIELD_STAT_LEN = 128;
const uint32 CSuperBlock::MAX_NUM_VAL_HUF	 = 512;

// ********************************************************************************************
//
// ********************************************************************************************
CSuperBlock::CSuperBlock(uint64 _sb_start_rec_no, uint32 _global_max_seq_length)
{
	b_count = 0;
	rec_count = 0;
	sb_start_rec_no = _sb_start_rec_no;

	n_field = 0;

	qua_huf_codes = NULL;
	raw_qua_huf_codes = NULL;

	run_huf_codes = NULL;
	raw_run_huf_codes = NULL;

	quality_stats = NULL;
	raw_quality_stats = NULL;

	run_stats = NULL;
	raw_run_stats = NULL;

	dna_occ.resize(256);
	quality_occ.resize(256);
	min_quality_len = (uint32) -1;
	max_quality_len = 0;

	global_max_seq_length = _global_max_seq_length;

	blocks.resize(CSuperBlock::MAX_SIZE / CBlock::MAX_SIZE);
	for(uint32 i = 0; i < CSuperBlock::MAX_SIZE / CBlock::MAX_SIZE; ++i)
		blocks[i] = NULL;
}

// ********************************************************************************************
CSuperBlock::~CSuperBlock() 
{
	for(uint32 i = 0; i < blocks.size(); ++i)
		delete blocks[i];
	blocks.clear();
	Reset();
}

// ********************************************************************************************
void CSuperBlock::Reset(uint64 _sb_start_rec_no, uint32 _global_max_seq_length, int32 single_block) 
{
	// Release memory for fields data
	fields.clear();
	n_field = 0;
	if(_global_max_seq_length)
		global_max_seq_length = _global_max_seq_length;

	// Release Huffman trees
	for(uint32 i = 0; i < Huffman_qua.size(); ++i)
		delete Huffman_qua[i];
	Huffman_qua.clear();
	delete[] qua_huf_codes;
	delete[] raw_qua_huf_codes;
	qua_huf_codes = NULL;
	raw_qua_huf_codes = NULL;

	// Release Huffman trees
	for(uint32 i = 0; i < Huffman_run.size(); ++i)
		delete Huffman_run[i];
	Huffman_run.clear();
	delete[] run_huf_codes;
	delete[] raw_run_huf_codes;
	run_huf_codes = NULL;
	raw_run_huf_codes = NULL;

	// Release blocks
	if(single_block < 0)
		for(uint32 i = 0; i < blocks.size(); ++i)
		{
			if(!blocks[i])
				blocks[i] = new CBlock(i * CBlock::MAX_SIZE + _sb_start_rec_no);
			blocks[i]->Reset(_sb_start_rec_no+i*CBlock::MAX_SIZE, global_max_seq_length);
		}
	else
	{
		if(!blocks[single_block])
			blocks[single_block] = new CBlock(single_block * CBlock::MAX_SIZE + _sb_start_rec_no);
		blocks[single_block]->Reset(_sb_start_rec_no+single_block*CBlock::MAX_SIZE, global_max_seq_length);
	}

	b_count = 0;

	// Release memory for DNA and quality stats
	if(quality_stats)
	{
		delete[] quality_stats;
		delete[] raw_quality_stats;
		quality_stats = NULL;
		raw_quality_stats = NULL;
	}

	if(run_stats)
	{
		delete[] run_stats;
		delete[] raw_run_stats;
		run_stats = NULL;
		raw_run_stats = NULL;
	}

	for(uint32 i = 0; i < 256; ++i)
	{
		dna_occ[i] = 0;
		quality_occ[i] = 0;
	}
	is_plus_only = true;
	min_quality_len = (uint32) -1;			// max ulong
	max_quality_len = 0;

	// Initialise record counters
	rec_count = 0;
	sb_start_rec_no = _sb_start_rec_no;
}

// ********************************************************************************************
bool CSuperBlock::InsertRecord(const CFastqRecord &rec)
{
	if(rec_count % CBlock::MAX_SIZE == 0)
		b_count++;

	if(rec.quality_len > max_quality_len)
		max_quality_len = rec.quality_len;
	if(rec.quality_len < min_quality_len)
		min_quality_len = rec.quality_len;
	blocks[b_count-1]->InsertRecord(rec, dna_occ, is_plus_only, quality_occ);

	rec_count++;

	return true;
}

// ********************************************************************************************
bool CSuperBlock::ReadRecord(CFastqRecord &rec)
{
	if(rec_no % CBlock::MAX_SIZE == 0)
		b_no++;
	if(rec_no >= rec_count)
		return false;

	rec.Set(blocks[b_no-1]->records[rec_no % CBlock::MAX_SIZE]);
	rec_no++;

	return true;
}

// ********************************************************************************************
bool CSuperBlock::ExtractRecord(CFastqRecord &rec, uint64 rec_id, uint64 &lz_rec_id, uint32 &lz_rec_offset, uint32 &lz_match_len)
{
	uint64 rec_id_in_sb = rec_id % CSuperBlock::MAX_SIZE;
	uint32 b_id_in_sb = (uint32) (rec_id_in_sb / CBlock::MAX_SIZE);
	uint32 rec_id_in_b = (uint32) (rec_id_in_sb % CBlock::MAX_SIZE);

	rec.Set(blocks[b_id_in_sb]->records[rec_id_in_b % CBlock::MAX_SIZE]);

	lz_match_len = blocks[b_id_in_sb]->lz_matches[rec_id_in_b].length;
	if(lz_match_len)
	{
		lz_rec_id     = blocks[b_id_in_sb]->lz_matches[rec_id_in_b].rec_no;
		lz_rec_offset = blocks[b_id_in_sb]->lz_matches[rec_id_in_b].rec_offset;
	}

	return true;
}

// ********************************************************************************************
bool CSuperBlock::Process(CBitStream &bit_stream, CLZMatcher &lz_matcher, bool _try_lz)
{
	AnalyzeDelta();
	if(is_delta)
		MakeUndelta();
	ComputeStatsDNAandPlus();
	quality_stats_mode = ChooseQualityMethod();
	if(quality_stats_mode == 2)
		ComputeStatsQualityRLE();
	else
		ComputeStatsQualityPlain();

	try_lz = _try_lz;
	if(max_seq_length <= CLZMatcher::MIN_MATCH_LEN)
		try_lz = false;
	
	sb_file_pos = bit_stream.file_pos;
	bit_stream.PutWord(rec_count);
	bit_stream.PutByte((unsigned char) is_plus_only);
	bit_stream.PutByte((unsigned char) is_length_variable);
	bit_stream.PutWord(max_seq_length);
	bit_stream.PutWord(global_max_seq_length);
	bit_stream.PutByte((unsigned char) dna_stats_mode);
	bit_stream.PutByte((unsigned char) try_lz);
	bit_stream.PutByte((unsigned char) symbols.size());
	bit_stream.PutByte((unsigned char) quality_stats_mode);
	bit_stream.PutByte((unsigned char) qualities.size());
	bit_stream.PutByte((unsigned char) is_delta);
	if(is_delta)
	{
		bit_stream.PutByte(is_delta_constant);
		if(is_delta_constant)
		{
			bit_stream.PutByte(seq_start);
			bit_stream.PutByte(qua_start);
		}
	}
	if(quality_stats_mode == 2)
		bit_stream.PutByte((unsigned char) max_run_len);

	StoreDNA(bit_stream);

	if(quality_stats_mode == 2)
		StoreQualityRLE(bit_stream);
	else
		StoreQualityPlain(bit_stream);

	StoreTitle(bit_stream);

	if(quality_stats_mode == 2)
		ComputeQualityRLEHuffman();
	else
		ComputeQualityPlainHuffman();

	// Block data
	file_pos.clear();
	for(uint32 i = 0; i < b_count; ++i)
	{
		file_pos.push_back((uint32) (bit_stream.file_pos - sb_file_pos));
		blocks[i]->global_max_seq_length = global_max_seq_length;
		blocks[i]->Process(bit_stream, lz_matcher, fields, n_field,
			is_plus_only, is_dna_plain, is_length_variable, sym_code, qua_code, qua_huf_codes, max_run_len, run_huf_codes, 
			(uint32) qualities.size(), i, quality_stats_mode, try_lz);
	}
	
	delete[] qua_huf_codes;
	delete[] raw_qua_huf_codes;
	qua_huf_codes = NULL;
	raw_qua_huf_codes = NULL;
	delete[] run_huf_codes;
	delete[] raw_run_huf_codes;
	run_huf_codes = NULL;
	raw_run_huf_codes = NULL;

	return true;
}

// ********************************************************************************************
bool CSuperBlock::Process1(CBitStream &bit_stream, CTrie &tr, bool _try_lz)
{
    /*
    AnalyzeDelta();
    if(is_delta)
        MakeUndelta();
     */
    is_delta = false;
    ComputeStatsDNAandPlus();
    quality_stats_mode = ChooseQualityMethod();
    if(quality_stats_mode == 2)
        ComputeStatsQualityRLE();
    else
        ComputeStatsQualityPlain();
    
    try_lz = _try_lz;
    if(max_seq_length <= CLZMatcher::MIN_MATCH_LEN)
        try_lz = false;
    
    sb_file_pos = bit_stream.file_pos;
    bit_stream.PutWord(rec_count);
    bit_stream.PutByte((unsigned char) is_plus_only);
    bit_stream.PutByte((unsigned char) is_length_variable);
    bit_stream.PutWord(max_seq_length);
    bit_stream.PutWord(global_max_seq_length);
    bit_stream.PutByte((unsigned char) dna_stats_mode);
    bit_stream.PutByte((unsigned char) try_lz);
    bit_stream.PutByte((unsigned char) symbols.size());
    bit_stream.PutByte((unsigned char) quality_stats_mode);
    bit_stream.PutByte((unsigned char) qualities.size());
    bit_stream.PutByte((unsigned char) is_delta);
    /*
    if(is_delta)
    {
        bit_stream.PutByte(is_delta_constant);
        if(is_delta_constant)
        {
            bit_stream.PutByte(seq_start);
            bit_stream.PutByte(qua_start);
        }
    }
     */
    
    if(quality_stats_mode == 2)
        bit_stream.PutByte((unsigned char) max_run_len);
    
    
    StoreDNA(bit_stream);
    
    if(quality_stats_mode == 2)
        StoreQualityRLE(bit_stream);
    else
        StoreQualityPlain(bit_stream);
    
    StoreTitle(bit_stream);
    
    if(quality_stats_mode == 2)
        ComputeQualityRLEHuffman();
    else
        ComputeQualityPlainHuffman();
    
    // Block data
    file_pos.clear();
    for(uint32 i = 0; i < b_count; ++i)
    {
        file_pos.push_back((uint32) (bit_stream.file_pos - sb_file_pos));
        blocks[i]->global_max_seq_length = global_max_seq_length;
        blocks[i]->Process1(bit_stream, tr, fields, n_field,
                           is_plus_only, is_dna_plain, is_length_variable, sym_code, qua_code, qua_huf_codes, max_run_len, run_huf_codes,
                           (uint32) qualities.size(), i, quality_stats_mode, try_lz);
    }
    
    delete[] qua_huf_codes;
    delete[] raw_qua_huf_codes;
    qua_huf_codes = NULL;
    raw_qua_huf_codes = NULL;
    delete[] run_huf_codes;
    delete[] raw_run_huf_codes;
    run_huf_codes = NULL;
    raw_run_huf_codes = NULL;
    
    return true;
}

// ********************************************************************************************
bool CSuperBlock::Process2(CBitStream &bit_stream, CLZMatcher &lz_matcher, bool _try_lz)
{
    AnalyzeDelta();
    if(is_delta)
        MakeUndelta();
    ComputeStatsDNAandPlus();
    quality_stats_mode = 1;//ChooseQualityMethod();
    /*
    if(quality_stats_mode == 2)
        ComputeStatsQualityRLE();
    else
        ComputeStatsQualityPlain();
     */
    
    try_lz = _try_lz;
    if(max_seq_length <= CLZMatcher::MIN_MATCH_LEN)
        try_lz = false;
    
    sb_file_pos = bit_stream.file_pos;
    bit_stream.PutWord(rec_count);
    bit_stream.PutByte((unsigned char) is_plus_only);
    bit_stream.PutByte((unsigned char) is_length_variable);
    bit_stream.PutWord(max_seq_length);
    bit_stream.PutWord(global_max_seq_length);
    bit_stream.PutByte((unsigned char) dna_stats_mode);
    bit_stream.PutByte((unsigned char) try_lz);
    bit_stream.PutByte((unsigned char) symbols.size());
    bit_stream.PutByte((unsigned char) quality_stats_mode);
    bit_stream.PutByte((unsigned char) qualities.size());
    bit_stream.PutByte((unsigned char) is_delta);
    /*
    if(is_delta)
    {
        bit_stream.PutByte(is_delta_constant);
        if(is_delta_constant)
        {
            bit_stream.PutByte(seq_start);
            bit_stream.PutByte(qua_start);
        }
    }
     */
    /*
    if(quality_stats_mode == 2)
        bit_stream.PutByte((unsigned char) max_run_len);
    */
     
    StoreDNA(bit_stream);
    
    /*
    if(quality_stats_mode == 2)
        StoreQualityRLE(bit_stream);
    else
        StoreQualityPlain(bit_stream);
    */
    
    StoreTitle(bit_stream);
    
    /*
    if(quality_stats_mode == 2)
        ComputeQualityRLEHuffman();
    else
        ComputeQualityPlainHuffman();
    */
    
    // Block data
    file_pos.clear();
    for(uint32 i = 0; i < b_count; ++i)
    {
        file_pos.push_back((uint32) (bit_stream.file_pos - sb_file_pos));
        blocks[i]->global_max_seq_length = global_max_seq_length;
        blocks[i]->Process2(bit_stream, lz_matcher, fields, n_field,
                           is_plus_only, is_dna_plain, is_length_variable, sym_code, i, quality_stats_mode, try_lz);
    }
    
    delete[] qua_huf_codes;
    delete[] raw_qua_huf_codes;
    qua_huf_codes = NULL;
    raw_qua_huf_codes = NULL;
    delete[] run_huf_codes;
    delete[] raw_run_huf_codes;
    run_huf_codes = NULL;
    raw_run_huf_codes = NULL;
    
    return true;
}

// ********************************************************************************************
bool CSuperBlock::Read(CBitStream &bit_stream, CLZMatcher &lz_matcher, int32 block_id, uint64 file_pos)
{
	uint32 tmp;
	uint32 i;

	bit_stream.GetWord(rec_count);
	b_count = (rec_count + CBlock::MAX_SIZE - 1) / CBlock::MAX_SIZE;

	bit_stream.GetByte(tmp);
	is_plus_only = tmp != 0;
	bit_stream.GetByte(tmp);
	is_length_variable = tmp != 0;
	bit_stream.GetWord(max_seq_length);
	bit_stream.GetWord(global_max_seq_length);
	bit_stream.GetByte(dna_stats_mode);
	bit_stream.GetByte(try_lz);
	bit_stream.GetByte(no_of_symbols);
	bit_stream.GetByte(quality_stats_mode);
	bit_stream.GetByte(no_of_qualities);
	bit_stream.GetByte(tmp);

	is_delta = tmp != 0;
	if(is_delta)
	{
		bit_stream.GetByte(tmp);
		is_delta_constant = tmp != 0;
		if(is_delta_constant)
		{
			bit_stream.GetByte(tmp);
			seq_start = (unsigned char) tmp;
			bit_stream.GetByte(tmp);
			qua_start = (unsigned char) tmp;
		}
	}
	if(quality_stats_mode == 2)
		bit_stream.GetByte(max_run_len);

	AllocateStats();

	ReadDNA(bit_stream);
	if(quality_stats_mode == 2)
		ReadQualityRLE(bit_stream);
	else
		ReadQualityPlain(bit_stream);
				
	ReadTitle(bit_stream);

	if(quality_stats_mode == 2)
		ComputeQualityRLEHuffman();
	else
		ComputeQualityPlainHuffman();

	for(i = 0; i < fields.size(); ++i)
		fields[i].block_desc.resize(b_count);

	// Block data
	if(block_id == -1)
		for(i = 0; i < b_count; ++i)
		{
			uint32 block_recs = rec_count - i*CBlock::MAX_SIZE;
			if(block_recs > CBlock::MAX_SIZE)
				block_recs = CBlock::MAX_SIZE;
			blocks[i]->Reset(sb_start_rec_no+i*CBlock::MAX_SIZE, global_max_seq_length - (is_delta && is_delta_constant), block_recs);
			blocks[i]->Read(bit_stream, lz_matcher, fields, n_field,
				is_plus_only, is_dna_plain, is_length_variable, symbols, qualities, Huffman_qua,  max_run_len, Huffman_run, 
				(uint32) qualities.size(), i, quality_stats_mode, try_lz, false);
		}
	else
	{
		bit_stream.SetPos(file_pos);
		uint32 block_recs = rec_count - block_id*CBlock::MAX_SIZE;
		if(block_recs > CBlock::MAX_SIZE)
			block_recs = CBlock::MAX_SIZE;
		blocks[block_id]->Reset(sb_start_rec_no+block_id*CBlock::MAX_SIZE, global_max_seq_length - (is_delta && is_delta_constant), block_recs);
		blocks[block_id]->Read(bit_stream, lz_matcher, fields, n_field,
			is_plus_only, is_dna_plain, is_length_variable, symbols, qualities, Huffman_qua,  max_run_len, Huffman_run, 
			(uint32) qualities.size(), block_id, quality_stats_mode, try_lz, true);
	}

	if(is_delta)
		MakeDelta((int32) block_id);

	delete[] qua_huf_codes;
	delete[] raw_qua_huf_codes;
	qua_huf_codes = NULL;
	raw_qua_huf_codes = NULL;
	delete[] run_huf_codes;
	delete[] raw_run_huf_codes;
	run_huf_codes = NULL;
	raw_run_huf_codes = NULL;

	if(quality_stats)
	{
		delete[] quality_stats;
		delete[] raw_quality_stats;
		quality_stats = NULL;
		raw_quality_stats = NULL;
	}

	rec_no = 0;
	b_no   = 0;

	fields.clear();

	return true;
}

// ********************************************************************************************
void CSuperBlock::ComputeStatsDNAandPlus()
{
	bool alphabet_seq[256] = {false};

	is_length_variable = min_quality_len != max_quality_len;
	max_seq_length = max_quality_len;
	if(max_seq_length > global_max_seq_length)
		global_max_seq_length = max_seq_length;

	symbols.clear();

	int32 sym_idx = 0;
	for(uint32 i = 0; i < 256; ++i)
		if(dna_occ[i])
		{
			symbols.push_back((unsigned char) i);
			sym_code[i] = sym_idx++;
		}

	dna_stats_mode = 0;				// global
	is_dna_plain = true;
}

// ********************************************************************************************
void CSuperBlock::ComputeStatsQualityPlain()
{
	bool alphabet_qua[256] = {false};
	uint32 i, j, k;

	// Find alphabets qualities
	// Find also seq_len
	for(i = 0; i < b_count; ++i)
		for(j = 0; j < blocks[i]->records.size(); ++j)
		{
			unsigned char *cur_quality = blocks[i]->records[j].quality;
			uint32 cur_quality_len = blocks[i]->records[j].quality_len;
			if(quality_stats_mode == 1)
				for(k = 0; k < cur_quality_len; ++k)
					alphabet_qua[cur_quality[k]] = true;
			else
			{
				uint32 r = 0;
				for(k = 0; k < cur_quality_len; ++k)
				{
					alphabet_qua[cur_quality[k]] = true;
					if(cur_quality[k] != '#')
						r = k;
				}
				blocks[i]->records[j].rec_th_len = r+1;
			}
		}

	qualities.clear();

	uint32 qua_idx = 0;
	for(i = 0; i < 256; ++i)
		if(alphabet_qua[i])
		{
			qualities.push_back((unsigned char) i);
			qua_code[i] = (unsigned char) qua_idx++;
		}

	quality_stats = new uint32*[max_seq_length+1];
	raw_quality_stats = new uint32[(max_seq_length+1)*qua_idx];
	
	for(i = 0; i <= max_seq_length; ++i)
		quality_stats[i] = raw_quality_stats+i*qua_idx;
	for(i = 0; i < (max_seq_length+1)*qua_idx; ++i)
		raw_quality_stats[i] = 0;
	
	for(i = 0; i < b_count; ++i)
		for(j = 0; j < blocks[i]->records.size(); ++j)
		{
			uint32 qua_len = blocks[i]->records[j].quality_len;
			uint32 qua_len_th = blocks[i]->records[j].rec_th_len;
			unsigned char *cur_quality = blocks[i]->records[j].quality;
			if(quality_stats_mode == 1)
				for(k = 0; k < qua_len; ++k)
				{
					quality_stats[k+1][qua_code[cur_quality[k]]]++;
					quality_stats[0][qua_code[cur_quality[k]]]++;
				}
			else
				for(uint32 k = 0; k < qua_len_th; ++k)
				{
					quality_stats[k+1][qua_code[cur_quality[k]]]++;
					quality_stats[0][qua_code[cur_quality[k]]]++;
				}
		}
}

// ********************************************************************************************
void CSuperBlock::ComputeStatsQualityRLE()
{
	uint32 i, j;

	for(i = 0; i < b_count; ++i)
		blocks[i]->MakeRLE();

	bool alphabet_qua[256] = {false};
	bool alphabet_run_len[256] = {false};
	// Find qualities alphabet and run alphabet
	for(i = 0; i < b_count; ++i)
	{
		unsigned char *cur_run_stream = blocks[i]->run_stream;
		unsigned char *cur_qua_stream = blocks[i]->qua_stream;
		uint32 cur_run_stream_len = blocks[i]->run_stream_len;
		for(j = 0; j < cur_run_stream_len; ++j)
		{
			alphabet_qua[cur_qua_stream[j]]     = true;
			alphabet_run_len[cur_run_stream[j]] = true;
		}
	}

	qualities.clear();
	max_run_len = 0;
	
	uint32 qua_idx = 0;
	qualities.push_back(0);
	qua_code[0] = (unsigned char) qua_idx++;
	for(i = 0; i < 256; ++i)
	{
		if(alphabet_qua[i])
		{
			qualities.push_back((unsigned char) i);
			qua_code[i] = (unsigned char) qua_idx++;
		}
		if(alphabet_run_len[i])
			max_run_len = i;
	}
	max_run_len++;

	quality_stats = new uint32*[qua_idx];
	raw_quality_stats = new uint32[qua_idx*qua_idx];
	run_stats = new uint32*[qua_idx];
	raw_run_stats = new uint32[qua_idx * max_run_len];
	
	for(i = 0; i < qua_idx; ++i)
		quality_stats[i] = raw_quality_stats+i*qua_idx;
	for(i = 0; i < qua_idx; ++i)
		run_stats[i] = raw_run_stats+i*max_run_len;

	for(i = 0; i < qua_idx*qua_idx; ++i)
		raw_quality_stats[i] = 0;
	for(i = 0; i < max_run_len*qua_idx; ++i)
		raw_run_stats[i] = 0;
	
	for(i = 0; i < b_count; ++i)
	{
		unsigned char *qua_stream = blocks[i]->qua_stream;
		unsigned char *run_stream = blocks[i]->run_stream;
		unsigned char prev = 0;
		for(j = 0; j < blocks[i]->qua_stream_len; ++j)
		{
			assert(prev < qua_idx);
			assert(qua_code[qua_stream[j]] < qua_idx);
			quality_stats[prev][qua_code[qua_stream[j]]]++;
			run_stats[qua_code[qua_stream[j]]][run_stream[j]]++;
			prev = qua_code[qua_stream[j]];
		}
	}
}

// ********************************************************************************************
void CSuperBlock::AllocateStats()
{
	uint32 i;

	bool alphabet_seq[256] = {false};
	bool alphabet_qua[256] = {false};
	
	is_plus_only = true;
	is_dna_plain = true;

	symbols.clear();
	qualities.clear();

	if(quality_stats_mode == 2)
	{
		quality_stats     = new uint32*[no_of_qualities];
		raw_quality_stats = new uint32[no_of_qualities*no_of_qualities];
		run_stats         = new uint32*[no_of_qualities];
		raw_run_stats     = new uint32[no_of_qualities*max_run_len];
		for(i = 0; i < no_of_qualities; ++i)
		{
			quality_stats[i] = raw_quality_stats+i*no_of_qualities;
			run_stats[i] = raw_run_stats+i*max_run_len;
		}
		for(i = 0; i < no_of_qualities*no_of_qualities; ++i)
			raw_quality_stats[i] = 0;
		for(i = 0; i < max_run_len*no_of_qualities; ++i)
			raw_run_stats[i] = 0;
	}
	else
	{
		quality_stats     = new uint32*[max_seq_length+1];
		raw_quality_stats = new uint32[(max_seq_length+1)*no_of_qualities];
		for(i = 0; i <= max_seq_length; ++i)
			quality_stats[i] = raw_quality_stats+i*no_of_qualities;
		for(i = 0; i < (max_seq_length+1)*no_of_qualities; ++i)
			raw_quality_stats[i] = 0;
	}	
}

// ********************************************************************************************
void CSuperBlock::AnalyzeDelta()
{
	is_delta = false;
	is_delta_constant = true;
	seq_start = blocks[0]->records[0].seq[0];
	qua_start = blocks[0]->records[0].quality[0];

	for(uint32 i = 0; i < b_count; ++i)
	{
		for(uint32 j = 0; j < blocks[i]->rec_count; ++j)
		{
			if(blocks[i]->records[j].seq[1] >= '0' && blocks[i]->records[j].seq[1] <= '3')
				is_delta = true;
			if(is_delta)
			{
				is_delta_constant &= blocks[i]->records[j].seq[0]     == seq_start;
				is_delta_constant &= blocks[i]->records[j].quality[0] == qua_start;
			}
		}
	}
}

// ********************************************************************************************
void CSuperBlock::MakeUndelta()
{
	uint32 translation = is_delta_constant;
	uint32 i, j, k;
	unsigned char symbol;

	static char delta_A[] = {'A', 'C', 'G', 'T'};
	static char delta_C[] = {'C', 'A', 'T', 'G'};
	static char delta_G[] = {'G', 'T', 'A', 'C'};
	static char delta_T[] = {'T', 'G', 'C', 'A'};

	for(i = 0; i < b_count; ++i)
	{
		for(j = 0; j < blocks[i]->rec_count; ++j)
		{
			unsigned char *seq = blocks[i]->records[j].seq;
			unsigned char *qua = blocks[i]->records[j].quality;
			symbol = seq[0];
			for(k = 1; k < blocks[i]->records[j].seq_len; ++k)
			{
				if(symbol == 'A')
					seq[k-translation] = symbol = delta_A[seq[k]-'0'];
				else if(symbol == 'C')
					seq[k-translation] = symbol = delta_C[seq[k]-'0'];
				else if(symbol == 'G')
					seq[k-translation] = symbol = delta_G[seq[k]-'0'];
				else
					seq[k-translation] = symbol = delta_T[seq[k]-'0'];
				qua[k-translation] = qua[k];
			}
			if(is_delta_constant)
			{
				seq[--blocks[i]->records[j].seq_len]     = 0;
				qua[--blocks[i]->records[j].quality_len] = 0;
			}
		}
	}

	for(i = 0; i < 256; ++i)
		dna_occ[i] = 0;
	dna_occ['A'] = 1;
	dna_occ['C'] = 1;
	dna_occ['G'] = 1;
	dna_occ['T'] = 1;
}

// ********************************************************************************************
void CSuperBlock::MakeDelta(int32 block_id)
{
	uint32 translation = is_delta_constant;
	uint32 i, j, k;
	unsigned char symbol;

	static char delta_A[] = {'A', 'C', 'G', 'T'};
	static char delta_C[] = {'C', 'A', 'T', 'G'};
	static char delta_G[] = {'G', 'T', 'A', 'C'};
	static char delta_T[] = {'T', 'G', 'C', 'A'};

	unsigned char *tmp_seq = new unsigned char[global_max_seq_length+2];
	unsigned char *tmp_qua = new unsigned char[global_max_seq_length+2];

	uint32 i_start = 0;
	uint32 i_stop  = b_count;

	if(block_id >= 0)
	{
		i_start = block_id;
		i_stop  = i_start+1;
	}

	for(i = i_start; i < i_stop; ++i)
	{
		for(j = 0; j < blocks[i]->rec_count; ++j)
		{
			unsigned char *seq = blocks[i]->records[j].seq;
			unsigned char *qua = blocks[i]->records[j].quality;

			if(is_delta_constant)
			{
				tmp_seq[0] = symbol = seq_start;
				tmp_qua[0] = qua_start;
			}
			else
				tmp_seq[0] = symbol = seq[0];

			for(k = 1-translation; k < blocks[i]->records[j].seq_len; ++k)
			{
				if(symbol == 'A')
					tmp_seq[k+translation] = (unsigned char) (find(delta_A, delta_A+4, seq[k]) - delta_A) + '0';
				else if(symbol == 'C')
					tmp_seq[k+translation] = (unsigned char) (find(delta_C, delta_C+4, seq[k]) - delta_C) + '0';
				else if(symbol == 'G')
					tmp_seq[k+translation] = (unsigned char) (find(delta_G, delta_G+4, seq[k]) - delta_G) + '0';
				else 
					tmp_seq[k+translation] = (unsigned char) (find(delta_T, delta_T+4, seq[k]) - delta_T) + '0';
				tmp_qua[k+translation] = qua[k];
				symbol = seq[k];
			}
			copy(tmp_seq, tmp_seq+blocks[i]->records[j].seq_len+translation, seq);
			copy(tmp_qua, tmp_qua+blocks[i]->records[j].seq_len+translation, qua);
			if(is_delta_constant)
			{
				seq[++blocks[i]->records[j].seq_len]     = 0;
				qua[++blocks[i]->records[j].quality_len] = 0;
			}
		}
	}

	delete[] tmp_seq;
	delete[] tmp_qua;
}

// ********************************************************************************************
void CSuperBlock::ComputeQualityPlainHuffman()
{
	uint32 i, j;

	qua_huf_codes     = new CHuffman::t_code*[max_seq_length+1];
	raw_qua_huf_codes = new CHuffman::t_code[(max_seq_length+1)*qualities.size()];
	for(i = 0; i <= max_seq_length; ++i)
		qua_huf_codes[i] = &raw_qua_huf_codes[i*qualities.size()];

	for(i = 0; i <= max_seq_length; ++i)
	{
		CHuffman *huf;
		Huffman_qua.push_back(huf = new CHuffman((uint32) qualities.size()));
		huf->Restart();
		for(j = 0; j < qualities.size(); ++j)
			huf->Insert(quality_stats[i][j]);
		CHuffman::t_code* codes = huf->Complete();

		for(j = 0; j < qualities.size(); ++j)
			qua_huf_codes[i][j] = codes[j];
	}
}

// ********************************************************************************************
void CSuperBlock::ComputeQualityRLEHuffman()
{
	uint32 i, j;

	qua_huf_codes = new CHuffman::t_code*[qualities.size()];
	raw_qua_huf_codes = new CHuffman::t_code[(qualities.size())*qualities.size()];
	for(i = 0; i < qualities.size(); ++i)
		qua_huf_codes[i] = &raw_qua_huf_codes[i*qualities.size()];
	
	run_huf_codes = new CHuffman::t_code*[(qualities.size())];
	raw_run_huf_codes = new CHuffman::t_code[max_run_len*qualities.size()];
	for(i = 0; i < qualities.size(); ++i)
		run_huf_codes[i] = &raw_run_huf_codes[i*max_run_len];

	for(i = 0; i < qualities.size(); ++i)
	{
		CHuffman *huf;
		Huffman_qua.push_back(huf = new CHuffman((uint32) qualities.size()));
		huf->Restart();
		for(j = 0; j < qualities.size(); ++j)
			huf->Insert(quality_stats[i][j]);
		CHuffman::t_code* codes = huf->Complete();

		for(j = 0; j < qualities.size(); ++j)
			qua_huf_codes[i][j] = codes[j];
	}

	for(i = 0; i < qualities.size(); ++i)
	{
		CHuffman *huf;
		Huffman_run.push_back(huf = new CHuffman(max_run_len));
		huf->Restart();
		for(j = 0; j < max_run_len; ++j)
			huf->Insert(run_stats[i][j]);
		CHuffman::t_code* codes = huf->Complete();

		for(j = 0; j < max_run_len; ++j)
			run_huf_codes[i][j] = codes[j];
	}
}

// ********************************************************************************************
void CSuperBlock::AnalyzeTitles(CBitStream &bit_stream)
{
	char *c_separators = " ._,=:/-";
	vector<unsigned char> separators(c_separators, c_separators+strlen(c_separators)+1);
	uint32 sep_num = (uint32) separators.size();
	fields.clear();
	CFastqRecord &rec_zero = blocks[0]->records[0];
	uint32 start_pos = 0;
	uint32 i, j, k, x;

	n_field = 0;

	// Find fields in 0th record;
	for(i = 0; i <= rec_zero.title_len; ++i)
	{
		if(!count(separators.begin(), separators.end(), rec_zero.title[i]))
			continue;

		fields.push_back(CField());

		fields[n_field].data			  = new unsigned char[i - start_pos+1];
		memcpy(fields[n_field].data, rec_zero.title+start_pos, i-start_pos);
		fields[n_field].data[i-start_pos] = '\0';
		fields[n_field].len               = i - start_pos;
		fields[n_field].max_len			  = fields[n_field].len;
		fields[n_field].min_len			  = fields[n_field].len;
		fields[n_field].sep               = rec_zero.title[i];
		fields[n_field].is_constant	      = true;
		fields[n_field].is_len_constant   = true;
		fields[n_field].is_numeric		  = fields[n_field].is_num_field();
		if(fields[n_field].is_numeric)
		{
			fields[n_field].min_value = fields[n_field].to_num();
			fields[n_field].max_value = fields[n_field].min_value;

			fields[n_field].num_values[fields[n_field].min_value]++;
		}
		fields[n_field].Ham_mask = new bool[fields[n_field].len];
		for(j = 0; j < fields[n_field].len; ++j)
			fields[n_field].Ham_mask[j] = true;

		fields[n_field].block_desc.clear();

		start_pos = i+1;
		n_field++;
	}
	vector<int32> prev_value;
	prev_value.resize(n_field);
	uint32 r_no = 0;

	// Analyze fields in all records in superblock
	is_num_fields_constant = true;
	for(i = 0; i < b_count; ++i)
	{
		for(j = 0; j < blocks[i]->records.size(); ++j)
		{
			uint32 c_field = 0;
			uint32 start_pos = 0;
			CFastqRecord &rec = blocks[i]->records[j];
			for(k = 0; k <= rec.title_len && c_field < n_field; ++k)
			{
				if(rec.title[k] != fields[c_field].sep)
					continue;
				if(k - start_pos > fields[c_field].max_len)
					fields[c_field].max_len = k - start_pos;
				else if(k - start_pos < fields[c_field].min_len)
					fields[c_field].min_len = k - start_pos;

				// Check whether field in a block is constant
				if(j == 0)
				{
					fields[c_field].block_str_start = start_pos;
					fields[c_field].block_str_len   = k-start_pos;
					fields[c_field].block_desc.push_back(CField::t_block_desc(true, true, true, 0));
				}
				else
				{
					if(fields[c_field].block_str_len != k-start_pos)
						fields[c_field].block_desc.back().is_block_constant = false;
					else if(fields[c_field].block_desc.back().is_block_constant)
					{
						fields[c_field].block_desc.back().is_block_constant = equal(rec.title+start_pos, rec.title+k,
							blocks[i]->records[0].title+fields[c_field].block_str_start);
					}
				}

				fields[c_field].chars.resize(fields[c_field].max_len);
				uint32 chars_len = MIN(MAX_FIELD_STAT_LEN, k-start_pos);
				for(x = 0; x < chars_len; ++x)
					fields[c_field].chars[x][rec.title[start_pos+x]]++;
				for(x = MAX_FIELD_STAT_LEN; x < k-start_pos; ++x)
					fields[c_field].chars[MAX_FIELD_STAT_LEN][rec.title[start_pos+x]]++;
				
				if(fields[c_field].is_constant)
				{
					if(k - start_pos != fields[c_field].len)
						fields[c_field].is_constant = false;
					else
						fields[c_field].is_constant = equal(fields[c_field].data, fields[c_field].data + fields[c_field].len, rec.title+start_pos);
				}
				if(fields[c_field].is_len_constant)
					fields[c_field].is_len_constant = fields[c_field].len == k-start_pos;
				if(fields[c_field].is_numeric)
				{
					fields[c_field].is_numeric = fields[c_field].is_num_field(rec.title+start_pos, k-start_pos);
					if(fields[c_field].is_numeric)
					{
						int32 value = fields[c_field].to_num(rec.title+start_pos, k-start_pos);
						if(value < fields[c_field].min_value)
							fields[c_field].min_value = value;
						else if(value > fields[c_field].max_value)
							fields[c_field].max_value = value;
						if(j == 0)
							fields[c_field].block_value = value;
						else
						{
							if(fields[c_field].block_value != value)
								fields[c_field].block_desc.back().is_block_value_constant = false;
						}
						if(j == 1)
						{
							fields[c_field].block_delta = value - prev_value[c_field];
							fields[c_field].block_desc.back().block_delta_constant = value - prev_value[c_field];
						}
						else if(j > 1)
						{
							if(value - prev_value[c_field] != fields[c_field].block_delta)
								fields[c_field].block_desc.back().is_block_delta_constant = false;
						}

						if(r_no == 1)
						{
							fields[c_field].max_delta = value - prev_value[c_field];
							fields[c_field].min_delta = value - prev_value[c_field];
						}
						else if(r_no > 1)
						{
							if(value - prev_value[c_field] > fields[c_field].max_delta)
								fields[c_field].max_delta = value - prev_value[c_field];
							if(value - prev_value[c_field] < fields[c_field].min_delta)
								fields[c_field].min_delta = value - prev_value[c_field];
						}
						if(fields[c_field].num_values.size())
						{
							fields[c_field].num_values[value]++;
							if(fields[c_field].num_values.size() > MAX_NUM_VAL_HUF)
								fields[c_field].num_values.clear();
						}
						if(r_no >= 1)
						{
							fields[c_field].delta_values[value - prev_value[c_field]]++;
							if(fields[c_field].delta_values.size() > MAX_NUM_VAL_HUF)
								fields[c_field].delta_values.clear();
						}
						prev_value[c_field] = value;
					}
				}
				if(!fields[c_field].is_constant)
				{
					for(uint32 p = 0; p < k - start_pos && p < fields[c_field].len; ++p)
						fields[c_field].Ham_mask[p] &= fields[c_field].data[p] == rec.title[p+start_pos];
				}
				start_pos = k+1;
				c_field++;
			}

			// Absent fields are not constant
			is_num_fields_constant &= (c_field == n_field) && (k == rec.title_len);
			for(; c_field < n_field; ++c_field)
			{
				fields[c_field].is_constant = false;
				fields[c_field].is_numeric  = false;
			}
			++r_no;
		}
	}

	// Find better encoding of numeric values
	for(i = 0; i < n_field; ++i)
	{
		if(!fields[i].is_numeric)
		{
			if(!fields[i].is_constant)
			{
				fields[i].chars.resize(MIN(fields[i].max_len, MAX_FIELD_STAT_LEN+1));
				fields[i].no_of_bits_per_len = bit_stream.BitLength(fields[i].max_len - fields[i].min_len);
			}
			continue;
		}

		int32 diff;
		if(fields[i].max_value - fields[i].min_value < fields[i].max_delta - fields[i].min_delta)
		{
			fields[i].is_delta_coding = false;
			diff = fields[i].max_value - fields[i].min_value;
		}
		else
		{
			fields[i].is_delta_coding = true;
			diff = fields[i].max_delta - fields[i].min_delta;
		}

		fields[i].no_of_bits_per_num = bit_stream.BitLength(diff);
		diff = fields[i].max_value - fields[i].min_value;
		fields[i].no_of_bits_per_value = bit_stream.BitLength(diff);
	}
}

// ********************************************************************************************
void CSuperBlock::StoreDNA(CBitStream &bit_stream)
{
	// DNA symbols
	for(uint32 i = 0; i < symbols.size(); ++i)
		bit_stream.PutByte(symbols[i]);
}

// ********************************************************************************************
void CSuperBlock::StoreQualityPlain(CBitStream &bit_stream)
{
	bit_stream.FlushPartialWordBuffer();
	for(uint32 i = 0; i < qualities.size(); ++i)
		bit_stream.PutByte(qualities[i]);

	StoreStats(bit_stream, quality_stats[0], (uint32) qualities.size(), true);

	if(quality_stats_mode != 2)		// local stats
		for(uint32 j = 0; j < max_seq_length; ++j)
			StoreStats(bit_stream, quality_stats[j+1], (uint32) qualities.size(), true);
	bit_stream.FlushPartialWordBuffer();
}

// ********************************************************************************************
void CSuperBlock::StoreQualityRLE(CBitStream &bit_stream)
{
	uint32 i;

	bit_stream.FlushPartialWordBuffer();
	for(i = 0; i < qualities.size(); ++i)
		bit_stream.PutByte(qualities[i]);

	for(i = 0; i < qualities.size(); ++i)
		StoreStats(bit_stream, quality_stats[i], (uint32) qualities.size(), true);
	for(i = 1; i < qualities.size(); ++i)
		StoreStats(bit_stream, run_stats[i], max_run_len, true, true);

	bit_stream.FlushPartialWordBuffer();
}

// ********************************************************************************************
void CSuperBlock::StoreTitle(CBitStream &bit_stream)
{
	uint32 i, j, k;

	AnalyzeTitles(bit_stream);

	bit_stream.PutWord(n_field);
	bit_stream.PutByte(is_num_fields_constant);
	for(i = 0; i < n_field; ++i)
	{
		bit_stream.PutByte(fields[i].sep);
		bit_stream.PutByte(fields[i].is_constant);
		if(fields[i].is_constant)
		{
			bit_stream.PutWord(fields[i].len);
			bit_stream.PutBytes(fields[i].data, fields[i].len);
			continue;
		}

		bit_stream.PutByte(fields[i].is_numeric);
		if(fields[i].is_numeric)
		{
			bit_stream.PutWord(fields[i].min_value);
			bit_stream.PutWord(fields[i].max_value);
			bit_stream.PutWord(fields[i].min_delta);
			bit_stream.PutWord(fields[i].max_delta);
			int32 diff, base;
			map<int32, int32> &map_stats = fields[i].num_values;
			if(fields[i].max_value-fields[i].min_value < fields[i].max_delta-fields[i].min_delta)
			{
				diff = fields[i].max_value - fields[i].min_value;
				base = fields[i].min_value;
			}
			else
			{
				diff = fields[i].max_delta - fields[i].min_delta;
				base = fields[i].min_delta;
				map_stats = fields[i].delta_values;
			}

			diff++;
			if(diff <= MAX_NUM_VAL_HUF && map_stats.size())			// few values, so use Huffman for them
			{
				fields[i].AddGlobalStats(diff);
				for(j = 0; (int32) j < diff; ++j)
					fields[i].global_stats[j] = map_stats[base+j];
				fields[i].Huffman_global->Restart();
				for(j = 0; (int32) j < diff; ++j)
					fields[i].Huffman_global->Insert(fields[i].global_stats[j]);
				fields[i].Huffman_global->Complete();
				StoreStats(bit_stream, fields[i].global_stats, diff);
				bit_stream.FlushPartialWordBuffer();
			}

			continue;
		}

		bit_stream.PutByte(fields[i].is_len_constant);
		bit_stream.PutWord(fields[i].len);
		bit_stream.PutWord(fields[i].max_len);
		bit_stream.PutWord(fields[i].min_len);
		bit_stream.PutBytes(fields[i].data, fields[i].len);
		for(j = 0; j < fields[i].len; ++j)
			bit_stream.PutBit(fields[i].Ham_mask[j]);

		fields[i].Huffman_local.resize(MIN(fields[i].max_len+1, MAX_FIELD_STAT_LEN+1));
		fields[i].AddStats(MAX_FIELD_STAT_LEN+1, 256);
		for(j = 0; j < MIN(fields[i].max_len, MAX_FIELD_STAT_LEN); ++j)
		{
			fields[i].Huffman_local[j] = NULL;
			if(j >= fields[i].len || !fields[i].Ham_mask[j])
			{
				fields[i].Huffman_local[j] = new CHuffman(256);
				for(k = 0; k < 256; ++k)
				{
					fields[i].stats[j][k] = fields[i].chars[j][k];
					fields[i].Huffman_local[j]->Insert(fields[i].stats[j][k]);
				}
				fields[i].Huffman_local[j]->Complete(true);
				StoreStats(bit_stream, fields[i].stats[j], 256, true, true);
			}
		} 
		if(fields[i].max_len >= MAX_FIELD_STAT_LEN)
		{
			fields[i].Huffman_local[MAX_FIELD_STAT_LEN] = new CHuffman(256);
			for(k = 0; k < 256; ++k)
			{
				fields[i].stats[MAX_FIELD_STAT_LEN][k] = fields[i].chars[(unsigned char) MAX_FIELD_STAT_LEN][k];
				fields[i].Huffman_local[MAX_FIELD_STAT_LEN]->Insert(fields[i].stats[MAX_FIELD_STAT_LEN][k]);
			}
			fields[i].Huffman_local[MAX_FIELD_STAT_LEN]->Complete(true);
			StoreStats(bit_stream, fields[i].stats[MAX_FIELD_STAT_LEN], 256, true, true);
		}
		bit_stream.FlushPartialWordBuffer();
	}
}

// ********************************************************************************************
void CSuperBlock::StoreStats(CBitStream &bit_stream, uint32 *stat, uint32 n_stat, bool use_bits, bool code_max)
{
	uint32 i;

	if(use_bits)
	{
		uint32 bit_len = bit_stream.BitLength(*max_element(stat, stat+n_stat));
		bit_stream.PutBits(bit_len, 8);

		uint32 last = 0;
		if(code_max)
		{
			for(i = 0; i < n_stat; ++i)
				if(stat[i])
					last = i;
			last++;					// no of, so last = last_index+1
			bit_stream.PutBits(last, bit_stream.BitLength(n_stat));
		}
		else
			last = n_stat;
		for(i = 0; i < last; ++i)
			bit_stream.PutBits(stat[i], bit_len);
	}
	else
	{
		for(i = 0; i < n_stat; ++i)
			bit_stream.PutWord(stat[i]);
	}
}

// ********************************************************************************************
bool CSuperBlock::ReadDNA(CBitStream &bit_stream)
{
	uint32 tmp;

	// DNA symbols
	symbols.resize(no_of_symbols);
	for(uint32 i = 0; i < no_of_symbols; ++i)
	{
		bit_stream.GetByte(tmp);
		symbols[i] = (unsigned char) tmp;
	}

	return true;
}

// ********************************************************************************************
bool CSuperBlock::ReadQualityPlain(CBitStream &bit_stream)
{
	uint32 tmp;

	// Quality symbols
	qualities.resize(no_of_qualities);
	for(uint32 i = 0; i < no_of_qualities; ++i)
	{
		bit_stream.GetByte(tmp);
		qualities[i] = (unsigned char) tmp;
	}
	
	// Quality stats
	ReadStats(bit_stream, quality_stats[0], (uint32) qualities.size(), true);

	if(quality_stats_mode != 2)		// local stats
		for(uint32 j = 0; j < max_seq_length; ++j)
			ReadStats(bit_stream, quality_stats[j+1], (uint32) qualities.size(), true);

	bit_stream.FlushInputWordBuffer();

	return true;
}

// ********************************************************************************************
bool CSuperBlock::ReadQualityRLE(CBitStream &bit_stream)
{
	uint32 tmp;
	uint32 i;

	bit_stream.FlushInputWordBuffer();
	qualities.resize(no_of_qualities);
	for(i = 0; i < qualities.size(); ++i)
	{
		bit_stream.GetByte(tmp);
		qualities[i] = (unsigned char) tmp;
	}

	for(i = 0; i < qualities.size(); ++i)
		ReadStats(bit_stream, quality_stats[i], (uint32) qualities.size(), true);
	for(i = 1; i < qualities.size(); ++i)
		ReadStats(bit_stream, run_stats[i], max_run_len, true, true);

	bit_stream.FlushInputWordBuffer();

	return true;
}

// ********************************************************************************************
bool CSuperBlock::ReadTitle(CBitStream &bit_stream)
{
	uint32 tmp;
	uint32 i, j, k;

	bit_stream.GetWord(n_field);
	bit_stream.GetByte(tmp);
	is_num_fields_constant = tmp != 0;
	fields.resize(n_field);

	for(i = 0; i < n_field; ++i)
	{
		bit_stream.GetByte(tmp);
		fields[i].sep = (unsigned char) tmp;
		bit_stream.GetByte(tmp);
		fields[i].is_constant = tmp != 0;
		if(fields[i].is_constant)
		{
			bit_stream.GetWord(tmp);
			fields[i].len = tmp;
			fields[i].data = new unsigned char[fields[i].len+1];
			bit_stream.GetBytes(fields[i].data, fields[i].len);
			continue;
		}

		bit_stream.GetByte(tmp);
		fields[i].is_numeric = tmp != 0;
		if(fields[i].is_numeric)
		{
			bit_stream.GetWord(tmp);
			fields[i].min_value = tmp;
			bit_stream.GetWord(tmp);
			fields[i].max_value = tmp;
			bit_stream.GetWord(tmp);
			fields[i].min_delta = (int32) tmp;
			bit_stream.GetWord(tmp);
			fields[i].max_delta = tmp;

			int32 diff, base;
			map<int32, int32> &map_stats = fields[i].num_values;
			if(fields[i].max_value - fields[i].min_value < fields[i].max_delta - fields[i].min_delta)
			{
				fields[i].is_delta_coding = false;
				diff = fields[i].max_value - fields[i].min_value;
				base = fields[i].min_value;
			}
			else
			{
				fields[i].is_delta_coding = true;
				diff = fields[i].max_delta - fields[i].min_delta;
				base = fields[i].min_delta;
				map_stats = fields[i].delta_values;
			}

			fields[i].no_of_bits_per_num = bit_stream.BitLength(diff);
			int32 v_diff = fields[i].max_value - fields[i].min_value;
			fields[i].no_of_bits_per_value = bit_stream.BitLength(v_diff);

			diff++;
			if(diff <= MAX_NUM_VAL_HUF)			// a few values, so use Huffman for them
			{
				fields[i].AddGlobalStats(diff);
				ReadStats(bit_stream, fields[i].global_stats, diff);
				bit_stream.FlushInputWordBuffer();
				fields[i].Huffman_global->Restart();
				for(j = 0; (int32) j < diff; ++j)
					fields[i].Huffman_global->Insert(fields[i].global_stats[j]);
				fields[i].Huffman_global->Complete();
			}

			continue;
		}

		bit_stream.GetByte(tmp);
		fields[i].is_len_constant = tmp != 0;
		bit_stream.GetWord(tmp);
		fields[i].len = tmp;
		bit_stream.GetWord(tmp);
		fields[i].max_len = tmp;
		bit_stream.GetWord(tmp);
		fields[i].min_len = tmp;
		fields[i].no_of_bits_per_len = bit_stream.BitLength(fields[i].max_len - fields[i].min_len);
		fields[i].data = new unsigned char[fields[i].len+1];
		bit_stream.GetBytes(fields[i].data, fields[i].len);
		fields[i].Ham_mask = new bool[fields[i].len+1];
		for(j = 0; j < fields[i].len; ++j)
		{
			bit_stream.GetBits(tmp, 1);
			fields[i].Ham_mask[j] = tmp != 0;
		}

		fields[i].Huffman_local.resize(MIN(fields[i].max_len, MAX_FIELD_STAT_LEN+1));
		fields[i].AddStats(MAX_FIELD_STAT_LEN+1, 256);
		for(j = 0; j < MIN(fields[i].max_len, MAX_FIELD_STAT_LEN); ++j)
		{
			fields[i].Huffman_local[j] = NULL;
			if(j >= fields[i].len || !fields[i].Ham_mask[j])
			{
				fields[i].Huffman_local[j] = new CHuffman(256);
				ReadStats(bit_stream, fields[i].stats[j], 256, true, true);
				for(k = 0; k < 256; ++k)
				{
					fields[i].Huffman_local[j]->Insert(fields[i].stats[j][k]);
				}
				fields[i].Huffman_local[j]->Complete(true);
			}
		}
		if(fields[i].max_len >= MAX_FIELD_STAT_LEN)
		{
			fields[i].Huffman_local[MAX_FIELD_STAT_LEN] = new CHuffman(256);
			ReadStats(bit_stream, fields[i].stats[MAX_FIELD_STAT_LEN], 256, true, true);
			for(k = 0; k < 256; ++k)
			{
				fields[i].Huffman_local[MAX_FIELD_STAT_LEN]->Insert(fields[i].stats[MAX_FIELD_STAT_LEN][k]);
			}
			fields[i].Huffman_local[MAX_FIELD_STAT_LEN]->Complete(true);
		}

		bit_stream.FlushInputWordBuffer();
	}

	return true;
}

// ********************************************************************************************
void CSuperBlock::ReadStats(CBitStream &bit_stream, uint32 *stat, uint32 n_stat, bool use_bits, bool code_max)
{
	uint32 i;

	if(use_bits)
	{
		uint32 bit_len;
		bit_stream.GetBits(bit_len, 8);

		uint32 last = 0;
		if(code_max)
			bit_stream.GetBits(last, bit_stream.BitLength(n_stat));
		else
			last = n_stat;
		for(i = 0; i < last; ++i)
			bit_stream.GetBits(stat[i], bit_len);
		for(i = last; i < n_stat; ++i)
			stat[i] = 0;
	}
	else
	{
		for(i = 0; i < n_stat; ++i)
			bit_stream.GetWord(stat[i]);
	}
}

// ********************************************************************************************
uint32 CSuperBlock::ChooseQualityMethod()
{
	double rle_len = 0;
	double raw_len = 0;
	double th_len  = 0;
	
	for(uint32 i = 0; i < b_count; i++)
	{
		raw_len += blocks[i]->records[0].quality_len;
		th_len  += TruncHashLength(blocks[i]->records[0].quality, blocks[i]->records[0].quality_len);
		rle_len += RLELength(blocks[i]->records[0].quality, blocks[i]->records[0].quality_len);
	}
	
	if(th_len / rle_len > 1.25)
		return 2;			// RLE
	else if(raw_len - th_len < 3 * b_count)
		return 1;			// Plain
	else
		return 3;			// Plain with truncate hashes
}


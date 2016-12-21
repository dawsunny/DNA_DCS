/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#include "ds_lz.h"
#include "ds_config.h"
#include <map>
#include <vector>
#include <algorithm>

#define EMPTY	0

uint32 CLZMatcher::MIN_MATCH_LEN = 36;
uint32 CLZMatcher::LZ_STEP = 12;
uint64 CLZMatcher::PART_SIZE_EXP = 24;
uint64 CLZMatcher::PART_SIZE = 1 << CLZMatcher::PART_SIZE_EXP;
uint64 CLZMatcher::PART_SIZE_MASK = CLZMatcher::PART_SIZE - 1;
uint32 CLZMatcher::MAX_ONE_ENTRY_COL = 10;
double CLZMatcher::LOG_PROB_THR = -1.0;

using namespace std;

// ********************************************************************************************
CLZMatcher::CLZMatcher()
{
	uint32 i;

	// Prepare buffer for sequence data
	cur_part = -1;

	ht_size            = 2;
	ht_size_mask       = 1;
	ht_max_fill_factor = 0.7;
	ht_max_elems       = 0;
	hash_table         = new t_ht_entry[ht_size];
	ht_elems           = 0;
	
	for(i = 0; i < ht_size; ++i)
		hash_table[i].buffer_pos = EMPTY;

	frozen = false;
	buf_memory = 0;
	ht_memory  = ht_size * sizeof(t_ht_entry);

	// min_factor is computed to rapidly comput hash value when moving sliding window over sequence
	min_factor = 1;
	for(i = 0; i < MIN_MATCH_LEN-1; ++i)
		min_factor *= 33;

	PreparePhred();
}

// ********************************************************************************************
CLZMatcher::~CLZMatcher()
{
	// Release buffer for sequence data
	for(uint32 i = 0; i < buffer.size(); ++i)
		if(buffer[i])
			delete[] buffer[i];

	if(hash_table)
		delete[] hash_table;
}

// ********************************************************************************************
void CLZMatcher::PreparePhred()
{
	int32 i;

	for(i = 0; i < 34; ++i)
		log_phred[i] = -10;
	for(i = 34; i < 127; ++i)
		log_phred[i] = log10(1.0 - pow(10, -(i-33)/10.0));
	for(i = 127; i < 255; ++i)
		log_phred[i] = -10;
}

// ********************************************************************************************
void CLZMatcher::PrepareNextPart()
{
	if((cur_part+2) * PART_SIZE + ht_memory >= max_lz_memory)
	{
		frozen = true;
		return;
	}

	buffer.push_back(new unsigned char[PART_SIZE]);
	++cur_part;
	part_pos = 1;		

	buf_memory = (cur_part+1) * PART_SIZE;
}

// ********************************************************************************************
void CLZMatcher::ResizeHT_Encode()
{
	uint32 i;
	uint32 old_ht_size = ht_size;
	t_ht_entry *old_hash_table = hash_table;
	ht_size = ht_size << 1;
	
	if(ht_size < 16)
	{
		ht_size     = 1 << 24;
		old_ht_size = 0;
	}
	else if(ht_size == (1 << 25))
		ht_size		= (uint32) (max_lz_memory / 2 / 16);

	uint64 new_ht_memory = ht_size * sizeof(t_ht_entry);

	if(buf_memory + ht_memory + new_ht_memory >= max_lz_memory)
	{
		frozen = true;
		return;
	}
	
	ht_size_mask = ht_size - 1;
	ht_max_elems = (uint32) (ht_size * ht_max_fill_factor);
	hash_table   = new t_ht_entry[ht_size];
	ht_elems     = 0;
	ht_memory    = new_ht_memory;

	for(i = 0; i < ht_size; ++i)
		hash_table[i].buffer_pos = EMPTY;
	
	for(i = 0; i < old_ht_size; ++i)
	{
		t_ht_entry &ent = old_hash_table[i];
		if(ent.buffer_pos != EMPTY)
			InsertHT_Encode(&buffer[ent.buffer_pos >> PART_SIZE_EXP][ent.buffer_pos & PART_SIZE_MASK],
				ent.buffer_pos, ent.rec_no, ent.rec_offset, false);
	}

	if(old_hash_table)
		delete[] old_hash_table;
}

// ********************************************************************************************
bool CLZMatcher::InsertHT_Encode(unsigned char *str, uint64 buffer_pos, uint64 rec_no, uint32 rec_offset, 
	bool force_insert)
{
	if(frozen)
		return false;

	if(rec_no >= (1ull << 32))
		return false;

	if(ht_elems >= ht_max_elems)
		ResizeHT_Encode();

	uint32 h1 = Hash1(str + rec_offset);

	if(hash_table[h1].buffer_pos == EMPTY)				
	{
		hash_table[h1].buffer_pos = buffer_pos;
		hash_table[h1].rec_no     = (uint32) rec_no;
		hash_table[h1].rec_offset = rec_offset;
	}
	else
	{
		uint32 n_this_try = 0;
		uint32 h;
		for(h = (h1 + 1) & ht_size_mask; hash_table[h].buffer_pos != EMPTY; h = (h + 1) & ht_size_mask)
			++n_this_try;

		if(n_this_try > MAX_ONE_ENTRY_COL && !force_insert)
			return false;
		hash_table[h].buffer_pos = buffer_pos;
		hash_table[h].rec_no     = (uint32) rec_no;
		hash_table[h].rec_offset = rec_offset;
	}

	ht_elems++;

	return true;
}

// ********************************************************************************************
void CLZMatcher::ResizeHT_Decode()
{
	uint32 i;

	uint32 old_ht_size = ht_size;
	t_ht_entry *old_hash_table = hash_table;

	if(ht_size < 16)
	{
		ht_size     = 1 << 24;
		old_ht_size = 0;
	}
	else
		ht_size <<= 1;
	ht_size_mask = ht_size - 1;
	ht_max_elems = (uint32) (ht_size * ht_max_fill_factor);
	hash_table = new t_ht_entry[ht_size];
	ht_elems = 0;
	
	for(i = 0; i < ht_size; ++i)
		hash_table[i].buffer_pos = EMPTY;
	
	for(i = 0; i < old_ht_size; ++i)
		if(old_hash_table[i].buffer_pos != EMPTY)
			InsertHT_Decode(old_hash_table[i].buffer_pos, old_hash_table[i].rec_no);

	if(old_hash_table)
		delete[] old_hash_table;
}

// ********************************************************************************************
void CLZMatcher::InsertHT_Decode(uint64 buffer_pos, uint32 rec_no)
{
	if(frozen)
		return;

	if(ht_elems >= ht_max_elems)
		ResizeHT_Decode();

	uint32 h1 = rec_no & ht_size_mask;

	if(hash_table[h1].buffer_pos == EMPTY)			
	{
		hash_table[h1].buffer_pos = buffer_pos;
		hash_table[h1].rec_no     = rec_no;
	}
	else
	{
		uint32 n_this_try = 0;
		uint32 h2 = 1;
		uint32 h;
		for(h = (h1 + h2) & ht_size_mask; hash_table[h].buffer_pos != EMPTY; h = (h + h2) & ht_size_mask)
			;
		
		hash_table[h].buffer_pos = buffer_pos;
		hash_table[h].rec_no     = rec_no;
	}

	ht_elems++;
}

// ********************************************************************************************
bool CLZMatcher::InsertEncoding(uint64 rec_no, unsigned char *seq, uint32 seq_len, unsigned char *quality, uint32 quality_len)
{
	if(frozen)
		return false;

	if(rec_no >= (1ull << 32))
		return false;

	if(cur_part < 0 || part_pos + seq_len + 1 > PART_SIZE)
	{
		PrepareNextPart();
		if(frozen)
			return false;
	}

	int64 buffer_pos = (cur_part << PART_SIZE_EXP) + part_pos;
	bool inserted = false;

	for(uint32 i = 0; i + MIN_MATCH_LEN < seq_len; i += LZ_STEP)
	{
		double log_prob_cor = 0.0;

		for(uint32 j = 0; j < MIN_MATCH_LEN && log_prob_cor > LOG_PROB_THR; ++j)
			log_prob_cor += log_phred[quality[i+j]];
		if(log_prob_cor > LOG_PROB_THR)
			inserted |= InsertHT_Encode(seq, buffer_pos, rec_no, i);
	}
	
	if(inserted)
	{
		copy(seq, seq+seq_len+1, &buffer[cur_part][part_pos]);
		part_pos += seq_len+1;
	}

	return inserted;
}

// ********************************************************************************************
bool CLZMatcher::FindMatch(unsigned char *seq, uint32 seq_len, unsigned char *quality, uint32 quality_len, 
	uint32 &rec_no, uint32 &rec_offset, uint32 &match_len)
{
	if(seq_len < MIN_MATCH_LEN)
		return false;

	uint32 i;
	double log_prob_cor = 0.0;

	for(i = 0; i < MIN_MATCH_LEN-1 && log_prob_cor < LOG_PROB_THR; ++i)
		log_prob_cor += log_phred[quality[i]];
	if(log_prob_cor < LOG_PROB_THR)
		return 0;

	bool match_found = false;
	uint32 best_len = 0;

	for(i = 0; i < LZ_STEP; ++i)
	{
		if(i + LZ_STEP > seq_len)
			break;
		log_prob_cor += log_phred[quality[i+MIN_MATCH_LEN-1]];
		if(log_prob_cor < LOG_PROB_THR)
			break;

		uint32 h1;

		if(i == 0)
			h1 = Hash1(seq+i);
		else
			h1 = Hash1_fast(seq+i);

		if(hash_table[h1].buffer_pos == EMPTY)	
			continue;
		else
		{
			uint32 h;

			uint32 cur_len = 0;
			for(h = h1; hash_table[h].buffer_pos != EMPTY; h = (h + 1) & ht_size_mask)
			{
				uint32 match_offset = hash_table[h].rec_offset;
				if(match_offset < i)
					continue;
				uint32 match_pos  = (uint32) (hash_table[h].buffer_pos & PART_SIZE_MASK);
				uint32 j;
				unsigned char *buffer_seq = &buffer[hash_table[h].buffer_pos >> PART_SIZE_EXP][match_pos+match_offset-i];
				for(j = 0; j < seq_len && j < 255+MIN_MATCH_LEN && buffer_seq[j] == seq[j] && quality[j] < 128; ++j)
					;
				cur_len = j;
				if(cur_len > best_len)
				{
					best_len = cur_len;
					rec_no = hash_table[h].rec_no;
					rec_offset = match_offset-i;
				}
			}				
		}
	}

	match_len = best_len;

	return best_len >= MIN_MATCH_LEN;
}

// ********************************************************************************************
bool CLZMatcher::InsertDecoding(uint64 rec_no, unsigned char *seq, uint32 seq_len, unsigned char *quality, uint32 quality_len)
{
	if(rec_no >= (((uint64) (1)) << 32))
		return false;

	if(cur_part < 0 || part_pos + seq_len + 1 > PART_SIZE)
		PrepareNextPart();

	if(frozen)
		return false;

	uint32 i, j;

	copy(seq, seq+seq_len+1, &buffer[cur_part][part_pos]);
	
	int64 buffer_pos = (cur_part << PART_SIZE_EXP) + part_pos;
	bool to_insert = false;

	for(i = 0; i + MIN_MATCH_LEN < seq_len && !to_insert; i += LZ_STEP)
	{
		double log_prob_cor = 0.0;

		for(j = 0; j < MIN_MATCH_LEN; ++j)
			log_prob_cor += log_phred[quality[i+j]];
		if(log_prob_cor > LOG_PROB_THR)
			to_insert = true;
	}
	InsertHT_Decode(buffer_pos, (uint32) rec_no);
	
	part_pos += seq_len+1;

	return true;
}

// ********************************************************************************************
bool CLZMatcher::DecodeMatch(unsigned char *seq, uint32 rec_no, uint32 rec_offset, uint32 match_len, 
	uint32 seq_len)
{
	uint32 h1 = rec_no & ht_size_mask;
	uint32 h2 = 1;

	uint32 h;
	for(h = h1; hash_table[h].buffer_pos != EMPTY && hash_table[h].rec_no != rec_no; h = (h + h2) & ht_size_mask)
		;

	if(hash_table[h].buffer_pos == EMPTY)
		return false;
	
	copy(&buffer[hash_table[h].buffer_pos >> PART_SIZE_EXP][(hash_table[h].buffer_pos & PART_SIZE_MASK) + rec_offset],
		&buffer[hash_table[h].buffer_pos >> PART_SIZE_EXP][(hash_table[h].buffer_pos & PART_SIZE_MASK) + rec_offset]+match_len,
		seq);

	return true;
}

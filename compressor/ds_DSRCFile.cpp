/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#include "ds_DSRCFile.h"
#include <algorithm>

using namespace std;

// ********************************************************************************************
//
// ********************************************************************************************
CDSRCFile::CDSRCFile()
{
	rec_count = 0;
	sb_count  = 0;
	opened    = false;
	mode      = mode_none;
}

// ********************************************************************************************
CDSRCFile::~CDSRCFile()
{
	if(opened)
		Close();
}

// ********************************************************************************************
bool CDSRCFile::Create(char *file_name, bool _try_lz, uint32 _max_lz_memory)
{
	opened = bit_stream.Create(file_name);
	if(opened)
		mode = mode_write;

	try_lz        = _try_lz;
	max_lz_memory = _max_lz_memory;

	lz_matcher.max_lz_memory = ((uint64) _max_lz_memory) << 20;

	return opened;
}

// ********************************************************************************************
bool CDSRCFile::Open(char *file_name)
{
	opened = bit_stream.Open(file_name);
	if(opened)
		mode = mode_read;

	try_lz = true;

	lz_matcher.max_lz_memory = 1ull << 62;

	return opened;
}

// ********************************************************************************************
bool CDSRCFile::OpenRA(char *file_name)
{
	uint64 i;

	opened = bit_stream.OpenRA(file_name);
	if(opened)
		mode = mode_read_ra;

	try_lz = true;
	lz_matcher.max_lz_memory = ((uint64) 1) << 62;

	bit_stream.SetPos(bit_stream.file_size - 25);
	bit_stream.GetByte(block_offset_bit_len);
	bit_stream.GetDWord(header_sb_count);
	bit_stream.GetDWord(header_b_count);
	bit_stream.GetDWord(header_pos);

	sb_file_pos.resize(header_sb_count);
	bit_stream.SetPos(header_pos);
	for(i = 0; i < header_sb_count; ++i)
	{
		uint64 tmp;
		bit_stream.GetDWord(tmp);
		sb_file_pos[i] = tmp;
	}

	if(header_b_count < (1 << 15))
	{
		block_file_pos.resize(header_b_count);
		for(i = 0; i < header_b_count; ++i)
		{
			uint32 tmp;
			bit_stream.GetBits(tmp, block_offset_bit_len);
			block_file_pos[i] = tmp;
		}
		bit_stream.FlushInputWordBuffer();
	}

	return opened;
}

// ********************************************************************************************
bool CDSRCFile::Close()
{
	uint32 i;

	if(mode == mode_write)
	{
		if(rec_count)
            superblock.Process1(bit_stream, tr, try_lz);
			//superblock.Process2(bit_stream, lz_matcher, try_lz);

		sb_file_pos.push_back(superblock.sb_file_pos);
		block_file_pos.insert(block_file_pos.end(), superblock.file_pos.begin(), superblock.file_pos.end());
		block_offset_bit_len = bit_stream.BitLength(*max_element(block_file_pos.begin(), block_file_pos.end()));

		// Save data for random access
		uint64 header_pos = bit_stream.file_pos;
		for(i = 0; i < sb_file_pos.size(); ++i)
			bit_stream.PutDWord(sb_file_pos[i]);
		for(i = 0; i < block_file_pos.size(); ++i)
			bit_stream.PutBits(block_file_pos[i], block_offset_bit_len);
		bit_stream.FlushPartialWordBuffer();
		bit_stream.PutByte(block_offset_bit_len);
		bit_stream.PutDWord(sb_file_pos.size());
		bit_stream.PutDWord(block_file_pos.size());
		bit_stream.PutDWord(header_pos);
	}

	opened = false;

	return bit_stream.Close();
}

// ********************************************************************************************
bool CDSRCFile::InsertRecord(const CFastqRecord &rec)
{
	if(mode != mode_write)
		return false;

	if(rec_count % CSuperBlock::MAX_SIZE == 0)
	{
		if(rec_count)
		{
			//superblock.Process2(bit_stream, lz_matcher, try_lz);
            superblock.Process1(bit_stream, tr, try_lz);
			sb_file_pos.push_back(superblock.sb_file_pos);
			block_file_pos.insert(block_file_pos.end(), superblock.file_pos.begin(), superblock.file_pos.end());
		}

		superblock.Reset(rec_count);	
		sb_count++;
	}

	superblock.InsertRecord(rec);
	rec_count++;

	return true;
}

// ********************************************************************************************
bool CDSRCFile::ReadRecord(CFastqRecord &rec)
{
	if(mode != mode_read)
		return false;

	if(rec_count % CSuperBlock::MAX_SIZE == 0)
	{
		superblock.Reset(rec_count);	
		superblock.Read(bit_stream, lz_matcher);
		sb_count++;
	}

	bool r = superblock.ReadRecord(rec);
	rec_count++;

	return r;
}

// ********************************************************************************************
bool CDSRCFile::ExtractRecord(CFastqRecord &rec, uint64 rec_id)
{
	if(mode != mode_read_ra)
		return false;

	// Find positions in compressed file
	uint64 sb_id = rec_id / CSuperBlock::MAX_SIZE;
	uint64 b_id = rec_id / CBlock::MAX_SIZE;
	uint32 b_in_sb_id = (uint32) ((rec_id % CSuperBlock::MAX_SIZE) / CBlock::MAX_SIZE);
	uint64 sb_start_pos;
	uint32 b_start_pos_offset;
	uint64 b_start_pos;

	sb_start_pos = sb_file_pos[sb_id];

	if(block_file_pos.empty())
	{
		uint32 b_id_off_pos = (uint32) (block_offset_bit_len * b_id / 8);
		bit_stream.SetPos(header_pos + 8*header_sb_count + b_id_off_pos);
		uint32 tmp;
		bit_stream.GetBits(tmp, block_offset_bit_len * b_id % 8);
		bit_stream.GetBits(b_start_pos_offset, block_offset_bit_len);
		bit_stream.FlushInputWordBuffer();
		
		b_start_pos = sb_start_pos + b_start_pos_offset;
	}
	else
		b_start_pos = sb_start_pos + block_file_pos[b_id];

	// Read superblock header and selected block
	bit_stream.SetPos(sb_start_pos);
	superblock.Reset(sb_id * CSuperBlock::MAX_SIZE, 0, b_in_sb_id);
	superblock.Read(bit_stream, lz_matcher, b_in_sb_id, b_start_pos);

	// Read record
	uint64 lz_rec_id;
	uint32 lz_rec_offset;
	uint32 lz_match_len;
	bool r = superblock.ExtractRecord(rec, rec_id, lz_rec_id, lz_rec_offset, lz_match_len);

	if(r && lz_match_len)
	{
		CFastqRecord lz_rec;
		if(!ExtractRecord(lz_rec, lz_rec_id))
			return false;

		copy(lz_rec.seq+lz_rec_offset, lz_rec.seq+lz_rec_offset+lz_match_len, rec.seq);
	}

	return r;
}


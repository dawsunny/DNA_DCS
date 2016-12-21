/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#ifndef _FIELD_H
#define _FIELD_H

#include <map>
#include <vector>
#include "ds_huffman.h"

using namespace std;

struct CField {
	unsigned char *data;
	uint32 len;
	uint32 min_len;
	uint32 max_len;
	unsigned char sep;
	bool is_constant;
	bool is_len_constant;
	bool is_numeric;
	int32 min_value;
	int32 max_value;
	bool *Ham_mask;
	int32 min_delta;
	int32 max_delta;
	uint32 no_of_bits_per_num;
	uint32 no_of_bits_per_value;
	uint32 no_of_bits_per_len;
	bool is_delta_coding;

	uint32 block_str_start, block_str_len;
	int32 block_value, block_delta;

	uint32 *global_stats;
	uint32 **stats;
	uint32 *raw_stats;
	CHuffman *Huffman_global;
	vector<CHuffman*> Huffman_local;
	uint32 size, size1, size2;
	map<int32, int32> num_values;
	map<int32, int32> delta_values;
	vector<map<uint32, uint32> > chars;
	typedef struct tag_block_desc {
		bool is_block_constant;
		bool is_block_value_constant;
		bool is_block_delta_constant;
		uint32 block_delta_constant;
		tag_block_desc(bool _is_block_constant = true, bool _is_block_value_constant = true, 
			bool _is_block_delta_constant = true, uint32 _block_delta_constant = 0) :
			is_block_constant(_is_block_constant), is_block_value_constant(_is_block_value_constant), 
			is_block_delta_constant(_is_block_delta_constant), block_delta_constant(_block_delta_constant)
			{};
	} t_block_desc;
	vector<t_block_desc> block_desc;

	inline bool is_num_field();
	inline bool is_num_field(const unsigned char* data, uint32 size);
	inline uint32 to_num();
	inline uint32 to_num(const unsigned char *data, uint32 size);
	inline uint32 to_string(unsigned char* str, uint32 value);

	CField();
	CField(const CField &y);
	~CField();

	void AddStats(uint32 _size1, uint32 _size2);
	void AddGlobalStats(uint32 _size);
};


// ********************************************************************************************
bool CField::is_num_field(const unsigned char* data, uint32 len)
{
	for(uint32 i = 0; i < len; ++i)
		if(data[i] < '0' || data[i] > '9')
			return false;

	return true;
}

// ********************************************************************************************
bool CField::is_num_field()
{
	for(uint32 i = 0; i < len && data[i]; ++i)
		if(data[i] < '0' || data[i] > '9')
			return false;

	return true;
}

// ********************************************************************************************
uint32 CField::to_num(const unsigned char *data, uint32 len)
{
	uint32 r = 0;

	for(uint32 i = 0; i < len; ++i)
		r = r * 10 + (data[i] - '0');

	return r;
}

// ********************************************************************************************
uint32 CField::to_num()
{
	uint32 r = 0;

	for(uint32 i = 0; i < len && data[i]; ++i)
		r = r*10 + (data[i] - '0');

	return r;
}

// ********************************************************************************************
uint32 CField::to_string(unsigned char* str, uint32 value)
{
	uint32 digits;
	uint32 power = 1;

	if(value == 0)
	{
		str[0] = '0';
		return 1;
	}

	for(digits = 0; digits < 10; ++digits)
		if(value < power)
			break;
		else
			power *= 10;

	power /= 10;
	for(uint32 i = 0; power; ++i, power /= 10)
	{
		int32 d = value / power;
		str[i] = '0' + d;
		value -= d * power;
	}

	return digits;
}

#endif

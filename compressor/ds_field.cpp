/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#include "ds_field.h"
#include "ds_defs.h"
#include <algorithm>

using namespace std;

// ********************************************************************************************
CField::CField()
{
	data     = NULL;
	Ham_mask = NULL;
	len      = 0;
	min_len  = 0;
	max_len  = 0;

	global_stats = NULL;
	stats        = NULL;
	raw_stats    = NULL;

	size = size1 = size2 = 0;

	Huffman_global = new CHuffman(512);
};

// ********************************************************************************************
CField::CField(const CField &y)
{
	CField &x = const_cast<CField &>(y);

	len     = x.len;
	max_len = x.max_len;
	min_len = x.min_len;
	sep     = x.sep;

	is_constant     = x.is_constant;
	is_len_constant = x.is_len_constant;
	is_numeric      = x.is_numeric;
	is_delta_coding = x.is_delta_coding;

	min_value = x.min_value;
	max_value = x.max_value;
	min_delta = x.min_delta;
	max_delta = x.max_delta;

	no_of_bits_per_num   = x.no_of_bits_per_num;
	no_of_bits_per_value = x.no_of_bits_per_value;
	no_of_bits_per_len   = x.no_of_bits_per_len;

	block_desc = x.block_desc;

	if(x.data && len)
	{
		data   = x.data;
		x.data = NULL;
	}
	else
		data = NULL;

	if(x.Ham_mask && len)
	{
		Ham_mask   = x.Ham_mask;
		x.Ham_mask = NULL;
	}
	else
		Ham_mask = NULL;

	if(x.stats)
	{
		size1       = x.size1;
		size2       = x.size2;
		stats       = x.stats;
		raw_stats   = x.raw_stats;
		x.stats     = NULL;
		x.raw_stats = NULL;
	}
	else
	{
		stats     = NULL;
		raw_stats = NULL;
		size1 = size2 = 0;
	}

	if(x.global_stats)
	{
		global_stats = x.global_stats;
		size = x.size;
		x.global_stats = NULL;
	}
	else
	{
		global_stats = NULL;
		size = 0;
	}

	num_values = x.num_values;
	delta_values = x.delta_values;
	chars = x.chars;

	if(x.Huffman_global)
	{
		Huffman_global = x.Huffman_global;
		x.Huffman_global = new CHuffman(512);
	}
	else
	{
		Huffman_global = new CHuffman(512);
	}
};

// ********************************************************************************************
CField::~CField()
{
	if(data)
		delete[] data;
	if(Ham_mask)
		delete[] Ham_mask;

	if(stats)
		delete[] stats;
	if(global_stats)
		delete[] global_stats;
	if(raw_stats)
		delete[] raw_stats;

	if(Huffman_global)
		delete Huffman_global;

	for(uint32 i = 0; i < Huffman_local.size(); ++i)
		if(Huffman_local[i])
			delete Huffman_local[i];
	Huffman_local.resize(0);
};


// ********************************************************************************************
void CField::AddStats(uint32 _size1, uint32 _size2)
{
	if(stats)
	{
		delete[] raw_stats;
		delete[] stats;
	}

	size1 = _size1;
	size2 = _size2;
	stats = new uint32*[size1];
	raw_stats = new uint32[size1*size2];
	for(uint32 i = 0; i < size1; ++i)
		stats[i] = raw_stats+i*size2;
}

// ********************************************************************************************
void CField::AddGlobalStats(uint32 _size)
{
	if(global_stats)
		delete[] global_stats;

	size1 = _size;
	global_stats = new uint32[size1];
}

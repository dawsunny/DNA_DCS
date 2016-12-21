/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#ifndef _HUFFMAN_H
#define _HUFFMAN_H

#include "ds_defs.h"

class CHuffman {
	int32 size;

	typedef struct tag_frequency {
		uint32 symbol;
		uint32 frequency;

		bool operator<(const struct tag_frequency &x) {
			return frequency > x.frequency || (frequency == x.frequency && symbol > x.symbol);
		}
	} t_frequency;

public:
	typedef struct {
		uint32 code;
		uint32 len;
	} t_code;

private:
	typedef struct {
		int32 left_child;
		int32 right_child;
	} t_node;

	t_frequency *heap;
	t_node *tree;
	int32 root_id;
	int32 n_symbols;
	int32 cur_id;

public:
	t_code *codes;

	CHuffman(uint32 _size = 0);
	~CHuffman();

	bool Restart();
	inline bool Insert(const uint32 frequency);
	t_code* Complete(bool compact = true);
	inline int32 Decode(const uint32 bit);
};

// ********************************************************************************************
bool CHuffman::Insert(const uint32 frequency)
{
	if(n_symbols == size)
		return false;

	heap[n_symbols].symbol    = n_symbols;
	heap[n_symbols].frequency = frequency;
	n_symbols++;

	return true;
}

// ********************************************************************************************
int32 CHuffman::Decode(const uint32 bit) 
{
	if(cur_id < n_symbols)
		cur_id = root_id;
	if(bit)
		cur_id = tree[cur_id].right_child;
	else
		cur_id = tree[cur_id].left_child;
	if(cur_id < n_symbols)
		return cur_id;				// Symbol found
	else
		return -1;					// Not found yet
};


#endif

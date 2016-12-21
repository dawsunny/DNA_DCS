/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#include <algorithm>
#include "ds_huffman.h"
#include "ds_defs.h"

using namespace std;

// ********************************************************************************************
CHuffman::CHuffman(uint32 _size)
{
	size = _size;
	if(size)
	{
		tree  = new t_node[2*size-1];
		codes = new t_code[2*size-1];
		heap  = new t_frequency[size];
	}
	else
	{
		tree  = NULL;
		codes = NULL;
		heap  = NULL;
	}
	n_symbols = 0;
}

// ********************************************************************************************
CHuffman::~CHuffman()
{
	if(tree)
		delete[] tree;
	if(codes)
		delete[] codes;
	if(heap)
		delete[] heap;
}

// ********************************************************************************************
bool CHuffman::Restart()
{
	n_symbols = 0;

	return true;
}

// ********************************************************************************************
CHuffman::t_code* CHuffman::Complete(bool compact)
{
	int32 i;

	if(!n_symbols)
		return NULL;

	// Make heap of symbols
	make_heap(heap, heap+n_symbols);
	
	// Prepare leaves of the tree
	for(i = 0; i < n_symbols; ++i)
	{
		codes[i].code = 0;
		codes[i].len  = 0;
		tree[i].left_child = -1;
		tree[i].right_child = -1;
	}
	for(i = n_symbols; i < 2*n_symbols-1; ++i)
	{
		codes[i].code = 0;
		codes[i].len  = 0;
	}

	// Build tree
	int32 heap_size = n_symbols;
	// Remove symbols with 0 frequency
	if(compact)
		while(heap_size > 2 && heap[0].frequency == 0)
			pop_heap(heap, heap+heap_size--);

	int32 present_symbols = heap_size;

	if(!present_symbols)
		return codes;

	for(i = 0; i < present_symbols-1; ++i)
	{
		t_frequency left = heap[0];
		pop_heap(heap, heap+heap_size--);
		t_frequency right = heap[0];
		pop_heap(heap, heap+heap_size--);
		
		heap[heap_size].symbol = n_symbols+i;
		heap[heap_size].frequency = left.frequency + right.frequency;
		push_heap(heap, heap+ ++heap_size);

		tree[n_symbols+i].left_child  = left.symbol;
		tree[n_symbols+i].right_child = right.symbol;
	}

	// Compute codes
	for(i = n_symbols+present_symbols-2; i >= n_symbols; --i)
	{
		codes[tree[i].left_child].len   = codes[i].len+1;
		codes[tree[i].left_child].code  = (codes[i].code << 1);
		codes[tree[i].right_child].len  = codes[i].len+1;
		codes[tree[i].right_child].code = (codes[i].code << 1) | 1;
	}

	root_id = n_symbols + present_symbols - 2;
	cur_id = root_id;

	return codes;
}

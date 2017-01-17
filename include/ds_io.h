/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#ifndef _IO_H
#define _IO_H

#include "ds_defs.h"
#include "ds_config.h"
#include "ds_huffman.h"
#include "dcs_const.h"
#include <iostream>
#include <cstring>
using namespace std;

typedef enum {mode_none, mode_read, mode_write, mode_read_ra} t_mode;

// ********************************************************************************************
struct CFastqRecord {
	unsigned char *title;
	unsigned char *seq;
	unsigned char *plus;
	unsigned char *quality;
    
    //bxz
    uint32 no_N1, no_N2;

	uint32 title_len, seq_len, plus_len, quality_len;
	uint32 title_size, seq_size, plus_size, quality_size;
	uint32 rec_len;
	uint32 rec_th_len;
	bool lz_inserted;

	CFastqRecord();
	CFastqRecord(const CFastqRecord& x);
	~CFastqRecord();

	CFastqRecord operator=(const CFastqRecord &x)
	{
		struct CFastqRecord r(x);
		return r;
	};

	bool Set(const CFastqRecord &x);

	inline bool Extend(unsigned char *&str, uint32 &size);
	inline bool ExtendTo(unsigned char *&str, uint32 &size, uint32 new_size);
	inline bool AppendTitle(unsigned char *str, uint32 len);
	inline bool AppendTitle(unsigned char c);
	void Reset();
};


// ********************************************************************************************
bool CFastqRecord::AppendTitle(unsigned char *str, uint32 len)
{
	if(title_len + len + 1 >= title_size)
		Extend(title, title_size);

	copy(str, str+len, title+title_len);
	title_len += len;

	return true;
}

// ********************************************************************************************
bool CFastqRecord::AppendTitle(unsigned char c)
{
	if(title_len + 1 >= title_size)
		Extend(title, title_size);
	title[title_len++] = c;

	return true;
}

// ********************************************************************************************
bool CFastqRecord::Extend(unsigned char *&str, uint32 &size)
{
	uint32 new_size = size * 2;
	unsigned char *p = new unsigned char[new_size];

	if(!p)
		return false;

	copy(str, str+size, p);
	size = new_size;
	delete[] str;
	str = p;

	return true;
};

// ********************************************************************************************
bool CFastqRecord::ExtendTo(unsigned char *&str, uint32 &size, uint32 new_size)
{
	if(new_size <= size)
		return true;

	unsigned char *p = new unsigned char[new_size+1];

	if(!p)
		return false;

	copy(str, str+size, p);

	size = new_size;
	delete[] str;
	str = p;

	return true;
};

// ********************************************************************************************
class CFastqFile {
	FILE *file;

	unsigned char *io_buffer;
	int32 io_buffer_pos;
	int32 io_buffer_size;
	int64 file_pos;
	int64 file_size;
	bool io_eof;
	t_mode mode;

	static int32 IO_BUFFER_SIZE;
    
    //bxz
    char *data;
    uint32 datasize;
    uint32 dataoffset;
    //string out_data;

	inline int32 Getc();
	inline void UnGetc();
	inline bool Putc(const unsigned char c, char *, int64&);
	inline bool Puts(const unsigned char *str, const uint32 len, bool eol, char *, int64&);

	bool ReadTitle(CFastqRecord &rec);
	bool ReadPlus(CFastqRecord &rec);
	bool ReadSeq(CFastqRecord &rec);
	bool ReadQuality(CFastqRecord &rec);
	inline bool TransferN(CFastqRecord &rec);
	inline bool UnTransferN(CFastqRecord &rec);

public:
	CFastqFile() {
		file = NULL;
		io_buffer = new unsigned char[IO_BUFFER_SIZE];
		io_buffer_pos = -1;
		io_buffer_size = 0;
		io_eof = false;
		mode = mode_none;
        
        data = new char[FQ_CHUNK_SIZE];
        datasize = 0;
        dataoffset = 0;
	};

	~CFastqFile() {
		delete[] io_buffer;
        delete[] data;
		if(file)
			fclose(file);
	}

	bool Open(char *data, uint32);
	bool Create(char *file_name);
	bool Close(char *output_data, int64&);
	bool ReadRecord(CFastqRecord &rec);
	bool WriteRecord(CFastqRecord &rec, char *, int64&);
	bool Eof() {return io_eof;}
	int64 GetFileSize() {return file_size;}
	int64 GetFilePos() {return file_pos;}
};

// ********************************************************************************************
int32 CFastqFile::Getc()
{
	if(io_eof)
		return EOF;

	if(io_buffer_pos == -1)
	{
		//io_buffer_size = (uint32) fread(io_buffer, 1, IO_BUFFER_SIZE, file);
        memset(io_buffer, 0, IO_BUFFER_SIZE);
        memcpy(io_buffer, data + dataoffset, IO_BUFFER_SIZE);
        io_buffer_size = strlen((char *)io_buffer);
        dataoffset += io_buffer_size;
        
		if(io_buffer_size == 0)
		{
			io_eof = true;
			return EOF;
		}
		io_buffer_pos = 0;
	}

	if(io_buffer_pos >= io_buffer_size)
	{
		io_eof = true;
		return EOF;
	}

	int32 c = io_buffer[io_buffer_pos++];
	file_pos++;
	if(io_buffer_pos >= IO_BUFFER_SIZE)
		io_buffer_pos = -1;

	return c;
}

// ********************************************************************************************
void CFastqFile::UnGetc()
{
	if(io_eof)
		return;

	if(io_buffer_pos == -1)
		io_buffer_pos = IO_BUFFER_SIZE-1;
	else
		io_buffer_pos--;
	file_pos--;
}

// ********************************************************************************************
bool CFastqFile::Putc(const unsigned char c, char *output_data, int64& offset)
{
	if(io_buffer_pos == IO_BUFFER_SIZE)
	{
		//fwrite(io_buffer, 1, IO_BUFFER_SIZE, file);
        //out_data += (char *)io_buffer;
        memcpy(output_data + offset, (char *)io_buffer, IO_BUFFER_SIZE);
        offset += IO_BUFFER_SIZE;
		io_buffer_pos = 0;
	}
	io_buffer[io_buffer_pos++] = c;
	file_pos++;

	return true;
}

// ********************************************************************************************
bool CFastqFile::Puts(const unsigned char *str, const uint32 len, bool eol, char *output_data, int64& offset)
{
	for(uint32 i = 0; i < len; ++i)
		Putc(str[i], output_data, offset);
	if(eol)
		Putc('\n', output_data, offset);

	return true;
}

// ********************************************************************************************
bool CFastqFile::TransferN(CFastqRecord &rec)
{
	uint32 i, j;
    //rec.no_N1 = 0;
    //rec.no_N2 = 0;
	for(i = j = 0; i < rec.seq_len; ++i)
	{
        if(rec.seq[i] == 'N') {
			rec.quality[i] += 128;
            /*
            if (i < rec.seq_len / 2) {
                rec.no_N1++;
            } else {
                rec.no_N2++;
            }
             */
        }
		else
			rec.seq[j++] = rec.seq[i];
	}
	rec.seq_len = j;
	rec.seq[j] = '\0';

	return true;
}

// ********************************************************************************************
bool CFastqFile::UnTransferN(CFastqRecord &rec)
{
	if(rec.seq_len == rec.quality_len)
		return true;

	if(rec.seq_size < rec.quality_size)
		rec.ExtendTo(rec.seq, rec.seq_size, rec.quality_size);

	int32 i = rec.seq_len-1;
	int32 j = rec.quality_len-1;

	for(; j >= 0; --j)
	{
		if(rec.quality[j] >= 128)
		{
			rec.seq[j] = 'N';
			rec.quality[j] -= 128;
		}
		else
			rec.seq[j] = rec.seq[i--];
	}
	rec.seq_len = rec.quality_len;

	return true;
}

// ********************************************************************************************
class CBitStream {
	FILE *file;

	unsigned char *io_buffer;
	int32 io_buffer_pos;
	int32 io_buffer_size;
	uint32 word_buffer;
	int32 word_buffer_pos;
	int32 word_buffer_size;
	t_mode mode;
	uint32 n_bit_mask[32];

public:
	inline bool FlushFullWordBuffer();
	inline bool FlushPartialWordBuffer();
	inline bool FlushInputWordBuffer();
	inline bool WriteBuffer();
	inline bool ReadBuffer();

	int32 IO_BUFFER_SIZE;
	uint64 file_pos;
	uint64 file_size;

	CBitStream();
	~CBitStream();

	bool Open(char *file_name);
	bool OpenRA(char *file_name);
	bool Create(char *file_name);
	bool Close();

	inline uint64 GetPos(void);
	bool SetPos(uint64 pos);

	inline bool PutBit(const uint32 word);
	inline bool Put2Bits(const uint32 word);
	inline bool PutBits(uint32 word, int32 n_bits);
	inline bool PutBytes(const unsigned char *data, int32 n_bytes);
	inline bool PutByte(const unsigned char byte);
	inline bool PutWord(const uint32 data);
	inline bool PutDWord(const uint64 data);
	inline bool Put2Bytes(const uint32 data);

	inline bool GetBit(uint32 &word);
	inline bool Get2Bits(uint32 &word);
	inline bool GetBits(uint32 &word, uint32 n_bits);
	inline bool GetBytes(unsigned char *data, uint32 n_bytes);
	inline bool GetByte(uint32 &byte);
	inline bool GetWord(uint32 &data);
	inline bool GetDWord(uint64 &data);
	inline bool Get2Bytes(uint32 &data);

	inline uint32 BitLength(const uint64 x);

	uint64 GetFilePos() {return file_pos;};
};


// ********************************************************************************************
uint64 CBitStream::GetPos(void)
{
	return file_pos;
}

// ********************************************************************************************
bool CBitStream::PutBit(const uint32 word)
{
	if(word_buffer_pos < word_buffer_size)
	{
		word_buffer <<= 1;
		word_buffer += word;
		++word_buffer_pos;
	}
	else
	{
		PutWord(word_buffer);
		word_buffer_pos = 1;
		word_buffer = word;
	}

	return true;			
};

// ********************************************************************************************
bool CBitStream::Put2Bits(const uint32 word)
{
	if(word_buffer_pos + 2 <= word_buffer_size)
	{
		word_buffer <<= 2;
		word_buffer += word;
		word_buffer_pos += 2;
	}
	else if(word_buffer_pos == word_buffer_size)
	{
		PutWord(word_buffer);
		word_buffer_pos = 2;
		word_buffer = word;
	}
	else
	{
		word_buffer <<= 1;
		word_buffer += word >> 1;
		PutWord(word_buffer);
		word_buffer = word & 1;
		word_buffer_pos = 1;
	}

	return true;			
};

// ********************************************************************************************
bool CBitStream::PutBits(uint32 word, int32 n_bits)
{
	int32 rest_bits = word_buffer_size - word_buffer_pos;
	if(n_bits >= rest_bits)
	{
		n_bits -= rest_bits;
		word_buffer <<= rest_bits;
		word_buffer += word >> n_bits;
		word &= n_bit_mask[n_bits];
		word_buffer_pos = 0;
		PutWord(word_buffer);
		word_buffer = 0;
	}
	
	word_buffer     <<= n_bits;
	word_buffer     += word;
	word_buffer_pos += n_bits;

	return true;
}

// ********************************************************************************************
bool CBitStream::FlushFullWordBuffer()
{
	PutWord(word_buffer);
	
	word_buffer     = 0;
	word_buffer_pos = 0;
	
	return true;
}

// ********************************************************************************************
bool CBitStream::FlushPartialWordBuffer()
{
	word_buffer <<= (32 - word_buffer_pos) & 7;

	if(word_buffer_pos > 24)
		PutByte(word_buffer >> 24);
	if(word_buffer_pos > 16)
		PutByte((word_buffer >> 16) & 0xFF);
	if(word_buffer_pos > 8)
		PutByte((word_buffer >> 8) & 0xFF);
	if(word_buffer_pos > 0)
		PutByte(word_buffer & 0xFF);
	
	word_buffer     = 0;
	word_buffer_pos = 0;

	return true;
}

// ********************************************************************************************
bool CBitStream::FlushInputWordBuffer()
{
	word_buffer_pos = 0;

	return true;
}

// ********************************************************************************************
bool CBitStream::WriteBuffer()
{
	fwrite(io_buffer, 1, (size_t) io_buffer_pos, file);
	io_buffer_pos = 0;

	return true;
}

// ********************************************************************************************
bool CBitStream::ReadBuffer()
{
	size_t to_read = IO_BUFFER_SIZE;

	if(to_read == 0)
		return false;

	io_buffer_size = (int32) fread(io_buffer, 1, to_read, file);
	io_buffer_pos = 0;

	return io_buffer_size > 0;
}

// ********************************************************************************************
bool CBitStream::PutByte(const unsigned char byte)
{
	if(io_buffer_pos >= IO_BUFFER_SIZE)
		WriteBuffer();
	io_buffer[io_buffer_pos++] = byte;
	file_pos++;

	return true;
}

// ********************************************************************************************
bool CBitStream::Put2Bytes(const uint32 data)
{
	PutByte((unsigned char) (data >> 8));
	PutByte((unsigned char) (data & 0xFF));

	return true;
}

// ********************************************************************************************
bool CBitStream::PutBytes(const unsigned char *data, int32 n_bytes)
{
	uint32 to_store = MIN(n_bytes, IO_BUFFER_SIZE-io_buffer_pos);
	do
	{
		copy(data, data+to_store, io_buffer+io_buffer_pos);
		io_buffer_pos += to_store;
		file_pos += to_store;
		if(io_buffer_pos >= IO_BUFFER_SIZE)
			WriteBuffer();
		if(n_bytes -= to_store)
		{
			data += to_store;
			to_store = MIN(n_bytes, IO_BUFFER_SIZE-io_buffer_pos); 
		}
		else
			to_store = 0;
	} while(to_store);

	return true;
}

// ********************************************************************************************
bool CBitStream::PutDWord(const uint64 data)
{
	PutByte(data >> 56);
	PutByte((data >> 48) & 0xFF);
	PutByte((data >> 40) & 0xFF);
	PutByte((data >> 32) & 0xFF);
	PutByte((data >> 24) & 0xFF);
	PutByte((data >> 16) & 0xFF);
	PutByte((data >> 8) & 0xFF);
	PutByte(data & 0xFF);

	return true;
}

// ********************************************************************************************
bool CBitStream::PutWord(const uint32 data)
{
	PutByte(data >> 24);
	PutByte((data >> 16) & 0xFF);
	PutByte((data >> 8) & 0xFF);
	PutByte(data & 0xFF);

	return true;
}
// ********************************************************************************************
uint32 CBitStream::BitLength(const uint64 x)
{
	for(uint32 i = 0; i < 32; ++i)
		if(x < (1ull << i))
			return i;

	return 64;
}

// ********************************************************************************************
bool CBitStream::GetBit(uint32 &word)
{
	if(word_buffer_pos == 0)
	{
		if(!GetByte(word_buffer))
			return false;
		word_buffer_pos = 7;
		word = word_buffer >> 7;
	}
	else
		word = (word_buffer >> (--word_buffer_pos)) & 1;

	return true;
};

// ********************************************************************************************
bool CBitStream::Get2Bits(uint32 &word)
{
	if(word_buffer_pos >= 2)
	{
		word = (word_buffer >> (word_buffer_pos-2)) & 3;
		word_buffer_pos -= 2;
	}
	else if(word_buffer_pos == 0)
	{
		if(!GetByte(word_buffer))
			return false;
		word = word_buffer >> 6;
		word_buffer_pos = 6;
	}
	else
	{
		word = (word_buffer & 1) << 1;
		if(!GetByte(word_buffer))
			return false;
		word += word_buffer >> 7;
		word_buffer_pos = 7;
	}

	return true;
};

// ********************************************************************************************
bool CBitStream::GetBits(uint32 &word, uint32 n_bits)
{
	word = 0;
	while(n_bits)
	{
		if(word_buffer_pos == 0)
		{
			if(!GetByte(word_buffer))
				return false;
			word_buffer_pos = 8;
		}

		if((int32) n_bits > word_buffer_pos)
		{
			word <<= word_buffer_pos;
			word += word_buffer & n_bit_mask[word_buffer_pos];
			n_bits -= word_buffer_pos;
			word_buffer_pos = 0;
		}
		else
		{
			word <<= n_bits;
			word_buffer_pos -= n_bits;
			word += (word_buffer >> word_buffer_pos) & n_bit_mask[n_bits];
			return true;
		}
	}

	return true;
}

// ********************************************************************************************
bool CBitStream::GetByte(uint32 &byte)
{
	if(io_buffer_pos >= IO_BUFFER_SIZE)
		if(!ReadBuffer())
			return false;
	byte = io_buffer[io_buffer_pos++];
	file_pos++;

	return true;
};

// ********************************************************************************************
bool CBitStream::GetBytes(unsigned char *data, uint32 n_bytes)
{
	uint32 to_read = MIN(n_bytes, (uint32) (IO_BUFFER_SIZE-io_buffer_pos));
	do
	{
		copy(io_buffer+io_buffer_pos, io_buffer+io_buffer_pos+to_read, data);
		io_buffer_pos += to_read;
		file_pos      += to_read;
		if(io_buffer_pos >= IO_BUFFER_SIZE)
			ReadBuffer();
		if(n_bytes -= to_read)
		{
			data += to_read;
			to_read = MIN(n_bytes, (uint32) (IO_BUFFER_SIZE-io_buffer_pos)); 
		}
		else
			to_read = 0;
	} while(to_read);
	
	return true;
}

// ********************************************************************************************
bool CBitStream::GetWord(uint32 &data)
{
	uint32 c;
	bool r;

	r = GetByte(c);
	data = c;
	r &= GetByte(c);
	data = (data << 8) + c;
	r &= GetByte(c);
	data = (data << 8) + c;
	r &= GetByte(c);
	data = (data << 8) + c;

	return r;
}

// ********************************************************************************************
bool CBitStream::GetDWord(uint64 &data)
{
	uint32 c;
	bool r;

	r = GetByte(c);
	data = c;
	r &= GetByte(c);
	data = (data << 8) + c;
	r &= GetByte(c);
	data = (data << 8) + c;
	r &= GetByte(c);
	data = (data << 8) + c;
	r &= GetByte(c);
	data = (data << 8) + c;
	r &= GetByte(c);
	data = (data << 8) + c;
	r &= GetByte(c);
	data = (data << 8) + c;
	r &= GetByte(c);
	data = (data << 8) + c;

	return r;
}

// ********************************************************************************************
bool CBitStream::Get2Bytes(uint32 &data)
{
	uint32 c;
	bool r;

	r = GetByte(c);
	data = c;
	r &= GetByte(c);
	data = (data << 8) + c;

	return r;
}

#endif

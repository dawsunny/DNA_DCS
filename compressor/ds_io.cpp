/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#include "ds_defs.h"
#include "ds_io.h"
#include <algorithm>

using namespace std;

int32 CFastqFile::IO_BUFFER_SIZE = 1 << 20;

// ********************************************************************************************
CFastqRecord::CFastqRecord() 
{
	title_size = seq_size = plus_size = quality_size = 32;
	title_len = seq_len = plus_len = quality_len = 0;
	title   = new unsigned char[title_size+1];
	seq     = new unsigned char[seq_size+1];
	plus    = new unsigned char[plus_size+1];
	quality = new unsigned char[quality_size+1];
	rec_len = 0;
	rec_th_len = 0;
	lz_inserted = false;
    
    //bxz
    no_N1 = 0;
    no_N2 = 0;
};
	
// ********************************************************************************************
CFastqRecord::CFastqRecord(const CFastqRecord& x) 
{
	title_size   = x.title_size;
	seq_size     = x.seq_size;
	plus_size    = x.plus_size;
	quality_size = x.quality_size;
		
	title_len   = x.title_len;
	seq_len     = x.seq_len;
	plus_len    = x.plus_len;
	quality_len = x.quality_len;

	title   = new unsigned char[title_size+1];
	seq     = new unsigned char[seq_size+1];
	plus    = new unsigned char[plus_size+1];
	quality = new unsigned char[quality_size+1];

	memcpy(title  , x.title  , title_len+1);
	memcpy(seq    , x.seq    , seq_len+1);
	memcpy(plus   , x.plus   , plus_len+1);
	memcpy(quality, x.quality, quality_len+1);

	rec_len     = x.rec_len;
	rec_th_len  = x.rec_th_len;
	lz_inserted = x.lz_inserted;
    
    //bxz
    no_N1 = x.no_N1;
    no_N2 = x.no_N2;
};

// ********************************************************************************************
CFastqRecord::~CFastqRecord()
{
	delete[] title;
	delete[] seq;
	delete[] plus;
	delete[] quality;
};

// ********************************************************************************************
bool CFastqRecord::Set(const CFastqRecord &x)
{
	if(title_size < x.title_len+1)
	{
		delete[] title;
		title_len = x.title_len;
		title_size = title_len;
		title = new unsigned char[title_size+1];
	}
	else
		title_len = x.title_len;
	copy(x.title, x.title+title_len+1, title);

	if(seq_size < x.seq_len+1)
	{
		delete[] seq;
		seq_len = x.seq_len;
		seq_size = seq_len;
		seq = new unsigned char[seq_size+1];
	}
	else
		seq_len = x.seq_len;
	copy(x.seq, x.seq+seq_len+1, seq);

	if(plus_size < x.plus_len+1)
	{
		delete[] plus;
		plus_len = x.plus_len;
		plus_size = plus_len;
		plus = new unsigned char[plus_size+1];
	}
	else
		plus_len = x.plus_len;
	copy(x.plus, x.plus+plus_len+1, plus);

	if(quality_size < x.quality_len+1)
	{
		delete[] quality;
		quality_len = x.quality_len;
		quality_size = quality_len;
		quality = new unsigned char[quality_size+1];
	}
	else
		quality_len = x.quality_len;
	copy(x.quality, x.quality+quality_len+1, quality);

	rec_len = x.rec_len;
	lz_inserted = x.lz_inserted;

	return true;
}

// ********************************************************************************************
void CFastqRecord::Reset()
{
	title_len = seq_len = plus_len = quality_len = 0;
	rec_len = 0;
	lz_inserted = false;
}

// ********************************************************************************************
bool CFastqFile::Open(char *file_name)
{
	if((file = my_fopen(file_name, "rb")) != NULL)
		mode = mode_read;

	if(mode != mode_read)
		return false;

	my_fseek(file, 0, SEEK_END);
	file_size = my_ftell(file);
	my_fseek(file, 0, SEEK_SET);

	return true;
}

// ********************************************************************************************
bool CFastqFile::Create(char *file_name)
{
	if((file = my_fopen(file_name, "wb")) != NULL)
		mode = mode_write;
	io_buffer_pos = 0;
	file_pos = 0;
	file_size = 0;
	return mode == mode_write;
}

// ********************************************************************************************
bool CFastqFile::Close()
{
	if(!file)
		return false;

	if(mode == mode_write && io_buffer_pos > 0)
	{
		fwrite(io_buffer, 1, io_buffer_pos, file);
		file_size = file_pos;
	}
	fclose(file);
	file = NULL;

	return true;
}

// ********************************************************************************************
bool CFastqFile::ReadRecord(CFastqRecord &rec)
{
	if(mode != mode_read)
		return false;
	
	bool r = ReadTitle(rec) && ReadSeq(rec) && ReadPlus(rec) && ReadQuality(rec);
	
	if(!r)
		return false;

    //return true;
	return TransferN(rec);
}

// ********************************************************************************************
bool CFastqFile::WriteRecord(CFastqRecord &rec)
{
	if(mode != mode_write)
		return false;

	UnTransferN(rec);

	return Puts(rec.title, rec.title_len) && Puts(rec.seq, rec.seq_len) &&
		Puts(rec.plus, rec.plus_len) && Puts(rec.quality, rec.quality_len);
}


// ********************************************************************************************
bool CFastqFile::ReadTitle(CFastqRecord &rec)
{
	uint32 i = 0;
	for(;;)
	{
		int32 c = Getc();
		if(c == EOF)
			break;
		if(c == '\n' || c == '\r')
		{
			if(i > 0)
				break;
		}
		else
		{
			if(i >= rec.title_size)
				rec.Extend(rec.title, rec.title_size);
			rec.title[i++] = (unsigned char) c;
		}
	}
	rec.title[i] = 0;
	rec.title_len = i;

	return i > 0 && rec.title[0] == '@';
}

// ********************************************************************************************
// Todo: Verify (plus == title)
bool CFastqFile::ReadPlus(CFastqRecord &rec)
{
	uint32 i = 0;
	int32 c;
	for(;;)
	{
		c = Getc();
		if(c == EOF)
			break;
		if(c == '\n' || c == '\r')
		{
			if(i > 0)
				break;
		}
		else
		{
			if(i >= rec.plus_size)
				rec.Extend(rec.plus, rec.plus_size);
			rec.plus[i++] = (unsigned char) c;
		}
	}
	rec.plus[i] = 0;
	rec.plus_len = i;

	return i > 0;
}

// ********************************************************************************************
bool CFastqFile::ReadSeq(CFastqRecord &rec)
{
	uint32 i = 0;
	int32 c;
	for(;;)
	{
		c = Getc();
		if(c == EOF)
			break;
		if(c == '\n' || c == '\r')
			;
		else if(c == '+')
		{
			UnGetc();
			break;
		}
		else
		{
			if(i >= rec.seq_size)
				rec.Extend(rec.seq, rec.seq_size);
			rec.seq[i++] = (unsigned char) c;
		}
	}
	rec.seq[i] = 0;
	rec.seq_len = i;

	return i > 0;
}

// ********************************************************************************************
bool CFastqFile::ReadQuality(CFastqRecord &rec)
{
	uint32 i;

	if(rec.seq_size > rec.quality_size)
		rec.ExtendTo(rec.quality, rec.quality_size, rec.seq_size);

	for(i = 0; i < rec.seq_len;)
	{
		int32 c = Getc();
		if(c == EOF)
			break;
		if(c == '\n' || c == '\r')
			;
		else
			rec.quality[i++] = c;
	}
	rec.quality[i] = 0;
	rec.quality_len = i;

	return (i > 0) && (i == rec.seq_len);
}

// ********************************************************************************************
CBitStream::CBitStream()
{
	file		     = NULL;
	io_buffer	     = NULL;
	io_buffer_size   = 0;
	io_buffer_pos    = 0;
	file_pos         = 0;
	word_buffer_pos  = 0;
	word_buffer		 = 0;
	word_buffer_size = 32;
	mode			 = mode_none;

	for(int32 i = 0; i < 32; ++i)
		n_bit_mask[i] = (1u << i) - 1;
}

// ********************************************************************************************
CBitStream::~CBitStream()
{
	delete[] io_buffer;

	if(file)
		fclose(file);
}

// ********************************************************************************************
bool CBitStream::Create(char *file_name)
{
	if(file)
		fclose(file);
	if((file = my_fopen(file_name, "wb")) != NULL)
		mode = mode_write;

	IO_BUFFER_SIZE   = 1 << 20;
	io_buffer	     = new unsigned char[IO_BUFFER_SIZE];
	io_buffer_size   = IO_BUFFER_SIZE;

	word_buffer_size = 32;
	file_pos = 0;

	return mode == mode_write;	
}

// ********************************************************************************************
bool CBitStream::Open(char *file_name)
{
	if(file)
		fclose(file);
	if((file = my_fopen(file_name, "rb")) != NULL)
		mode = mode_read;

	IO_BUFFER_SIZE   = 1 << 20;
	io_buffer	     = new unsigned char[IO_BUFFER_SIZE];
	io_buffer_size   = IO_BUFFER_SIZE;

	word_buffer_size = 8;
	io_buffer_pos    = IO_BUFFER_SIZE;
	file_pos         = 0;

	if(mode == mode_read)
	{
		my_fseek(file, 0, SEEK_END);
		file_size = my_ftell(file);
		my_fseek(file, 0, SEEK_SET);
	} 
	
	return mode == mode_read;
}

// ********************************************************************************************
bool CBitStream::OpenRA(char *file_name)
{
	if(file)
		fclose(file);
	if((file = my_fopen(file_name, "rb")) != NULL)
		mode = mode_read_ra;

	IO_BUFFER_SIZE   = 1 << 12;
	io_buffer	     = new unsigned char[IO_BUFFER_SIZE];
	io_buffer_size   = IO_BUFFER_SIZE;

	word_buffer_size = 8;
	io_buffer_pos    = IO_BUFFER_SIZE;
	file_pos         = 0;

	if(mode == mode_read_ra)
	{
		my_fseek(file, 0, SEEK_END);
		file_size = my_ftell(file);

		my_fseek(file, 0, SEEK_SET);
	} 
	
	return mode == mode_read_ra;
}

// ********************************************************************************************
bool CBitStream::Close()
{
	if(!file)
		return true;

	if(mode == mode_write)
	{
		if(word_buffer_pos)
			FlushPartialWordBuffer();
		WriteBuffer();
	}

	fclose(file);
	file = NULL;
	mode = mode_none;

	return true;
}

// ********************************************************************************************
bool CBitStream::SetPos(uint64 pos)
{
	if(mode != mode_read && mode != mode_read_ra)
		return false;

	if(pos >= file_pos - io_buffer_pos && pos < file_pos - io_buffer_pos + io_buffer_size)
		io_buffer_pos = (int32) (pos - (file_pos - io_buffer_pos));
	else
	{
		if(my_fseek(file, pos, SEEK_SET) == EOF)
			return false;
		io_buffer_pos = IO_BUFFER_SIZE;
		file_pos      = pos;
	}
	
	word_buffer_pos = 0;
	word_buffer     = 0;

	return true;
}


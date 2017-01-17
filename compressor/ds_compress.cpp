/*
  This file is a part of DSRC software distributed under GNU GPL 2 licence.
  The homepage of the DSRC project is http://sun.aei.polsl.pl/dsrc
  
  Authors: Sebastian Deorowicz and Szymon Grabowski
  
  Version: 0.2
*/

#include "ds_config.h"
#include <map>
#include <vector>
#include <algorithm>
#include <time.h>

#include "ds_io.h"
#include "ds_compress.h"
#include "ds_DSRCFile.h"

using namespace std;

CFastqFile FastqFile;

bool compress(char *data, uint32 datasize, char *out_file_name, bool try_lz, uint32 max_lz_memory)
{
	if(!FastqFile.Open(data, datasize))
		return false;

	CFastqRecord rec;
	CDSRCFile comp_file;
	int64 rec_no = 0;
	clock_t t1 = clock();

	comp_file.Create(out_file_name, try_lz, max_lz_memory);
	//cout << "Compressing " << " of size " << FastqFile.GetFileSize() / 1000000 << "MB\n";

	int64 file_pos;
	int64 comp_file_pos;
	int64 file_size     = FastqFile.GetFileSize();

	while(FastqFile.ReadRecord(rec))
	{
		comp_file.InsertRecord(rec);

        /*
		if((rec_no & (CSuperBlock::MAX_SIZE/8-1)) == 0)
		{
			file_pos      = FastqFile.GetFilePos();
			comp_file_pos = comp_file.GetFilePos();
			cout << "\rProcessed " << file_pos / 1000000 << "MB (";
			cout.precision(2);
			cout.width(6);
			cout.setf(ios::fixed,ios::floatfield);
			cout <<	(100.0 * file_pos / file_size) << "%)";
			if((rec_no & (CSuperBlock::MAX_SIZE-1)) == 0 && rec_no)
			{
				double duration = (double) (clock() - t1) / CLOCKS_PER_SEC;
				double speed = 0.0;
				if(duration)
					speed = file_pos / duration / 1000000;
				if(duration > 0)
					cout << "   Speed: " << speed << "MB/s";
				if(comp_file_pos)
					cout << "   Comp. factor: " << (double) file_pos / comp_file_pos << ":1";
			}
		}
         */
		rec_no++;
	}

	comp_file.Close();
	FastqFile.Close(NULL, rec_no);

	file_pos      = FastqFile.GetFilePos();
	comp_file_pos = comp_file.GetFilePos();
    /*
	cout << "\rProcessed " << file_pos / 1000000 << "MB (";
	cout.precision(2);
	cout.width(6);
	cout.setf(ios::fixed,ios::floatfield);
	cout <<	(100.0 * file_pos / file_size) << "%)";
	double duration = (double) (clock() - t1) / CLOCKS_PER_SEC;
	double speed = 0.0;
	if(duration)
		speed = file_pos / duration / 1000000;
	if(duration > 0)
		cout << "   Speed: " << speed << "MB/s";
	if(comp_file_pos)
		cout << "   Comp. factor: " << (double) file_pos / comp_file_pos << ":1";
	cout << "\n";
     */

	return true;
}

//****************************************************************************************
bool decompress(char *in_file_name, char *output_data)
{
	if(!FastqFile.Create(output_data))
		return false;

	CFastqRecord rec;
	CDSRCFile comp_file;
	int64 rec_no = 0;
	clock_t t1 = clock();

	comp_file.Open(in_file_name);
	cout << "Decompressing " << in_file_name << " of size " << comp_file.GetFileSize() / 1000000 << "MB\n";

	int64 file_size      = FastqFile.GetFileSize();
	int64 comp_file_size = comp_file.GetFileSize();
	int64 file_pos;
	int64 comp_file_pos;

    int64 offset = 0;
    
	while(comp_file.ReadRecord(rec))
	{
		rec_no++;
		if((rec_no & (CSuperBlock::MAX_SIZE/8-1)) == 0)
		{
			file_pos      = FastqFile.GetFilePos();
			comp_file_pos = comp_file.GetFilePos();
			cout << "\rProcessed " << comp_file_pos / 1000000 << "MB (";
			cout.precision(2);
			cout.width(6);
			cout.setf(ios::fixed,ios::floatfield);
			cout <<	(100.0 * comp_file_pos / comp_file_size) << "%)";
			if((rec_no & (CSuperBlock::MAX_SIZE-1)) == 0 && rec_no)
			{
				double duration = (double) (clock() - t1) / CLOCKS_PER_SEC;
				double speed = 0.0;
				if(duration)
					speed = file_pos / duration / 1000000;
				if(duration > 0)
					cout << "   Speed: " << speed << "MB/s";
				if(comp_file_pos)
					cout << "   Comp. factor: " << (double) file_pos / comp_file_pos << ":1   ";
			}
		}

		FastqFile.WriteRecord(rec, output_data, offset);
	}

	FastqFile.Close(output_data, offset);
	comp_file.Close();

	file_size     = FastqFile.GetFileSize();
	file_pos      = file_size;
	comp_file_pos = comp_file_size;
	cout << "\rProcessed " << comp_file_pos / 1000000 << "MB (";
	cout.precision(2);
	cout.width(6);
	cout.setf(ios::fixed,ios::floatfield);
	cout <<	(100.0 * comp_file_pos / comp_file_size) << "%)";
	double duration = (double) (clock() - t1) / CLOCKS_PER_SEC;
	double speed = 0.0;
	if(duration)
		speed = file_pos / duration / 1000000;
	if(duration > 0)
		cout << "   Speed: " << speed << "MB/s";
	if(comp_file_pos)
		cout << "   Comp. factor: " << (double) file_pos / comp_file_pos << ":1   ";
	cout << "\n";
	
	return true;
}

//****************************************************************************************
bool extract_record(char *in_file_name, char *out_file_name, uint64 rec_id)
{
	if(!FastqFile.Create(out_file_name))
		return false;

	CFastqRecord rec;
	CDSRCFile comp_file;
	int64 rec_no = 0;
	clock_t t1 = clock();

	comp_file.OpenRA(in_file_name);
	cout << "Extracting record from " << in_file_name << " of size " << comp_file.GetFileSize() / 1000000 << "MB\n";

	int64 comp_file_size = comp_file.GetFileSize();

	comp_file.ExtractRecord(rec, rec_id);
	//FastqFile.WriteRecord(rec);

	//comp_file.Close();
	//FastqFile.Close(NULL);

	return true;
}

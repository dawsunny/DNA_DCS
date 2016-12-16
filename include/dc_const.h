#ifndef __DC_CONST_H_
#define __DC_CONST_H_
#include "dcs_const.h"

//read reference sequences
#define REF_CHUNK 1000000
#define INPUT_CHUNK FA_CHUNK_SIZE//10000
//float DIF_RATE;  //compress
//int OVERLAP;  //(REF_CHUNK + (int)(REF_CHUNK * DIF_RATE))

//find match
#define MIN_PROGRESS 200
#define MAX_CONSEC_MISMATCH 3
#define MAX_CHECK_DIST 10
#define CHECK_IDENT_THRESH 0.4
//int MAX_DP_LEN; //10+(int)(50*DIF_RATE) [10,25] //compress
#define MIN_DP_LEN 4

//seed
#define SAMPLE_RATE 64
#define SEED_LEN 10
#define MAX_SEED_FREQ 500
#define MEGA (1 << (2 * SEED_LEN))

//thread number
//int THREAD_NUM;  //compress

//file
#define FILE_PATH_LEN 500
#define LINE_BUF_LEN  500
//int LINE_LEN; //decompress

#define MIN(a,b) ((a) >= (b) ? (b) : (a))

#endif

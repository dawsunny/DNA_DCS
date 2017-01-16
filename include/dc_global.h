#ifndef __DC_GLOBAL_H_
#define __DC_GLOBAL_H_

#include <dc_type.h>
#include <string>
using namespace std;

//defined in dc_io.c

//read reference sequences
extern float dc_c_DIF_RATE;
extern int dc_c_OVERLAP;  //(REF_CHUNK + (int)(REF_CHUNK * dc_c_DIF_RATE))

//find match
extern int MAX_DP_LEN; //10+(int)(50*dc_c_DIF_RATE) [10,25]

//thread number
extern int THREAD_NUM;

void print_time(char *explain, struct timeval start_time, struct timeval end_time);
dc_s32_t dc_c_main(dc_s8_t *, dc_u32_t, dc_s8_t *);


//decompress
extern float dc_d_DIF_RATE;
extern int dc_d_OVERLAP;  //(REF_CHUNK + (int)(REF_CHUNK * dc_d_DIF_RATE))

//file
extern int LINE_LEN;
dc_s32_t dc_d_main(dc_s8_t *, string &, dc_u32_t);
#endif

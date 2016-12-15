#ifndef __DC_GLOBAL_H_
#define __DC_GLOBAL_H_

#include <dc_type.h>

//defined in dc_io.c

//read reference sequences
extern float dc_c_DIF_RATE;
extern int dc_c_OVERLAP;  //(REF_CHUNK + (int)(REF_CHUNK * dc_c_DIF_RATE))

//find match
extern int MAX_DP_LEN; //10+(int)(50*dc_c_DIF_RATE) [10,25]

//thread number
extern int THREAD_NUM;

dc_s32_t dc_c_main( dc_s32_t argc, dc_s8_t *argv[] );
#endif

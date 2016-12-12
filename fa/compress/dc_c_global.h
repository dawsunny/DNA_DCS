#ifndef __DC_GLOBAL_H_
#define __DC_GLOBAL_H_

//defined in dc_io.c

//read reference sequences
extern float DIF_RATE;
extern int OVERLAP;  //(REF_CHUNK + (int)(REF_CHUNK * DIF_RATE))

//find match
extern int MAX_DP_LEN; //10+(int)(50*DIF_RATE) [10,25]

//thread number
extern int THREAD_NUM;

#endif

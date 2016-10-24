/*data chunking*/

#include "amp.h"
#include "dcs_const.h"
#include "dcs_debug.h"
#include "dcs_list.h"
#include "dcs_msg.h"
#include "dcs_type.h"

#include "hash.h"
#include "server_cnd.h"
#include "server_thread.h"
#include "server_op.h"
#include "server_map.h"
#include "chunker.h"

#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/ioctl.h>
#include <errno.h>
//#include <stropts.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <string.h>
#include <semaphore.h>
#include "checksum.h"

/*server split the superchunk into some small chunks
  and comput the SHA1 value.
 * malloc space for sha_array_t to store sha1 value
 * get fix size char from buf and compute sha1 value 
 * finish if the offset equal to bufsize
 */
chunk_info_t *__dcs_get_fix_chunk(dcs_s8_t *buf,
                                    dcs_u32_t bufsize,
                                    dcs_u64_t fileoffset)
{
    dcs_s32_t rc = 0;
    dcs_u32_t bufoffset = 0;
    dcs_u32_t chunksize = 0;
    dcs_u32_t chunk_num = 0;
    dcs_u32_t array_id = 0;
    dcs_u32_t tail = 0;
    dcs_u8_t  *chunk = NULL;
    dcs_u8_t  *tmpsha = NULL;

    sha_array_t *sha_array = NULL;
    chunk_info_t *chunk_detail = NULL;

    DCS_ENTER("__dcs_get_fix_chunk enter \n");

    chunk_detail = (chunk_info_t *)malloc(sizeof(chunk_info_t));
    if(chunk_detail == NULL){
        DCS_ERROR("__dcs_get_fix_chunk malloc for chunk_detail err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(chunk_detail, 0, sizeof(chunk_info_t));

    chunksize = server_chunk_size;
    DCS_MSG("__dcs_get_fix_chunk chunksize %d \n", chunksize);

    chunk = (dcs_u8_t *)malloc(chunksize);
    if(chunk == NULL){
        DCS_ERROR("__dcs_get_fix_chunk malloc for chunk err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    tmpsha = (dcs_u8_t *)malloc(sizeof(dcs_u8_t)*20);
    if(tmpsha == NULL){
        DCS_ERROR("__dcs_get_fix_chunk malloc for tmpsha err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    chunk_num = bufsize / chunksize;
    if(bufsize%chunksize){
        chunk_num++;
    }
    chunk_detail->chunk_num = chunk_num;

    //DCS_MSG("__dcs_get_fix_chunk bufsize is %d, chunksize is %d \n", bufsize, chunksize);
    DCS_MSG("__dcs_get_fix_chunk chunk_num is %d \n", chunk_num);

    //DCS_MSG("1\n");
    sha_array = (sha_array_t *)malloc(sizeof(sha_array_t)*chunk_num);
    if(sha_array == NULL){
        DCS_ERROR("__dcs_get_fix_chunk malloc for sha_array err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(sha_array, 0, sizeof(sha_array_t)*chunk_num);
    //DCS_MSG("2\n");

    while(bufoffset < bufsize){
    //DCS_MSG("3\n");
        tail = bufsize - bufoffset;
        memset(chunk, 0, chunksize);
        //memcpy(chunk, buf + bufoffset, chunksize);
        
        memset(tmpsha, 0, 20);
        if(tail < chunksize){
    //DCS_MSG("4\n");
            memcpy(chunk, buf + bufoffset, tail);
            SHA1(chunk, tail, tmpsha);
            sha_array[array_id].offset = bufoffset + fileoffset;
            sha_array[array_id].chunksize = tail;
            memcpy(sha_array[array_id].sha, tmpsha, 20);
        } else{
    //DCS_MSG("5\n");
            memcpy(chunk, buf + bufoffset, chunksize);
            SHA1(chunk, chunksize, tmpsha);
            sha_array[array_id].offset = bufoffset + fileoffset;
            sha_array[array_id].chunksize = chunksize;
            memcpy(sha_array[array_id].sha, tmpsha, 20);
        }
        
    //DCS_MSG("6\n");
        bufoffset = bufoffset + chunksize;
        array_id++;
    }

    DCS_MSG("the array_id is %d \n", array_id);
    //DCS_MSG("the sha string is %s \n", sha_array[array_id - 2].sha);
     chunk_detail->sha_array = sha_array;
EXIT:
    if(tmpsha != NULL){
        free(tmpsha);
        tmpsha = NULL;
    }
    if(chunk != NULL){
        free(chunk);
        chunk = NULL;
    }

    DCS_LEAVE("__dcs_get_fix_chunk leave \n");
    if(rc == 0){
        return chunk_detail;
    }
    else
    {
        free(sha_array);
        sha_array = NULL;
        return NULL;
    }
}

/*split the superchunk into different size chunk
 and compute sha1 value
 * prepare the array to stroe chunk info
 * get the chunk according cdc algorithms
 * stop if bufoffset equal to bufsize
 */
chunk_info_t *__dcs_get_var_chunk(dcs_s8_t *buf,
                                    dcs_u32_t bufsize,
                                    dcs_u64_t fileoffset)
{
    dcs_s32_t rc = 0;
    dcs_u32_t tail = 0;
    dcs_u32_t bufoffset = 0;
    dcs_u32_t chunkoffset = 0;
    dcs_u32_t chunksize = 0;
    dcs_u32_t array_id = 0;
    dcs_u32_t chunk_num = 0;
    dcs_u8_t  *tmpchunk = NULL;
    dcs_s8_t  *winbuf  = NULL;
    dcs_u8_t  *tmpsha = NULL;

    sha_array_t *sha_array = NULL;
    sha_array_t *tmp_sha_array = NULL;
    chunk_info_t *chunk_detail = NULL;
    
    dcs_u32_t rabin = 0;

    DCS_ENTER("__dcs_get_var_chunk enter \n");

    tmpchunk = (dcs_u8_t *)malloc(CHUNK_MAX_SIZE);
    if(tmpchunk == NULL){
        DCS_ERROR("__dcs_get_var_chunk malloc for tmpchunk err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    chunk_detail = (chunk_info_t *)malloc(sizeof(chunk_info_t));
    if(chunk_detail == NULL){
        DCS_ERROR("__dcs_get_var_chunk malloc for chunk detail err:%d \n",errno);
        rc = errno;
        goto EXIT;
    }
    memset(chunk_detail, 0, sizeof(chunk_info_t));

    //DCS_MSG("1 \n");
    tmpsha = (dcs_u8_t *)malloc(20);
    if(tmpsha == NULL){
        DCS_ERROR("__dcs_get_var_chunk malloc for tmpsha err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }

    //DCS_MSG("2 \n");
    winbuf = (dcs_s8_t *)malloc(CHUNK_WIN_SIZE);
    if(winbuf == NULL){
        DCS_ERROR("__dcs_get_var_chunk malloc for winbuf err:%d \n", errno);
        rc = errno;
        goto EXIT;
    }

    chunk_num = bufsize / CHUNK_MIN_SIZE;
    if((bufsize%CHUNK_MIN_SIZE)){
        chunk_num++;
    }

    tmp_sha_array = (sha_array_t *)malloc(sizeof(sha_array_t)*chunk_num);
    if(tmp_sha_array == NULL){
        DCS_ERROR("__dcs_get_var_chunk malloc for tmp_sha_array err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }
    memset(tmp_sha_array, 0, sizeof(sha_array_t)*chunk_num);

    //DCS_MSG("3 \n");
    while(bufoffset < bufsize){
        chunksize = 0;
        chunkoffset = 0;
        tail = bufsize - bufoffset;
        memset(tmpchunk, 0 , CHUNK_MAX_SIZE);
        memset(tmpsha, 0, 20);
        
        //DCS_MSG("4 \n");
        if(tail <= CHUNK_MIN_SIZE){
            //DCS_MSG("bufoffset is %d , tail is %d \n", bufoffset, tail);
            memcpy(tmpchunk, buf + bufoffset, tail);
            chunksize = tail;
            SHA1(tmpchunk, tail, tmpsha);
            //DCS_MSG("chunk_num is %d, and the array_id %d \n", chunk_num, array_id);
            tmp_sha_array[array_id].offset = bufoffset + fileoffset;
            tmp_sha_array[array_id].chunksize = tail;
            memcpy(tmp_sha_array[array_id].sha, tmpsha, 20);
            //DCS_MSG("array_id is %d \n", array_id);
        }
        else{
            while(chunkoffset < CHUNK_MAX_SIZE && 
                    (bufoffset + chunkoffset) < bufsize){
                memset(winbuf, 0, CHUNK_WIN_SIZE);

                /*get winbuf*/
                if((bufsize - bufoffset - chunkoffset) >= CHUNK_WIN_SIZE){
                    memcpy(winbuf, buf + bufoffset + chunkoffset, CHUNK_WIN_SIZE);
                }
                else{
                    memcpy(winbuf, buf + bufoffset + chunkoffset, bufsize - bufoffset - chunkoffset);
                }

                //rabin = rabin_hash(winbuf);
                //rabin = adler32_checksum(winbuf, CHUNK_WIN_SIZE);
                //rabin = sha_hash((dcs_u8_t *)winbuf, CHUNK_WIN_SIZE);
                rabin = ELF_hash((dcs_u8_t *)winbuf);
                if((rabin % CHUNK_CDC_D) == CHUNK_CDC_R){
                    //DCS_MSG("6000 \n");
                    if(chunkoffset + CHUNK_WIN_SIZE >= CHUNK_MIN_SIZE){

                        if((bufsize - bufoffset - chunkoffset) >= CHUNK_WIN_SIZE){
                            chunkoffset = chunkoffset + CHUNK_WIN_SIZE;
                        }
                        else{
                            chunkoffset = chunkoffset + bufsize - bufoffset - chunkoffset;
                        }

                        //chunkoffset = chunkoffset + strlen(winbuf);
                        //DCS_MSG("the chunkoffset is %d , and winbuf len is %ld \n", chunkoffset, strlen(winbuf));
                        break;
                    }
                }
                //DCS_MSG("60000 \n");
                //chunkoffset = chunkoffset + strlen(winbuf);
                if((bufsize - bufoffset - chunkoffset) >= CHUNK_WIN_SIZE){
                    chunkoffset = chunkoffset + CHUNK_WIN_SIZE;
                }
                else{
                    chunkoffset = chunkoffset + bufsize - bufoffset - chunkoffset;
                }
                //DCS_MSG("the chunkoffset is %d , and winbuf len is %ld \n", chunkoffset, strlen(winbuf));
            }
            
            //DCS_MSG("700 \n");
            //DCS_MSG("bufsize is %d , bufoffset is %d and chunkoffset is %d \n", bufsize, bufoffset, chunkoffset);
            memcpy(tmpchunk, buf + bufoffset, chunkoffset);
            chunksize = chunkoffset;
            
            //DCS_MSG("701 \n");
            SHA1(tmpchunk, chunksize, tmpsha);
            tmp_sha_array[array_id].offset = bufoffset + fileoffset;
            tmp_sha_array[array_id].chunksize = chunksize;
            //DCS_MSG("702 \n");
            memcpy(tmp_sha_array[array_id].sha, tmpsha, 20);
            //DCS_MSG("703 \n");
        }

        //DCS_MSG("7 \n");
        bufoffset = bufoffset + chunksize;
        array_id++;
        //DCS_MSG("the array_id is %d \n", array_id);
    }

    //DCS_MSG("8 \n");
    //chunk_detail->chunk_num = array_id;
    DCS_MSG("the array_id is %d and chunk_num is %d\n", array_id, chunk_num);
    //DCS_MSG("80 \n");
    sha_array = (sha_array_t *)malloc(sizeof(sha_array_t)*array_id);
    if(sha_array == NULL){
        DCS_ERROR("__dcs_get_var_chunk malloc for sha_array err: %d \n", errno);
        rc = errno;
        goto EXIT;
    }
    //DCS_MSG("9 \n");
    //DCS_MSG("the array_id is %d \n", array_id);
    memset(sha_array, 0, sizeof(sha_array_t)*array_id);
    memcpy(sha_array, tmp_sha_array, sizeof(sha_array_t)*array_id);

    //DCS_MSG("10 \n");
    chunk_detail->sha_array = sha_array;
    //DCS_MSG("11 \n");
    chunk_detail->chunk_num = array_id;
    //DCS_MSG("12 \n");
    //DCS_MSG("last chunk offset:%ld, and last chunksize %d \n", sha_array[array_id -1].offset, sha_array[array_id -1].chunksize);
EXIT:
    //DCS_MSG("4 \n");
    if(tmpchunk != NULL){
        free(tmpchunk);
        tmpchunk = NULL;
    }

    //DCS_MSG("5 \n");
    if(winbuf != NULL){
        free(winbuf);
        winbuf = NULL;
    }

    //DCS_MSG("6 \n");
    if(tmpsha != 0){
        free(tmpsha);
        tmpsha = NULL;
    }

    //DCS_MSG("7 \n");
    if(tmp_sha_array != NULL){
        free(tmp_sha_array);
        tmp_sha_array = NULL;
    }

    DCS_LEAVE("__dcs_get_var_chunk leave \n");
    if(rc == 0){
        return chunk_detail;
    }
    else
        return NULL;
}

static int P = 1;
static int table32[256] = {0};
static int table40[256] = {0};
static int table48[256] = {0};
static int table56[256] = {0};

void initialize_tables() 
{
	int i, j;
	int mods[P_DEGREE];
	// We want to have mods[i] == x^(P_DEGREE+i)
	mods[0] = P;
	for (i = 1; i < P_DEGREE; i++) {
		const int lastmod = mods[i - 1];
		// x^i == x(x^(i-1)) (mod P)
		int thismod = lastmod << 1;
		// if x^(i-1) had a x_(P_DEGREE-1) term then x^i has a
		// x^P_DEGREE term that 'fell off' the top end.
		// Since x^P_DEGREE == P (mod P), we should add P
		// to account for this:
		if ((lastmod & X_P_DEGREE) != 0) {
			thismod ^= P;
		}
		mods[i] = thismod;

	}
	// Let i be a number between 0 and 255 (i.e. a byte).
	// Let its bits be b0, b1, ..., b7.
	// Let Q32 be the polynomial b0*x^39 + b1*x^38 + ... + b7*x^32 (mod P).
	// Then table32[i] is Q32, represented as an int (see below).
	// Likewise Q40 be the polynomial b0*x^47 + b1*x^46 + ... + b7*x^40 (mod P).
	// table40[i] is Q40, represented as an int. Likewise table48 and table56.

	for (i = 0; i < 256; i++) {
		int c = i;
		for (j = 0; j < 8 && c > 0; j++) {
			if ((c & 1) != 0) {
				table32[i] ^= mods[j];
				table40[i] ^= mods[j + 8];
				table48[i] ^= mods[j + 16];
				table56[i] ^= mods[j + 24];
			}
			c >>= 1;
		}
	}
}

int compute_w_shifted(const int w){

	return table32[w & 0xFF] ^table40[(w >> 8) & 0xFF] ^table48[(w >> 16) & 0xFF] ^ table56[(w >> 24) & 0xFF];

}

int rabinhash32_func(const char A[], const int offset, const int length, int w) {

    int s = offset;

    // First, process a few bytes so that the number of bytes remaining is a multiple of 4.
    // This makes the later loop easier.
    const int starter_bytes = length % 4;
    if (starter_bytes != 0) {
        const int max = offset + starter_bytes;
        while (s < max) {
            w = (w << 8) ^ (A[s] & 0xFF);
            s++;
        }
    }

    const int max = offset + length;
    while (s < max) {
        w = compute_w_shifted(w) ^
            (A[s] << 24) ^
            ((A[s + 1] & 0xFF) << 16) ^
            ((A[s + 2] & 0xFF) << 8) ^
            (A[s + 3] & 0xFF);
        s += 4;
    }

    return w;
}

int rabinhash32(const char A[], int poly, const int size) {
	P = poly;
	initialize_tables();
	return rabinhash32_func(A, 0, size, 0);
}

unsigned int rabin_hash(char *str)
{
	return rabinhash32(str, 1, strlen(str));
}

/*
 *   a simple 32 bit checksum that can be upadted from either end
 *   (inspired by Mark Adler's Adler-32 checksum)
 */
unsigned int adler32_checksum(char *buf, int len)
{
    int i;
    unsigned int s1, s2;

    s1 = s2 = 0;
    for (i = 0; i < (len - 4); i += 4) {
        s2 += 4 * (s1 + buf[i]) + 3 * buf[i+1] + 2 * buf[i+2] + buf[i+3] +
          10 * CHAR_OFFSET;
        s1 += (buf[i+0] + buf[i+1] + buf[i+2] + buf[i+3] + 4 * CHAR_OFFSET);
    }
    for (; i < len; i++) {
        s1 += (buf[i]+CHAR_OFFSET); 
	s2 += s1;
    }

    return (s1 & 0xffff) + (s2 << 16);
}

/*
 * adler32_checksum(X0, ..., Xn), X0, Xn+1 ----> adler32_checksum(X1, ..., Xn+1)
 * where csum is adler32_checksum(X0, ..., Xn), c1 is X0, c2 is Xn+1
 */
unsigned int adler32_rolling_checksum(unsigned int csum, int len, char c1, char c2)
{
	unsigned int s1, s2;

	s1 = csum & 0xffff;
	s2 = csum >> 16;
	s1 -= (c1 - c2);
	s2 -= (len * c1 - s1);

	return (s1 & 0xffff) + (s2 << 16);
}

/*sha1 hash funtion*/
dcs_u32_t sha_hash(dcs_u8_t *buf, dcs_u32_t len)
{
    dcs_u8_t *tmpsha = NULL;
    dcs_u32_t hkey = 0;
    dcs_s32_t i = 0;

    tmpsha = (dcs_u8_t *)malloc(SHA_LEN);
    if(tmpsha == NULL){
        DCS_MSG("sha_hash malloc for sha err:%d \n", errno);
        exit(1);
    }

    SHA1(buf, len ,tmpsha);

    for(i=0; i<2; i++){
        hkey = hkey << CHAR_BIT;
        hkey = hkey | tmpsha[SHA_LEN - 2 + i]; 
    }

    free(tmpsha);
    tmpsha = NULL;
    DCS_MSG("the sha hash key is %d \n", hkey);

    return hkey;
}

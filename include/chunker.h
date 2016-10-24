/*chunker header file*/

#ifndef __CHUNKER_H__
#define __CHUNKER_H__

#include "server_op.h"
/*
struct sha_word{
    dcs_u64_t offset;
    dcs_u32_t chunksize;
    dcs_u8_t  sha[20];
};
typedef struct sha_word sha_array_t;
*/

#define CHUNK_CDC_D  256
#define CHUNK_CDC_R  255
#define CHUNK_WIN_SIZE  8
#define CHUNK_AVG_SIZE  server_chunk_size
#define CHUNK_MIN_SIZE  (server_chunk_size/4)
#define CHUNK_MAX_SIZE  (server_chunk_size*4)

chunk_info_t *__dcs_get_fix_chunk(dcs_s8_t *buf,
                                    dcs_u32_t bufsize,
                                    dcs_u64_t fileoffset);

chunk_info_t *__dcs_get_var_chunk(dcs_s8_t *buf,
                                    dcs_u32_t bufsize,
                                    dcs_u64_t fileoffset);

/*sha1 hash funtion*/
dcs_u32_t sha_hash(dcs_u8_t *buf, dcs_u32_t len);

#ifdef __cplusplus
extern "C" {
#endif

#define DEFAULT_IRREDUCIBLE_POLY	0x0000008D
#define P_DEGREE			32
#define X_P_DEGREE			(1 << (P_DEGREE - 1))
#define READ_BUFFER_SIZE		1024

int rabinhash32(const char A[], int poly, const int size);
unsigned int rabin_hash(char *str);

#ifdef __cplusplus
}
#endif

#endif

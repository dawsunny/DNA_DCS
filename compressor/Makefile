LDDINC := $(shell pwd)
INC_CLI := $(LDDINC)
INC_PUB  := $(LDDINC)/../include
INC_COM  := $(LDDINC)/../amp/include
EXTRA_CFLAGS	+= -I$(INC_CLI) -I$(INC_PUB) -I$(INC_COM)
EXTRA_CFLAGS	+= -g -Wall 
EXTRA_CFLAGS	+= -O3 -m64 -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE 
EXTRA_CFLAGS	+= -D_GNU_SOURCE
#EXTRA_CFLAGS    += -D__DEDUP_DEBUG__
#EXTRA_CFLAGS    += -D__DEDUP_TIER_BLOOM__
LDPATH_AMP = ${INC_COM}/../lib/
LIBNAME_AMP = amp
LDFLAGS_AMP = -L${LDPATH_AMP} -l${LIBNAME_AMP}

LDFLAGS = -lm -lpthread -lrt
#LDFLAGS = -lm -lpthread -lssl -lcrypto -lrt


CC = g++
CP = cp
RM = rm -f
AR = ar rvs
MV = mv -f

EXE_FILES = compressor

OBJS =	compressor.o	\
		compressor_op.o\
		compressor_cnd.o\
		compressor_thread.o	\
		compressor_cache.o\
		compressor_con.o\
		compressor_index.o\
		bloom.o\
		hash.o\
		dc_c_global.o\
		dc_c_alignment.o\
		dc_c_thread.o\
		dc_c_encode.o\
		dc_c_io.o\
		dc_d_global.o\
		dc_d_io.o\
		dc_d_decode.o\
		dc_d_decompress.o\
		ds_block.o\
		ds_compress.o\
		ds_dsrc.o\
		ds_DSRCFile.o\
		ds_field.o\
		ds_huffman.o\
		ds_io.o\
		ds_lz.o\
		ds_superblock.o\
		ds_Trie.o\


#LIB = libcom.a
.cpp.o:
	${CC}  ${EXTRA_CFLAGS}  -c $*.cpp

compressor: ${OBJS}
	${CC}  -g -o $@ ${OBJS} ${LDFLAGS} ${LDFLAGS_AMP}

#scp mds xingjing@a250:/home/xingjing/sunfs/bin
clean:
	${RM} *.o core ~* ${EXE_FILES} 

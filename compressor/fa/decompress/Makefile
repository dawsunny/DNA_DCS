LDDINC 		 := $(shell pwd)
INC_CLI 	 := $(LDDINC)
INC_PUB 	 := $(LDDINC)/../include
EXTRA_CFLAGS += -I$(INC_CLI) -I$(INC_PUB)
EXTRA_CFLAGS += -g -Wall

#LDFLAGS = -lpthread

BIN_DIR := $(LDDINC)/../../bin/

CC = g++
CP = cp
RM = rm -f
AR = ar rvs
MV = mv -f

EXE_FILES = dc_decompress

OBJS =	dc_d_main.o	\
		dc_d_io.o	\
		dc_d_decompress.o	\
		dc_d_decode.o

.cpp.o:
	${CC} ${EXTRA_CFLAGS} -c $*.cpp

decompress: ${OBJS}
	${CC} -g -o ${EXE_FILES} ${OBJS} 
#	${CP} ${EXE_FILES} ${BIN_DIR} 
	
.PHONY:clean

clean:
	${RM} *.o core *~ ${EXE_FILES} 

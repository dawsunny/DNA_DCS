LDDINC 		 := $(shell pwd)
INC_CLI 	 := $(LDDINC)
INC_PUB 	 := $(LDDINC)/../include
EXTRA_CFLAGS += -I$(INC_CLI) -I$(INC_PUB)
EXTRA_CFLAGS += -g -Wall

LDFLAGS = -lpthread

BIN_DIR := $(LDDINC)/../../bin/

CC = g++
CP = cp
RM = rm -f
AR = ar rvs
MV = mv -f

EXE_FILES = dc_compress

OBJS =	dc_c_main.o	\
		dc_c_io.o	\
		dc_c_thread.o	\
		dc_c_alignment.o	\
		dc_c_encode.o	\


.cpp.o:
	${CC} ${EXTRA_CFLAGS} -c $*.cpp

compress: ${OBJS}
	${CC} -g -o ${EXE_FILES} ${OBJS} ${LDFLAGS}
#	${CP} ${EXE_FILES} ${BIN_DIR}
	
.PHONY:clean

clean:
	${RM} *.o core *~ ${EXE_FILES} 

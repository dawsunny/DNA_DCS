LDDINC := $(shell pwd)
INC_CLI := $(LDDINC)
INC_PUB  := $(LDDINC)/../include
INC_COM  := $(LDDINC)/../amp/include
EXTRA_CFLAGS	+= -I$(INC_CLI) -I$(INC_PUB) -I$(INC_COM) 
EXTRA_CFLAGS	+= -O2 -g -Wall 
EXTRA_CFLAGS	+= -D_GNU_SOURCE
EXTRA_CFLAGS   += -D__DEDUP_DEBUG__
LDPATH_AMP = ${INC_COM}/../lib/
LIBNAME_AMP = amp
LDFLAGS_AMP = -L${LDPATH_AMP} -l${LIBNAME_AMP}

#LDFLAGS = -lm -lpthread -lssl -lcrypto -lrt

LDFLAGS = -lm -lpthread -lrt

CC = g++
CP = cp
RM = rm -f
AR = ar rvs
MV = mv -f

EXE_FILES = client

OBJS =	client.o	\
		client_cnd.o\
		client_thread.o	\
		client_op.o \
        md5.o

#LIB = libcom.a
.cpp.o:
	${CC}  ${EXTRA_CFLAGS}  -c $*.cpp

client: ${OBJS}
	${CC}  -O2 -g -o $@ ${OBJS} ${LDFLAGS} ${LDFLAGS_AMP}

#scp mds xingjing@a250:/home/xingjing/sunfs/bin
clean:
	${RM} *.o core ~* ${EXE_FILES} 

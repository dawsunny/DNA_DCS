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

LDFLAGS = -lm -lpthread -lssl -lcrypto -lrt


CC = g++
CP = cp
RM = rm -f
AR = ar rvs
MV = mv -f

EXE_FILES = server

OBJS =	server.o	\
		server_op.o\
		server_cnd.o\
		server_thread.o	\
		server_map.o\
		chunker.o\
		hash.o\


#LIB = libcom.a
.cpp.o:
	${CC}  ${EXTRA_CFLAGS}  -c $*.cpp

server: ${OBJS}
	${CC}  -O2 -g -o $@ ${OBJS} ${LDFLAGS} ${LDFLAGS_AMP}

#scp mds xingjing@a250:/home/xingjing/sunfs/bin
clean:
	${RM} *.o core ~* ${EXE_FILES} 

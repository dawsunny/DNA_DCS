
CC = g++
RM = rm -f
AR = ar rvs
MV = mv -f
RANLIB = ranlib
CFLAGS = -I ../../include/ -g -Wall 
LDFLAGS = -lm -lpthread 
LIBPATH = ../../lib/

#EXTRA_CFLAGS	+= -D__AMP_LISTEN_SELECT
#EXTRA_CFLAGS	+= -D__AMP_LISTEN_POLL
EXTRA_CFLAGS	+= -D__AMP_LISTEN_EPOLL

# if or not start amp_netmorn_thread
#EXTRA_CFLAGS    +=-D__AMP_ICMP_NETMORN

OBJS = amp_interface.o amp_conn.o amp_utcp.o amp_protos.o amp_request.o \
       amp_uthread.o amp_help.o 
LIB = libamp.a

.cpp.o:
	${CC} ${CFLAGS} ${EXTRA_CFLAGS}  -c $*.cpp


lib: ${OBJS}
	${AR} ${LIB} ${OBJS}
	${RANLIB} ${LIB}
	${MV} ${LIB} ${LIBPATH}

clean:
	${RM} *.o core ~* *.c libamp.a

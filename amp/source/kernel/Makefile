# Comment/uncomment the following line to disable/enable debugging
#DEBUG = y


# Add your debugging flag (or not) to CFLAGS
DEBFLAGS = -O2 -g  
AMPINC=${LDDINC}/../../include
CFLAGS += $(DEBFLAGS)
EXTRA_CFLAGS += -I$(AMPINC)

amp-objs := amp_module.o amp_interface.o amp_conn.o amp_help.o amp_protos.o \
			amp_request.o amp_tcp.o amp_udp.o amp_thread.o

obj-m	:= amp.o


KERNELDIR ?= /lib/modules/$(shell uname -r)/build
PWD       := $(shell pwd)

modules:
	$(MAKE) -C $(KERNELDIR) M=$(PWD) LDDINC=$(PWD) modules


clean:
	rm -rf *.o *~ core .depend .*.cmd *.ko *.mod.c .tmp_versions

depend .depend dep:
	$(CC) $(CFLAGS) -M *.c > .depend


ifeq (.depend,$(wildcard .depend))
include .depend
endif

DIR=/DNA_DCS
all:
	make -C amp/source/userspace
	make -C client
	make -C server
#	make -C master
	make -C compressor

force:
	make -C amp/source/userspace -B
	make -C client -B
	make -C server -B
#	make -C master -B
	make -C compressor -B
	
install:
	mv -f client/client ${DIR}/bin/
	mv -f server/server ${DIR}/bin/
#	mv -f master/master ${DIR}/bin/
	mv -f compressor/compressor ${DIR}/bin/

clean:
	make -C amp/source/userspace clean
	make -C client clean
	make -C server clean
#	make -C master clean
	make -C compressor clean
	rm -rf ${DIR}/bin/client
	rm -rf ${DIR}/bin/server
#	rm -rf ${DIR}/bin/master
	rm -rf ${DIR}/bin/compressor


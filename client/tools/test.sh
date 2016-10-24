#./client -w -b -p ~/VMDK/ 1
#./client -w -f -p ~/VM/ 1
#./client -w -f -p ~/test/temp/ 1
<<COM
rm -rf ~/tmp/*
rm ~/test/DDSS/data/*
rm ~/test/DDSS/fp/*
rm ~/test/data/*
rm ~/test/fp/*
rm ~/test/DDSS/mapfile1/*
rm ~/test/DDSS/server/abtest
rm ~/test/DDSS/server/maptest
rm ~/test/DDSS/deduper/abtest
rm ~/test/DDSS/deduper/writetest
rm ~/test/DDSS/deduper/maptest
#cp ~/test/DNAdata/chr[1-9].fa ~/test/temp/
#cp ~/test/DNAdata/chr1.fa ~/test/temp/
COM
rm ~/test/temp/*
#cp ~/test/DNAdata/chr1.fa ~/test/temp/
#cp ~/test/DNAdata/tmp.fa ~/test/temp/
#cp ~/test/Data/cn_windows_7_ultimate_with_sp1_x64_dvd_618537.iso ~/test/temp/
cp ~/VMDK/* ~/test/temp/
./client -w -b -p ~/test/temp/ 1

<<COM
rm ~/test/temp/*
#./client -r -f -p ~/test/temp/tmp.fa 1
#./client -r -f -p ~/test/temp/chr1.fa 1
./client -r -f -p ~/test/temp/1.vmdk 1
#md5sum ~/test/temp/chr1.fa ~/test/DNAdata/chr1.fa
#md5sum ~/test/temp/tmp.fa ~/test/DNAdata/tmp.fa
md5sum ~/test/temp/1.vmdk ~/VMDK/1.vmdk
rm ~/test/DDSS/server/abtest
rm ~/test/DDSS/server/maptest
rm ~/test/DDSS/deduper/abtest
rm ~/test/DDSS/deduper/writetest
rm ~/test/DDSS/deduper/maptest
rm ~/test/temp/*
cp ~/VMDK/1.vmdk ~/test/temp/
./client -w -f -p ~/test/temp/ 1
rm ~/test/temp/*
#./client -r -f -p ~/test/temp/chr1.fa 1
#md5sum ~/test/temp/chr1.fa ~/test/DNAdata/chr1.fa
md5sum ~/test/temp/1.vmdk ~/VMDK/1.vmdk
<<COM
./client -r -f -p ~/test/temp/chr2.fa 1
./client -r -f -p ~/test/temp/chr3.fa 1
./client -r -f -p ~/test/temp/chr4.fa 1
./client -r -f -p ~/test/temp/chr5.fa 1
./client -r -f -p ~/test/temp/chr6.fa 1
./client -r -f -p ~/test/temp/chr7.fa 1
./client -r -f -p ~/test/temp/chr8.fa 1
./client -r -f -p ~/test/temp/chr9.fa 1
COM
<<COM
rm ~/test/temp/1.vmdk
rm ~/test/temp/2.vmdk
rm ~/test/temp/3.vmdk
rm ~/test/temp/4.vmdk
rm ~/test/temp/5.vmdk
rm ~/test/temp/6.vmdk
rm ~/test/temp/7.vmdk
rm ~/test/temp/8.vmdk
rm ~/test/temp/9.vmdk
rm ~/test/temp/10.vmdk
rm ~/test/temp/11.vmdk
./client -r -f -p ~/test/temp/1.vmdk 1
./client -r -f -p ~/test/temp/2.vmdk 1
./client -r -f -p ~/test/temp/3.vmdk 1
./client -r -f -p ~/test/temp/4.vmdk 1
./client -r -f -p ~/test/temp/5.vmdk 1
./client -r -f -p ~/test/temp/6.vmdk 1
./client -r -f -p ~/test/temp/7.vmdk 1
./client -r -f -p ~/test/temp/8.vmdk 1
./client -r -f -p ~/test/temp/9.vmdk 1
./client -r -f -p ~/test/temp/10.vmdk 1
./client -r -f -p ~/test/temp/11.vmdk 1
COM

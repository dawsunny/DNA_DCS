nprsh -on snode22 /root/liuhougui/DDSS/server/server -c 1 -s 4096 -t 1 -b 1
<<COM
nprsh -on snode11 /root/liuhougui/DDSS/server/server -c 1 -s 4096 -t 1 -b 2
nprsh -on snode12 /root/liuhougui/DDSS/server/server -c 1 -s 4096 -t 1 -b 3
nprsh -on snode08 /root/liuhougui/DDSS/server/server -c 1 -s 4096 -t 1 -b 4
<<COM
nprsh -on snode28 /root/liuhougui/DDSS/server/server -c 1 -s 8192 -t 1 -b 5
nprsh -on snode29 /root/liuhougui/DDSS/server/server -c 1 -s 8192 -t 1 -b 6
nprsh -on snode30 /root/liuhougui/DDSS/server/server -c 1 -s 8192 -t 1 -b 7
nprsh -on snode31 /root/liuhougui/DDSS/server/server -c 1 -s 8192 -t 1 -b 8
<<COM
#~/test/DDSS/deduper/deduper -d 1
<<COM
nprsh -on gh61 ~/test/DDSS/deduper/deduper -d 2
<<COM
nprsh -on gh62 ~/test/DDSS/deduper/deduper -d 3
nprsh -on gh63 ~/test/DDSS/deduper/deduper -d 4
nprsh -on gh64 ~/test/DDSS/deduper/deduper -d 5
nprsh -on gh65 ~/test/DDSS/deduper/deduper -d 6
nprsh -on gh67 ~/test/DDSS/deduper/deduper -d 7
nprsh -on gh68 ~/test/DDSS/deduper/deduper -d 8
nprsh -on gh69 ~/test/DDSS/deduper/deduper -d 9
nprsh -on gh71 ~/test/DDSS/deduper/deduper -d 10
nprsh -on gh72 ~/test/DDSS/deduper/deduper -d 11
nprsh -on gh73 ~/test/DDSS/deduper/deduper -d 12
nprsh -on gh74 ~/test/DDSS/deduper/deduper -d 13
nprsh -on gh75 ~/test/DDSS/deduper/deduper -d 14
nprsh -on gh76 ~/test/DDSS/deduper/deduper -d 15
nprsh -on gh77 ~/test/DDSS/deduper/deduper -d 16
COM

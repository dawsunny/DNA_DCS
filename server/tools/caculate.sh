nprsh -f nprsh_list ls -l /tmp/data/|grep container|wc -l
nprsh -on snode22 ls -l /tmp/data/|grep container|wc -l
nprsh -on snode23 ls -l /tmp/data/|grep container|wc -l
nprsh -on snode24 ls -l /tmp/data/|grep container|wc -l
nprsh -on snode25 ls -l /tmp/data/|grep container|wc -l

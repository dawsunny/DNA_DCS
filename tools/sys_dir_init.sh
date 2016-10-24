#!/bin/bash

dir=/DNA_DCS

mkdir -p ${dir}/client/md
mkdir -p ${dir}/server/mapfile1
mkdir -p ${dir}/server/mapfile2
mkdir -p ${dir}/master/bloom
mkdir -p ${dir}/compressor/fp
mkdir -p ${dir}/compressor/data
mkdir -p ${dir}/compressor/hash_index
mkdir -p ${dir}/log

#configure file
mkdir -p ${dir}/conf
touch ${dir}/conf/client_addr
touch ${dir}/conf/server_addr
touch ${dir}/conf/master_addr
touch ${dir}/conf/compressor_addr

#exec file
mkdir -p ${dir}/bin

#!/bin/sh

#  Script.sh
#  DNA_DCS
#
#  Created by bxz on 2016/10/16.
#  Copyright © 2016年 bxz. All rights reserved.
ALL_FILES="./include/*.h"
ALL_FILES+=" ./client/*.cpp"
ALL_FILES+=" ./server/*.cpp"
ALL_FILES+=" ./compressor/*.cpp"
#ALL_FILES+=" ./master/*.cpp"

sed -i 's/DEDUPER/COMPRESSOR/g' $ALL_FILES
sed -i 's/DEDUP/DCS/g' $ALL_FILES
sed -i 's/deduper/compressor/g' $ALL_FILES
sed -i 's/dedup/dcs/g' $ALL_FILES
sed -i 's/DDSS/DNA_DCS/g' $ALL_FILES

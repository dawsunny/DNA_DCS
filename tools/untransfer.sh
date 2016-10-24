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
ALL_FILES+=" ./master/*.cpp"

sed -i 's/COMPRESSOR/DEDUPER/g' $ALL_FILES
sed -i 's/DCS/DEDUP/g' $ALL_FILES
sed -i 's/compressor/deduper/g' $ALL_FILES
sed -i 's/dcs/dedup/g' $ALL_FILES
sed -i 's/DNA_DCS/DDSS/g' $ALL_FILES

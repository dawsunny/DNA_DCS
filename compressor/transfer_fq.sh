#!/bin/sh

#  transfer_fq.sh
#  DNA_DCS
#
#  Created by bxz on 2016/12/21.
#  Copyright © 2016年 bxz. All rights reserved.

ALL_FILES="./ds_*.cpp"
ALL_FILES+=" ../include/ds_*.h"
sed -i 's/include \"/include \"ds_/g' $ALL_FILES

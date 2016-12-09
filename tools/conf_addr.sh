#!/bin/sh

#  conf_addr.sh
#  DNA_DCS
#
#  Created by bxz on 2016/10/17.
#  Copyright © 2016年 bxz. All rights reserved.
dir=/DNA_DCS/conf
addr="10.18.129.171"
echo $addr > $dir/client_addr
echo $addr > $dir/server_addr
#echo $addr > $dir/master_addr
echo $addr > $dir/compressor_addr

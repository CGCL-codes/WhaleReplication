#!/bin/bash
###
 # @Author: your name
 # @Date: 2022-02-22 22:09:51
 # @LastEditTime: 2022-02-22 23:09:09
 # @LastEditors: Please set LastEditors
 # @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 # @FilePath: /star_midd/launch.sh
### 
# scp -r ../LineKV/src  gtwang@node20:/home/gtwang/LineKV/          2>/dev/null    &&
# scp -r ../LineKV/include  gtwang@node20:/home/gtwang/LineKV/  2>/dev/null   &&

# scp -r ../LineKV/src  gtwang@node22:/home/gtwang/LineKV/          2>/dev/null    &&
# scp -r ../LineKV/include  gtwang@node22:/home/gtwang/LineKV/  2>/dev/null   &&

# scp -r ../LineKV/src  gtwang@node24:/home/gtwang/LineKV/          2>/dev/null    &&
# scp -r ../LineKV/include  gtwang@node24:/home/gtwang/LineKV/  2>/dev/null   &&


scp -r ../LineKV/  hdlu@node20:/home/hdlu/  2>/dev/null   
scp -r ../LineKV/  hdlu@node22:/home/hdlu/  2>/dev/null   
scp -r ../LineKV/  hdlu@node24:/home/hdlu/  2>/dev/null
#scp -r ../LineKV/  hdlu@node13:/home/hdlu/  2>/dev/null  


#scp  ./config.xml hdlu@node24:/home/hdlu/LineKV/config.xml

#scp  ./config.xml hdlu@node22:/home/hdlu/LineKV/config.xml
#scp  ./config.xml hdlu@node20:/home/hdlu/LineKV/config.xml

#scp  ./bin/mica  hdlu@node24:/home/hdlu/LineKV 
#scp  ./bin/test_RTT  hdlu@node24:/home/hdlu/LineKV

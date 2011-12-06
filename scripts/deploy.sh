#!/bin/sh

HDFS_PROJ=/home/wmalik/hadoopnn/kthfs/hadoop-hdfs-project/
HDFS_PROJ_TAR=/tmp/hdfs-project.tgz
COMMON_PROJ=/home/wmalik/hadoopnn/kthfs/hadoop-common-project
COMMON_PROJ_TAR=/tmp/common-proj.tgz


#scp -r release kthfs@cloud3.sics.se:/home/kthfs/release/
#tar czvf code.tgz $KTHFS_HOME
#scp $HADOOP_HDFS_HOME/../hadoop-hdfs-0.24.0-SNAPSHOT.tar.gz kthfs@cloud3.sics.se:/home/kthfs/release/
#scp $HADOOP_COMMON_HOME/../hadoop-common-0.24.0-SNAPSHOT.tar.gz kthfs@cloud3.sics.se:/home/kthfs/release/

#tar czvf $HDFS_PROJ_TAR $HDFS_PROJ
#tar czvf $COMMON_PROJ_TAR $COMMON_PROJ
#scp  $HDFS_PROJ_TAR kthfs@cloud3.sics.se:/home/kthfs/release/hdfs-proj.tar
#scp $COMMON_PROJ_TAR kthfs@cloud3.sics.se:/home/kthfs/release/common-proj.tar

scp $HADOOP_HDFS_HOME/../hadoop-hdfs-0.24.0-SNAPSHOT.tar.gz kthfs@cloud3.sics.se:/home/kthfs/release/
scp $HADOOP_COMMON_HOME/../hadoop-common-0.24.0-SNAPSHOT.tar.gz kthfs@cloud3.sics.se:/home/kthfs/release/


#pushing the release to other cloud machines from cloud3
ssh kthfs@cloud3.sics.se '/home/kthfs/release/scripts/broadcast.sh'

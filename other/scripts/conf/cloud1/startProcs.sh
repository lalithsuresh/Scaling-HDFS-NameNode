#!/bin/sh

export HADOOP_HDFS_HOME='/home/kthfs/release/hadoop-hdfs-0.24.0-SNAPSHOT'
export HADOOP_COMMON_HOME='/home/kthfs/release/hadoop-common-0.24.0-SNAPSHOT'
export LD_LIBRARY_PATH='/home/kthfs/release/'

#$HADOOP_HDFS_HOME/bin/hdfs namenode -format > /home/kthfs/release/namenode-format.log
nohup $HADOOP_HDFS_HOME/bin/hdfs datanode > /home/kthfs/release/datanode.log &
#nohup $HADOOP_HDFS_HOME/bin/hdfs namenode > /home/kthfs/release/namenode.log &


exit


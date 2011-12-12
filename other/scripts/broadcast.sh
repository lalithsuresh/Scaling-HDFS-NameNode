#!/bin/sh
# This script broadcasts the binaries to other nodes and can startup Datanodes if required


releaseDir=/home/kthfs/release
file1=$HADOOP_HDFS_HOME/../hadoop-hdfs-0.24.0-SNAPSHOT.tar.gz
file2=$HADOOP_COMMON_HOME/../hadoop-common-0.24.0-SNAPSHOT.tar.gz
file3=$releaseDir/scripts/untar.sh
file7=$releaseDir/conf/libndbclient.so

for i in cloud1 cloud2 cloud3 cloud4 cloud5 cloud6 cloud7
do

	remotePath="kthfs@"$i".sics.se:"$releaseDir
	connectStr="kthfs@"$i".sics.se"

	echo "\n** Pushing the release to $i"
	scp $file1 $remotePath
	scp $file2 $remotePath
	scp $file3 $remotePath
	scp $releaseDir/conf/$i/hdfs-site.xml $remotePath
	scp $releaseDir/conf/$i/core-site.xml $remotePath
	scp $releaseDir/conf/$i/startProcs.sh $remotePath
	scp $file7 $remotePath

	echo "[Unpacking and configuring the release]"
	# unpacking the release
	ssh $connectStr $releaseDir'/untar.sh' 2>&1 >> /dev/null

	# moving the config files to common/etc/hadoop
	ssh $connectStr 'mv '$releaseDir'/*xml '$releaseDir'/hadoop-common*/etc/hadoop/'

	# tarballs not required anymore - removing
	ssh $connectStr 'rm -rf '$releaseDir'/*.tar.gz'
	ssh $connectStr 'rm -rf '$releaseDir'/untar.sh'

	#creating a tmp dir for namenode and datanode
	ssh $connectStr 'mkdir /home/kthfs/release/tmp' >> /dev/null 2>&1

	# TODO: Datanodes to be brought up here
	#ssh $connectStr 'nohup '$releaseDir'/startDatanode.sh &'
	#ssh $connectStr $releaseDir'/startProcs.sh'
done

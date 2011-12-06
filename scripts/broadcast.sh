#!/bin/sh
# This script broadcasts the binaries to other nodes and can startup Datanodes if required

releaseDir=/home/kthfs/release
file1=$releaseDir/hadoop-hdfs-0.24.0-SNAPSHOT.tar.gz
file2=$releaseDir/hadoop-common-0.24.0-SNAPSHOT.tar.gz
file3=$releaseDir/scripts/untar.sh
file4=$releaseDir/conf/hdfs-site.xml
file5=$releaseDir/conf/core-site.xml

for i in cloud1 cloud2 cloud4 cloud5 cloud6 cloud7
do

	remotePath="kthfs@"$i".sics.se:"$releaseDir
	connectStr="kthfs@"$i".sics.se"

	echo "\n\n** Pushing the release to $i"
	scp $file1 $remotePath
	scp $file2 $remotePath
	scp $file3 $remotePath
	scp $file4 $remotePath
	scp $file5 $remotePath

	# unpacking the release
	ssh $connectStr $releaseDir'/untar.sh' 2>&1 >> /dev/null

	# moving the config files to common/etc/hadoop
	ssh $connectStr 'mv '$releaseDir'/*xml '$releaseDir'/hadoop-common*/etc/hadoop/'

	# tarballs not required anymore - removing
	ssh $connectStr 'rm -rf '$releaseDir'/*.tar.gz'
	ssh $connectStr 'rm -rf '$releaseDir'/untar.sh'

	# TODO: Datanodes to be brought up here
	#ssh $connectStr 'nohup '$releaseDir'/startDatanode.sh &'
	ssh $connectStr $releaseDir'/startDatanode.sh'
done

#!/bin/sh

for file in `ls /home/kthfs/release/*.gz`
do
tar -xzvf $file -C /home/kthfs/release/
done

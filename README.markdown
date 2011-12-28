Scaling HDFS Namenode
======================

The single namespace server based architecture of HDFS has recently raised [doubts on the file system’s scalability and availability](http://www.usenix.org/publications/login/2010-04/openpdfs/shvachko.pdf). In light of these issues, this work is an attempt to achieve:

* High availability for the Namenode, i.e, no single point of failure.
* Horizontal scalability for the Namenode, i.e, to handle heavier loads, one would need to only add more Namenodes to the system than having to upgrade a single Namenode’s hardware.

To do this, we’ve modified the HDFS Namenode to store metadata in MySQL Cluster as opposed to keeping it in memory. The work is still in-progress and experimental in nature.

Current status
------------------
* Multiple stateless Namenodes can be run. Clients and Datanodes should be statically partitioned to talk to a particular Namenode.
* Inodes, Blocks and Triplet data structures have been migrated to the DB.
* Performance of an individual Namenode is being limited by the FSNamesystem/FSDirectory write locks. These can be phased out once we migrate all of the Namenode’s data structures to MySQL Cluster, which might bring improvements with write-heavy workloads.

Notes
-------
* If you would like to plug-in a DB other than MySQL cluster behind the Namenode, you will need to re-implement the InodeTableHelper and BlocksHelper classes. We’ll clean this up soon. :)
* For best results, set "dfs.dbconnector.num-session-factories” to a value that matches the number of Namenode worker threads, and make sure that the sum of this value across all Namenodes is <= the number of free mysqld/api slots in the MySQL cluster!

On-going work
-------------------
* Full statelessness.
* A protocol/service for clients and Datanodes to associate with a random Namenode (needs full statelessness to work correctly).
* A clean interface class to talk to the DB rather than the static helper methods we’re using currently.




Setup
--------
### Requirements
* Linux 32/64 bit (should use 32/64 bit libndbclient.so accordingly)
* [Maven 3](“http://maven.apache.org/download.html”)
* A running [MySQL cluster](“http://www.mysql.com/products/cluster/”) environment with at least 1 empty slots to allow client connections.

### Steps
In the description of the steps needed, we will refer to the project’s root folder by using `Scaling-HDFS-Namenode`

* Build the Clusterj connector according to your architecture.

>`$: cd Scaling-HDFS-Namenode/clusterj`
>
> Edit `Scaling-HDFS-Namenode/clusterj/pom.xml`, change `linux_32` for `linux_64` if needed.  
>
>`$: mvn clean install`

* Dump the SQL files in `Scaling-HDFS-Namenode/other/database_create_sqls.txt` into the MySQL cluster DB. This can be done using a MySQL client or a GUI tool like MySQL Workbench (whichever suits you better :) )

* Build the hadoop-common, hadoop-mapreduce and hadoop-hdfs projects from the root folder (be aware that [protobuf](http://code.google.com/p/protobuf/) is needed to build the Hadoop project)

>`$: mvn package -DskipTests -Dtar -Pdist `
>
> This will build the project without running the tests for it. If this doesn’t work add the flag `-P-cbuild` to the maven package command

* Now move to the project target folder: 

>`$: cd Scaling-HDFS-NameNode/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-0.24.0-SNAPSHOT`

* Once there: First format the Namenode, the start it and also start the DataNodes:

>`$: bin/hdfs namenode -format`
>
>`$: bin/hdfs namenode`
>
>`$: bin/hdfs datanode`

* Write some files into the file system:

> `$: bin/hdfs dfs -copyFromLocal /localFile /remoteName`

* Enjoy!

Useful links
----------------
* A [presentation](“http://goo.gl/W1kJI”) done in KTH and SICS for this project.

Credits
------------- 
+ Jim Dowling - Swedish Institute of Computer Science
+ Wasif Malik *
+ Ying Solomon * 
+ Lalith Suresh *
+ Mariano Vallés *

*KTH - Royal Institute of Technology

package org.apache.hadoop.hdfs.server.namenode;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.SessionFactory;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_CONNECTOR_STRING_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_DATABASE_KEY;

/*
 * DB Connector with static block, to be executed
 * every time
 * */
public class DBConnector {
	static SessionFactory sessionFactory;
	public static void setConfiguration (Configuration conf){
		 // [STATELESS]
	    Properties p = new Properties();
	    p.setProperty("com.mysql.clusterj.connectstring", conf.get(DFS_DB_CONNECTOR_STRING_KEY, "cloud3.sics.se"));
	    p.setProperty("com.mysql.clusterj.database", conf.get(DFS_DB_DATABASE_KEY, "ying test"));
	    sessionFactory = ClusterJHelper.getSessionFactory(p);
	}
}

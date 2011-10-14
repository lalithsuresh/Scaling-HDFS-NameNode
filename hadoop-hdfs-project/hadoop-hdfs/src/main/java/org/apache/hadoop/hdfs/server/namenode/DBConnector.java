package org.apache.hadoop.hdfs.server.namenode;

import java.util.Properties;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.SessionFactory;
/*
 * DB Connector with static block, to be executed
 * every time
 * */
public class DBConnector {
	static SessionFactory sessionFactory;
	static {
		 // [STATELESS]
	    Properties p = new Properties();
	    p.setProperty("com.mysql.clusterj.connectstring", "cloud3.sics.se:1186");
	    p.setProperty("com.mysql.clusterj.database", "test");
	    sessionFactory = ClusterJHelper.getSessionFactory(p);
	}
}

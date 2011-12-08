package org.apache.hadoop.hdfs.server.namenode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Transaction;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_CONNECTOR_STRING_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DB_DATABASE_KEY;

/*
 * DB Connector with static block, to be executed
 * every time
 * */
	
	public class DBConnector {
	static SessionFactory sessionFactory;
	static Map<Long, Session> sessionPool = new ConcurrentHashMap<Long, Session>();
	
	
	public static void setConfiguration (Configuration conf){
		 // [STATELESS]
	    Properties p = new Properties();
	    p.setProperty("com.mysql.clusterj.connectstring", conf.get(DFS_DB_CONNECTOR_STRING_KEY, "cloud3.sics.se"));
	    p.setProperty("com.mysql.clusterj.database", conf.get(DFS_DB_DATABASE_KEY, "lalithtest"));
	    sessionFactory = ClusterJHelper.getSessionFactory(p);
	}
	
	// The Namenode has a pool of worker threads
	// that perform all internal tasks. Since
	// ClusterJ sessions aren't thread safe, we
	// need one session per worker thread. The
	// below code creates a session for each worker
	// thread the first time and maintains this mappings
	// in sessionPool
	public static Session obtainSession (){
		long threadId = Thread.currentThread().getId();
		
		if (sessionPool.containsKey(threadId))	{
			return sessionPool.get(threadId); 
		}
		else {
			Session session = sessionFactory.getSession();
			sessionPool.put(threadId, session);
			return session;
		}
	}
	public static boolean startTransaction()
	{
		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();
		boolean internal = true;
		if(!tx.isActive())
		{
			tx.begin();
			internal = false;
		}
		return internal;
	}
	
	public static void closeTransaction(boolean internal)
	{
		Session session = DBConnector.obtainSession();
		Transaction tx = session.currentTransaction();
		if(!internal)
		{
			tx.commit();
    		session.flush();
		}
	}
	
}

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
	static Map<Long, Transaction> txMap = new ConcurrentHashMap<Long, Transaction>();
	static Map<Long, Integer> lockCountMap = new ConcurrentHashMap<Long, Integer>();	

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
	
	// We should have only one Tx per session per thread,
	// so check if there is any tx active within the session
	// to protect against calling a tx.begin() within another.
	public static void startTransaction (){
		Transaction tx = obtainSession().currentTransaction();
		long threadId = Thread.currentThread().getId();
		
		if (!tx.isActive())
		{
			tx.begin();
			txMap.put(threadId, tx);
			lockCountMap.put(threadId, 1);
		}
		else
		{
			lockCountMap.put(threadId, lockCountMap.get(threadId) + 1);
		}
	}
	
	public static void endTransaction (){
		Transaction tx = txMap.get(Thread.currentThread().getId());
		long threadId = Thread.currentThread().getId();
		int currentLockCount = lockCountMap.get(threadId);
		
		if (currentLockCount == 1)
		{
			tx.commit();
		}
		else
		{
			lockCountMap.put(threadId, currentLockCount - 1);
		}
		obtainSession().flush();	
	}
}

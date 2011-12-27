package se.sics.clusterj.loadgenerator;


import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;


import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;


/* 
 * This singleton class serves sessions to the Inode/Block
 * helper classes to talk to the DB. 
 * 
 * Three design decisions here:
 * 1) Serve one ClusterJ Session per Namenode
 *    worker thread, because Sessions are not thread
 *    safe.
 * 2) Have a pool of ClusterJ SessionFactory instances
 *    to serve the Sessions. This will help work around
 *    contention at the ClusterJ internal buffers.
 * 3) Set the connection pool size to be as many as
 *    the number of SessionFactory instances. This will
 *    allow multiple simultaneous connections to exist,
 *    and the read/write locks in FSNamesystem and 
 *    FSDirectory will make sure this stays safe. * 
 */
public class DBConnector {
	static int NUM_SESSION_FACTORIES;
	static SessionFactory [] sessionFactory;
	static Map<Long, Session> sessionPool = new ConcurrentHashMap<Long, Session>();
	
	
	public static void setConfiguration (String connectStr, String database, int num_factories){
		NUM_SESSION_FACTORIES = num_factories;
		sessionFactory = new SessionFactory[NUM_SESSION_FACTORIES];
		
		for (int i = 0; i < NUM_SESSION_FACTORIES; i++)
		{
			Properties p = new Properties();
			p.setProperty("com.mysql.clusterj.connectstring", connectStr);
			p.setProperty("com.mysql.clusterj.database", database);
			p.setProperty("com.mysql.clusterj.connection.pool.size", String.valueOf(NUM_SESSION_FACTORIES));
			sessionFactory[i] = ClusterJHelper.getSessionFactory(p);
		}
	}
	
	/*
	 * Return a session from a random session factory in our
	 * pool.
	 * 
	 * NOTE: Do not close the session returned by this call
	 * or you will die.
	 */
	public static Session obtainSession (){
		long threadId = Thread.currentThread().getId();
		
		if (sessionPool.containsKey(threadId))	{
			return sessionPool.get(threadId); 
		}
		else {
			// Pick a random sessionFactory
			Random r = new Random();
			Session session = sessionFactory[r.nextInt(NUM_SESSION_FACTORIES)].getSession();
			sessionPool.put(threadId, session);
			return session;
		}
	}
}

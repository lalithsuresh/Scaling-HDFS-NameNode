package org.apache.hadoop.hdfs.server.namenode;


import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Transaction;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.io.DataInputBuffer;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.List;
import se.sics.clusterj.*;

/**
 * 
 * 
 *  to Run:
 * java -jar clusterj.jar -Djava.library.path=target/lib/ 
 *
 */
public class DatabaseHelper {

	static final int MAX_DATA = 128;

	private static void initDB() {
	}


	public static List<INode> getChildren(String parentDir) throws IOException {

		Session s = DBConnector.sessionFactory.getSession();
		Transaction tx = s.currentTransaction();
		long t1 = System.currentTimeMillis();


		QueryBuilder qb = s.getQueryBuilder();
		QueryDomainType<InodeTable> dobj = qb.createQueryDefinition(InodeTable.class);


		dobj.where(dobj.get("parent").equal(dobj.param("parent")));

		Query<InodeTable> query = s.createQuery(dobj);
		query.setParameter("parent", parentDir); //W: WHERE parent = parentDir

		List<InodeTable> resultList = query.getResultList();

		//List<String> children = new ArrayList<String>();
		List<INode> children = new ArrayList<INode>();

		for (InodeTable result : resultList) {
		
				//create a directory object
				DataInputBuffer buffer = new DataInputBuffer();
				buffer.reset(result.getPermission(), result.getPermission().length);
				PermissionStatus ps = PermissionStatus.read(buffer);
				KthFsHelper.printKTH("PermissionStatus: "+ps.getGroupName() + ps.getUserName() + " " + ps.getPermission().toString());
				

				INode node = INode.newINode(
						ps,//this.getPermissionStatus(),
						null, //TODO: W: blocks to be read from DB also - null for directories
						"", //symlink name
						(short)1, //replication factor
						result.getModificationTime(), 
						result.getATime(),
						result.getNSQuota(),
						result.getDSQuota(),
						-1);
				node.setLocalName(result.getLocalName());
				children.add(node);
		}

		if (children.size() > 0 )
			return children;
		else 
			return null;


	}

	public static INode getChildDirectory(String parentDir, String searchDir) throws IOException {

		/*W: TODO
		 *  1. Get all children of parentDir
            2. search for searchDir in parentDir's children
		 *  3. if found then create an INode and return it
		 *  4. else return null;
		 */

		Properties p = new Properties();
		p.setProperty("com.mysql.clusterj.connectstring", "cloud3.sics.se:1186");
		p.setProperty("com.mysql.clusterj.database", "test");
		SessionFactory sf = ClusterJHelper.getSessionFactory(p);
		Session s = sf.getSession();
		Transaction tx = s.currentTransaction();
		long t1 = System.currentTimeMillis();

		/*
		 * Full table scan
		 * */

		QueryBuilder qb = s.getQueryBuilder();
		QueryDomainType dobj = qb.createQueryDefinition(InodeTable.class);


		dobj.where(dobj.get("parent").equal(dobj.param("parent")));

		Query<InodeTable> query = s.createQuery(dobj);
		query.setParameter("parent", parentDir); //W: the WHERE clause of SQL

		//Query query = s.createQuery(dobj);
		List<InodeTable> resultList = query.getResultList();


		//TODO: localname needs to be added to the InodeTable to make this work

		for (InodeTable result : resultList) {
			//if(result.getIsDir()) {
				String str = result.getName();
				str = str.substring(str.lastIndexOf("/")+1);
				if(str.equals(searchDir) ) {
					System.out.println("FOUND - " + searchDir + " in "+parentDir);
					System.out.println(result.getName() + " " + result.getClientName());
					
					DataInputBuffer buffer = new DataInputBuffer();
					buffer.reset(result.getPermission(), result.getPermission().length);
					PermissionStatus ps = PermissionStatus.read(buffer);

					if(result.getIsDir()) {
						KthFsHelper.printKTH("About to return a directory: "+result.getName());
						return new INodeDirectory(result.getName(), ps);
					}
					else {
						INodeFile inf = new INodeFile(ps,0,(short)1,result.getModificationTime(), result.getATime(), 64); //FIXME: change this when we store blockinfo
						inf.setLocalName(result.getName());
						return  inf;
						//KthFsHelper.printKTH("Returning a file: "+node.toString());
					}

				}
			//}
		}

		System.out.println("NOT FOUND - " + searchDir + " in "+parentDir);
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		/*W: For testing*/
		//Main_LW.getChildDirectory("/", "Lennon");
		try {
			DatabaseHelper.getChildren("/");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void doStupidStuff() {
		System.out.println("I am stupid!");
	}
}

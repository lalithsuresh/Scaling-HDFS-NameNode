package se.sics.clusterj;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Transaction;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * 
 *  to Run:
 * java -jar clusterj.jar -Djava.library.path=target/lib/ 
 *
 */
public class Main_LW {

    static final int MAX_DATA = 128;

    private static void initDB() {
    }

    public static List<String> getChildren(String parentDir) {
        
        Properties p = new Properties();
        p.setProperty("com.mysql.clusterj.connectstring", "cloud3.sics.se:1186");
        p.setProperty("com.mysql.clusterj.database", "test");
        SessionFactory sf = ClusterJHelper.getSessionFactory(p);
        Session s = sf.getSession();
        Transaction tx = s.currentTransaction();
        long t1 = System.currentTimeMillis();


        QueryBuilder qb = s.getQueryBuilder();
        QueryDomainType dobj = qb.createQueryDefinition(InodeTable.class);

        
        dobj.where(dobj.get("parent").equal(dobj.param("parent")));
            
        Query<InodeTable> query = s.createQuery(dobj);
        query.setParameter("parent", parentDir); //W: the WHERE clause of SQL

        List<InodeTable> resultList = query.getResultList();
        
        List<String> children = new ArrayList<String>();
        
        for (InodeTable result : resultList) {
               children.add(result.getName());
               System.out.println(result.getName());
        }
        
        if (children.size() > 0 )
            return children;
        else 
            return null;
        
        
    }
    
    public static String getChildDirectory(String parentDir, String searchDir) {
        
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

        /*get the directory*/
        /* InodeTable myRow = s.find(InodeTable.class, "/1/2/lolziness");
      
         */
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
           if(result.getIsDir()) {
               String str = result.getName();
               str = str.substring(str.lastIndexOf("/")+1);
               System.out.println("[KTHFS] str="+str);
               if(str.equals(searchDir) ) {
                   System.out.println("FOUND - " + searchDir + " in "+parentDir);
                   System.out.println(result.getName() + " " + result.getClientName());
                   return result.getName();
                   //TODO: create an INode and return it
               }
           }
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
        Main_LW.getChildren("/");

    }

    public static void doStupidStuff() {
        System.out.println("I am stupid!");
    }
}

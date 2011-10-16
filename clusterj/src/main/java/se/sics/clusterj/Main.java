package se.sics.clusterj;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Transaction;
import java.util.Properties;


/**
 * 
 * 
 *  to Run:
 * java -jar clusterj.jar -Djava.library.path=target/lib/ 
 *
 */
public class Main {

    static final int MAX_DATA = 128;

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        byte[] data = new byte[(int) MAX_DATA];

        /**
         * Initialise the data array with data
         */
     
        for (int i = 0; i < MAX_DATA; i++) {
            data[i] = 'W';
        }



        Properties p = new Properties();
        p.setProperty("com.mysql.clusterj.connectstring", "cloud3.sics.se:1186");
//		p.setProperty("com.mysql.clusterj.connectstring", "localhost:1186");
        p.setProperty("com.mysql.clusterj.database", "test");


//                System.setProperty("java.library.path", "/home/jdowling/.mysql/mysql/lib/");
//                System.setProperty("java.library.path", "/home/jdowling/.mysql/mysql-cluster-gpl-7.1.15a-linux-x86_64-glibc23/lib/");

        /**
         * SessionFactory will now create a connection to MySQL Cluster
         */
        SessionFactory sf = ClusterJHelper.getSessionFactory(p);
        /**
         * Get a session to operate on (note, sessions are not thread safe).
         */
        Session s = sf.getSession();
        /**
         * Get a transaction from the session.
         * This is optional, but we are going to use batching when we 
         * insert the data.
         * We are going to insert batches of 32 and prepare
         * 32 operations, and then perform a commit.
         */
        Transaction tx = s.currentTransaction();
        int i = 0;
        int start = 0;
        int stop = 1000;
        /**
         * Start the transaction since this is the first lap in the loop
         */
        long t1 = System.currentTimeMillis();
        tx.begin();
        while (i < stop) {
            /**
             * Get an instance of MyData
             */
            MyData my_data = s.newInstance(MyData.class);
            /**
             * Set the data on the object
             */
            my_data.setId(i);
            my_data.setData(data);
            my_data.setLastUpdated(System.currentTimeMillis());
            /**
             * Persist the object, but since we have done
             * tx.begin()
             * we will not send the data yet.
             * The data will be sent when we do tx.commit();
             */
            s.makePersistent(my_data);
            i++;
            if (i % 32 == 0) {
                /**
                 * Now we have prepared 32 inserts.
                 * Time to commit, and then we need to begin a new
                 * transaction (tx.begin()), since the previous 
                 * transaction was committed.
                 */
                tx.commit();
                tx.begin();
            }
        }
        /**
         * At the end of the loop, commit the remainder of the records.	 
         * */
        tx.commit();
        long t2 = System.currentTimeMillis();

        System.err.println("Done inserting " + stop + " records in " + (t2 - t1) + " ms");

    }
}

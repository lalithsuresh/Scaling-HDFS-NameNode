package se.sics.clusterj.loadgenerator;


import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;

import java.util.ArrayList;
import java.util.List;

import java.util.Random;

import se.sics.clusterj.InodeTable;



/**
 * 
 * This executable class:
 *  1) generates workloads for MySQL Cluster datanodes
 *  2) prints a summary of the average select/insert times after completion
 * 
 * To run this class: 
 * java -cp target/clusterj-1.0-SNAPSHOT.jar:
 * ~/.m2/repository/se/sics/ndb/clusterj-jar-linux32/7.1.15a/clusterj-jar-linux32-7.1.15a.jar 
 *  -Djava.library.path=target/lib/ 
 *  se.sics.clusterj.loadgenerator.LoadGenerator 200 40 2
 *
 */
public class LoadGenerator 
{

	static int NUM_ROWS;
	static int NUM_THREADS;
	static int NUM_FACTORIES;
	static String CONNECT_STR = "";
	static String DATABASE = "";


	private static class ClientThread extends Thread {

		private int id;
		private int num_rows;
		private List<Long> idList = new ArrayList<Long>();
		private long insertTimeTotal = 0;
		private long insertCount = 0;
		private long selectTimeTotal = 0;
		private long selectCount = 0;
		
		private ClientThread(int id, int num_rows) {
			this.id = id;
			this.num_rows = num_rows;
		}
		public void run() {
			insert(this.num_rows);
			select();
		}

		private void insert(int num_rows) {

			Random r = new Random();
			Session session = DBConnector.obtainSession();

			for (int i=0; i< num_rows; i++) {

				System.out.print(".");

				long randLong = r.nextLong();
				idList.add(randLong);

				long startTime = System.currentTimeMillis();
				Transaction tx = session.currentTransaction();
				tx.begin();
				InodeTable inode = session.newInstance(InodeTable.class);
				inode.setId(randLong);
				inode.setATime(randLong);
				inode.setClientMachine("loadgen-clusterj");
				inode.setClientName("loadgen-clusterj");
				inode.setName("loadgen-clusterj");
				inode.setLocalName("loadgen-clusterj");
				inode.setParent("loadgen-clusterj");
				inode.setIsDir(false);
				session.makePersistent(inode);
				tx.commit();

				insertTimeTotal +=  System.currentTimeMillis() - startTime;
				insertCount++;

			}

			
		}


		private void select() {

			Session session = DBConnector.obtainSession();
			for (Long iNodeID: idList) {

				System.out.print("."); //progress bar

				//start counting
				long startTime = System.currentTimeMillis();

				InodeTable inodetable = session.find(InodeTable.class, iNodeID);

				//stop counting
				selectTimeTotal +=  System.currentTimeMillis() - startTime;
				selectCount++;
			}


		}
		
		public void printSummary() {
			
			System.out.println("\n\n## Summary for tid " + this.id);
			System.out.println("Number of rows inserted: " + insertCount++);
			System.out.println("Total time taken: " + insertTimeTotal + " ms");
			System.out.println("Average insert time: " + insertTimeTotal/insertCount + " ms");
			
			System.out.println("Number of rows selected: " + selectCount++);
			System.out.println("Total time taken: " + selectTimeTotal + " ms");
			System.out.println("Average select time: " + selectTimeTotal/selectCount + " ms");
			
		}
	}


	/**
	 * @param args
	 * 
	 * arg[0] num_rows
	 * arg[1] num_threads
	 * arg[2] num_factories
	 * 
	 */
	public static void main(String[] args) {

		if(args.length < 5) {
			System.out.println("\n\nUsage: \nLoadGenerator [num_rows] [num_threads] [num_factories] [connect_str] [database]\n\n");
			System.exit(-1);
		}
		
		NUM_ROWS = args[0] != null ? Integer.valueOf(args[0]) : 100;
		NUM_THREADS = args[1] != null ? Integer.valueOf(args[1]) : 1;
		NUM_FACTORIES = args[2] != null ? Integer.valueOf(args[2]) : 1;
		CONNECT_STR = args[3];
		DATABASE = args[4];
		
		DBConnector.setConfiguration(CONNECT_STR, DATABASE, NUM_FACTORIES);
		
		System.out.println("Number of rows to be inserted: " + NUM_ROWS);
		System.out.println("Number of threads: " + NUM_THREADS);
		System.out.println("Number of cluster session factories: " + NUM_FACTORIES);
		System.out.println("Connect string: " + CONNECT_STR);
		System.out.println("Database name: " + DATABASE);
		// Spawn threads
		ClientThread[] clientThreads = new ClientThread[NUM_THREADS]; 
		for (int i = 0; i < NUM_THREADS; i++) {
			clientThreads[i] = new ClientThread(i, NUM_ROWS);
			clientThreads[i].start();
		}
		
		
		// Wait for all threads to terminate and print summary for each 
		for (int i = 0; i < NUM_THREADS; i++) {
			try {
				clientThreads[i].join();
				clientThreads[i].printSummary();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		//insert(num_rows);
		//select();

	}
}

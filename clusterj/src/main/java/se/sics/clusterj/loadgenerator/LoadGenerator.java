package se.sics.clusterj.loadgenerator;


import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import java.util.Random;

import se.sics.clusterj.InodeTable;


/**
 * 
 * This class: 
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
	static DecimalFormat df = new DecimalFormat("#.##");


	
	/**
	 * Internal thread class for reading/writing to database
	 *
	 */
	private static class ClientThread extends Thread {

		private int id;
		private int num_rows;
		private List<Long> idList = new ArrayList<Long>();
		DecimalFormat df = new DecimalFormat("#.##");
		private double insertTimeTotal = 0;
		private double insertCount = 0;
		private double selectTimeTotal = 0;
		private double selectCount = 0;

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

				double startTime = System.nanoTime();
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

				insertTimeTotal +=  System.nanoTime() - startTime;
				insertCount++;
			}
		}


		@SuppressWarnings("unused")
		private void select() {

			Session session = DBConnector.obtainSession();
			for (Long iNodeID: idList) {

				System.out.print("."); //old-school progress bar :)

				//start timer
				double startTime = System.nanoTime();
				
				//do primary key lookup
				InodeTable inodetable = session.find(InodeTable.class, iNodeID);
		
				//stop timer
				selectTimeTotal +=  System.nanoTime() - startTime;
				selectCount++;
			}
		}

		public void printSummary() {
			System.out.println("\n\n## Summary for tid " + this.id);
			System.out.println("Average insert time: " + df.format((insertTimeTotal/insertCount)/1000000) + " ms");
			System.out.println("Average select time: " + df.format((selectTimeTotal/selectCount)/1000000) + " ms");
		}
		
		public double getInsertTime() {
			return (insertTimeTotal/insertCount)/1000000;
		}
		
		public double getSelectTime() {
			return (selectTimeTotal/selectCount)/1000000;
		}
		
	}


	/**
	 * @param args
	 * 
	 * args[0] num_rows
	 * args[1] num_threads
	 * args[2] num_factories
	 * args[3] connect_str
	 * args[4] database
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

		double totalInsertTime = 0;
		double totalSelectTime = 0;
		
		// Wait for all threads to terminate and print summary for each 
		for (int i = 0; i < NUM_THREADS; i++) {
			try {
				clientThreads[i].join();
				clientThreads[i].printSummary();
				totalInsertTime += clientThreads[i].getInsertTime();
				totalSelectTime += clientThreads[i].getSelectTime();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("\n## Overall Summary of all threads ##");
		System.out.println("Average insert time: " + df.format(totalInsertTime/NUM_THREADS) + " ms");
		System.out.println("Average select time: " + df.format(totalSelectTime/NUM_THREADS) + " ms");

	}
}

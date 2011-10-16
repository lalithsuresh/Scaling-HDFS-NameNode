/**
 * 
 */
package org.apache.hadoop.hdfs.server.namenode;

/**
 * @author wmalik
 *
 */
public class KthFsHelper {

	/*Added for kthfs debugging*/
	public static void printKTH(String msg) {
		System.err.println("[KTHFS] (tid:" + Thread.currentThread().getId() + ") " + msg);
	}

}

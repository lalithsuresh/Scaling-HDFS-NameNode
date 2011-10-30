package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;

import se.sics.clusterj.*;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;

public class BlocksHelper {

	
	public static BlockInfo getBlockInfo(long blockId) {
		Session s = DBConnector.sessionFactory.getSession();
		BlockInfoTable bit = s.find(BlockInfoTable.class, blockId);

		if(bit == null)
			return null;
		else {
			Block b = new Block(bit.getBlockId(), bit.getNumBytes(), bit.getGenerationStamp());
			BlockInfo blockInfo = new BlockInfo(b, bit.getReplication());
			return blockInfo;
		}

	}

	public static void putBlockInfo(BlockInfo binfo) {

		Session s = DBConnector.sessionFactory.getSession();
		Transaction tx = s.currentTransaction();
		tx.begin();
		
		BlockInfoTable bit =  s.newInstance(BlockInfoTable.class);
		bit.setBlockId(binfo.getBlockId());
		bit.setGenerationStamp(binfo.getGenerationStamp());
		bit.setINodePath(binfo.getINode().getFullPathName());
		bit.setNumBytes(binfo.getNumBytes());
		//FIXME: KTHFS: Ying and Wasif: replication is null at the moment - remove the column if not required later on
		
		s.makePersistent(bit);
		tx.commit();

	}

	
	
}

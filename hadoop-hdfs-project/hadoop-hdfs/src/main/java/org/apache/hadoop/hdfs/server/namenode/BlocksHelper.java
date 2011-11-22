package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;

import se.sics.clusterj.*;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;

import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;

public class BlocksHelper {

	public static FSNamesystem ns = null;
	public static Session session= DBConnector.sessionFactory.getSession() ;


	/*
	 * Helper function for appending an array of blocks - used by concat
	 * 
	 * Replacement for INodeFile.appendBlocks
	 * 
	 * */
	public static void appendBlocks(INodeFile thisNode, INodeFile [] inodes, int totalAddedBlocks) {

		Transaction tx = session.currentTransaction();
		tx.begin();

		for(INodeFile in: inodes) {
			KthFsHelper.printKTH("loop starts!!");
			BlockInfo[] inBlocks = in.getBlocks();
			for(int i=0;i<inBlocks.length;i++) {
				KthFsHelper.printKTH("loop!!!");
				BlockInfoTable bInfoTable = createBlockInfoTable(thisNode, inBlocks[i]);
				session.makePersistent(bInfoTable);
			}
		}
		tx.commit();
	}

	/*
	 * Helper function for inserting a block in the BlocksInfo table
	 * 
	 * Replacement for INodeFile.addBlock

	 * */
	public static void addBlock(BlockInfo newblock) {

		KthFsHelper.printKTH("addBlock called");
		putBlockInfo(newblock);
		/*
		Transaction tx = session.currentTransaction();
		tx.begin();

		BlockInfoTable bInfoTable = session.newInstance(BlockInfoTable.class);
		bInfoTable.setBlockId(newblock.getBlockId());
		bInfoTable.setGenerationStamp(newblock.getGenerationStamp());
		bInfoTable.setINodeID(newblock.getINode().getID()); //FIXME: store ID in INodeFile objects - use Mariano :)
		bInfoTable.setNumBytes(newblock.getNumBytes());
		bInfoTable.setReplication(-1); //FIXME: see if we need to store this or not

		session.makePersistent(bInfoTable);
		tx.commit();*/
	}

	/*Helper function for creating a BlockInfoTable object */
	private static BlockInfoTable createBlockInfoTable(INode node, BlockInfo newblock) {

		KthFsHelper.printKTH("createBlockInfoTable called");
		BlockInfoTable bInfoTable = session.newInstance(BlockInfoTable.class);
		bInfoTable.setBlockId(newblock.getBlockId());
		bInfoTable.setGenerationStamp(newblock.getGenerationStamp());
		bInfoTable.setINodeID(newblock.getINode().getID()); //FIXME: store ID in INodeFile objects - use Mariano :)
		bInfoTable.setNumBytes(newblock.getNumBytes());
		bInfoTable.setReplication(-1); //FIXME: see if we need to store this or not
		return bInfoTable;
	}

	private static List<TripletsTable> getTriplets(long blockId) {
		Session s = DBConnector.sessionFactory.getSession();
		QueryBuilder qb = s.getQueryBuilder();

		QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);


		dobj.where(dobj.get("blockId").equal(dobj.param("blockId"))); //works?

		Query<TripletsTable> query = s.createQuery(dobj);
		query.setParameter("blockId", blockId); //W: WHERE blockId = blockId


		return query.getResultList(); 
	}

	/**
	 * @param blockId
	 * @return
	 * @throws IOException 
	 */
	public static BlockInfo getBlockInfo(long blockId)  {
		Session s = DBConnector.sessionFactory.getSession();
		DatanodeManager dm = ns.getBlockManager().getDatanodeManager();

		BlockInfoTable bit = s.find(BlockInfoTable.class, blockId);

		if(bit == null)
			return null;
		else {
			Block b = new Block(bit.getBlockId(), bit.getNumBytes(), bit.getGenerationStamp());
			BlockInfo blockInfo = new BlockInfo(b, bit.getReplication());

			//FIXME: change primary key of table - sort the results on index
			List<TripletsTable> tripletsTable = getTriplets(blockId); 
			Object[] tripletsKTH = new Object[3*tripletsTable.size()];

			for(int i=0;i<tripletsTable.size();i++) {

				KthFsHelper.printKTH("triplets loop called");
				DatanodeDescriptor dd = dm.getDatanodeByHost(tripletsTable.get(i).getDatanodeName()); //KTHFS: see if this works
				long prevBlockId = tripletsTable.get(i).getPreviousBlockId();
				long nextBlockId = tripletsTable.get(i).getNextBlockId();
				int index = tripletsTable.get(i).getIndex();

				tripletsKTH[3*index] = dd;
				tripletsKTH[(3*index) + 1] = prevBlockId;
				tripletsKTH[(3*index) + 2] = nextBlockId;
			}

			blockInfo.setTripletsKTH(tripletsKTH);

			//W: assuming that this function will only be called on an INodeFile

			INodeFile node = (INodeFile)INodeTableHelper.getINode(bit.getINodeID());
			if(node == null)
				System.out.println("[NOTKTHFS] getBlockInfo node is null!!!!!!!!!!!!!");
			node.setBlocksList(getBlocksArray(node));//circular?

			blockInfo.setINode(node);

			return blockInfo;
		}

	}


	public static BlockInfo getBlockInfoSingle(long blockId) throws IOException {
		Session s = DBConnector.sessionFactory.getSession();
		System.err.println("ME NULL: " + ns);
		System.err.println("ME probably NULL: " + ns.getBlockManager());
		DatanodeManager dm = ns.getBlockManager().getDatanodeManager();

		BlockInfoTable bit = s.find(BlockInfoTable.class, blockId);

		if(bit == null)
			return null;
		else {
			Block b = new Block(bit.getBlockId(), bit.getNumBytes(), bit.getGenerationStamp());
			BlockInfo blockInfo = new BlockInfo(b, bit.getReplication());

			//FIXME: change primary key of table - sort the results on index
			List<TripletsTable> tripletsTable = getTriplets(blockId); 
			Object[] tripletsKTH = new Object[3*tripletsTable.size()];

			for(int i=0;i<tripletsTable.size();i++) {

				KthFsHelper.printKTH("triplets loop called");
				DatanodeDescriptor dd = dm.getDatanodeByHost(tripletsTable.get(i).getDatanodeName()); //KTHFS: see if this works
				long prevBlockId = tripletsTable.get(i).getPreviousBlockId();
				long nextBlockId = tripletsTable.get(i).getNextBlockId();
				int index = tripletsTable.get(i).getIndex();

				tripletsKTH[3*index] = dd;
				tripletsKTH[(3*index) + 1] = prevBlockId;
				tripletsKTH[(3*index) + 2] = nextBlockId;
			}

			blockInfo.setTripletsKTH(tripletsKTH);


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

		if(binfo.isComplete()) {
			INodeFile ifile = binfo.getINode();
			long nodeID = ifile.getID();
			bit.setINodeID(nodeID); 
		}


		bit.setNumBytes(binfo.getNumBytes());
		//FIXME: KTHFS: Ying and Wasif: replication is null at the moment - remove the column if not required later on


		Object[] tripletsKTH = binfo.getTripletsKTH();

		for(int i=0;i<(tripletsKTH.length/3);i++) {
			DatanodeDescriptor dd = (DatanodeDescriptor)tripletsKTH[3*i];
			long prevBlockId, nextBlockId;

			if (tripletsKTH[(3*i)+1]==null)
				prevBlockId = -1;
			else
				prevBlockId = ((Long)tripletsKTH[(3*i)+1]).longValue();
			if (tripletsKTH[(3*i)+2]==null)
				nextBlockId = -1;
			else
				nextBlockId = ((Long)tripletsKTH[(3*i)+2]).longValue();

			TripletsTable t = s.newInstance(TripletsTable.class);
			t.setBlockId(binfo.getBlockId());
			if(dd==null)
				t.setDatanodeName(null);
			else
				t.setDatanodeName(dd.getHostName());
			t.setIndex(i);
			t.setPreviousBlockId(prevBlockId);
			t.setNextBlockId(nextBlockId);
			s.savePersistent(t);

		}

		s.savePersistent(bit);
		tx.commit();

	}


	/**
	 * @param idx index of the BlockInfo
	 * @param binfo BlockInfo object that already exists in the database
	 */
	public static void updateIndex(int idx, BlockInfo binfo) {
		Session s = DBConnector.sessionFactory.getSession();
		Transaction tx = s.currentTransaction();
		tx.begin();
		BlockInfoTable bit =  s.newInstance(BlockInfoTable.class);
		bit.setBlockId(binfo.getBlockId());
		bit.setGenerationStamp(binfo.getGenerationStamp());
		bit.setINodeID(binfo.getINode().getID()); //FIXME: verify if this is working - use Mariano
		bit.setBlockIndex(idx); //setting the index in the table
		bit.setNumBytes(binfo.getNumBytes());
		s.updatePersistent(bit);
		tx.commit();
	}

	public static void updateINodeID(long iNodeID, BlockInfo binfo) {
		Session s = DBConnector.sessionFactory.getSession();
		Transaction tx = s.currentTransaction();
		tx.begin();
		BlockInfoTable bit =  s.newInstance(BlockInfoTable.class);
		bit.setBlockId(binfo.getBlockId());
		bit.setGenerationStamp(binfo.getGenerationStamp());
		bit.setINodeID(iNodeID); //setting the iNodeID here - the rest is same
		bit.setNumBytes(binfo.getNumBytes());
		s.updatePersistent(bit);
		tx.commit();
	}



	public static List<BlockInfoTable> getResultListUsingField(String field, long value){
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<BlockInfoTable> dobj = qb.createQueryDefinition(BlockInfoTable.class);

		dobj.where(dobj.get(field).equal(dobj.param("param")));

		Query<BlockInfoTable> query = session.createQuery(dobj);
		query.setParameter("param", value); //the WHERE clause of SQL

		return 	query.getResultList();

	}

	public static BlockInfo[] getBlocksArray(INodeFile inode) {

		if(inode==null)
		{
			System.out.println("[NOT KTHFS]: getBLocksArray inode is null!!!!");
			return null;
		}
		System.out.println("[NOT KTHFS]: getBLocksArray inodeID" + inode.getID());
		List<BlockInfoTable> blocksList = getResultListUsingField("iNodeID", inode.getID());

		if(blocksList.size() == 0 || blocksList == null) {
			KthFsHelper.printKTH("7PM!!!!!!!!!!!!!   blocksList is null");
			return null;
		}

		BlockInfo[] blocksArray = new BlockInfo[blocksList.size()];
		try {
			for(int i=0; i<blocksArray.length; i++) {

				blocksArray[i] = getBlockInfoSingle(blocksList.get(i).getBlockId());
				KthFsHelper.printKTH("7:30PM!!!!!  inode.getID(): "+inode.getID());
				blocksArray[i].setINode(inode);
			
			}
			KthFsHelper.printKTH("8:36pm inside try catch blocksArrayy[0].getINode(): "+blocksArray[0].getINode());
			return blocksArray;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		KthFsHelper.printKTH("8:31pm blocksArrayy[0].getINode(): "+blocksArray[0].getINode());
		return blocksArray;
	}

	/**remove this later*/
	public static BlockInfo[] getBlocksArrayWithINodes(INodeFile inode) {

		if(inode==null)
		{
			System.out.println("[NOT KTHFS]: getBLocksArray inode is null!!!!");
			return null;
		}
		System.out.println("[NOT KTHFS]: getBLocksArray inodeID" + inode.getID());
		List<BlockInfoTable> blocksList = getResultListUsingField("iNodeID", inode.getID());

		if(blocksList.size() == 0 || blocksList == null)
			return null;

		BlockInfo[] blocksArray = new BlockInfo[blocksList.size()];
		for(int i=0; i<blocksArray.length; i++) {
			blocksArray[i] = getBlockInfo(blocksList.get(i).getBlockId());
			blocksArray[i].setINode(inode);

		}

		return blocksArray;
	}



}

/*
 * change commitOrCompleteLastBlock //gets called by datanode and the client both
 * change commitBlockSynchronization //this gets called by the datanode
 * change NameNodeRpcServer.blockReport //this gets called when datanode comes up and then every heartbeat
 *  * 
 * */

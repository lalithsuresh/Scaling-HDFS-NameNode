package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
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
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

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
			BlockInfo[] inBlocks = in.getBlocks();
			for(int i=0;i<inBlocks.length;i++) {
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
		{
			return null;
		}
			else {
			Block b = new Block(bit.getBlockId(), bit.getNumBytes(), bit.getGenerationStamp());
			BlockInfo blockInfo = new BlockInfo(b, bit.getReplication());

			if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.COMMITTED.ordinal())
			{
				blockInfo = new BlockInfoUnderConstruction(b, bit.getReplication());
				((BlockInfoUnderConstruction) blockInfo).setBlockUCState(HdfsServerConstants.BlockUCState.COMMITTED);
				//System.err.println("Retrieved Block that is COMMITTED");
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.COMPLETE.ordinal())
			{
				blockInfo = new BlockInfo(b, bit.getReplication());
				//System.err.println("Retrieved Block that is COMPLETE " + blockInfo.getBlockUCState());
				//System.err.println("Retrieved Block that is COMPLETE");	
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION.ordinal())
			{
				blockInfo = new BlockInfoUnderConstruction(b, bit.getReplication());
				((BlockInfoUnderConstruction) blockInfo).setBlockUCState(HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION);
				//System.err.println("Retrieved Block that is UNDER_CONSTRUCTION");
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.UNDER_RECOVERY.ordinal())
			{
				//System.err.println("Retrieved Block that is UNDER_RECOVERY");
			}

//			//FIXME: change primary key of table - sort the results on index
//			List<TripletsTable> tripletsTable = getTriplets(blockId); 
//			Object[] tripletsKTH = new Object[3*tripletsTable.size()];
//
//			for(int i=0;i<tripletsTable.size();i++) {
//
//				DatanodeDescriptor dd = dm.getDatanodeByHost(tripletsTable.get(i).getDatanodeName()); //KTHFS: see if this works
//				long prevBlockId = tripletsTable.get(i).getPreviousBlockId();
//				long nextBlockId = tripletsTable.get(i).getNextBlockId();
//				int index = tripletsTable.get(i).getIndex();
//
//				tripletsKTH[3*index] = dd;
//				tripletsKTH[(3*index) + 1] = prevBlockId;
//				tripletsKTH[(3*index) + 2] = nextBlockId;
//			}
//
//			blockInfo.setTripletsKTH(tripletsKTH);

			//W: assuming that this function will only be called on an INodeFile

			INodeFile node = (INodeFile)INodeTableHelper.getINode(bit.getINodeID());
			if(node == null)
			{
				System.out.println("[NOTKTHFS] getBlockInfo node is null!!!!!!!!!!!!!");
				return null;
			}
			node.setBlocksList(getBlocksArray(node));//circular?

			blockInfo.setINode(node);
			
			//blockInfo.setTripletsKTH(setTripletsForBlock (blockInfo));
			//setTripletsForBlock (blockInfo);
			blockInfo.setBlockIndex(bit.getBlockIndex());
			blockInfo.setTimestamp(bit.getTimestamp());

			return blockInfo;
		}

	}


	public static BlockInfo getBlockInfoSingle(long blockId) throws IOException {
		Session s = DBConnector.sessionFactory.getSession();
		DatanodeManager dm = ns.getBlockManager().getDatanodeManager();

		BlockInfoTable bit = s.find(BlockInfoTable.class, blockId);
		
		if(bit == null)
			return null;
		else {
			BlockInfo blockInfo = null;
			Block b = new Block(bit.getBlockId(), bit.getNumBytes(), bit.getGenerationStamp());


			if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.COMMITTED.ordinal())
			{
				blockInfo = new BlockInfoUnderConstruction(b, bit.getReplication());
				((BlockInfoUnderConstruction) blockInfo).setBlockUCState(HdfsServerConstants.BlockUCState.COMMITTED);
				//System.err.println("Retrieved Block that is COMMITTED");
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.COMPLETE.ordinal())
			{
				blockInfo = new BlockInfo(b, bit.getReplication());
				//System.err.println("Retrieved Block that is COMPLETE " + blockInfo.getBlockUCState());
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION.ordinal())
			{
				blockInfo = new BlockInfoUnderConstruction(b, bit.getReplication());
				((BlockInfoUnderConstruction) blockInfo).setBlockUCState(HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION);
	
				//blockInfo.convertToBlockUnderConstruction(HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION, getDataNodesFromBlock(bit.getBlockId()));
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.UNDER_RECOVERY.ordinal())
			{
				//System.err.println("Retrieved Block that is UNDER_RECOVERY");
			}
/*			

			//FIXME: change primary key of table - sort the results on index
			List<TripletsTable> tripletsTable = getTriplets(blockId); 
			Object[] tripletsKTH = new Object[3*tripletsTable.size()];

			for(int i=0;i<tripletsTable.size();i++) {

				DatanodeDescriptor dd = dm.getDatanodeByHost(tripletsTable.get(i).getDatanodeName()); //KTHFS: see if this works
				long prevBlockId = tripletsTable.get(i).getPreviousBlockId();
				long nextBlockId = tripletsTable.get(i).getNextBlockId();
				int index = tripletsTable.get(i).getIndex();

				tripletsKTH[3*index] = dd;
				tripletsKTH[(3*index) + 1] = prevBlockId;
				tripletsKTH[(3*index) + 2] = nextBlockId;
			}

			blockInfo.setTripletsKTH(tripletsKTH);
*/
			//blockInfo.setTripletsKTH(setTripletsForBlock (blockInfo));
			//setTripletsForBlock (blockInfo);
	
			blockInfo.setBlockIndex(bit.getBlockIndex()); 
			blockInfo.setTimestamp(bit.getTimestamp());
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
		bit.setBlockUCState(binfo.getBlockUCState().ordinal());

		bit.setTimestamp(System.currentTimeMillis()); //added by W - for sorting the blocks properly
		
		if(binfo.isComplete()) {
			INodeFile ifile = binfo.getINode();
			long nodeID = ifile.getID();
			bit.setINodeID(nodeID); 
		}

		//System.err.println("[KTHFS] numBytes here is: " + binfo.getNumBytes());
		
		bit.setNumBytes(binfo.getNumBytes());
		//FIXME: KTHFS: Ying and Wasif: replication is null at the moment - remove the column if not required later on
		
		
		List<TripletsTable> results = getTripletsListUsingField ("blockId", binfo.getBlockId(), session);
		
		if (results.isEmpty())
		{
			// We don't have the triplets table yet,
			// let's create it now so that future
			// requests to the DB from BlockInfo get/sets
			// work.
			
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
				{
					t.setDatanodeName(null);
				}
				else
				{
					t.setDatanodeName(dd.getHostName());
				}
				t.setIndex(i);
				t.setPreviousBlockId(prevBlockId);
				t.setNextBlockId(nextBlockId);
				s.savePersistent(t);
			}

		}
/*
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
			{
				t.setDatanodeName(null);
			}
			else
			{
				t.setDatanodeName(dd.getHostName());
			}
			t.setIndex(i);
			t.setPreviousBlockId(prevBlockId);
			t.setNextBlockId(nextBlockId);
			s.savePersistent(t);
		}*/

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
		bit.setBlockUCState(binfo.getBlockUCState().ordinal());
		s.updatePersistent(bit);
		tx.commit();		

		//System.err.println("[KTHFS] numBytes here in updateIndex is: " + binfo.getNumBytes());
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
		bit.setBlockUCState(binfo.getBlockUCState().ordinal());
		s.updatePersistent(bit);
		tx.commit();
		
		//System.err.println("[KTHFS] numBytes here in updateInodeId is: " + binfo.getNumBytes());
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
			return null;
		}
		List<BlockInfoTable> blocksList = getResultListUsingField("iNodeID", inode.getID());

		if(blocksList.size() == 0 || blocksList == null) {
			return null;
		}
		
		BlockInfo[] blocksArray = new BlockInfo[blocksList.size()];
		
		try {
			for(int i=0; i<blocksArray.length; i++) {
				blocksArray[i] = getBlockInfoSingle(blocksList.get(i).getBlockId());
				blocksArray[i].setINode(inode);
				
			}
			//sorting the array in descending order w.r.t blockIndex
			Arrays.sort(blocksArray); 
			return blocksArray;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return blocksArray;
	}

	/**remove this later*/
	public static BlockInfo[] getBlocksArrayWithNoINodes(long inodeID) {
		
		List<BlockInfoTable> blocksList = getResultListUsingField("iNodeID", inodeID);

		if(blocksList.size() == 0 || blocksList == null)
			return null;

		BlockInfo[] blocksArray = new BlockInfo[blocksList.size()];
		for(int i=0; i<blocksArray.length; i++) {
			try {
				//Calling getBlockInfoSingle will return blocks without any Inodes
				blocksArray[i] = getBlockInfoSingle(blocksList.get(i).getBlockId());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return blocksArray;
	}

	/** Update Previous or next block in the triplets table for a given BlockId.
	 *  next=true: update nextBlockId, false: updatePrevious */
	public static void setNextPrevious(long blockid, int idx, BlockInfo nextBlock, boolean next){
		Session session = DBConnector.sessionFactory.getSession();
		Transaction tx = session.currentTransaction();
		Object[] pKey = new Object[2];
		pKey[0]=blockid;
		pKey[1]=idx;
		TripletsTable triplet = session.find(TripletsTable.class, pKey);
		tx.begin();
		if (triplet != null)
		{
			System.err.println("Datanode name at this point is: " + triplet.getDatanodeName());
			if(next)
			triplet.setNextBlockId(nextBlock.getBlockId());
			else
				triplet.setPreviousBlockId(nextBlock.getBlockId());

			triplet.setIndex(idx);
			session.savePersistent(triplet);
			tx.commit();
		}

	}

	/** Update the DataNode in the triplets table.*/

	public static void setDatanode(long blockId, int index, String name) {
		Session session = DBConnector.sessionFactory.getSession();
		Transaction tx = session.currentTransaction();
		Object[] pKey = new Object[2];
		pKey[0]=blockId;
		pKey[1]=index;
		TripletsTable triplet = session.find(TripletsTable.class, pKey);
		tx.begin();
		if (triplet != null)
		{
			System.err.println("Datanode name at this point is: " + name);
			triplet.setDatanodeName(name);
			triplet.setIndex(index);
			session.savePersistent(triplet);
			tx.commit();
		}
		else
		{
			System.err.println("new triplets Datanode name at this point is: " + name);
			TripletsTable newTriplet = session.newInstance(TripletsTable.class);
			newTriplet.setBlockId(blockId);
			newTriplet.setDatanodeName(name);
			newTriplet.setIndex(index);
			newTriplet.setPreviousBlockId(-1);
			newTriplet.setNextBlockId(-1);
			session.savePersistent(newTriplet);
			tx.commit();
		}
	}
	
	public static DatanodeDescriptor getDatanode (long blockId, int index){
		Session session = DBConnector.sessionFactory.getSession();
		Object[] pKey = new Object[2];
		pKey[0]=blockId;
		pKey[1]=index;
		System.err.println("Searching for: " + blockId + " " + index);
		TripletsTable triplet = session.find(TripletsTable.class, pKey);
		
		if (triplet != null && triplet.getDatanodeName() != null)
		{
			//System.err.println("triplet: " + triplet.getDatanodeName()) ;
			String hostname = triplet.getDatanodeName().split(":")[0];
			//System.err.println("host: " + hostname);
			DatanodeDescriptor ret = ns.getBlockManager().getDatanodeManager().getDatanodeByHost(hostname);
			return ret;
		}
		
		return null;
	}
	
	public static BlockInfo getNextPrevious (long blockId, int index, boolean next) throws IOException{
		Session session = DBConnector.sessionFactory.getSession();
		Object[] pKey = new Object[2];
		pKey[0]=blockId;
		pKey[1]=index;
		System.err.println("getNextPrevious() on " + blockId + " " + index);
		TripletsTable triplet = session.find(TripletsTable.class, pKey);
	
		if (next == true)
			return getBlockInfoSingle(triplet.getNextBlockId());
		else
			return getBlockInfoSingle(triplet.getPreviousBlockId());
	}

	public static DatanodeDescriptor [] getDataNodesFromBlock (long blockId){
		Session session = DBConnector.sessionFactory.getSession();
		List<TripletsTable> result = getTripletsListUsingField("blockId", blockId, session);
		DatanodeDescriptor[] nodeDescriptor = new DatanodeDescriptor[result.size()];

		int i = 0;
		for (TripletsTable t: result){
			DatanodeID dn = new DatanodeID(t.getDatanodeName());
			nodeDescriptor[i] = new DatanodeDescriptor(dn);
			i++; // giggidy
		}
		
		return nodeDescriptor;
	}

	public static List<TripletsTable> getTripletsListUsingField(String field, long value, Session s){
		QueryBuilder qb = s.getQueryBuilder();
		QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
		dobj.where(dobj.get(field).equal(dobj.param("param")));
		Query<TripletsTable> query = s.createQuery(dobj);
		query.setParameter("param", value); //the WHERE clause of SQL
		return query.getResultList();

	}
	
	public static BlockInfo removeBlocks(Block key)
	{
		System.out.println("removeBlocks called");
		Session s = DBConnector.sessionFactory.getSession();
		long blockId = key.getBlockId();
		BlockInfo bi = new BlockInfo(1);
		bi.setBlockId(blockId);
		BlockInfoTable bit = s.find(BlockInfoTable.class, blockId);
		System.out.println("blockId is "+blockId);
		Transaction tx = session.currentTransaction();
		tx.begin();
		s.deletePersistent(bit);
		tx.commit();
		if(bi!=null)
			System.out.println("should see this print");
		return bi;
	}
	
	public static void removeTriplets(BlockInfo blockInfo, int index)
	{
		Session session = DBConnector.sessionFactory.getSession();
		Transaction tx = session.currentTransaction();
		Object[] pKey = new Object[2];
		pKey[0]=blockInfo.getBlockId();
		pKey[1]=index;
		System.err.println("removeTriplets() on " + blockInfo.getBlockId() + " " + index);
		TripletsTable triplet = session.find(TripletsTable.class, pKey);
		tx.begin();
		session.deletePersistent(triplet);
		tx.commit();
	}

	public static Object[] getTripletsForBlock (BlockInfo blockinfo) {
		Session session = DBConnector.sessionFactory.getSession();
		List<TripletsTable> results = getTripletsListUsingField ("blockId", blockinfo.getBlockId(), session);
		
		Object[] triplets = new Object[3 * results.size()];
		for (TripletsTable t:results){
			System.err.println("[0]:" + t.getDatanodeName() + " [1]:" + t.getPreviousBlockId() + " [2]:" + t.getNextBlockId());
			
			triplets[3 * t.getIndex()] = ns.getBlockManager().getDatanodeManager().getDatanodeByHost(t.getDatanodeName());;
			triplets[3 * t.getIndex() + 1] = t.getPreviousBlockId();
			triplets[3 * t.getIndex() + 2] = t.getNextBlockId();
		}
		
		return triplets;
	}
	
	public static INode getInodeFromBlockId (long blockId) {
		Session session = DBConnector.sessionFactory.getSession();
		BlockInfoTable blockInfoTable = session.find(BlockInfoTable.class, blockId);

		long inodeId = blockInfoTable.getINodeID();
		
		return INodeTableHelper.getINode(inodeId);
	}
	
	/** Gets the second last block of an INodeFile
	 * @param inodefile
	 * @return
	 */
	public static BlockInfo getPenultimateBlock(INodeFile inodefile) {
		List<BlockInfoTable> blocksFromDB = getResultListUsingField("iNodeID", inodefile.getID());
		int max= -1;
		long blockId = -1;

		BlockInfo block = null;

		if(blocksFromDB == null || blocksFromDB.size() <= 1) 
			return null;


		for (int i = 0; i < blocksFromDB.size(); i++) {
			if (blocksFromDB.get(i).getBlockIndex() > max) {
				max = blocksFromDB.get(i).getBlockIndex();
				blockId = blocksFromDB.get(i).getBlockId();
			}
			else
				continue;
		}


		try {

			//Immature
			BlockInfoTable bit = getResultListUsingField("blockIndex", max-1).get(0);
			block = getBlockInfoSingle(bit.getBlockId());

			return block;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} 

	}


	/** Gets the last block of an INodeFile
	 * @param inodefile
	 * @return
	 */
	public static BlockInfo getLastBlock(INodeFile inodefile) {
		List<BlockInfoTable> blocksFromDB = getResultListUsingField("iNodeID", inodefile.getID());
		int max= -1;
		long blockId = -1;

		BlockInfo block = null;

		try {
		if(blocksFromDB == null || blocksFromDB.size() == 0) 
			return null;
		else if(blocksFromDB.size() == 1)
			return getBlockInfoSingle(blocksFromDB.get(0).getBlockId());


		for (int i = 0; i < blocksFromDB.size(); i++) {
			if (blocksFromDB.get(i).getBlockIndex() > max) {
				max = blocksFromDB.get(i).getBlockIndex();
				blockId = blocksFromDB.get(i).getBlockId();
			}
			else
				continue;
		}

		
			//Immature
			 return getBlockInfoSingle(blockId);

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} 

	}

}

/*
 * change commitOrCompleteLastBlock //gets called by datanode and the client both
 * change commitBlockSynchronization //this gets called by the datanode
 * change NameNodeRpcServer.blockReport //this gets called when datanode comes up and then every heartbeat
 *  * 
 * */

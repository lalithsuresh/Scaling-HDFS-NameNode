package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;

import se.sics.clusterj.*;

import com.mysql.clusterj.ClusterJException;
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
	static final int RETRY_COUNT = 3; 

	/**
	 * Helper function for appending an array of blocks - used by concat
	 * Replacement for INodeFile.appendBlocks
	 */
	public static void appendBlocks(INodeFile thisNode, INodeFile [] inodes, int totalAddedBlocks) {
		int tries=RETRY_COUNT;
		boolean done = false;
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0 ){
			try{	
				tx.begin();
				appendBlocksInternal(thisNode, inodes, totalAddedBlocks, session);
				tx.commit();
				done=true;
				session.flush();
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.addChild() threw error " + e.getMessage());
				tries--;
			}
		}
		
	}
	private static void appendBlocksInternal (INodeFile thisNode, INodeFile[] inodes, int totalAddedBlocks, Session session){
		for(INodeFile in: inodes) {
			BlockInfo[] inBlocks = in.getBlocks();
			for(int i=0;i<inBlocks.length;i++) {
				BlockInfoTable bInfoTable = createBlockInfoTable(thisNode, inBlocks[i], session);
				session.makePersistent(bInfoTable);
			}
		}
	}

	/**
	 * Helper function for inserting a block in the BlocksInfo table
	 * Replacement for INodeFile.addBlock
	 * */
	public static void addBlock(BlockInfo newblock) {
		putBlockInfo(newblock);
	}

	/**Helper function for creating a BlockInfoTable object, no DB access */
	private static BlockInfoTable createBlockInfoTable(INode node, BlockInfo newblock, Session session) {
		BlockInfoTable bInfoTable = session.newInstance(BlockInfoTable.class);
		bInfoTable.setBlockId(newblock.getBlockId());
		bInfoTable.setGenerationStamp(newblock.getGenerationStamp());
		bInfoTable.setINodeID(newblock.getINode().getID()); //FIXME: store ID in INodeFile objects - use Mariano :)
		bInfoTable.setNumBytes(newblock.getNumBytes());
		bInfoTable.setReplication(-1); //FIXME: see if we need to store this or not
		return bInfoTable;
	}

//	private static List<TripletsTable> getTriplets(long blockId) {
//		Session s = DBConnector.sessionFactory.getSession();
//		QueryBuilder qb = s.getQueryBuilder();
//		QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
//		dobj.where(dobj.get("blockId").equal(dobj.param("blockId"))); //works?
//		Query<TripletsTable> query = s.createQuery(dobj);
//		query.setParameter("blockId", blockId); //W: WHERE blockId = blockId
//		
//
//		return query.getResultList(); 
//	}
//
	/** Return a BlockInfo object from an blockId 
	 * @param blockId
	 * @return
	 * @throws IOException 
	 */
	public static BlockInfo getBlockInfo(long blockId)  {
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				BlockInfo ret = getBlockInfoInternal(blockId, session, false);
				tx.commit();
				session.flush();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("getBlockInfo failed " + e.getMessage());
				tries--;
			}
		}
		return null;
	}
	/** When called with single=false, will not retrieve the INodes for the Block */
	private static BlockInfo getBlockInfoInternal(long blockId, Session session, boolean single){	
		BlockInfoTable bit = session.find(BlockInfoTable.class, blockId);

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
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.COMPLETE.ordinal())
			{
				blockInfo = new BlockInfo(b, bit.getReplication());
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION.ordinal())
			{
				blockInfo = new BlockInfoUnderConstruction(b, bit.getReplication());
				((BlockInfoUnderConstruction) blockInfo).setBlockUCState(HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION);
			}
			else if (bit.getBlockUCState() == HdfsServerConstants.BlockUCState.UNDER_RECOVERY.ordinal())
			{
				// FIXME: Handle me
			}

			//W: assuming that this function will only be called on an INodeFile
			if (single == false){
				INodeFile node = (INodeFile)INodeTableHelper.getINode(bit.getINodeID());
				if (node == null){
					return null;
				}
				node.setBlocksList(getBlocksArray(node));

				blockInfo.setINode(node);
			}
			blockInfo.setBlockIndex(bit.getBlockIndex()); 
			blockInfo.setTimestamp(bit.getTimestamp());

			return blockInfo;
		}

	}
	/** Returns a BlockInfo object without any Inodes set for it (single=true) */
	public static BlockInfo getBlockInfoSingle(long blockId) throws IOException {
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				BlockInfo ret = getBlockInfoInternal(blockId, session, true);
				tx.commit();
				session.flush();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("getBlockInfo failed " + e.getMessage());
				tries--;
			}
		}
		return null;
	}
	
	public static void putBlockInfo(BlockInfo binfo) {
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				putBlockInfoInternal(binfo, session);
				tx.commit();
				session.flush();
				done=true;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("putBlockInfo failed " + e.getMessage());
				tries--;
			}
		}
	}
	private static void putBlockInfoInternal(BlockInfo binfo, Session s){
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

		bit.setNumBytes(binfo.getNumBytes());
		//FIXME: KTHFS: Ying and Wasif: replication is null at the moment - remove the column if not required later on
		
		List<TripletsTable> results = getTripletsListUsingField ("blockId", binfo.getBlockId());
		if (results.isEmpty())
		{
			//Getting triplets from Memory, before saving to DB
			Object[] tripletsKTH = binfo.getTriplets();
			
			//For as many triplets as there are in mem
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
				
				//Save the triplets
				TripletsTable t = s.newInstance(TripletsTable.class);
				t.setBlockId(binfo.getBlockId());

				if (dd==null)
					t.setDatanodeName(null);
				else {
					t.setDatanodeName(dd.getHostName());
				}
				t.setIndex(i);
				t.setPreviousBlockId(prevBlockId);
				t.setNextBlockId(nextBlockId);
				s.savePersistent(t);
			}

		}

		s.savePersistent(bit);
	}


	/** Update index for a BlockInfo object in the DB
	 * @param idx index of the BlockInfo
	 * @param binfo BlockInfo object that already exists in the database
	 */
	public static void updateIndex(int idx, BlockInfo binfo) {
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				updateIndexInternal(idx, binfo, session);
				tx.commit();
				session.flush();
				done=true;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("updateIndex failed " + e.getMessage());
				tries--;
			}
		}
	}
	private static void updateIndexInternal(int idx, BlockInfo binfo, Session s){
		BlockInfoTable bit =  s.newInstance(BlockInfoTable.class);
		bit.setBlockId(binfo.getBlockId());
		bit.setGenerationStamp(binfo.getGenerationStamp());
		bit.setINodeID(binfo.getINode().getID()); //FIXME: verify if this is working - use Mariano
		bit.setBlockIndex(idx); //setting the index in the table
		bit.setNumBytes(binfo.getNumBytes());
		bit.setBlockUCState(binfo.getBlockUCState().ordinal());
		s.updatePersistent(bit);
		
	}
	public static void updateINodeID(long iNodeID, BlockInfo binfo){
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				updateINodeIDInternal(iNodeID, binfo, session);
				tx.commit();
				session.flush();
				done=true;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("updateINodeID failed " + e.getMessage());
				tries--;
			}
		}
	}
	private static void updateINodeIDInternal(long iNodeID, BlockInfo binfo, Session s) {
		BlockInfoTable bit =  s.newInstance(BlockInfoTable.class);
		bit.setBlockId(binfo.getBlockId());
		bit.setGenerationStamp(binfo.getGenerationStamp());
		//setting the iNodeID here - the rest remains the same
		bit.setINodeID(iNodeID); 
		bit.setNumBytes(binfo.getNumBytes());
		bit.setBlockUCState(binfo.getBlockUCState().ordinal());
		s.updatePersistent(bit);		
	}

	public static List<BlockInfoTable> getResultListUsingField(String field, long value){
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				List <BlockInfoTable> ret = getResultListUsingFieldInternal(field, value, session);
				tx.commit();
				session.flush();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("updateIndex failed " + e.getMessage());
				tries--;
			}
		}
		return null;
	}

	private static List<BlockInfoTable> getResultListUsingFieldInternal(String field, long value, Session session){
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<BlockInfoTable> dobj = qb.createQueryDefinition(BlockInfoTable.class);
		dobj.where(dobj.get(field).equal(dobj.param("param")));
		Query<BlockInfoTable> query = session.createQuery(dobj);
		query.setParameter("param", value);
		return 	query.getResultList();

	}
	public static BlockInfo[] getBlocksArray(INodeFile inode) {
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				BlockInfo[] ret = getBlocksArrayInternal(inode, session);
				tx.commit();
				session.flush();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("getBlocksArray failed " + e.getMessage());
				tries--;
			}
		}
		return null;
	}
	public static BlockInfo[] getBlocksArrayInternal(INodeFile inode, Session session){
		if(inode==null)
			return null;
		
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
			Arrays.sort(blocksArray, new BlockInfoComparator());
			return blocksArray;
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		return blocksArray;
	}
	
	
	public static class BlockInfoComparator implements Comparator<BlockInfo> {

		@Override
		public int compare(BlockInfo o1, BlockInfo o2) {
			return o1.getTimestamp() < o2.getTimestamp() ? -1 : 1;
		}
		
	}
	

	/** Update Previous or next block in the triplets table for a given BlockId.
	 *  next=true: update nextBlockId, false: updatePrevious */
	public static void setNextPrevious(long blockid, int idx, BlockInfo nextBlock, boolean next){
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				setNextPreviousInternal(blockid, idx, nextBlock, next, session);
				tx.commit();
				session.flush();
				done=true;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("setNextPrevious failed " + e.getMessage());
				tries--;
			}
		}
	}
	
	private static void setNextPreviousInternal(long blockid, int idx, BlockInfo nextBlock, boolean next, Session session){
		Object[] pKey = new Object[2];
		pKey[0]=blockid;
		pKey[1]=idx;
		TripletsTable triplet = session.find(TripletsTable.class, pKey);
		if (triplet != null)
		{
			if(next)
				triplet.setNextBlockId(nextBlock.getBlockId());
			else
				triplet.setPreviousBlockId(nextBlock.getBlockId());

			triplet.setIndex(idx);
			session.savePersistent(triplet);
		}
	}

	/** Update the DataNode in the triplets table.*/
	public static void setDatanode(long blockId, int index, String name) {
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				setDatanodeInternal(blockId, index, name, session);
				tx.commit();
				session.flush();
				done=true;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("setDataNode failed " + e.getMessage());
				tries--;
			}
		}
	}	
	
		private  static void setDatanodeInternal(long blockId, int index, String name, Session session) {
			Object[] pKey = new Object[2];
			pKey[0]=blockId;
			pKey[1]=index;
			TripletsTable triplet = session.find(TripletsTable.class, pKey);
			if (triplet != null) {
				triplet.setDatanodeName(name);
				triplet.setIndex(index);
				session.savePersistent(triplet);
			}
			else {
				TripletsTable newTriplet = session.newInstance(TripletsTable.class);
				newTriplet.setBlockId(blockId);
				newTriplet.setDatanodeName(name);
				newTriplet.setIndex(index);
				newTriplet.setPreviousBlockId(-1);
				newTriplet.setNextBlockId(-1);
				session.savePersistent(newTriplet);
			}
		}
	
	public static DatanodeDescriptor getDatanode (long blockId, int index){
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				DatanodeDescriptor ret = getDataDescriptorInternal(blockId, index, session);
				tx.commit();
				session.flush();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("setDataNode failed " + e.getMessage());
				tries--;
			}
		}
		return null;
	}	
	
	private static DatanodeDescriptor getDataDescriptorInternal(long blockId, int index, Session session){
		Object[] pKey = new Object[2];
		pKey[0]=blockId;
		pKey[1]=index;
		TripletsTable triplet = session.find(TripletsTable.class, pKey);
		
		if (triplet != null && triplet.getDatanodeName() != null) {
			String hostname = triplet.getDatanodeName().split(":")[0];
			DatanodeDescriptor ret = ns.getBlockManager().getDatanodeManager().getDatanodeByHost(hostname);
			return ret;
		}
		
		return null;
	}
	
	public static BlockInfo getNextPrevious (long blockId, int index, boolean next) throws IOException{
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				BlockInfo ret = getNextPreviousInternal(blockId, index, next, session);
				tx.commit();
				session.flush();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("setDataNode failed " + e.getMessage());
				tries--;
			}
		}
		return null;
	}	
	
	private static BlockInfo getNextPreviousInternal (long blockId, int index, boolean next, Session session) throws IOException{
		Object[] pKey = new Object[2];
		pKey[0]=blockId;
		pKey[1]=index;
		TripletsTable triplet = session.find(TripletsTable.class, pKey);
	
		if (next == true)
			return getBlockInfoSingle(triplet.getNextBlockId());
		else
			return getBlockInfoSingle(triplet.getPreviousBlockId());
	}

	
	public static DatanodeDescriptor[] getDataNodesFromBlock (long blockId){
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				DatanodeDescriptor[] ret = getDataNodesFromBlockInternal(blockId, session);
				tx.commit();
				session.flush();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("getDataNodesFromBlock failed " + e.getMessage());
				tries--;
			}
		}
		return null;
	}
	
	
	private static DatanodeDescriptor[] getDataNodesFromBlockInternal (long blockId, Session session){
		List<TripletsTable> result = getTripletsListUsingField("blockId", blockId);
		DatanodeDescriptor[] nodeDescriptor = new DatanodeDescriptor[result.size()];
		int i = 0;
		for (TripletsTable t: result){
			DatanodeID dn = new DatanodeID(t.getDatanodeName());
			nodeDescriptor[i] = new DatanodeDescriptor(dn);
			i++;
		}
		return nodeDescriptor;
	}
	
	public static List<TripletsTable> getTripletsListUsingField(String field, long value){
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				List<TripletsTable> ret = getTripletsListUsingFieldInternal(field, value, session);
				tx.commit();
				session.flush();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("getDataNodesFromBlock failed " + e.getMessage());
				tries--;
			}
		}
		return null;
	}
	
	
	private static List<TripletsTable> getTripletsListUsingFieldInternal(String field, long value, Session s){
		QueryBuilder qb = s.getQueryBuilder();
		QueryDomainType<TripletsTable> dobj = qb.createQueryDefinition(TripletsTable.class);
		dobj.where(dobj.get(field).equal(dobj.param("param")));
		Query<TripletsTable> query = s.createQuery(dobj);
		query.setParameter("param", value);
		return query.getResultList();

	}
	
	
	public static BlockInfo removeBlocks(Block key){
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				BlockInfo ret = removeBlocksInternal(key, session);
				tx.commit();
				session.flush();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("removeBlocks failed " + e.getMessage());
				tries--;
			}
		}
		return null;
	}
	
	
	private static BlockInfo removeBlocksInternal(Block key, Session s)
	{
		long blockId = key.getBlockId();
		BlockInfo bi = new BlockInfo(1);
		bi.setBlockId(blockId);
		BlockInfoTable bit = s.find(BlockInfoTable.class, blockId);
		//System.out.println("blockId is "+blockId);
		s.deletePersistent(bit);
		return bi;
	}
	
	
	public static void removeTriplets(BlockInfo blockInfo, int index)
	{
		
		Transaction tx = session.currentTransaction();
		tx.begin();
		
		Object[] pKey = new Object[2];
		pKey[0]=blockInfo.getBlockId();
		pKey[1]=index;
		TripletsTable triplet = session.find(TripletsTable.class, pKey);
		session.deletePersistent(triplet);
		session.flush();
		
		// The triplets entries in the DB for a block have an ordered list of
		// indices. Removal of an entry of an index X means that all entries
		// with index greater than X needs to be corrected (decremented by 1
		// basically).
		List<TripletsTable> results = getTripletsListUsingFieldInternal ("blockId", blockInfo.getBlockId(), session);
		
		for (TripletsTable t: results)	{
			long oldIndex = t.getIndex();
			
			// entry that needs its index corrected
			if (index < oldIndex)
			{				
				// ClusterJ sucks royal ass, because we can't use auto-incrementing
				// MySQL cluster indices. Thus, editing a primary key or a part of
				// a composite key => we need to remove the entry, and re-insert it
				// into the DB.
				
				TripletsTable replacementEntry = session.newInstance(TripletsTable.class);
				replacementEntry.setBlockId(t.getBlockId());
				replacementEntry.setDatanodeName(t.getDatanodeName());
				replacementEntry.setIndex(t.getIndex() - 1); // Correct the index
				replacementEntry.setNextBlockId(t.getNextBlockId());
				replacementEntry.setPreviousBlockId(t.getPreviousBlockId());
				
				session.deletePersistent(t); // Delete old entry
				session.makePersistent(replacementEntry); // Add new one
			}
		}
		tx.commit();
	}
	
	
	/** Given a BlockInfo object, fetch the rows of the Triplets table as a Triplets object  */
	public static Object[] getTripletsForBlock (BlockInfo blockinfo) {
		int tries = RETRY_COUNT;
		boolean done = false;
		
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				Object[] ret = getTripletsForBlockInternal(blockinfo, session);
				tx.commit();
				session.flush();
				done=true;
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("removeBlocks failed " + e.getMessage());
				tries--;
			}
		}
		return null;
	}
	
	
	private static Object[] getTripletsForBlockInternal (BlockInfo blockinfo, Session session) {
		List<TripletsTable> results = getTripletsListUsingField ("blockId", blockinfo.getBlockId());
		
		Object[] triplets = new Object[3 * results.size()];
		
		for (TripletsTable t:results){			
			triplets[3 * t.getIndex()] = ns.getBlockManager().getDatanodeManager().getDatanodeByHost(t.getDatanodeName());
			triplets[3 * t.getIndex() + 1] = t.getPreviousBlockId();
			triplets[3 * t.getIndex() + 2] = t.getNextBlockId();
		}
		
		return triplets;
	}
	
	
	public static INode getInodeFromBlockId (long blockId) {
		
		BlockInfoTable blockInfoTable = session.find(BlockInfoTable.class, blockId);

		long inodeId = blockInfoTable.getINodeID();
		
		return INodeTableHelper.getINode(inodeId);
	}
}

/*
 * change commitOrCompleteLastBlock //gets called by datanode and the client both
 * change commitBlockSynchronization //this gets called by the datanode
 * change NameNodeRpcServer.blockReport //this gets called when datanode comes up and then every heartbeat
 *  * 
 * */
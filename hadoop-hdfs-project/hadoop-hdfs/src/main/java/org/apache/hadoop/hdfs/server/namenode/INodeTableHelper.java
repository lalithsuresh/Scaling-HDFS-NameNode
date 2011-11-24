package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import se.sics.clusterj.InodeTable;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;

import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;

public class INodeTableHelper {
	//public static Session session = DBConnector.sessionFactory.getSession();
	static final int MAX_DATA = 128;
	public static FSNamesystem ns = null;
	static final int RETRY_COUNT = 3; 


	/*AddChild should take care of the different InodeOperations
	 * InodeDirectory, InodeDirectoryWithQuota, etc.
	 * TODO: InodeSymLink
	 */
	public static void addChild(INode node){
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.sessionFactory.getSession();
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				addChildInternal(node, session);
				done = true;
				
				tx.commit();
				session.flush();
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.addChild() threw error " + e.getMessage());
				tries--;
			}
			finally {
				session.close ();
			}
		}
	}
	
	public static void addChildInternal(INode node, Session session){
		boolean entry_exists;

		List<InodeTable> results = getResultListUsingField("name", node.getFullPathName(), session);
		InodeTable inode;
		entry_exists = true;
		if (results.isEmpty())
		{
			inode = session.newInstance(InodeTable.class);
			Random id = DFSUtil.getRandom();
			long tempId = id.nextLong();
			inode.setId(tempId);
			inode.setName(node.getFullPathName());
			entry_exists = false;
		}
		else{
			inode = results.get(0);
		}
		inode.setModificationTime(node.modificationTime);
		inode.setATime(node.getAccessTime());
		inode.setLocalName(node.getLocalName());
		DataOutputBuffer permissionString = new DataOutputBuffer();
		try {
			node.getPermissionStatus().write(permissionString);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		inode.setPermission(permissionString.getData());

		// Corner case for rootDir
		if (node.getParent() != null)
			inode.setParent(node.getParent().getFullPathName());

		inode.setNSQuota(node.getNsQuota());
		inode.setDSQuota(node.getDsQuota());
		if (node instanceof INodeDirectory)
		{
			inode.setIsClosedFile(false);
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(false);    
			inode.setIsDir(true);
		}
		if (node instanceof INodeDirectoryWithQuota)
		{
			inode.setIsClosedFile(false);
			inode.setIsDir(false);	    	
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(true);    	
			inode.setNSCount(((INodeDirectoryWithQuota) node).getNsCount());
			inode.setDSCount(((INodeDirectoryWithQuota) node).getDsCount());
		}
		if (node instanceof INodeFile)
		{
			inode.setIsDir(false);
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(false);
			inode.setIsClosedFile(true);
			inode.setHeader(((INodeFile) node).getHeader());
		}
		if (node instanceof INodeFileUnderConstruction)
		{
			inode.setIsClosedFile(false);
			inode.setIsDir(false);
			inode.setIsDirWithQuota(false);
			inode.setIsUnderConstruction(true);
			try {
				inode.setClientName(((INodeFileUnderConstruction) node).getClientName());
				inode.setClientMachine(((INodeFileUnderConstruction) node).getClientMachine());
				inode.setClientNode(((INodeFileUnderConstruction) node).getClientNode().getName());
			} catch (NullPointerException e) { // Can trigger when NN is also the client
				inode.setClientName(null);
				inode.setClientMachine(null);
				inode.setClientNode(null);
			}
		}
		if (node instanceof INodeSymlink)
			inode.setSymlink(((INodeSymlink) node).getSymlink());

		if (entry_exists)
			session.updatePersistent(inode);
		else
			session.makePersistent(inode);
		
	}

	public static List<INode> getChildren(String parentDir) throws IOException {
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.sessionFactory.getSession();
		Transaction tx = session.currentTransaction();
		List<INode> ret = null;
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				ret = getChildrenInternal(parentDir, session);
				done = true;
				
				tx.commit();
				session.flush();
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.getChildren() threw error " + e.getMessage());
				tries--;
			}
			finally {
				session.close ();
			}
		}
		
		return null;
	}
	
	public static List<INode> getChildrenInternal(String parentDir, Session session) throws IOException {

		List<InodeTable> resultList = getResultListUsingField("parent", parentDir, session);

		List<INode> children = new ArrayList<INode>();

		for (InodeTable result : resultList) {
			INode inode = getINodeByNameBasic (result.getName(), session);
			children.add(inode);
		}

		if (children.size() > 0 )
			return children;
		else 
			return null;
	}

	public static INode removeChild(INode node) throws ClusterJDatastoreException {
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.sessionFactory.getSession();
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				INode ret = removeChildInternal(node, session);
				done = true;
				
				tx.commit();
				session.flush();
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.removeChild() threw error " + e.getMessage());
				tries--;
			}
			finally {
				session.close();
			}
		}
		
		return node;
	}
	
	/**Deletes a child inode from the DB. In this case, it removes it using the whole path
	 * TODO: Check if correct, it enters the function more than one time. 
	 * FIXME: If node not found, still return it?
	 * @param node
	 * @return node : deleted node
	 * @throws ClusterJDatastoreException
	 */
	public static INode removeChildInternal(INode node, Session session) throws ClusterJDatastoreException {
		List<InodeTable> results = getResultListUsingField("name", node.getFullPathName(), session);
		if( !results.isEmpty()){
			session.deletePersistent(results.get(0));
		}
		return node;
	}

	public static void updateParentAcrossSubTree (String oldFullPathOfParent, String newFullPathOfParent) {
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.sessionFactory.getSession();
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				updateParentAcrossSubTreeInternal(oldFullPathOfParent, newFullPathOfParent, session);
				done = true;
				
				tx.commit();
				session.flush();
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.updateParentAcrossSubTreeInternal() threw error " + e.getMessage());
				tries--;
			}
			finally {
				session.close();
			}
		}
	}
	
	/**This method is to be used in case an INodeDirectory is
	 * renamed (typically from the mv operation). When dealing
	 * with the DB, such an operation would isolate the sub-tree
	 * from the parent that was moved, and thus all nodes in the
	 * sub-tree need to have their fullpath and parent fields updated
	 * 
	 * @param oldFullPathOfParent
	 * @param newFullPathOfParent
	 */
	public static void updateParentAcrossSubTreeInternal (String oldFullPathOfParent, String newFullPathOfParent, Session session) {

		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<InodeTable> dobj = qb.createQueryDefinition(InodeTable.class);

		// FIXME: DB will be indexed by parent in the future.
		dobj.where(dobj.get("name").like(dobj.param("name_param")));

		Query<InodeTable> query = session.createQuery(dobj);

		// If fullPathOfParent is "/a/b", then find all
		// nodes which have a full pathname of "/a/b/"
		query.setParameter("name_param", oldFullPathOfParent + "/%");

		List<InodeTable> resultList = query.getResultList();

		for (InodeTable result: resultList) {

			String subPath = result.getName().substring(oldFullPathOfParent.length());
			String updatedFullPath = newFullPathOfParent + subPath;

			result.setName(updatedFullPath);

			if (updatedFullPath.length() == 1) // root
				result.setParent(Path.SEPARATOR);
			else
				result.setParent (updatedFullPath.substring(0, updatedFullPath.lastIndexOf(Path.SEPARATOR)));

			session.updatePersistent(result);
		}
	}
	
	public static void replaceChild (INode thisInode, INode newChild){
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.sessionFactory.getSession();
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				replaceChildInternal(thisInode, newChild, session);
				done = true;
				
				tx.commit();
				session.flush();
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.replaceChild() threw error " + e.getMessage());
				tries--;
			}
			finally {
				session.close ();
			}
		}
	}
	
	public static void replaceChildInternal (INode thisInode, INode newChild, Session session){

		List <InodeTable> results = getResultListUsingField("name",newChild.getFullPathName(), session); 
		assert ! results.isEmpty() : "Child to replace not in DB";
		InodeTable inode= results.get(0);

		inode.setModificationTime(thisInode.modificationTime);
		inode.setATime(thisInode.getAccessTime());
		inode.setLocalName(thisInode.getLocalName());
		DataOutputBuffer permissionString = new DataOutputBuffer();

		try {
			newChild.getPermissionStatus().write(permissionString);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		inode.setPermission(permissionString.getData());
		inode.setParent(newChild.getParent().getFullPathName());
		inode.setNSQuota(newChild.getNsQuota());
		inode.setDSQuota(newChild.getDsQuota());

		if (newChild instanceof INodeDirectory)
		{
			inode.setIsClosedFile(false);
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(false);    
			inode.setIsDir(true);
		}
		if (newChild instanceof INodeDirectoryWithQuota)
		{
			inode.setIsClosedFile(false);
			inode.setIsDir(false);	    	
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(true);    	
			inode.setNSCount(((INodeDirectoryWithQuota) newChild).getNsCount());
			inode.setDSCount(((INodeDirectoryWithQuota) newChild).getDsCount());
		}

		session.updatePersistent(inode);
	}
	
	public static INodeFile completeFileUnderConstruction(INode thisInode, INodeFile newChild){
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.sessionFactory.getSession();
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				INodeFile nodeToBeReturned = completeFileUnderConstructionInternal(thisInode, newChild, session);
				done = true;
				
				tx.commit();
				session.flush();
				return nodeToBeReturned;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.replaceChild() threw error " + e.getMessage());
				tries--;
				return null;
			}
			finally {
				session.close ();
			}
		}
		//Silence
		return null;
	}
	public static INodeFile completeFileUnderConstructionInternal (INode thisInode, INodeFile newChild, Session session){
		// [STATELESS]
		INodeFile nodeToBeReturned = null;

		List <InodeTable> results = getResultListUsingField("name",thisInode.getFullPathName(), session ); 
		assert ! results.isEmpty() : "Child to replace not in DB";
		InodeTable inode= results.get(0);
		
		inode.setModificationTime(thisInode.modificationTime);
		inode.setATime(thisInode.getAccessTime());
		inode.setLocalName(thisInode.getLocalName());
		DataOutputBuffer permissionString = new DataOutputBuffer();

		try {
			thisInode.getPermissionStatus().write(permissionString);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		inode.setPermission(permissionString.getData());
		inode.setParent(thisInode.getParent().getFullPathName());
		inode.setNSQuota(thisInode.getNsQuota());
		inode.setDSQuota(thisInode.getDsQuota());
		inode.setIsUnderConstruction(false);
		inode.setIsClosedFile(true);

		// TODO: Does not handle InodeDirectoryWithQuota yet
		if (thisInode instanceof INodeDirectory)
		{
			inode.setIsDir(true);
			inode.setIsDirWithQuota(false);
			inode.setIsClosedFile(false);
			inode.setIsUnderConstruction(false);
		}
		if (thisInode instanceof INodeDirectoryWithQuota)
		{
			inode.setIsClosedFile(false);
			inode.setIsDir(false);	    	
			inode.setIsUnderConstruction(false);
			inode.setIsDirWithQuota(true);     	
			inode.setNSCount(((INodeDirectoryWithQuota) thisInode).getNsCount());
			inode.setDSCount(((INodeDirectoryWithQuota) thisInode).getDsCount());
		}
		
		
		if (thisInode instanceof INodeFileUnderConstruction)
		{
			inode.setIsClosedFile(true);
			inode.setIsDir(false);
			inode.setIsDirWithQuota(false);
			inode.setIsUnderConstruction(false);
			
			try{
				//see if this works!?
				newChild = (INodeFile)convertINodeTableToINode(inode);
				BlockInfo blklist[] = BlocksHelper.getBlocksArray(newChild);
				
				newChild.setBlocksList(blklist);
				nodeToBeReturned = newChild;
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		else if (thisInode instanceof INodeFile)
		{
			inode.setIsDir(false);
			inode.setIsUnderConstruction(true);
			inode.setIsDirWithQuota(false);
			inode.setIsClosedFile(false);
			
			try{
				//see if this works!?
				newChild = (INodeFileUnderConstruction)convertINodeTableToINode(inode);
				BlockInfo blklist[] = BlocksHelper.getBlocksArray(newChild);
				
				newChild.setBlocksList(blklist);
				nodeToBeReturned = newChild;
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		
		session.updatePersistent(inode);
		
		return nodeToBeReturned; //see if this works!

	}

	public static INode getChildDirectory(String parentDir, String searchDir) throws IOException {
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.sessionFactory.getSession();
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				INode ret = getChildDirectoryInternal(parentDir, searchDir, session);
				done = true;
				
				tx.commit();
				session.flush();
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.removeChild() threw error " + e.getMessage());
				tries--;
			}
			finally {
				session.close();
			}
		}
		
		return null;
	}
	
	public static INode getChildDirectoryInternal(String parentDir, String searchDir, Session session) throws IOException {

		/*W: TODO
		 *  1. Get all children of parentDir
            2. search for searchDir in parentDir's children
		 *  3. if found then create an INode and return it
		 *  4. else return null;
		 */

		List<InodeTable> resultList = getResultListUsingField("parent", parentDir, session);

		for (InodeTable result : resultList) {
			
			String str = result.getName();
			str = str.substring(str.lastIndexOf("/")+1);
			
			if(str.equals(searchDir) ) {
				INode inode = getINodeByNameBasic (result.getName(), session);

				// Attach a parent to the Inode we just retrieved
				INodeDirectory inodeParent = (INodeDirectory) getINodeByNameBasic(result.getParent(), session);				
				inode.setParent(inodeParent);
				return inode;
			}
		}

		return null;
	}


	/**
	 * Use this method to retrieve an INode from the
	 * database by its name. This method will not
	 * attach a reference to the parent of the INode
	 * being returned.
	 * 
	 * @param name Inode name to be retrieved
	 * @return INode corresponding to 'name'
	 * @throws IOException 
	 */
	public static INode getINodeByNameBasic (String name, Session session) throws IOException{

		List<InodeTable> resultList = getResultListUsingField("name", name, session);

		assert (resultList.size() == 1) : "More than one Inode exists with name " + name;

		for (InodeTable result: resultList) {
			if (result.getName().equals(name))	{
				return convertINodeTableToINode (result);
			}
		}

		// Silence compiler
		return null;
	}

	public static INode convertINodeTableToINode (InodeTable inodetable) throws IOException
	{

		DataInputBuffer buffer = new DataInputBuffer();
		buffer.reset(inodetable.getPermission(), inodetable.getPermission().length);
		PermissionStatus ps = PermissionStatus.read(buffer);

		INode inode = null;

		if (inodetable.getIsDir()){
			inode = new INodeDirectory(inodetable.getName(), ps);
			inode.setAccessTime(inodetable.getATime());
			inode.setModificationTime(inodetable.getModificationTime());
		}
		if (inodetable.getIsDirWithQuota()) {
			inode = new INodeDirectoryWithQuota(inodetable.getName(), ps, inodetable.getNSCount(), inodetable.getDSQuota());
			inode.setAccessTime(inodetable.getATime());
			inode.setModificationTime(inodetable.getModificationTime());
		}
		if (inodetable.getIsUnderConstruction()) {
			//Get the full list of blocks for this inodeID, 
			// at this point no blocks have no INode reference
			BlockInfo [] blocks = new BlockInfo [1];
			blocks[0]= new BlockInfo(3);
			//BlockInfo[] blocksArray = BlocksHelper.getBlocksArrayWithNoINodes(inodetable.getId());
			//try {
				inode = new INodeFileUnderConstruction(inodetable.getName().getBytes(),
						getReplicationFromHeader(inodetable.getHeader()),
						inodetable.getModificationTime(),
						getPreferredBlockSize(inodetable.getHeader()),
						blocks,
						ps,
						inodetable.getClientName(),
						inodetable.getClientMachine(),
						ns.getBlockManager().getDatanodeManager().getDatanodeByHost(inodetable.getClientNode()));
			/*}
			catch (NullPointerException e) {
				inode = new INodeFileUnderConstruction(inodetable.getName().getBytes(),
						(short) 1,
						inodetable.getModificationTime(),
						64,
						blocks,
						ps,
						inodetable.getClientName(),
						inodetable.getClientMachine(),
						null);
			}*/
			//M: Might need finally here			
			//W: not sure if we need to do this for INodeFileUnderConstruction			
			((INodeFile)(inode)).setID(inodetable.getId()); //W: ugly cast - not sure if we should do this
			
			BlockInfo[] blocksArray = BlocksHelper.getBlocksArray((INodeFile)inode);
			((INodeFile)(inode)).setBlocksList(blocksArray);
		}
		if (inodetable.getIsClosedFile()) {
			/* FIXME: Double check numbers later */
			INodeFile tmp = new INodeFile(ps,
					0,
					getReplicationFromHeader(inodetable.getHeader()),
					inodetable.getModificationTime(),
					inodetable.getATime(), getPreferredBlockSize(inodetable.getHeader()));
			
			//Fixed the header after retrieving the object
			tmp.setHeader(inodetable.getHeader());
			inode = tmp;
			
			((INodeFile)(inode)).setID(inodetable.getId()); //W: ugly cast - not sure if we should do this
			BlockInfo[] blocksArray = BlocksHelper.getBlocksArray((INodeFile)inode);
			((INodeFile)(inode)).setBlocksList(blocksArray);
		}

		/* FIXME: Call getLocalName() */
		inode.setFullPathName(inodetable.getName());
		inode.setLocalName(inodetable.getLocalName());
		
		
		return inode;
	}
	
	/** Get the replication value out of the header
	 * 	useful to reconstruct InodeFileUnderConstruction from DB 
	 */
	private static short getReplicationFromHeader(long header) {
		//Number of bits for Block size
		final short blockBits = 48;
		//Format: [16 bits for replication][48 bits for PreferredBlockSize]
		final long headerMask = 0xffffL << blockBits;
	    return (short) ((header & headerMask) >> blockBits);
	  }
	/**
	 * Return preferredBlockSize for the file
	 * @return
	 */
	private static long getPreferredBlockSize(long header) {
		final short blockBits = 48;
		//Format: [16 bits for replication][48 bits for PreferredBlockSize]
		final long headerMask = 0xffffL << blockBits;
        return header & ~headerMask;
  }
	/**
	 * Returns an INode object which has iNodeID
	 */
	public static INode getINode(long iNodeID) {
		Session session = DBConnector.sessionFactory.getSession();

		InodeTable inodetable = session.find(InodeTable.class, iNodeID);

		DataInputBuffer buffer = new DataInputBuffer();
		buffer.reset(inodetable.getPermission(), inodetable.getPermission().length);
		PermissionStatus ps;
		try {
			ps = PermissionStatus.read(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			ps = null;
		}
		INode inode = null;
		if (inodetable.getIsUnderConstruction()) {

			inode = new INodeFileUnderConstruction(inodetable.getName().getBytes(),
					getReplicationFromHeader(inodetable.getHeader()),
					inodetable.getModificationTime(),
					getPreferredBlockSize(inodetable.getHeader()),
					/*blocks,*/null, //we don't need to set blocks here
					ps,
					inodetable.getClientName(),
					inodetable.getClientMachine(),
					ns.getBlockManager().getDatanodeManager().getDatanodeByHost(inodetable.getClientNode()));

			((INodeFile)(inode)).setID(inodetable.getId()); //W: ugly cast - not sure if we should do this
		}
		if (inodetable.getIsClosedFile()) {
			/* FIXME: Double check numbers later */
			inode = new INodeFile(ps,
					0,
					getReplicationFromHeader(inodetable.getHeader()),
					inodetable.getModificationTime(),
					inodetable.getATime(), 64);

			((INodeFile)(inode)).setID(inodetable.getId()); //W: ugly cast - not sure if we should do this
		}

		return inode;
		
	}
	
	/** This method is invoked by other functions in InodeTableHelper to query the DB for 
	 * a specific value in a certain field
	 * @param field: the field in the InodeTable definition
	 * @param value: the value to match 
	 * @return List InodeTable objects
	 */
	public static List<InodeTable> getResultListUsingField(String field, String value, Session s){
		QueryBuilder qb = s.getQueryBuilder();
		QueryDomainType<InodeTable> dobj = qb.createQueryDefinition(InodeTable.class);

		dobj.where(dobj.get(field).equal(dobj.param("param")));

		Query<InodeTable> query = s.createQuery(dobj);
		query.setParameter("param", value); //the WHERE clause of SQL

		return 	query.getResultList();

	}
	
	public static INode updateSrcDst(String src, String dst){
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.sessionFactory.getSession();
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				INode ret = updateSrcDstInternal(src, dst, session);
				done = true;
				
				tx.commit();
				session.flush();
				return ret;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.addChild() threw error " + e.getMessage());
				tries--;
			}
			finally {
				session.close();
			}
		}
		
		return null;
	}
	public static INode updateSrcDstInternal(String src, String dst, Session session){

		List<InodeTable> results = getResultListUsingField("name", src, session);
		assert  results.size() == 1 : "mv operation found more than one node to update";

		InodeTable newINode = results.get(0);
		//For the case of 'mv /1/2/3 /New and mv /1/2/3 /1/New3
		String parent = dst.substring(0,dst.lastIndexOf(Path.SEPARATOR));
		if(parent.isEmpty())
			parent= Path.SEPARATOR;
		newINode.setParent(parent);
		newINode.setName(dst);
		newINode.setLocalName(dst.substring(dst.lastIndexOf(Path.SEPARATOR)+1));
		newINode.setModificationTime(System.currentTimeMillis());
		session.updatePersistent(newINode);

		try {
			return convertINodeTableToINode(newINode);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;	
	}
	public static boolean updateHeader (String name ,long header) throws IOException{
		boolean done = false;
		int tries = RETRY_COUNT;
		Session session = DBConnector.sessionFactory.getSession();
		List<InodeTable> list =  getResultListUsingField("name", name, session);
		assert list != null : "InodeTable object not found";
		InodeTable inode = list.get(0); 
		//InodeTable inode = session.find(InodeTable.class, id);
		Transaction tx = session.currentTransaction();
		while (done == false && tries > 0) {
			try {
				tx.begin();
				updateHeaderInternal(inode, header, session);
				tx.commit();
				done = true;
				session.flush();
				return done;
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.addChild() threw error " + e.getMessage());
				tries--;
			}
			finally{
				session.close();
			}
		}
		
		return false;
	}
	
	private static void updateHeaderInternal (InodeTable inode, long header, Session session){
			inode.setHeader(header);
			session.updatePersistent(inode);
	}
}

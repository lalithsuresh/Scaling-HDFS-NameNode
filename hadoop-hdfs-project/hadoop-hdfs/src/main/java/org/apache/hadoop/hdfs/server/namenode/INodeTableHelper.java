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
	public static Session session= DBConnector.sessionFactory.getSession() ;
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
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				addChildInternal(node);
				done = true;
				
				tx.commit();
				session.flush();
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.addChild() threw error " + e.getMessage());
				tries--;
			}
		}
	}
	
	public static void addChildInternal(INode node){
		boolean entry_exists;

		List<InodeTable> results = getResultListUsingField("name", node.getFullPathName());
		InodeTable inode;
		entry_exists = true;
		if (results.isEmpty())
		{
			inode = session.newInstance(InodeTable.class);
			Random id = DFSUtil.getRandom();
			inode.setId(Math.abs(id.nextLong()));
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
			inode.setClientName(((INodeFileUnderConstruction) node).getClientName());
			inode.setClientMachine(((INodeFileUnderConstruction) node).getClientMachine());
			inode.setClientNode(((INodeFileUnderConstruction) node).getClientNode().getName());
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
		Transaction tx = session.currentTransaction();
		List<INode> ret = null;
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				ret = getChildrenInternal(parentDir);
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
		}
		
		return null;
	}
	
	public static List<INode> getChildrenInternal(String parentDir) throws IOException {

		List<InodeTable> resultList = getResultListUsingField("parent", parentDir);

		List<INode> children = new ArrayList<INode>();

		for (InodeTable result : resultList) {
			INode inode = getINodeByNameBasic (result.getName ());
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
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				INode ret = removeChildInternal(node);
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
	public static INode removeChildInternal(INode node) throws ClusterJDatastoreException {
		List<InodeTable> results = getResultListUsingField("name", node.getFullPathName());
		if( !results.isEmpty()){
			session.deletePersistent(results.get(0));
		}
		return node;
	}

	public static void updateParentAcrossSubTree (String oldFullPathOfParent, String newFullPathOfParent) {
		boolean done = false;
		int tries = RETRY_COUNT;
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				updateParentAcrossSubTreeInternal(oldFullPathOfParent, newFullPathOfParent);
				done = true;
				
				tx.commit();
				session.flush();
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.updateParentAcrossSubTreeInternal() threw error " + e.getMessage());
				tries--;
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
	public static void updateParentAcrossSubTreeInternal (String oldFullPathOfParent, String newFullPathOfParent) {

		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<InodeTable> dobj = qb.createQueryDefinition(InodeTable.class);

		// FIXME: DB will be indexed by parent in the future.
		dobj.where(dobj.get("name").like(dobj.param("name_param")));

		Query<InodeTable> query = session.createQuery(dobj);

		// If fullPathOfParent is "/a/b", then find all
		// nodes which have a full pathname of "/a/b/"
		query.setParameter("name_param", oldFullPathOfParent + "/%");

		List<InodeTable> resultList = query.getResultList();

		Transaction tx = session.currentTransaction();
		tx.begin();

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

		tx.commit();
		session.flush();
	}
	
	public static void replaceChild (INode thisInode, INode newChild){
		boolean done = false;
		int tries = RETRY_COUNT;
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				replaceChildInternal(thisInode, newChild);
				done = true;
				
				tx.commit();
				session.flush();
			}
			catch (ClusterJException e){
				tx.rollback();
				System.err.println("InodeTableHelper.replaceChild() threw error " + e.getMessage());
				tries--;
			}
		}
	}
	
	public static void replaceChildInternal (INode thisInode, INode newChild){

		List <InodeTable> results = getResultListUsingField("name",newChild.getFullPathName() ); 
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
			inode.setIsDir(true);
			inode.setIsDirWithQuota(true);
		}
		if (newChild instanceof INodeDirectoryWithQuota)
		{
			inode.setIsDir(false);
			inode.setIsDirWithQuota(true);      	
			inode.setNSCount(((INodeDirectoryWithQuota) newChild).getNsCount());
			inode.setDSCount(((INodeDirectoryWithQuota) newChild).getDsCount());
		}

		session.updatePersistent(inode);
	}

	public static INode getChildDirectory(String parentDir, String searchDir) throws IOException {

		/*W: TODO
		 *  1. Get all children of parentDir
            2. search for searchDir in parentDir's children
		 *  3. if found then create an INode and return it
		 *  4. else return null;
		 */

		List<InodeTable> resultList = getResultListUsingField("parent", parentDir);

		for (InodeTable result : resultList) {
			
			String str = result.getName();
			str = str.substring(str.lastIndexOf("/")+1);
			
			if(str.equals(searchDir) ) {
				INode inode = getINodeByNameBasic (result.getName ());

				// Attach a parent to the Inode we just retrieved
				INodeDirectory inodeParent = (INodeDirectory) getINodeByNameBasic(result.getParent());				
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
	public static INode getINodeByNameBasic (String name) throws IOException{

		List<InodeTable> resultList = getResultListUsingField("name", name);

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
			/* FIXME: Handle blocks */
			/* FIXME: Double check numbers later */
			BlockInfo [] blocks = new BlockInfo [1];
			blocks[0] = new BlockInfo(3);
			try {
				inode = new INodeFileUnderConstruction(inodetable.getName().getBytes(),
						(short) 1,
						inodetable.getModificationTime(),
						64,
						blocks,
						ps,
						inodetable.getClientName(),
						inodetable.getClientMachine(),
						ns.getBlockManager().getDatanodeManager().getDatanodeByHost(inodetable.getClientNode()));
			}
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
			}
		}
		if (inodetable.getIsClosedFile()) {
			/* FIXME: Double check numbers later */
			inode = new INodeFile(ps,
					0,
					(short)1,
					inodetable.getModificationTime(),
					inodetable.getATime(), 64);	
		}

		/* FIXME: Call getLocalName() */
		inode.setFullPathName(inodetable.getName());
		inode.setLocalName(inodetable.getLocalName());

		return inode;
	}

	/*Returns an INode object which has iNodeID*/
	public static INode getINode(long iNodeID) {
		
		InodeTable inTable = session.find(InodeTable.class, iNodeID);
		INode node = null;
		try {
			node =  convertINodeTableToINode(inTable);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return node;
		
	}
	
	/** This method is invoked by other functions in InodeTableHelper to query the DB for 
	 * a specific value in a certain field
	 * @param field: the field in the InodeTable definition
	 * @param value: the value to match 
	 * @return List InodeTable objects
	 */
	public static List<InodeTable> getResultListUsingField(String field, String value){
		QueryBuilder qb = session.getQueryBuilder();
		QueryDomainType<InodeTable> dobj = qb.createQueryDefinition(InodeTable.class);

		dobj.where(dobj.get(field).equal(dobj.param("param")));

		Query<InodeTable> query = session.createQuery(dobj);
		query.setParameter("param", value); //the WHERE clause of SQL

		return 	query.getResultList();

	}
	
	public static INode updateSrcDst(String src, String dst){
		boolean done = false;
		int tries = RETRY_COUNT;
		Transaction tx = session.currentTransaction();
		
		while (done == false && tries > 0) {
			try {
				tx.begin();
				
				INode ret = updateSrcDstInternal(src, dst);
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
		}
		
		return null;
	}
	public static INode updateSrcDstInternal(String src, String dst){

		List<InodeTable> results = getResultListUsingField("name", src);
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

		// FIXME: Can we avoid flushing to DB here?
		session.flush();
		try {
			return getINodeByNameBasic(dst);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;	
	}

}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;


import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.BlocksHelper;
import org.apache.hadoop.hdfs.server.namenode.DBConnector;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeTableHelper;
import org.apache.hadoop.hdfs.server.namenode.KthFsHelper;
import org.apache.hadoop.hdfs.util.LightWeightGSet;

/**
 * Internal class for block metadata.
 */
public class BlockInfo extends Block implements LightWeightGSet.LinkedElement {
	private INodeFile inode;

	/** For implementing {@link LightWeightGSet.LinkedElement} interface */
	private LightWeightGSet.LinkedElement nextLinkedElement;

	private int blockIndex = -1; //added for KTHFS
	private long timestamp = 1;

	/**
	 * Construct an entry for blocksmap
	 * @param replication the block's replication factor
	 */
	public BlockInfo(int replication) {
		this.inode = null;
	}

	public BlockInfo(Block blk, int replication) {
		super(blk);
		this.inode = null;

		this.getBlockId(); 
		this.getBlockName();
		this.getNumBytes(); 
		this.getGenerationStamp();
	}

	/**
	 * Copy construction.
	 * This is used to convert BlockInfoUnderConstruction
	 * @param from BlockInfo to copy from.
	 */
	protected BlockInfo(BlockInfo from) {
		this(from, from.inode.getReplication());
		this.inode = from.inode;
	}

	public INodeFile getINode() {
		return (INodeFile) BlocksHelper.getInodeFromBlockId(this.getBlockId());
	}

	public void setINode(INodeFile inode) {
		this.inode = inode;
		if(inode!=null)
			BlocksHelper.updateINodeID(inode.getID(), this);
	}
	
	public void setINodeWithoutTransaction(INodeFile inode) {	
		this.inode = inode;	
	}

	public DatanodeDescriptor getDatanode(int index) {
		assert index >= 0;
		DatanodeDescriptor node = BlocksHelper.getDatanode(this.getBlockId(), index);
		assert node == null || 
				DatanodeDescriptor.class.getName().equals(node.getClass().getName()) : 
					"DatanodeDescriptor is expected at " + index*3;
				return node;
	}

	void setDatanode(int index, DatanodeDescriptor node) {
		assert index >= 0;
		if(node != null)
			BlocksHelper.setDatanode(this.getBlockId(), index, node.name);
	}

	/** Checks the size of the triplets and how many more, we can add (in theory) */
	int getCapacity() {
		int length = BlocksHelper.getTripletsForBlock(this).length;
		return length;
	}

	/**
	 * Ensure that there is enough  space to include num more triplets.
	 * @return first free triplet index.
	 */
	private int ensureCapacity(int num) {
		int last = numNodes();
		return last;
	}

	/**
	 * Count the number of data-nodes the block belongs to.
	 */
	int numNodes() {
		return BlocksHelper.numDatanodesForBlock(this.getBlockId());
	}


	  /**
	* Add data-node this block belongs to.
	*/
	  public boolean addNode(DatanodeDescriptor node) {
	    if(findDatanode(node) >= 0) // the node is already there
	      return false;

	    // find the last available datanode index
	    int lastNode = ensureCapacity(1);
	    setDatanode(lastNode, node);
	    
	    return true;
	  }

	  /**
	* Remove data-node from the block.
	*/
	  public boolean removeNode(DatanodeDescriptor node) {
	    int dnIndex = findDatanode(node);
	    if(dnIndex < 0) // the node is not found
	      return false;

	    BlocksHelper.removeTriplets(this,dnIndex);
	    return true;
	  }


	/**
	 * Find specified DatanodeDescriptor.
	 * @param dn
	 * @return index or -1 if not found.
	 */
	int findDatanode(DatanodeDescriptor dn) {
		return BlocksHelper.findDatanodeForBlock(dn, this.getBlockId());
	}

	/**
	 * BlockInfo represents a block that is not being constructed.
	 * In order to start modifying the block, the BlockInfo should be converted
	 * to {@link BlockInfoUnderConstruction}.
	 * @return {@link BlockUCState#COMPLETE}
	 */
	public BlockUCState getBlockUCState() {
		return BlockUCState.COMPLETE;
	}

	/**
	 * Is this block complete?
	 * 
	 * @return true if the state of the block is {@link BlockUCState#COMPLETE}
	 */
	public boolean isComplete() {
		return getBlockUCState().equals(BlockUCState.COMPLETE);
	}

	/**
	 * Convert a complete block to an under construction block.
	 * 
	 * @return BlockInfoUnderConstruction -  an under construction block.
	 */
	public BlockInfoUnderConstruction convertToBlockUnderConstruction(
			BlockUCState s, DatanodeDescriptor[] targets) {
		if(isComplete()) {
			return new BlockInfoUnderConstruction(
					this, getINode().getReplication(), s, targets);
		}
		// the block is already under construction
		BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction)this;
		ucBlock.setBlockUCState(s);
		ucBlock.setExpectedLocations(targets);
		return ucBlock;
	}

	@Override
	public int hashCode() {
		// Super implementation is sufficient
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// Sufficient to rely on super's implementation
		return (this == obj) || super.equals(obj);
	}

	@Override
	public LightWeightGSet.LinkedElement getNext() {
		return nextLinkedElement;
	}

	@Override
	public void setNext(LightWeightGSet.LinkedElement next) {
		this.nextLinkedElement = next;
	}

	/*added for KTHFS*/
	public int getBlockIndex() {
		return this.blockIndex;
	}
	/*added for KTHFS*/
	public void setBlockIndex(int bindex) {
		this.blockIndex = bindex;
	}
 
	/*added for KTHFS*/
	public long getTimestamp() {
		return this.timestamp;
	}
	/*added for KTHFS*/
	public void setTimestamp(long ts) {
		this.timestamp = ts;
	}
}
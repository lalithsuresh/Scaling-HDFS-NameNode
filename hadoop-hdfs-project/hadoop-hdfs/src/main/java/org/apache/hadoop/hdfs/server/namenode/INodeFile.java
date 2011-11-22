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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;

/** I-node for closed file. */
public class INodeFile extends INode {
	static final FsPermission UMASK = FsPermission.createImmutable((short)0111);

	//Number of bits for Block size
	static final short BLOCKBITS = 48;

	//Header mask 64-bit representation
	//Format: [16 bits for replication][48 bits for PreferredBlockSize]
	static final long HEADERMASK = 0xffffL << BLOCKBITS;

	protected long header;

	protected BlockInfo blocks[] = null;

	/*added for KTHFS*/
	protected long id = -1;

	INodeFile(PermissionStatus permissions,
			int nrBlocks, short replication, long modificationTime,
			long atime, long preferredBlockSize) {
		this(permissions, new BlockInfo[nrBlocks], replication,
				modificationTime, atime, preferredBlockSize);
	}
	

  protected INodeFile(PermissionStatus permissions, BlockInfo[] blklist,
                      short replication, long modificationTime,
                      long atime, long preferredBlockSize) {
    super(permissions, modificationTime, atime);
    this.setReplication(replication);
    this.setPreferredBlockSize(preferredBlockSize);
    blocks = blklist;
  }

	protected INodeFile() {
		blocks = null;
		header = 0;
	}

  public boolean isDirectory() {
    return false;
  }

  /**
   * Get block replication for the file 
   * @return block replication value
   */
  public short getReplication() {
    return (short) ((header & HEADERMASK) >> BLOCKBITS);
  }
  
  public void setReplication(short replication) {
	  if(replication <= 0)
		  throw new IllegalArgumentException("Unexpected value for the replication");
	  header = ((long)replication << BLOCKBITS) | (header & ~HEADERMASK);
  }

  public void setReplicationDB(short replication) {
	  if(replication <= 0)
		  throw new IllegalArgumentException("Unexpected value for the replication");
	  header = ((long)replication << BLOCKBITS) | (header & ~HEADERMASK);
	  //[KTHFS] Call for update in replication
	  try {
		  INodeTableHelper.updateHeader(this.getFullPathName(), header);
	  } catch (IOException e) {
		  // TODO Auto-generated catch block
		  e.printStackTrace();
	  }
  }


  
  /*FIXME: This should be called when the inodes are created
	 * 
	 * added for KTHFS
	 * 
	 * */
	public void setID(long id) {
		this.id = id;
	}

	/*
	 * added for KTHFS
	 * 
	 * */
	public long getID() {
		return this.id;
	}

  /**
   * [STATELESS] get header
   * If you need preferred block size, or other properties,
   * select/set this value first. 
   */
  public long getHeader () {
	  return header;
  }
  /**
   * [STATELESS] set header
   */
  public void setHeader (long val) {
	  header = val;
  }
  /**
   * Get preferred block size for the file
   * @return preferred block size in bytes
   */
  public long getPreferredBlockSize() {
        return header & ~HEADERMASK;
  }

  public void setPreferredBlockSize(long preferredBlkSize)
  {
    if((preferredBlkSize < 0) || (preferredBlkSize > ~HEADERMASK ))
       throw new IllegalArgumentException("Unexpected value for the block size");
    header = (header & HEADERMASK) | (preferredBlkSize & ~HEADERMASK);
  }
	/**
	 * Get file blocks 
	 * @return file blocks
	 */
	public BlockInfo[] getBlocks() {
		return this.blocks;
	}

	/**
	 * Set the {@link FsPermission} of this {@link INodeFile}.
	 * Since this is a file,
	 * the {@link FsAction#EXECUTE} action, if any, is ignored.
	 */
	protected void setPermission(FsPermission permission) {
		super.setPermission(permission.applyUMask(UMASK));
	}

  /**
   * append array of blocks to this.blocks
   */
  //FIXME: KTHFSBLOCKS use BlocksHelper.appendBlocks instead
 /* void appendBlocks(INodeFile [] inodes, int totalAddedBlocks) {
    int size = this.blocks.length;

    BlockInfo[] newlist = new BlockInfo[size + totalAddedBlocks];
    System.arraycopy(this.blocks, 0, newlist, 0, size);

    for(INodeFile in: inodes) {
      System.arraycopy(in.blocks, 0, newlist, size, in.blocks.length);
      size += in.blocks.length;
    }

    for(BlockInfo bi: newlist) {
      bi.setINode(this);
    }
    this.blocks = newlist;
  }*/

	void appendBlocks(INodeFile [] inodes, int totalAddedBlocks) {

		KthFsHelper.printKTH("append blocks begin");
		BlocksHelper.appendBlocks(this, inodes, totalAddedBlocks);
		KthFsHelper.printKTH("append blocks Done");

	}


	/**
	 * add a block to the block list
	 */
	//FIXME: KTHFSBLOCKS use BlocksHelper.addBlock instead
/*	void addBlock(BlockInfo newblock) {
		if (this.blocks == null) {
			this.blocks = new BlockInfo[1];
			this.blocks[0] = newblock;
		} else {
			int size = this.blocks.length;
			BlockInfo[] newlist = new BlockInfo[size + 1];
			System.arraycopy(this.blocks, 0, newlist, 0, size);
			newlist[size] = newblock;
			this.blocks = newlist;
		}
	}*/
	
	void addBlock(BlockInfo newblock) {
		if (this.blocks == null) {
			this.blocks = new BlockInfo[1];
			this.blocks[0] = newblock;
		} else {
			BlocksHelper.addBlock(newblock);
			this.blocks = BlocksHelper.getBlocksArray(this);
		}

	}

	/**
	 * Set file block
	 *//*
  //FIXME: KTHFSBLOCKS
  public void setBlock_old(int idx, BlockInfo blk) {
    this.blocks[idx] = blk;
  }*/

	/**
	 * Set file block - KTHFS
	 */

	public void setBlock(int idx, BlockInfo blk) {
		BlocksHelper.updateIndex(idx, blk);
	}

	public void setBlocksList(BlockInfo[] blklist) {
		if(blklist != null) {
		this.blocks = blklist;
		}
	}

	int collectSubtreeBlocksAndClear(List<Block> v) {
		parent = null;
		if(blocks != null && v != null) {
			for (BlockInfo blk : blocks) {
				v.add(blk);
				blk.setINode(null);
			}
		}
		blocks = null;

		// [Stateless] Remove me from DB
		INodeTableHelper.removeChild(this);
		return 1;
	}

	/** {@inheritDoc} */
	long[] computeContentSummary(long[] summary) {
		summary[0] += computeFileSize(true);
		summary[1]++;
		summary[3] += diskspaceConsumed();
		return summary;
	}

	/** Compute file size.
	 * May or may not include BlockInfoUnderConstruction.
	 */
	//FIXME: KTHFSBLOCKS
	long computeFileSize(boolean includesBlockInfoUnderConstruction) {
		if (blocks == null || blocks.length == 0) {
			return 0;
		}
		final int last = blocks.length - 1;
		//check if the last block is BlockInfoUnderConstruction
		long bytes = blocks[last] instanceof BlockInfoUnderConstruction
				&& !includesBlockInfoUnderConstruction?
						0: blocks[last].getNumBytes();
		for(int i = 0; i < last; i++) {
			bytes += blocks[i].getNumBytes();
		}
		return bytes;
	}


	@Override
	DirCounts spaceConsumedInTree(DirCounts counts) {
		counts.nsCount += 1;
		counts.dsCount += diskspaceConsumed();
		return counts;
	}

	long diskspaceConsumed() {
		return diskspaceConsumed(blocks);
	}

	long diskspaceConsumed(Block[] blkArr) {
		long size = 0;
		if(blkArr == null) 
			return 0;

		for (Block blk : blkArr) {
			if (blk != null) {
				size += blk.getNumBytes();
			}
		}
		/* If the last block is being written to, use prefferedBlockSize
		 * rather than the actual block size.
		 */
		if (blkArr.length > 0 && blkArr[blkArr.length-1] != null && 
				isUnderConstruction()) {
			size += getPreferredBlockSize() - blkArr[blkArr.length-1].getNumBytes();
		}
		return size * getReplication();
	}

	/**
	 * Return the penultimate allocated block for this file.
	 */
	//FIXME: KTHFSBLOCKS
	BlockInfo getPenultimateBlock() {
		if (blocks == null || blocks.length <= 1) {
			return null;
		}
		return blocks[blocks.length - 2];
	}

	/**
	 * Get the last block of the file.
	 * Make sure it has the right type.
	 */
	//FIXME: KTHFSBLOCKS
	public <T extends BlockInfo> T getLastBlock() throws IOException {
		if (blocks == null || blocks.length == 0)
			return null;
		T returnBlock = null;
		try {
			@SuppressWarnings("unchecked")  // ClassCastException is caught below
			T tBlock = (T)blocks[blocks.length - 1];
			returnBlock = tBlock;
		} catch(ClassCastException cce) {
			throw new IOException("Unexpected last block type: " 
					+ blocks[blocks.length - 1].getClass().getSimpleName());
		}
		return returnBlock;
	}

	/** @return the number of blocks */ 
	//FIXME: KTHFSBLOCKS
	public int numBlocks() {
		return blocks == null ? 0 : blocks.length;
	}





}

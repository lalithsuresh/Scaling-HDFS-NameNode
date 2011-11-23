/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;


/**
 *
 * @author wmalik
 */
@PersistenceCapable(table="BlockInfo")
public interface BlockInfoTable {

    
    @PrimaryKey
    @Column(name = "iNodeID")
    long getINodeID();
    void setINodeID(long iNodeID);

    @Column(name = "blockIndex")
    int getBlockIndex();     
    void setBlockIndex(int idx);

    
    @Column(name = "blockId")
    long getBlockId();     
    void setBlockId(long bid);
    
    @Column(name = "numBytes")
    long getNumBytes();
    void setNumBytes (long numbytes);

     @Column(name = "generationStamp")
    long getGenerationStamp();
    void setGenerationStamp(long genstamp);
    
     @Column(name = "replication")
    int getReplication();
    void setReplication(int replication);

      @Column(name = "BlockUCState")
    int getBlockUCState();
    void setBlockUCState(int blockUCState);
    
}

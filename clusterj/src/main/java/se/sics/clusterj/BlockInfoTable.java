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

    // Inode
    @PrimaryKey
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
    
     @Column(name = "iNodePath")
    String getINodePath();
    void setINodePath(String path);
    
}

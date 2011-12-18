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
@PersistenceCapable(table="triplets")
public interface TripletsTable {

    @PrimaryKey
    @Column(name = "blockId")
    long getBlockId();     
    void setBlockId(long bid);
    
    @PrimaryKey
    @Column(name = "index")
    int getIndex();
    void setIndex(int index);
    
    @Column(name = "datanodeName")
    String getDatanodeName();
    void setDatanodeName(String name);
}

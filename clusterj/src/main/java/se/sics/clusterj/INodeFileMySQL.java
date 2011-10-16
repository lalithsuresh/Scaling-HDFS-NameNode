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
 *  
CREATE TABLE `INodeFile` (
  `id` int(11) NOT NULL,
  `name` varchar(45) DEFAULT NULL,
  `clientname` varchar(45) DEFAULT NULL,
  `clientmachine` varchar(45) DEFAULT NULL,
  `modificationtime` long DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


 *
 * @author Wasif Malik
 */
@PersistenceCapable(table="INodeFile")
public interface INodeFileMySQL {

//    @PrimaryKey
//    int getId();
//    void setId(int i);

    @PrimaryKey
    @Column(name = "name")
    String getName();
    void setName(String str);

    @Column(name = "clientname")
    String getClientName();
    void setClientName(String str);
    
    @Column(name = "clientmachine")
    String getClientMachine();
    void setClientMachine(String str);
    
    @Column(name = "modificationtime")
    int getModificationTime();
    void setModificationTime(int time);
    
}
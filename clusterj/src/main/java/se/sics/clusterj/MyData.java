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
CREATE TABLE `my_data` (
`id` bigint(20) NOT NULL AUTO_INCREMENT,
`data` varbinary(255) DEFAULT NULL,
`last_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`id`) ) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

 *
 * @author Jim Dowling<jdowling@sics.se>
 */
@PersistenceCapable(table="my_data")
public interface MyData {

    @PrimaryKey
    long getId();

    void setId(long i);

    @Column(name = "data")
    byte[] getData();

    void setData(byte[] b);

    @Column(name = "last_updated")
    long getLastUpdated();

    void setLastUpdated(long ts);
}
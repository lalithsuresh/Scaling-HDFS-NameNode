/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.hdfs.server.namenode.kthfs.clusterj;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.annotation.Lob;

/**
 * 
 *  
name             varchar(45) PK
isdir            tinyint(1)
modificationTime bigint(20)
atime            bigint(20)
permissions      varchar(45)
nsQuota          bigint(20)
dsQuota          bigint(20)
isUnderConstruction tinyint(1)
clientName       varchar(45)
clientMachine    varchar(45)
clientNode       varchar(45)
isClosedFile     tinyint(1)
header           bigint(20) // Stores replication factor, preferredBlocks
Parent           varchar(45)

 *
 * @author Lalith Suresh<suresh.lalith@gmail.com>
 */
@PersistenceCapable(table="InodeTable")
public interface InodeTable {

    // Inode
    @PrimaryKey
    @Column(name = "name")
    String getName ();     // id of the file system
    void setName (String name);

    // marker for InodeDirectory
    @Column(name = "isDir")
    boolean getIsDir ();
    void setIsDir (boolean isDir);

    // marker for InodeDirectoryWithQuota
    @Column(name = "isDirWithQuota")
    boolean getIsDirWithQuota ();
    void setIsDirWithQuota (boolean isDirWithQuota);

    // Inode
    @Column(name = "modificationTime")
    long getModificationTime ();
    void setModificationTime (long modificationTime);

    // Inode
    @Column(name = "aTime")
    long getATime ();
    void setATime (long modificationTime);

    // Inode
    
    @Lob
    @Column(name = "permission")
    byte[] getPermission (); 
    void setPermission (byte[] permission);

    // InodeDirectoryWithQuota
    @Column(name = "nscount")
    long getNSCount ();
    void setNSCount (long nsCount);

    // InodeDirectoryWithQuota
    @Column(name = "dscount")
    long getDSCount ();
    void setDSCount (long dsCount);

    // InodeDirectoryWithQuota
    @Column(name = "nsquota")
    long getNSQuota ();
    void setNSQuota (long nsQuota);

    // InodeDirectoryWithQuota
    @Column(name = "dsquota")
    long getDSQuota ();
    void setDSQuota (long dsQuota);

    //  marker for InodeFileUnderConstruction
    @Column(name = "isUnderConstruction")
    boolean getIsUnderConstruction ();
    void setIsUnderConstruction (boolean isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = "clientName")
    String getClientName ();
    void setClientName (String isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = "clientMachine")
    String getClientMachine ();
    void setClientMachine (String clientMachine);

    // InodeFileUnderConstruction -- TODO
    @Column(name = "clientNode")
    String getClientNode ();
    void setClientNode (String clientNode);

    //  marker for InodeFile
    @Column(name = "isClosedFile")
    boolean getIsClosedFile ();
    void setIsClosedFile (boolean isClosedFile);

    // InodeFile
    @Column(name = "header")
    long getHeader ();
    void setHeader (long header);

    // Inode
    // point to another entry in the InodeTable
    @Column(name = "parent")
    String getParent ();
    void setParent (String parent);
    
     // InodeFile
    @Column(name = "localName")
    String getLocalName ();
    void setLocalName (String localName);

   
}
package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.coordinator.StorageNodeHashSpace;

import java.util.Hashtable;

public class StMetaData {

    private String nodeIp;
    private int nodeId;
    private Hashtable<Integer, StorageNodeHashSpace> routingTable;

    public StMetaData(String nodeIp, int nodeId, Hashtable<Integer, StorageNodeHashSpace> routingTable) {
        this.nodeIp = nodeIp;
        this.nodeId = nodeId;
        this.routingTable = routingTable;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public void setNodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public Hashtable<Integer, StorageNodeHashSpace> getRoutingTable() {
        return routingTable;
    }

    public void setRoutingTable(Hashtable<Integer, StorageNodeHashSpace> routingTable) {
        this.routingTable = routingTable;
    }
}

package edu.usfca.cs.dfs.coordinator;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.storageNode.StorageNodeInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class CoorMetaData {

    private Hashtable<Integer, StorageNodeHashSpace> routingTable;
    private Hashtable<Integer, StorageNodeInfo> metaDataTable;
    private int nodeId;
    private double rtVersion;
    private String coorIp;

    public CoorMetaData(Hashtable<Integer, StorageNodeHashSpace> routingTable, Hashtable<Integer, StorageNodeInfo> metaDataTable, int nodeId, double rtVersion, String coorIp) {
        this.routingTable = routingTable;
        this.metaDataTable = metaDataTable;
        this.nodeId = nodeId;
        this.rtVersion = rtVersion;
        this.coorIp = coorIp;
    }

    public synchronized Hashtable<Integer, StorageNodeHashSpace> getRoutingTable() {
        return routingTable;
    }

    public synchronized void setRoutingTable(Hashtable<Integer, StorageNodeHashSpace> routingTable) {
        this.routingTable = routingTable;
    }

    public synchronized int getRoutingTableSize() {
        return this.routingTable.size();
    }

    public synchronized void addNodeToRoutingTable(int nodeId, StorageNodeHashSpace snhs) {
        this.routingTable.put(nodeId, snhs);
    }

    public synchronized Map<Integer, StorageMessages.StorageNodeHashSpace> constructSnHashSpaceProto() {
        Map<Integer, StorageMessages.StorageNodeHashSpace> mp = new HashMap<>();
        for(Map.Entry<Integer, StorageNodeHashSpace> e : routingTable.entrySet()){
            StorageNodeHashSpace s = e.getValue();
            StorageMessages.StorageNodeHashSpace sns = StorageMessages.StorageNodeHashSpace.newBuilder()
                    .setNodeIp(s.getNodeIp())
                    .setSpaceBegin(s.getSpaceRange()[0])
                    .setSpaceEnd(s.getSpaceRange()[1]).build();
            mp.put(e.getKey(), sns);
        }

        return mp;
    }

    public synchronized Hashtable<Integer, StorageNodeInfo> getMetaDataTable() {
        return metaDataTable;
    }

    public synchronized void setMetaDataTable(Hashtable<Integer, StorageNodeInfo> metaDataTable) {
        this.metaDataTable = metaDataTable;
    }

    public synchronized void addNodeToMetaDataTable(int nodeId, StorageNodeInfo sn) {
        this.metaDataTable.put(nodeId, sn);
    }

    public synchronized ArrayList<StorageMessages.ActiveNode> getActiveNodesList() {
        ArrayList<StorageMessages.ActiveNode> sa = new ArrayList<>();
        for(StorageNodeInfo sn: metaDataTable.values()) {
            if(sn.isActive()) {
                StorageMessages.ActiveNode activeNodeMsg
                        = StorageMessages.ActiveNode.newBuilder()
                        .setNodeId(sn.getNodeId())
                        .setNodeIp(sn.getNodeIp())
                        .build();
                sa.add(activeNodeMsg);
            }
        }
        return sa;
    }

    public synchronized ArrayList<StorageMessages.DiskSpace> getTotalDiskSpace() {
        ArrayList<StorageMessages.DiskSpace> sd = new ArrayList<>();
        for(StorageNodeInfo sn: metaDataTable.values()) {
            StorageMessages.DiskSpace diskSpaceMsg
                    = StorageMessages.DiskSpace.newBuilder()
                    .setNodeId(sn.getNodeId())
                    .setNodeIp(sn.getNodeIp())
                    .setSpace(sn.getSpaceCap())
                    .build();
            sd.add(diskSpaceMsg);
        }
        return sd;
    }

    public synchronized ArrayList<StorageMessages.NodeRequestsNum> getNodeRuquestsNum() {
        ArrayList<StorageMessages.NodeRequestsNum> sr = new ArrayList<>();
        for(StorageNodeInfo sn: metaDataTable.values()) {
            StorageMessages.NodeRequestsNum nodeRequestsNumMsg
                    = StorageMessages.NodeRequestsNum.newBuilder()
                    .setNodeId(sn.getNodeId())
                    .setNodeIp(sn.getNodeIp())
                    .setRequestsNum(sn.getRequestsNum())
                    .build();
            sr.add(nodeRequestsNumMsg);
        }
        return sr;
    }

    public synchronized int getNodeId() {
        return nodeId;
    }

    public synchronized void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public synchronized  double getRtVersion() {
        return rtVersion;
    }

    public synchronized void setRtVersion(double rtVersion) {
        this.rtVersion = rtVersion;
    }

    public String getCoorIp() {
        return coorIp;
    }

}

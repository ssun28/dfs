package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.coordinator.StorageNodeHashSpace;
import edu.usfca.cs.dfs.coordinator.StorageNodeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;

public class StMetaData {

    private Hashtable<Integer, StorageNodeHashSpace> routingTable;
    private Hashtable<String, ArrayList<Integer>> allFilesPosTable;
    private StorageNodeInfo storageNodeInfo;
    private ArrayList<Chunk> chunksList;

    public StMetaData(Hashtable<Integer, StorageNodeHashSpace> routingTable, Hashtable<String,
            ArrayList<Integer>> allFilesPosTable, StorageNodeInfo storageNodeInfo, ArrayList<Chunk> chunksList) {
        this.routingTable = routingTable;
        this.allFilesPosTable = allFilesPosTable;
        this.storageNodeInfo = storageNodeInfo;
        this.chunksList = chunksList;
    }

    public synchronized Hashtable<Integer, StorageNodeHashSpace> getRoutingTable() {
        return routingTable;
    }

    public void setRoutingTable(Hashtable<Integer, StorageNodeHashSpace> routingTable) {
        this.routingTable = routingTable;
    }

    public synchronized void updateRoutingTable(Map<Integer, StorageMessages.StorageNodeHashSpace> mp) {
        for(Map.Entry<Integer, StorageMessages.StorageNodeHashSpace> e : mp.entrySet()) {
            int[] spaceRange = new int[]{e.getValue().getSpaceBegin(), e.getValue().getSpaceEnd()};
            StorageNodeHashSpace sn = new StorageNodeHashSpace(e.getValue().getNodeIp(), spaceRange);
            routingTable.put(e.getKey(), sn);
        }
    }

    public synchronized ArrayList<String> getNodeIpList() {
        ArrayList<String> nodeIpList = new ArrayList<>();
        for(Map.Entry<Integer, StorageNodeHashSpace> e : routingTable.entrySet()) {
            nodeIpList.add(e.getValue().getNodeIp());
        }
        return nodeIpList;
    }

    public synchronized ArrayList<Integer> getNodeIdList() {
        ArrayList<Integer> nodeIdList = new ArrayList<>();
        for(Integer i : routingTable.keySet()) {
            nodeIdList.add(i);
        }
        return nodeIdList;
    }

    public synchronized int[] get2ChunkCopyNodeId (int firstChunkNodeId) {
        int[] nodeIdArray = new int[2];
        ArrayList<Integer> nodeIdList = new ArrayList<>();
        for(Integer i : routingTable.keySet()) {
            nodeIdList.add(i);
        }
        Collections.sort(nodeIdList);

        for(int i = 0; i < nodeIdList.size(); i++) {
            if(firstChunkNodeId == nodeIdList.get(i)) {
                nodeIdArray[0] = (i + 1) % nodeIdList.size();
                nodeIdArray[1] = (i + 2) % nodeIdList.size();
                break;
            }
        }

        return nodeIdArray;
    }

    public synchronized String getPositionNodeIp(int nodeId) {
        return routingTable.get(nodeId).getNodeIp();
    }

    public Hashtable<String, ArrayList<Integer>> getAllFilesPosTable() {
        return allFilesPosTable;
    }

    public void setAllFilesPosTable(Hashtable<String, ArrayList<Integer>> allFilesPosTable) {
        this.allFilesPosTable = allFilesPosTable;
    }

    public synchronized void updateAllFilesPosTable(String inputFileChunk, int nodeId) {
        if(allFilesPosTable.contains(inputFileChunk)) {
            allFilesPosTable.get(inputFileChunk).add(nodeId);
        }else {
            ArrayList<Integer> nodeIdList = new ArrayList<>();
            nodeIdList.add(nodeId);
            allFilesPosTable.put(inputFileChunk, nodeIdList);
        }
    }

    ///
    public synchronized void increaseReqNum() {
        storageNodeInfo.setRequestsNum(storageNodeInfo.getRequestsNum() + 1);
    }

    public StorageNodeInfo getStorageNodeInfo() {
        return storageNodeInfo;
    }

    public void setStorageNodeInfo(StorageNodeInfo storageNodeInfo) {
        this.storageNodeInfo = storageNodeInfo;
    }

    public ArrayList<Chunk> getChunksList() {
        return chunksList;
    }

    public void setChunksList(ArrayList<Chunk> chunksList) {
        this.chunksList = chunksList;
    }

    public synchronized void addChunkToChunksList(Chunk chunk) {
        chunksList.add(chunk);
    }
}

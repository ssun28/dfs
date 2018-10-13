package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.coordinator.StorageNodeHashSpace;

import java.util.*;

/**
 * MetaData for the storage nodes: multi threads can share the data
 */
public class StMetaData {

    /**
     * routingTable: key: nodeId
     * allFilesPosTable: key: chunk name
     * numOfChunksTable: key:fileName, value:numChunks
     */
    private Hashtable<Integer, StorageNodeHashSpace> routingTable;
    private Hashtable<String, ArrayList<Integer>> allFilesPosTable;
    private Hashtable<String, Integer> numOfChunksTable;
    private StorageNodeInfo storageNodeInfo;
//    private ArrayList<Chunk> chunksList;
    private Hashtable<String, Chunk> chunksMap;

    public StMetaData(Hashtable<Integer, StorageNodeHashSpace> routingTable,
                      Hashtable<String, ArrayList<Integer>> allFilesPosTable,
                      Hashtable<String, Integer> numOfChunksTable,
                      StorageNodeInfo storageNodeInfo,
                      Hashtable<String, Chunk> chunksMap) {
        this.routingTable = routingTable;
        this.allFilesPosTable = allFilesPosTable;
        this.numOfChunksTable = numOfChunksTable;
        this.storageNodeInfo = storageNodeInfo;
        this.chunksMap = chunksMap;
    }

    public synchronized Hashtable<Integer, StorageNodeHashSpace> getRoutingTable() {
        return routingTable;
    }

    public void setRoutingTable(Hashtable<Integer, StorageNodeHashSpace> routingTable) {
        this.routingTable = routingTable;
    }

    /**
     * Update the routing table store in the storage node
     * @param mp
     */
    public synchronized void updateRoutingTable(Map<Integer, StorageMessages.StorageNodeHashSpace> mp) {
        for(Map.Entry<Integer, StorageMessages.StorageNodeHashSpace> e : mp.entrySet()) {
            int[] spaceRange = new int[]{e.getValue().getSpaceBegin(), e.getValue().getSpaceEnd()};
            StorageNodeHashSpace sn = new StorageNodeHashSpace(e.getValue().getNodeIp(), spaceRange);
            routingTable.put(e.getKey(), sn);
        }
    }

    /**
     * Get node Ip list from routing table
     * @return
     */
    public synchronized ArrayList<String> getNodeIpList() {
        ArrayList<String> nodeIpList = new ArrayList<>();
        for(Map.Entry<Integer, StorageNodeHashSpace> e : routingTable.entrySet()) {
            nodeIpList.add(e.getValue().getNodeIp());
        }
        return nodeIpList;
    }

    /**
     * Get node Id list from routing table
     * @return
     */
    public synchronized ArrayList<Integer> getNodeIdList() {
        ArrayList<Integer> nodeIdList = new ArrayList<>();
        for(Integer i : routingTable.keySet()) {
            nodeIdList.add(i);
        }
        return nodeIdList;
    }

    /**
     * Get 2 storage node id to store the 2 chunk copy
     * @param firstChunkNodeId
     * @return
     */
    public synchronized int[] get2ChunkCopyNodeId (int firstChunkNodeId) {
        int[] nodeIdArray = new int[2];
        ArrayList<Integer> nodeIdList = new ArrayList<>();
        for(Integer i : routingTable.keySet()) {
            nodeIdList.add(i);
        }
        Collections.sort(nodeIdList);

        for(int i = 0; i < nodeIdList.size(); i++) {
            if(firstChunkNodeId == nodeIdList.get(i)) {
                nodeIdArray[0] = nodeIdList.get((i + 1) % nodeIdList.size());
                nodeIdArray[1] = nodeIdList.get((i + 2) % nodeIdList.size());
                break;
            }
        }

        return nodeIdArray;
    }

    /**
     * Update the numberofChunks Table
     * @param fileName
     * @param fileType
     * @param numChunks
     */
    public synchronized void updateNumOfChunks(String fileName, String fileType, int numChunks) {
        numOfChunksTable.put(fileName+fileType, numChunks);
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

    /**
     * Update the all files position table on the storage node
     * @param inputFileChunk
     * @param nodeId
     */
    public synchronized void updateAllFilesPosTable(String inputFileChunk, int nodeId) {
        ArrayList<Integer> nodeIdList = allFilesPosTable.get(inputFileChunk);

        if(nodeIdList == null) {
            nodeIdList = new ArrayList<>();
            nodeIdList.add(nodeId);
        }else {
            nodeIdList.add(nodeId);
        }
        allFilesPosTable.put(inputFileChunk, nodeIdList);
    }

    /**
     * When a client give a file name, give back a table contains all the file chunks
     * key: chunkname
     * value : nodeId list
     * @param chunkName
     * @return
     */
    public synchronized StorageMessages.NodeIdList getRetrieveChunksPos(String chunkName) {
        StorageMessages.NodeIdList nodeIdListMsg = null;
        if(allFilesPosTable.containsKey(chunkName)){
            nodeIdListMsg = StorageMessages.NodeIdList.newBuilder()
                    .addAllNodeId(getFilePos(chunkName))
                    .build();
        }
        return nodeIdListMsg;
    }

    public synchronized ArrayList<Integer> getFilePos(String chunkName){
        return allFilesPosTable.get(chunkName);
    }

    /**
     * Get a nodeIp table has all nodeIp in the routingTable
     * @return
     */
    public synchronized Hashtable<Integer, String> getNodeIpTable() {
        Hashtable<Integer, String> nodeIpTable = new Hashtable<>();
        for(Map.Entry<Integer, StorageNodeHashSpace> node: routingTable.entrySet()) {
            nodeIpTable.put(node.getKey(), node.getValue().getNodeIp());
        }
        return nodeIpTable;
    }

    /**
     * Increase the number of requests by one
     */
    public synchronized void increaseReqNum() {
        storageNodeInfo.setRequestsNum(storageNodeInfo.getRequestsNum() + 1);
    }

    public synchronized StorageNodeInfo getStorageNodeInfo() {
        return storageNodeInfo;
    }

    public void setStorageNodeInfo(StorageNodeInfo storageNodeInfo) {
        this.storageNodeInfo = storageNodeInfo;
    }

    public synchronized Hashtable<String, Chunk> getChunksMap() {
        return chunksMap;
    }

    /**
     * Get a files list store on the node
     * @return
     */
    public synchronized ArrayList<StorageMessages.StoreChunk> getNodeFilesList() {
        ArrayList<StorageMessages.StoreChunk> nodeFilesList = new ArrayList<>();
        for(Chunk c : chunksMap.values()) {
            System.out.println(c.toString());
            StorageMessages.StoreChunk storeChunkMsg =
                    StorageMessages.StoreChunk.newBuilder()
                    .setFileName(c.getFileName())
                    .setChunkId(c.getChunkId())
                    .setFileType(c.getFileType())
                    .setNumChunks(c.getNumChunks())
                    .setChunkSize(c.getSize())
                    .build();

            nodeFilesList.add(storeChunkMsg);
        }

        return nodeFilesList;
    }

    /**
     * Get a specific chunk
     * @param chunkName: the name of the chunk
     * @return
     */
    public synchronized Chunk getChunk(String chunkName) {
        return chunksMap.get(chunkName);
    }

    public void setChunksMap(Hashtable<String, Chunk> chunksMap) {
        this.chunksMap = chunksMap;
    }

    public synchronized void addChunkToChunksMap(String chunkName, Chunk chunk) {
        chunksMap.put(chunkName, chunk);
    }

    public Hashtable<String, Integer> getNumOfChunksTable() {
        return numOfChunksTable;
    }

    public void setNumOfChunksTable(Hashtable<String, Integer> numOfChunksTable) {
        this.numOfChunksTable = numOfChunksTable;
    }

    public synchronized Hashtable<String, Hashtable<String, String>> filesOnFailNode(int failNodeId) {
        Hashtable<String, Hashtable<String, String>> filesOnFailNodeTable = new Hashtable<>();
        for(Map.Entry<String, ArrayList<Integer>> chunk : allFilesPosTable.entrySet()) {

        }
        return filesOnFailNodeTable;
    }
}

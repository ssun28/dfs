package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.coordinator.StorageNodeHashSpace;

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
        ArrayList<Integer> nodeIdList = allFilesPosTable.get(inputFileChunk);

        if(nodeIdList == null) {
            nodeIdList = new ArrayList<>();
            nodeIdList.add(nodeId);
        }else {
            nodeIdList.add(nodeId);
        }
        allFilesPosTable.put(inputFileChunk, nodeIdList);

    }

    public synchronized Hashtable<String, StorageMessages.NodeIdList> getRetrieveChunksPos(String fileName, String fileType) {
        Hashtable<String, StorageMessages.NodeIdList> retrieveChunksPosTable = new Hashtable<>();
        for(String s : allFilesPosTable.keySet()) {
            String sfileName;
            String sfileType = "";
            if(s.contains(".")) {
                sfileName = s.split("\\.")[0];
                sfileType = "." + s.split("\\.")[1];
            }else {
                sfileName = s;
            }

            int index = sfileName.lastIndexOf("_");
            String fileNamePre = sfileName.substring(0, index);
            if(fileName.equals(fileNamePre) && sfileType.equals(fileType)) {
                StorageMessages.NodeIdList nodeIdListMsg
                        = StorageMessages.NodeIdList.newBuilder()
                        .addAllNodeId(allFilesPosTable.get(s))
                        .build();
                retrieveChunksPosTable.put(s, nodeIdListMsg);
            }
        }
        return retrieveChunksPosTable;
    }

    public synchronized Hashtable<Integer, String> getNodeIpTable() {
        Hashtable<Integer, String> nodeIpTable = new Hashtable<>();
        for(Map.Entry<Integer, StorageNodeHashSpace> node: routingTable.entrySet()) {
            nodeIpTable.put(node.getKey(), node.getValue().getNodeIp());
        }
        return nodeIpTable;
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

    public synchronized ArrayList<Chunk> getChunksList() {
        return chunksList;
    }

    public synchronized ArrayList<StorageMessages.StoreChunk> getNodeFilesList() {
        ArrayList<StorageMessages.StoreChunk> nodeFilesList = new ArrayList<>();
        for(Chunk c : chunksList) {
            StorageMessages.StoreChunk storeChunkMsg =
                    StorageMessages.StoreChunk.newBuilder()
                    .setFileName(c.getFileName())
                    .setChunkId(c.getChunkId())
                    .setFileType(c.getFileType())
                    .build();

            nodeFilesList.add(storeChunkMsg);
        }

        return nodeFilesList;
    }

    public synchronized Chunk getChunk(String fileName, int chunkId, String fileType) {
        for(Chunk c : chunksList) {
            if(fileName.equals(c.getFileName()) && chunkId == c.getChunkId() && fileType.equals(c.getFileType())) {
                return c;
            }
        }
        return null;
    }

    public void setChunksList(ArrayList<Chunk> chunksList) {
        this.chunksList = chunksList;
    }

    public synchronized void addChunkToChunksList(Chunk chunk) {
        chunksList.add(chunk);
    }
}

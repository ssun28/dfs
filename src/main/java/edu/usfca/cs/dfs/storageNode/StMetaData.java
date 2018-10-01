package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.coordinator.StorageNodeHashSpace;
import edu.usfca.cs.dfs.coordinator.StorageNodeInfo;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

public class StMetaData {

    private Hashtable<Integer, StorageNodeHashSpace> routingTable;
    private Hashtable<Integer, ArrayList<String>> allFilesPosTable;
    private StorageNodeInfo storageNodeInfo;
    private ArrayList<Chunk> chunksList;


    public StMetaData(Hashtable<Integer, StorageNodeHashSpace> routingTable, Hashtable<Integer,
            ArrayList<String>> allFilesPosTable, StorageNodeInfo storageNodeInfo, ArrayList<Chunk> chunksList) {
        this.routingTable = routingTable;
        this.allFilesPosTable = allFilesPosTable;
        this.storageNodeInfo = storageNodeInfo;
        this.chunksList = chunksList;
    }

    public Hashtable<Integer, StorageNodeHashSpace> getRoutingTable() {
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

    public Hashtable<Integer, ArrayList<String>> getAllFilesPosTable() {
        return allFilesPosTable;
    }

    public void setAllFilesPosTable(Hashtable<Integer, ArrayList<String>> allFilesPosTable) {
        this.allFilesPosTable = allFilesPosTable;
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
}

package edu.usfca.cs.dfs.storageNode;

public class Chunk {

    private String fileName;
    private int chunkId;
    private byte[] data;
    private int numChunks;

    public Chunk(String fileName, int chunkId, byte[] data, int numChunks) {
        this.fileName = fileName;
        this.chunkId = chunkId;
        this.data = data;
        this.numChunks = numChunks;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getChunkId() {
        return chunkId;
    }

    public void setChunkId(int chunkId) {
        this.chunkId = chunkId;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public int getNumChunks() {
        return numChunks;
    }

    public void setNumChunks(int numChunks) {
        this.numChunks = numChunks;
    }
}
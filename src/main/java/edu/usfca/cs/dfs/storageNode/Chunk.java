package edu.usfca.cs.dfs.storageNode;

public class Chunk {

    private String fileName;
    private int chunkId;
    private String fileType;
    private int numChunks;
    private int size;

    public Chunk(String fileName, int chunkId, String fileType, int numChunks, int size) {
        this.fileName = fileName;
        this.chunkId = chunkId;
        this.fileType = fileType;
        this.numChunks = numChunks;
        this.size = size;
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

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public int getNumChunks() {
        return numChunks;
    }

    public void setNumChunks(int numChunks) {
        this.numChunks = numChunks;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}

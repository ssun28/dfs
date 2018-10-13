package edu.usfca.cs.dfs.storageNode;

/**
 * Chunk class consists of the chunk's fileName, chunkId, fileType,
 * the original file be break into how many chunks, the size of each chunk,
 * the checkSum of the chunk
 */
public class Chunk {

    private String fileName;
    private int chunkId;
    private String fileType;
    private int numChunks;
    private int size;
    private String checkSum;

    public Chunk(String fileName, int chunkId, String fileType, int numChunks, int size, String checkSum) {
        this.fileName = fileName;
        this.chunkId = chunkId;
        this.fileType = fileType;
        this.numChunks = numChunks;
        this.size = size;
        this.checkSum = checkSum;
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

    public String getCheckSum() {
        return checkSum;
    }

    public void setCheckSum(String checkSum) {
        this.checkSum = checkSum;
    }

    @Override
    public String toString(){
        return fileName + "_" + chunkId + fileType + "  " +numChunks+ "  " + size;
    }
}

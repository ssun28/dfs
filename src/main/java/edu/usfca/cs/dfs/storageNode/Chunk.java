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

    public int getChunkId() {
        return chunkId;
    }

    public String getFileType() {
        return fileType;
    }

    public int getNumChunks() {
        return numChunks;
    }

    public int getSize() {
        return size;
    }

    public String getCheckSum() {
        return checkSum;
    }

    @Override
    public String toString(){
        return fileName + "_" + chunkId + fileType + "  " +numChunks+ "  " + size;
    }
}

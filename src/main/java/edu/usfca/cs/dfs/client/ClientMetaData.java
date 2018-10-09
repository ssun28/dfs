package edu.usfca.cs.dfs.client;

import java.net.InetAddress;

public class ClientMetaData {

    private String storeFileName;
    private String storeFileType;
    private int numChunks;
    private String clientIp;
    private InetAddress ServerIP;

    public ClientMetaData(String storeFileName, String storeFileType, int numChunks, String clientIp, InetAddress serverIP) {
        this.storeFileName = storeFileName;
        this.storeFileType = storeFileType;
        this.numChunks = numChunks;
        this.clientIp = clientIp;
        ServerIP = serverIP;
    }

    public String getStoreFileName() {
        return storeFileName;
    }

    public void setStoreFileName(String storeFileName) {
        this.storeFileName = storeFileName;
    }

    public String getStoreFileType() {
        return storeFileType;
    }

    public void setStoreFileType(String storeFileType) {
        this.storeFileType = storeFileType;
    }

    public int getNumChunks() {
        return numChunks;
    }

    public void setNumChunks(int numChunks) {
        this.numChunks = numChunks;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public InetAddress getServerIP() {
        return ServerIP;
    }

    public void setServerIP(InetAddress serverIP) {
        ServerIP = serverIP;
    }
}

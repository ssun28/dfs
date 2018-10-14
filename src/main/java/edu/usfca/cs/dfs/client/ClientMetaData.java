package edu.usfca.cs.dfs.client;

import java.net.InetAddress;

/**
 * MetaData for the client: multi threads can share the data
 */
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

    public String getStoreFileType() {
        return storeFileType;
    }

    public int getNumChunks() {
        return numChunks;
    }

    public String getClientIp() {
        return clientIp;
    }


    public InetAddress getServerIP() {
        return ServerIP;
    }

}

package edu.usfca.cs.dfs.coordinator;

import java.io.File;
import java.text.DecimalFormat;

public class StorageNodeInfo {

    private static final double  GIGABYTES = 1024 * 1024 * 1024;
    private static DecimalFormat df2 = new DecimalFormat(".##");

    private int nodeId;
    private String nodeIp;
    private boolean active;
    private double spaceCap;
    private int requestsNum;

    public StorageNodeInfo(int nodeId, String nodeIp, boolean active, double spaceCap, int requestsNum) {
        this.nodeId = nodeId;
        this.nodeIp = nodeIp;
        this.active = active;
        this.spaceCap = spaceCap;
        this.requestsNum = requestsNum;
    }

    public int getNodeId() {
        return nodeId;
    }

    public synchronized  void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public void setNodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    public synchronized boolean isActive() {
        return active;
    }

    public synchronized void setActive(boolean active) {
        this.active = active;
    }

    public synchronized double getSpaceCap() {
        spaceCap = Double.parseDouble(df2.format(new File("/")
                .getUsableSpace()/ GIGABYTES));
        return spaceCap;
    }

    public synchronized void setSpaceCap(double spaceCap) {
        this.spaceCap = spaceCap;
    }

    public synchronized int getRequestsNum() {
        return requestsNum;
    }

    public synchronized void setRequestsNum(int requestsNum) {
        this.requestsNum = requestsNum;
    }
}

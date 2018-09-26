package edu.usfca.cs.dfs.coordinator;

public class StorageNodeInfo {

    private String nodeIp;
    private int nodeId;
    private boolean active;
    private double spaceCap;
    private int requestsNum;

    public StorageNodeInfo(String nodeIp, int nodeId, boolean active, double spaceCap, int requestsNum) {
        this.nodeIp = nodeIp;
        this.nodeId = nodeId;
        this.active = active;
        this.spaceCap = spaceCap;
        this.requestsNum = requestsNum;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public void setNodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public double getSpaceCap() {
        return spaceCap;
    }

    public void setSpaceCap(double spaceCap) {
        this.spaceCap = spaceCap;
    }

    public int getRequestsNum() {
        return requestsNum;
    }

    public void setRequestsNum(int requestsNum) {
        this.requestsNum = requestsNum;
    }
}

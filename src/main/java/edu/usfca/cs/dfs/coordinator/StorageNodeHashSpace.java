package edu.usfca.cs.dfs.coordinator;

public class StorageNodeHashSpace {

    private String nodeIp;
    private int[] spaceRange;

    public StorageNodeHashSpace(String nodeIp, int[] spaceRange) {
        this.nodeIp = nodeIp;
        this.spaceRange = spaceRange;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public void setNodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    public int[] getSpaceRange() {
        return spaceRange;
    }

    public void setSpaceRange(int[] spaceRange) {
        this.spaceRange = spaceRange;
    }
}

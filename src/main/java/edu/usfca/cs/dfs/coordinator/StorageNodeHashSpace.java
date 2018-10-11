package edu.usfca.cs.dfs.coordinator;

/**
 * StorageNodeHashSpace class consists of a Storage Node's ip,
 * the Storage Node's space range hold in the system hash ring
 */
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


    @Override
    public String toString(){
        return nodeIp + "    " + "from   " + spaceRange[0] +"   to   " + spaceRange[1];
    }
}

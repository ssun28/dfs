package edu.usfca.cs.dfs.coordinator;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;

public class SocketTask extends Thread {

    public static final int HASHRING_PIECES = 16;

    private Socket socket;
    private CoorMetaData coorMetaData;
    private StorageMessages.ProtoWrapper protoWrapper;

    public SocketTask(Socket socket, CoorMetaData coorMetaData) {
        this.socket = socket;
        this.coorMetaData = coorMetaData;

        try {
            this.protoWrapper = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while(true) {
            try {
                this.protoWrapper = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                        socket.getInputStream());
                String requestor = protoWrapper.getRequestor();
                String functionType = protoWrapper.getFunctionCase().toString();

                if(requestor.equals("client") && functionType.equals("ASKINFO")) {
                    clientRequest();
                }else if (requestor.equals("storageNode")) {
                    storageNodeRequest(functionType);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void clientRequest() {
        StorageMessages.AskInfo AskInfo
                = protoWrapper.getAskInfo();
        String askInfoType = AskInfo.getFunctionCase().toString();

        switch(askInfoType) {
            case "ACTIVENODESLIST":
                getActiveNodesList();
                break;
            case "TOTALDISKSPACE":
                getTotalDiskSpace();
                break;
            case "REQUESTSNUM":
                getRequestsNum();
                break;
            default: break;
        }

    }

    private void getActiveNodesList() {
        System.out.println("Here is the list of active nodes from coordinator: ");
        ArrayList<StorageNodeInfo> activeNodesList = this.coorMetaData.getActiveNodesList();
        for(StorageNodeInfo s: activeNodesList) {
            System.out.println("NodeId: " + s.getNodeId() + "  , active  " + "NodeIp: " + s.getNodeIp());
        }
    }

    private void getTotalDiskSpace() {

    }

    private void getRequestsNum() {

    }

    private void storageNodeRequest(String functionType) {
        switch (functionType) {
            case "ADDNODE":
                addNodeRequest();
                break;
            case "REMOVENODE":
                removeNodeRequest();
                break;
            case "HEARTBEAT":
                heartBeat();
                break;
            default: break;
        }
    }

    private void addNodeRequest() {
        int currentNodeId = this.coorMetaData.getNodeId() + 1;
        int newNodeNum = this.coorMetaData.getRoutingTableSize() + 1;
        int range = HASHRING_PIECES / newNodeNum;
        int rangeBegin = (newNodeNum - 1) * range;
        int[] spaceRange = new int[]{rangeBegin, 15};

        StorageNodeHashSpace snhs = new StorageNodeHashSpace(socket.getRemoteSocketAddress().toString().substring(1),spaceRange);

        this.coorMetaData.addNodeToRoutingTable(currentNodeId, snhs);


        System.out.println("Node_" + currentNodeId + " is allowed to add into the hash space!");
        this.coorMetaData.setNodeId(currentNodeId);

        StorageMessages.ProtoWrapper protoWrapperOut =
                StorageMessages.ProtoWrapper.newBuilder()
                        .setRequestor("coordinator")
                        .setIp(this.coorMetaData.getCoorIp())
                        .setAddNode(Integer.toString(currentNodeId))
                        .build();
        try {
            protoWrapperOut.writeDelimitedTo(this.socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void heartBeat() {
        StorageMessages.Heartbeat heartBeatInMsg
                = protoWrapper.getHeartbeat();
        double rtVersion = heartBeatInMsg.getRtVersion();

        StorageMessages.StorageNodeInfo snMsg
                = heartBeatInMsg.getStorageNodeInfo();

        StorageNodeInfo sn = new StorageNodeInfo(snMsg.getNodeId(), this.protoWrapper.getIp(), snMsg.getActive(), snMsg.getSpaceAvailable(), snMsg.getRequestsNum());
        this.coorMetaData.addNodeToMetaDataTable(sn.getNodeId(), sn);

        Map<Integer, StorageMessages.StorageNodeHashSpace> mp = this.coorMetaData.constructSnHashSpaceProto();

        StorageMessages.Heartbeat heartBeatOutMsg
                = StorageMessages.Heartbeat.newBuilder()
                .setRtVersion(this.coorMetaData.getRtVersion())
                .putAllRoutingEles(mp)
                .build();

        StorageMessages.ProtoWrapper protoWrapper
                = StorageMessages.ProtoWrapper.newBuilder()
                .setRequestor("coordinator")
                .setIp(this.coorMetaData.getCoorIp())
                .setHeartbeat(heartBeatOutMsg)
                .build();
        try {
            protoWrapper.writeDelimitedTo(this.socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void removeNodeRequest() {

    }
}

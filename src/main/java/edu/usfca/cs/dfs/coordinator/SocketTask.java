package edu.usfca.cs.dfs.coordinator;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.storageNode.StorageNodeInfo;

import java.io.IOException;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;

public class SocketTask implements Runnable {

    private static final int HASHRING_PIECES = 16;
    private static final String CLIENT = "client";
    private static final String STORAGENODE = "storageNode";
    private static final String COORDINATOR = "coordinator";

    private enum AskInfoType {ACTIVENODESLIST, TOTALDISKSPACE, REQUESTSNUM};

    private Socket socket;
    private CoorMetaData coorMetaData;
    private StorageMessages.ProtoWrapper protoWrapperIn;
    private StorageMessages.ProtoWrapper protoWrapperOut;

    public SocketTask(Socket socket, CoorMetaData coorMetaData) {
        this.socket = socket;
        this.coorMetaData = coorMetaData;
    }

    public void run() {
        while(true) {
            try {
                protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                        socket.getInputStream());
                String requestor = protoWrapperIn.getRequestor();
                String functionType = protoWrapperIn.getFunctionCase().toString();

                System.out.println(getLocalDataTime() + " New connection from " + socket.getRemoteSocketAddress()+ " is connected! ");
                System.out.println("requestor is "+ requestor);
                System.out.println("IP is "+ protoWrapperIn.getIp());

                if(requestor.equals(CLIENT) && functionType.equals("ASKINFO")) {
                    clientRequest();
                }else if (requestor.equals(STORAGENODE)) {
                    storageNodeRequest(functionType);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void clientRequest() {
        StorageMessages.AskInfo AskInfo
                = protoWrapperIn.getAskInfo();
        AskInfoType askInfoType = AskInfoType.valueOf(AskInfo.getFunctionCase().toString());
//        String askInfoType = AskInfo.getFunctionCase().toString();

        switch(askInfoType) {
            case ACTIVENODESLIST:
                getActiveNodesList();
                break;
            case TOTALDISKSPACE:
                getTotalDiskSpace();
                break;
            case REQUESTSNUM:
                getRequestsNum();
                break;
            default: break;
        }
        quit();
    }

    private void getActiveNodesList() {
        ArrayList<StorageMessages.ActiveNode> activeNodesList = coorMetaData.getActiveNodesList();
        StorageMessages.ActiveNodesList activeNodesListMsgOut
                = StorageMessages.ActiveNodesList.newBuilder()
                .addAllActiveNode(activeNodesList)
                .build();

        StorageMessages.AskInfo askInfoMsgOut
                = StorageMessages.AskInfo.newBuilder()
                .setResActiveNodesList(activeNodesListMsgOut)
                .build();

        clientRequestWrapperOut(askInfoMsgOut);
    }

    private void getTotalDiskSpace() {
        ArrayList<StorageMessages.DiskSpace> diskSpaceList = coorMetaData.getTotalDiskSpace();
        StorageMessages.TotalDiskSpace totalDiskSpaceMsgOut
                = StorageMessages.TotalDiskSpace.newBuilder()
                .addAllDiskSpace(diskSpaceList)
                .build();

        StorageMessages.AskInfo askInfoMsgOut
                = StorageMessages.AskInfo.newBuilder()
                .setResTotalDiskSpace(totalDiskSpaceMsgOut)
                .build();

        clientRequestWrapperOut(askInfoMsgOut);
    }

    private void getRequestsNum() {
        ArrayList<StorageMessages.NodeRequestsNum> nodeRequestsNumsList = coorMetaData.getNodeRuquestsNum();
        StorageMessages.RequestsNum requestsNumMsgOut
                = StorageMessages.RequestsNum.newBuilder()
                .addAllNodeRequestsNum(nodeRequestsNumsList)
                .build();

        StorageMessages.AskInfo askInfoMsgOut
                = StorageMessages.AskInfo.newBuilder()
                .setResRequestsNum(requestsNumMsgOut)
                .build();

        clientRequestWrapperOut(askInfoMsgOut);
    }

    private void clientRequestWrapperOut(StorageMessages.AskInfo askInfoMsgOut) {
        try {
            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                            .setRequestor(COORDINATOR)
                            .setIp(coorMetaData.getCoorIp())
                            .setAskInfo(askInfoMsgOut)
                            .build();

            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        int currentNodeId = coorMetaData.getNodeId() + 1;
        int newNodeNum = coorMetaData.getRoutingTableSize() + 1;
        int range = HASHRING_PIECES / newNodeNum;
        int rangeBegin = (newNodeNum - 1) * range;
        int[] spaceRange = new int[]{rangeBegin, 15};

        StorageNodeHashSpace snhs = new StorageNodeHashSpace(socket.getRemoteSocketAddress().toString().substring(1),spaceRange);

        coorMetaData.addNodeToRoutingTable(currentNodeId, snhs);

        System.out.println("Node_" + currentNodeId + " is allowed to add into the hash space!");
        coorMetaData.setNodeId(currentNodeId);

        StorageMessages.ProtoWrapper protoWrapperOut =
                StorageMessages.ProtoWrapper.newBuilder()
                        .setRequestor("coordinator")
                        .setIp(coorMetaData.getCoorIp())
                        .setAddNode(Integer.toString(currentNodeId))
                        .build();
        try {
            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void heartBeat() {
        StorageMessages.Heartbeat heartBeatInMsg
                = protoWrapperIn.getHeartbeat();
        double rtVersion = heartBeatInMsg.getRtVersion();

        StorageMessages.StorageNodeInfo snMsg
                = heartBeatInMsg.getStorageNodeInfo();

        StorageNodeInfo sn = new StorageNodeInfo(snMsg.getNodeId(), protoWrapperIn.getIp(), snMsg.getActive(), snMsg.getSpaceAvailable(), snMsg.getRequestsNum());
        coorMetaData.addNodeToMetaDataTable(sn.getNodeId(), sn);

        StorageMessages.Heartbeat heartBeatOutMsg;
        StorageMessages.ProtoWrapper protoWrapperOut;

        if (rtVersion >= coorMetaData.getRtVersion()) {
            heartBeatOutMsg
                    = StorageMessages.Heartbeat.newBuilder()
                    .setRtVersion(coorMetaData.getRtVersion())
                    .build();
        }else {
            Map<Integer, StorageMessages.StorageNodeHashSpace> mp = coorMetaData.constructSnHashSpaceProto();

            heartBeatOutMsg
                    = StorageMessages.Heartbeat.newBuilder()
                    .setRtVersion(coorMetaData.getRtVersion())
                    .putAllRoutingEles(mp)
                    .build();
        }

        protoWrapperOut
                = StorageMessages.ProtoWrapper.newBuilder()
                .setRequestor("coordinator")
                .setIp(coorMetaData.getCoorIp())
                .setHeartbeat(heartBeatOutMsg)
                .build();

        try {
            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void removeNodeRequest() {

    }

    private String getLocalDataTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    private void quit() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

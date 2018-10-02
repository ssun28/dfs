package edu.usfca.cs.dfs.coordinator;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;

public class SocketTask implements Runnable {

    public static final int HASHRING_PIECES = 16;

    private Socket socket;
    private CoorMetaData coorMetaData;
    private StorageMessages.ProtoWrapper protoWrapperIn;

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
                = protoWrapperIn.getAskInfo();
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
        quit();
    }

    private void getActiveNodesList() {
        System.out.println("Here is the list of active nodes from coordinator: ");
        ArrayList<StorageNodeInfo> activeNodesList = coorMetaData.getActiveNodesList();
        for(StorageNodeInfo s: activeNodesList) {
            System.out.println("NodeId: " + s.getNodeId() + "  , active  " + ", NodeIp: " + s.getNodeIp());
        }
    }

    private void getTotalDiskSpace() {
        System.out.println("The total disk space available in the cluster (in GB) from coordinator is " + coorMetaData.getTotalDiskSpace());
    }

    private void getRequestsNum() {
        System.out.println("Here is the list of number of requests handled by each node: ");
        ArrayList<StorageNodeInfo> activeNodesList = coorMetaData.getActiveNodesList();
        for(StorageNodeInfo s: activeNodesList) {
            System.out.println("NodeId: " + s.getNodeId() + "  , number of requests: " + s.getRequestsNum() +  " , NodeIp: " + s.getNodeIp());
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
        StorageMessages.ProtoWrapper protoWrapper;

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

        protoWrapper
                = StorageMessages.ProtoWrapper.newBuilder()
                .setRequestor("coordinator")
                .setIp(coorMetaData.getCoorIp())
                .setHeartbeat(heartBeatOutMsg)
                .build();

        try {
            protoWrapper.writeDelimitedTo(socket.getOutputStream());
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

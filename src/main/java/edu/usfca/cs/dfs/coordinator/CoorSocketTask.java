package edu.usfca.cs.dfs.coordinator;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.storageNode.StorageNodeInfo;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

/**
 * CoorSocketTask: deal with a socket request from client or
 * storage nodes
 */
public class CoorSocketTask implements Runnable {

    private static final int HASHRING_PIECES = (int)Math.pow(2,16);
    private static final String CLIENT = "client";
    private static final String STORAGENODE = "storageNode";
    private static final String COORDINATOR = "coordinator";

    private enum AskInfoType {ACTIVENODESLIST, TOTALDISKSPACE, REQUESTSNUM};

    private Socket socket;
    private CoorMetaData coorMetaData;
    private StorageMessages.ProtoWrapper protoWrapperIn;
    private StorageMessages.ProtoWrapper protoWrapperOut;
    private static Logger log;
    private int nodeId = -1;

    public CoorSocketTask(Socket socket, CoorMetaData coorMetaData) {
        this.socket = socket;
        this.coorMetaData = coorMetaData;
        log = Logger.getLogger(CoorSocketTask.class);
    }

    /**
     * Main run method:
     * See what kind of the requestor is
     */
    @Override
    public void run() {
        try {
            protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    socket.getInputStream());
            String requestor = protoWrapperIn.getRequestor();
            String functionType = protoWrapperIn.getFunctionCase().toString();

            int removeNodeId = protoWrapperIn.getRemoveNodeId();

            log.info("A " + requestor +" has connected");
            log.info("IP address is " + protoWrapperIn.getIp());

            if(requestor.equals(CLIENT) && functionType.equals("ASKINFO")) {
                clientRequest();
            }else if (requestor.equals(STORAGENODE)) {
                storageNodeRequest(functionType);
            }


//            else if (requestor.equals(CLIENT) && removeNodeId >= 0) {
//                log.error(removeNodeId +" is being removed");
//                removeNode(removeNodeId);
//            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * All requests from client
     */
    private void clientRequest() {
        StorageMessages.AskInfo AskInfo
                = protoWrapperIn.getAskInfo();
        AskInfoType askInfoType = AskInfoType.valueOf(AskInfo.getFunctionCase().toString());
        log.info("Client is asking for "+ askInfoType);

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

    /**
     * Get the activeNodesList from metaDataTable and send it to the client
     */
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

    /**
     * Get the totalDiskSpace list from metaDataTable and send it to the client
     */
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

    /**
     * Get the nodeRequestsNum list from metaDataTable and send it to the client
     */
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

    /**
     * A basic protoWapperOut for response to client askInfo requests
     * @param askInfoMsgOut
     */
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

    /**
     * All requests from storage node
     * @param functionType
     */
    private void storageNodeRequest(String functionType) {
        log.info("Storage Node wants to " + functionType);
        switch (functionType) {
            case "ADDNODE":
                addNodeRequest();
                heartBeat();
                break;
            case "RECORVERYNODEINFO":
                recorveryNodeInfo();
                heartBeat();
                break;
            default: break;
        }
    }

    /**
     * A new storage node first connect to the coordinator and
     * add to the hash space, give the space range and nodeId
     * update the routing table, rtVersion in coordinator
     */
    private void addNodeRequest() {
        int newNodeId = coorMetaData.getNodeId() + 1;
        int totalNodeNum = coorMetaData.getRoutingTableSize() + 1;
        int range = HASHRING_PIECES / totalNodeNum;
        int rangeBegin = (totalNodeNum - 1) * range;
        int[] spaceRange = new int[]{rangeBegin, (int)Math.pow(2, 16) - 1};

        StorageNodeHashSpace snhs = new StorageNodeHashSpace(protoWrapperIn.getIp(), spaceRange);

        coorMetaData.addNodeToRoutingTable(totalNodeNum, newNodeId, snhs);

        for(Map.Entry<Integer, StorageNodeHashSpace> e : coorMetaData.getRoutingTable().entrySet()){
            log.info(e.getKey() +"    "+ e.getValue().toString());
        }

        coorMetaData.setNodeId(newNodeId);
        this.nodeId = newNodeId;

        StorageMessages.ProtoWrapper protoWrapperOut =
                StorageMessages.ProtoWrapper.newBuilder()
                        .setRequestor("coordinator")
                        .setIp(coorMetaData.getCoorIp())
                        .setAddNode(Integer.toString(newNodeId))
                        .build();
        try {
            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

        coorMetaData.setRtVersion(coorMetaData.getRtVersion() + 0.1);
    }

    /**
     * Recorvery the node Info from node
     * update the routing table
     */
    private void recorveryNodeInfo() {
        StorageMessages.RoutingEle routingEleMsgIn = protoWrapperIn.getRecorveryNodeInfo();
        StorageMessages.StorageNodeHashSpace storageNodeHashSpaceMsgIn = routingEleMsgIn.getStorageNodeHashSpace();
        int nodeId = routingEleMsgIn.getNodeId();
        int totalNodeNum = coorMetaData.getRoutingTableSize() + 1;
        int[] spaceRange = new int[]{storageNodeHashSpaceMsgIn.getSpaceBegin(), storageNodeHashSpaceMsgIn.getSpaceEnd()};
        int nodeNum = routingEleMsgIn.getNodeNum();

        StorageNodeHashSpace snhs = new StorageNodeHashSpace(protoWrapperIn.getIp(), spaceRange);

        coorMetaData.addNodeToRoutingTable(totalNodeNum, nodeId, snhs);
        coorMetaData.setRtVersion(coorMetaData.getRtVersion() + 0.1);
        coorMetaData.setNodeId(nodeNum - 1);
    }

    /**
     * After the new storage node add into the hash space,
     * heartbeat to get/update storage node Info from storage node
     * send the new routing table to the storage node
     */
    private void heartBeat() {
        while(true) {
            try {
                // this is for the time out of the socket inputStream
                socket.setSoTimeout(10000);

                protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                        socket.getInputStream());

                StorageMessages.Heartbeat heartBeatInMsg
                        = protoWrapperIn.getHeartbeat();

                setStorageNodeInfo(heartBeatInMsg);

                StorageMessages.Heartbeat heartBeatMsgOut = setHeartBeatMsgOut(heartBeatInMsg);

                protoWrapperOut
                        = StorageMessages.ProtoWrapper.newBuilder()
                        .setRequestor("coordinator")
                        .setIp(coorMetaData.getCoorIp())
                        .setHeartbeat(heartBeatMsgOut)
                        .build();

                protoWrapperOut.writeDelimitedTo(socket.getOutputStream());

            } catch(SocketTimeoutException e){
                log.error(this.nodeId +"has failed!!!");
                removeNode(this.nodeId);
                break;

            } catch (SocketException e) {
                log.error("Heart Beat error from " + this.nodeId + " : " + e.getMessage());
                log.info("This node has been removed");
                removeNode(this.nodeId);
                e.printStackTrace();
                break;
            } catch (NullPointerException e){
                log.info("This node has been removed");
                if(protoWrapperIn == null){
                    removeNode(this.nodeId);
                }
                break;
            } catch (IOException e) {
                log.error("IO Exception");
                e.printStackTrace();
                break;
            }
        }
    }

    /**
     * Add/update the storage node info to the metaDataTable
     * @param heartBeatInMsg
     */
    private void setStorageNodeInfo(StorageMessages.Heartbeat heartBeatInMsg){
        StorageMessages.StorageNodeInfo snMsg
                = heartBeatInMsg.getStorageNodeInfo();

        int nodeId = snMsg.getNodeId();
        StorageNodeInfo sn;
        if(!coorMetaData.getMetaDataTable().containsKey(nodeId) || coorMetaData.getMetaDataTable().get(nodeId).isActive()){
             sn = new StorageNodeInfo(snMsg.getNodeId(), protoWrapperIn.getIp(), true, snMsg.getSpaceAvailable(), snMsg.getRequestsNum());

        } else {
             sn = new StorageNodeInfo(snMsg.getNodeId(), protoWrapperIn.getIp(), false, snMsg.getSpaceAvailable(), snMsg.getRequestsNum());

        }
        coorMetaData.addNodeToMetaDataTable(sn.getNodeId(), sn);
    }

    /**
     * Construct a heartBeat Msg Out protoWrapper
     * @param heartBeatInMsg
     * @return
     */
    private StorageMessages.Heartbeat setHeartBeatMsgOut(StorageMessages.Heartbeat heartBeatInMsg){
        double rtVersion = heartBeatInMsg.getRtVersion();

        StorageMessages.Heartbeat heartBeatMsgOut;

        if (rtVersion >= coorMetaData.getRtVersion()) {
            heartBeatMsgOut
                    = StorageMessages.Heartbeat.newBuilder()
                    .setRtVersion(coorMetaData.getRtVersion())
                    .build();
        } else {
            Map<Integer, StorageMessages.StorageNodeHashSpace> mp = coorMetaData.constructSnHashSpaceProto();

            heartBeatMsgOut
                    = StorageMessages.Heartbeat.newBuilder()
                    .setRtVersion(coorMetaData.getRtVersion())
                    .putAllRoutingEles(mp)
                    .build();
        }
        return heartBeatMsgOut;
    }

    /**
     * Reomove node from routing table
     * @param nodeId
     */
    private void removeNode(int nodeId){
        coorMetaData.removeFailNode(nodeId);
        log.info(nodeId +" has been removed from the routing table");
        Hashtable<Integer, StorageNodeHashSpace> routingTable = coorMetaData.getRoutingTable();
        for(int id : routingTable.keySet()){
            if(id != nodeId){
                moveFiles(id, nodeId);
                break;
            }
        }
    }

    /**
     * Find the files on the failed storage node and move those files to other
     * storage node
     * @param nodeId
     * @param failNodeId
     */
    private void moveFiles(int nodeId, int failNodeId){
        String nodeIp = coorMetaData.getRoutingTable().get(nodeId).getNodeIp();

        Runnable r = new MoveFileTask(nodeIp, failNodeId);
        r.run();
    }

    /**
     * Socket close
     */
    private void quit() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

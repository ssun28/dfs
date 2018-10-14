package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.coordinator.Coordinator;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.Scanner;

/**
 * HeartBeatTask class: add node to the hash ring, routing table and
 * heartbeat with the coordinator.
 * If the coordinator goes down, reconnect with the coordinator when it comes back
 */
public class HeartBeatTask extends Thread{

    private static final long CHECK_DELAY = 10;
    private static final long KEEP_ALIVE = 5000;

    private Socket hbSocket;
    private StMetaData stMetaData;
    private double rtVerstion;
    private boolean isConnectedCoor;
    private StorageMessages.ProtoWrapper protoWrapperIn;
    private StorageMessages.ProtoWrapper protoWrapperOut;
    private InetAddress serverIP;

    private static Logger log;

    public HeartBeatTask(StMetaData stMetaData) {
        this.stMetaData = stMetaData;
        this.isConnectedCoor = false;
        this.rtVerstion = -1.0;
        this.protoWrapperIn = null;
        this.protoWrapperOut = null;
        log = Logger.getLogger(HeartBeatTask.class);
    }

    /**
     * Main run:
     * Connect to the coordinator, add to the hash ring, routing table
     * and heartbeat with the coordinator
     */
    @Override
    public void run() {
        connectCoordinator();
        addNodeToCor();
        heartBeat();
    }

    /**
     * Connect to the coordinator
     */
    private void connectCoordinator() {
        while(!isConnectedCoor) {
            System.out.print("Enter the coordinator's IP address : ");
            Scanner scanner = new Scanner(System.in);
            try {
                serverIP = InetAddress.getByName(scanner.nextLine());
                boolean result = establishConnection();
                if(!result){
                    System.out.println("Please enter the coordinator's IP address correctly!");
                    connectCoordinator();
                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * Establish connection with the coordinator
     * @return
     */
    private boolean establishConnection() {
        try {
            hbSocket = new Socket();
            hbSocket.connect(new InetSocketAddress(serverIP, Coordinator.PORT), 2000);
            isConnectedCoor = true;
            System.out.println("Successfully connecting with the coordinator !");
            return true;

        } catch (UnknownHostException e) {
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Send addNode request to the coordinator which means to add to the hash ring
     * new node to the hash ring and get nodeId back
     * clean the directory if there are some old files store in the directory
     *
     */
    private void addNodeToCor() {
        try {
            StorageMessages.ProtoWrapper protoWrapperOut
                    = StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor(SnSocketTask.STORAGENODE)
                    .setIp(stMetaData.getStorageNodeInfo().getNodeIp())
                    .setAddNode("true")
                    .build();

            protoWrapperOut.writeDelimitedTo(hbSocket.getOutputStream());

            protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    hbSocket.getInputStream());
            int nodeId = Integer.parseInt(protoWrapperIn.getAddNode());

            stMetaData.getStorageNodeInfo().setNodeId(nodeId);
            stMetaData.getStorageNodeInfo().setActive(true);

            File dirFile = new File(SnSocketTask.DIR);

            if(dirFile.exists()) {
                String[] entries = dirFile.list();
                for (String s : entries) {
                    File currentFile = new File(dirFile.getPath(), s);
                    currentFile.delete();
                }
                System.out.println("All the old files present in the storage directory have been removed successfully!");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Heartbeat with the coordinator
     * send the storage node info to the coordinator
     * get back new routing table back if it is updated
     */
    private void heartBeat() {
        long lastSendTime = System.currentTimeMillis();

        StorageMessages.StorageNodeInfo storageNodeInfoMsgOut;
        StorageMessages.Heartbeat heartbeatMsgOut;
        StorageMessages.ProtoWrapper protoWrapperOut;

        while(true) {
            if(System.currentTimeMillis() - lastSendTime > KEEP_ALIVE) {
                try {
                    storageNodeInfoMsgOut
                            = StorageMessages.StorageNodeInfo.newBuilder()
                            .setNodeId(stMetaData.getStorageNodeInfo().getNodeId())
                            .setActive(stMetaData.getStorageNodeInfo().isActive())
                            .setSpaceAvailable(stMetaData.getStorageNodeInfo().getSpaceCap())
                            .setRequestsNum(stMetaData.getStorageNodeInfo().getRequestsNum())
                            .build();

                    heartbeatMsgOut
                            = StorageMessages.Heartbeat.newBuilder()
                            .setRtVersion(rtVerstion)
                            .setStorageNodeInfo(storageNodeInfoMsgOut)
                            .build();

                    protoWrapperOut
                            = StorageMessages.ProtoWrapper.newBuilder()
                            .setRequestor(SnSocketTask.STORAGENODE)
                            .setIp(stMetaData.getStorageNodeInfo().getNodeIp())
                            .setHeartbeat(heartbeatMsgOut)
                            .build();

                    protoWrapperOut.writeDelimitedTo(hbSocket.getOutputStream());

                    protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                            hbSocket.getInputStream());
                    StorageMessages.Heartbeat heartbeatMsgIn = protoWrapperIn.getHeartbeat();

                    // judge the version at the coordinator side
                    if(heartbeatMsgIn.getRtVersion() > rtVerstion) {
                        setRtVerstion(heartbeatMsgIn.getRtVersion());
                        stMetaData.updateRoutingTable(heartbeatMsgIn.getRoutingElesMap());
                    }

                    lastSendTime = System.currentTimeMillis();

                    log.info(stMetaData.getStorageNodeInfo().toString());

                    int[] rangeArray = stMetaData.getRoutingTable().get(stMetaData.getStorageNodeInfo().getNodeId()).getSpaceRange();
                    log.info("Hash Space Range: " + rangeArray[0] + "~" + rangeArray[1]);

                } catch (SocketException e) {
                    reConnectCoor();
                } catch (NullPointerException e) {
                    reConnectCoor();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else {
                try {
                    Thread.sleep(CHECK_DELAY);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * If the coordinator goes down, reconnect with the coordinator
     */
    private void reConnectCoor(){
        log.info("Waiting for the coordinator coming back");
        while(!establishConnection()){
        }

        int nodeId = stMetaData.getStorageNodeInfo().getNodeId();
        if(stMetaData == null){
            System.out.println("metadata null");
        }
        if(stMetaData.getRoutingTable() == null){
            System.out.println("routingtable null");
        }
        if(stMetaData.getRoutingTable().get(nodeId) == null){
            System.out.println("sopace range null");
        }

        StorageMessages.StorageNodeHashSpace shMsgOut =
                StorageMessages.StorageNodeHashSpace.newBuilder()
                        .setSpaceBegin(stMetaData.getRoutingTable().get(nodeId).getSpaceRange()[0])
                        .setSpaceEnd(stMetaData.getRoutingTable().get(nodeId).getSpaceRange()[1])
                        .build();

        StorageMessages.RoutingEle routingEleMsgOut =
                StorageMessages.RoutingEle.newBuilder()
                        .setNodeId(nodeId)
                        .setStorageNodeHashSpace(shMsgOut)
                        .setNodeNum(stMetaData.getRoutingTable().size())
                        .build();

        protoWrapperOut =
                StorageMessages.ProtoWrapper.newBuilder()
                        .setRequestor(SnSocketTask.STORAGENODE)
                        .setIp(stMetaData.getStorageNodeInfo().getNodeIp())
                        .setRecorveryNodeInfo(routingEleMsgOut)
                        .build();

        try {
            protoWrapperOut.writeDelimitedTo(hbSocket.getOutputStream());
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        heartBeat();
    }

    public void setRtVerstion(double rtVerstion) {
        this.rtVerstion = rtVerstion;
    }

}

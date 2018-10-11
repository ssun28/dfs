package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.coordinator.Coordinator;
import edu.usfca.cs.dfs.coordinator.StorageNodeHashSpace;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Scanner;

public class HeartBeatTask extends Thread{

//    public static final int PORT = 37000;
//    private static final double  GIGABYTES = 1024 * 1024 * 1024;
//    private static DecimalFormat df2 = new DecimalFormat(".##");
    private static final long CHECK_DELAY = 10;
    private static final long KEEP_ALIVE = 5000;

    private Socket hbSocket;
    private StMetaData stMetaData;
    private double rtVerstion;
    private boolean isConnectedCoor;
    private StorageMessages.ProtoWrapper protoWrapperIn;
    private StorageMessages.ProtoWrapper protoWrapperOut;
    private static Logger log;

    public HeartBeatTask(StMetaData stMetaData) {
        this.hbSocket = new Socket();
        this.stMetaData = stMetaData;
        this.isConnectedCoor = false;
        this.rtVerstion = -1.0;
        this.protoWrapperIn = null;
        this.protoWrapperOut = null;
        log = Logger.getLogger(HeartBeatTask.class);
    }

    public void run() {
        connectCoordinator();
        addNodeToCor();
        heartBeat();
    }

    private void connectCoordinator() {
        InetAddress serverIP;
        while(!isConnectedCoor) {
            System.out.print("Enter the coordinator's IP address : ");
            Scanner scanner = new Scanner(System.in);
            try {
                serverIP = InetAddress.getByName(scanner.nextLine());
                hbSocket.connect(new InetSocketAddress(serverIP, Coordinator.PORT), 2000);
                isConnectedCoor = true;
                System.out.println("Successfully connecting with the coordinator !");

            } catch (UnknownHostException e) {
                System.out.println("Please enter the coordinator's IP address correctly!");
                connectCoordinator();
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("Please enter the coordinator's IP address correctly!");
                connectCoordinator();
                e.printStackTrace();
            }
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
            // protocol

            protoWrapperOut.writeDelimitedTo(hbSocket.getOutputStream());

//            StorageMessages.ProtoWrapper protoWrapperIn
//                    = StorageMessages.ProtoWrapper.parseDelimitedFrom(hbSocket.getInputStream());
            protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    hbSocket.getInputStream());
            int nodeId = Integer.parseInt(protoWrapperIn.getAddNode());
            System.out.println("nodeId = " + nodeId);
            stMetaData.getStorageNodeInfo().setNodeId(nodeId);
            stMetaData.getStorageNodeInfo().setActive(true);
//            stMetaData.getStorageNodeInfo().setSpaceCap(Double.parseDouble(df2.format(new File("/")
//                    .getUsableSpace()/ GIGABYTES)));

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
//        long checkDelay = 10;
//        long keepAlive = 5000;
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

//                    log.info("Current Routing table");
//                    for(Map.Entry<Integer, StorageNodeHashSpace> e : stMetaData.getRoutingTable().entrySet()){
//                        log.info(e.getKey() + "    " + e.getValue().toString());
//                    }
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

    public double getRtVerstion() {
        return rtVerstion;
    }

    public void setRtVerstion(double rtVerstion) {
        this.rtVerstion = rtVerstion;
    }


}

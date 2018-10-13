package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.coordinator.Coordinator;
import edu.usfca.cs.dfs.coordinator.StorageNodeHashSpace;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * StorageNode class: the entrance for the storage node
 * storageNode will send requests to the coordinator and other
 * storageNodes and receive requests from client and other storage nodes
 */
public class StorageNode {

    public static final String HASH_ALGORITHM_SHA1 = "SHA1";
    public static final String DIR = "/bigdata/ssun28/";
    private static final int NTHREADS = 20;
    public static final int PORT = 37100;

    private ExecutorService executorService;
    private ServerSocket serverSocket = null;
    private boolean isStarted = true;
    private StMetaData stMetaData;
    private Hashtable<Integer, StorageNodeHashSpace> routingTable;
    private Hashtable<String, ArrayList<Integer>> allFilesPosTable;
    private Hashtable<String, Integer> numOfChunksTable;
    private StorageNodeInfo storageNodeInfo;
    private Hashtable<String, Chunk> chunksMap;
    private int requestsNum;

    private static Logger log;


    public StorageNode() {
        String snIp = getIpAddress();
        this.routingTable = new Hashtable<>();
        this.allFilesPosTable = new Hashtable<>();
        this.numOfChunksTable = new Hashtable<>();
        this.storageNodeInfo = new StorageNodeInfo(-1,snIp, false, 0.0, 0);
        this.chunksMap = new Hashtable<>();
        this.stMetaData = new StMetaData(routingTable, allFilesPosTable, numOfChunksTable, storageNodeInfo, chunksMap);
        this.requestsNum = 0;
        this.log = Logger.getLogger(StorageNode.class);
//        if(!createDirectory()){
//            System.out.println("Creating Directory failed!!");
//        }
        try {
            executorService = Executors.newFixedThreadPool(NTHREADS);
//            serverSocket = new ServerSocket(Coordinator.PORT);
            serverSocket = new ServerSocket(PORT);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean createDirectory() {
        File dirFile = new File(DIR);
        if(!dirFile.exists()){
            return dirFile.mkdir();
        }
        return true;
    }

    /**
     * Main start method
     * make a particular to do the heartbeat task
     * receive a socket and pick a thread to do that snSocket task
     */
    public void start() {
        HeartBeatTask hbTask = new HeartBeatTask(stMetaData);
        hbTask.start();
        System.out.println("Start to listening for the client");
        try {
            while(isStarted) {
                Socket socket = serverSocket.accept();
                log.info(socket.getRemoteSocketAddress().toString() + " has connected ");
                SnSocketTask snSocketTask = new SnSocketTask(socket, stMetaData);
                executorService.execute(snSocketTask);
//                while(true){
//                    StorageMessages.ProtoWrapper protoWrapper =
//                            StorageMessages.ProtoWrapper.parseDelimitedFrom(
//                                    socket.getInputStream());
//                    System.out.println("======================");
//                    if(protoWrapper == null){
//                        break;
//                    }
//                    String functionType = protoWrapper.getFunctionCase().toString();
//                    if(protoWrapper.getRequestor().equals("client")) {
//                        switch(functionType) {
//                            case "STORECHUNK":
//                                String requestor = protoWrapper.getRequestor();
//                                String ip = protoWrapper.getIp();
//                                StorageMessages.StoreChunk storeChunkMsg
//                                        = protoWrapper.getStoreChunk();
//                                String fileName = storeChunkMsg.getFileName();
//                                int chunkId = storeChunkMsg.getChunkId();
//                                System.out.println("chunkId = " + chunkId);
//                                String fileType = storeChunkMsg.getFileType();
//                                System.out.println("fileType = " + fileType);
//                                int numChunks = storeChunkMsg.getNumChunks();
//
//                                byte[] b = storeChunkMsg.getData().toByteArray();
//                                File file = new File(DIR + fileName + "_" + chunkId);
//
//                                try(FileOutputStream fo = new FileOutputStream(file)) {
//                                    fo.write(b);
//                                } catch (IOException e) {
//                                    e.printStackTrace();
//                                }
//                                break;
//
//                        }
//                    }
//                    System.out.println("requestor is "+ protoWrapper.getRequestor());
//                    System.out.println("IP is "+ protoWrapper.getIp());
//                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the ip address of the current host.
     *
     * @return ip address
     */
    private String getIpAddress() {
        InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getLocalHost();
            System.out.println(getLocalDataTime() + " Starting storage node on " + inetAddress.getHostAddress() + "  ...");
            return inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Get the local time
     * @return
     */
    private String getLocalDataTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    public static void main(String[] args) {
        String filePath = System.getProperty("user.dir")
                + "/log4j.properties";
        PropertyConfigurator.configure(filePath);
        StorageNode storageNode = new StorageNode();
        storageNode.start();
    }
}

package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StorageNode {

    private static final String DIR = "./output/";
    public static final int PORT = 37000;
    private ServerSocket serverSocket = null;
    private Socket socket;
    private InetAddress inetAddress;
    private boolean isStarted = true;
    private int requestsNum;


    public StorageNode() {
        this.requestsNum = 0;
        if(!createDirectory()){
            System.out.println("Creating Directory failed!!");
        }
        try {
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

    public void start() {
        getIpAddress();

        try {
            while (isStarted) {
                socket = serverSocket.accept();

                System.out.println(getLocalDataTime() + " New connection from " + socket.getRemoteSocketAddress()+ " is connected!");
                while(true){
                    StorageMessages.ProtoWrapper protoWrapper =
                            StorageMessages.ProtoWrapper.parseDelimitedFrom(
                                    socket.getInputStream());
                    System.out.println("======================");
                    if(protoWrapper == null){
                        break;
                    }
                    String functionType = protoWrapper.getFunctionCase().toString();
                    if (protoWrapper.getRequestor().equals("client")) {
                        switch (functionType) {
                            case "STORECHUNK":
                                String requestor = protoWrapper.getRequestor();
                                String ip = protoWrapper.getIp();
                                StorageMessages.StoreChunk storeChunkMsg
                                        = protoWrapper.getStoreChunk();
                                String fileName = storeChunkMsg.getFileName();
                                int chunkId = storeChunkMsg.getChunkId();
                                int numChunks = storeChunkMsg.getNumChunks();

                                byte[] b = storeChunkMsg.getData().toByteArray();
                                File file = new File(DIR + fileName + "_" + chunkId);

                                try (FileOutputStream fo = new FileOutputStream(file)) {
                                    fo.write(b);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                        }
                    }
                    System.out.println("requestor is "+ protoWrapper.getRequestor());
                    System.out.println("IP is "+ protoWrapper.getIp());
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void getIpAddress() {
        try {
            inetAddress = InetAddress.getLocalHost();
            System.out.println(getLocalDataTime() + " Starting storage node on " + inetAddress.getHostAddress() + "  ...");
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private String getLocalDataTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    public static void main(String[] args) {
//        String hostname = getHostname();
//        System.out.println("Starting storage node on " + hostname + "...");
        StorageNode storageNode = new StorageNode();
        storageNode.start();
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname()
    throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

}

package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;

public class SnSocketTask implements Runnable{

    private static final String DIR = "./bigdata/ssun28/";

    private Socket socket;
    private StMetaData stMetaData;
    private StorageMessages.ProtoWrapper protoWrapperIn;
    private StorageMessages.ProtoWrapper protoWrapperOut;

    public SnSocketTask(Socket socket, StMetaData stMetaData) {
        this.socket = socket;
        this.stMetaData = stMetaData;
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

                stMetaData.increaseReqNum();
                if(requestor.equals("client")) {
                    clientRequest(functionType);
                }else if (requestor.equals("storageNode")) {
                    storageNodeRequest(functionType);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            quit();
        }
    }

    private void clientRequest(String functionType) {
        switch(functionType) {
            case "ASKPOSITION":
                askPosition();
                break;
            case "STORECHUNK":
                storeFirstChunk();
                break;
            case "RETRIEVEFILE":
                break;
            case "NODEFILESLIST":
                break;
            default: break;
        }
    }

    private void askPosition() {
        try {
            String fileNameWithType = protoWrapperIn.getAskPosition();
            MessageDigest mDigest = MessageDigest.getInstance("SHA1");

            byte[] bytes = mDigest.digest(fileNameWithType.getBytes());
            int hash16bits = bytesToInt(bytes);

            ArrayList<Integer> nodeIdList = stMetaData.getNodeIdList();
            Collections.sort(nodeIdList);
            int nodesNum = nodeIdList.size();
            int result = hash16bits / ((int)Math.pow(2, 16) / nodesNum);

            int nodeId = nodeIdList.get(result);
            String positionNodeIp = stMetaData.getPositionNodeIp(nodeId);

            StorageMessages.ReturnPosition returnPositionMsg
                    = StorageMessages.ReturnPosition.newBuilder()
                    .setNodeId(nodeId)
                    .setToStoreNodeIp(positionNodeIp)
                    .build();

            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor("storageNode")
                    .setIp(stMetaData.getStorageNodeInfo().getNodeIp())
                    .setReturnPosition(returnPositionMsg)
                    .build();

            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int bytesToInt(byte[] bytes) {
        int b0 = bytes[0] & 0xFF;
        int b1 = bytes[1] & 0xFF;

        return (b0 << 8) | b1 ;
    }

    private void storeFirstChunk() {

    }

    private boolean storeChunk() {
        if(!createDirectory()){
            System.out.println("Creating Directory failed!!");
        }
        StorageMessages.StoreChunk storeChunkMsg
                = protoWrapperIn.getStoreChunk();
        String fileName = storeChunkMsg.getFileName();
        int chunkId = storeChunkMsg.getChunkId();
        String fileType = storeChunkMsg.getFileType();
        int numChunks = storeChunkMsg.getNumChunks();


        byte[] b = storeChunkMsg.getData().toByteArray();
        File file = new File(DIR + fileName + "_" + chunkId);

        try(FileOutputStream fo = new FileOutputStream(file)) {
            fo.write(b);
            System.out.println("Store " + fileName + "_" + chunkId + fileType + " Successfully!");
            return true;
        } catch (IOException e) {
            System.out.println("Store " + fileName + "_" + chunkId + fileType + " Failed!");
            e.printStackTrace();
        }
        System.out.println("Store " + fileName + "_" + chunkId + fileType + " Failed!");
        return false;
    }

    private boolean createDirectory() {
        File dirFile = new File(DIR);
        if(!dirFile.exists()){
            return dirFile.mkdir();
        }
        return true;
    }

    private void storageNodeRequest(String functionType) {
        switch(functionType) {
            case "UPDATEALLFILESTABLE":
                break;
            case "STORECHUNK":
                storeChunkCopy();
                break;
            default: break;
        }
    }

    private void storeChunkCopy() {

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

package edu.usfca.cs.dfs.storageNode;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SnSocketTask implements Runnable{

    public static final String STORAGENODE = "storageNode";
    public static final String COORDINATOR = "coordinator";
    public static final String CLIENT = "client";
    public static final String DIR = "./bigdata/ssun28/";
    private static final int PORT = 37000;

    private Socket socket;
    private StMetaData stMetaData;
    private StorageMessages.ProtoWrapper protoWrapperIn;
    private StorageMessages.ProtoWrapper protoWrapperOut;
    private String oriNodeIp;

    public SnSocketTask(Socket socket, StMetaData stMetaData) {
        this.socket = socket;
        this.stMetaData = stMetaData;
        this.oriNodeIp = stMetaData.getStorageNodeInfo().getNodeIp();
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
                if(requestor.equals(CLIENT)) {
                    clientRequest(functionType);
                }else if (requestor.equals(STORAGENODE)) {
                    storageNodeRequest(functionType);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            quit();
        }
    }

    /**
     * All requests from client
     * @param functionType
     */
    private void clientRequest(String functionType) {
        switch(functionType) {
            case "ASKPOSITION":
                askPosition();
                break;
            case "STORECHUNK":
                try {
                    if(store3Chunks()) {
                        protoWrapperOut =
                                StorageMessages.ProtoWrapper.newBuilder()
                                        .setRequestor(STORAGENODE)
                                        .setIp(oriNodeIp)
                                        .setResponse("Store chunk successfully!")
                                        .build();
                    }else {
                        protoWrapperOut =
                                StorageMessages.ProtoWrapper.newBuilder()
                                        .setRequestor(STORAGENODE)
                                        .setIp(oriNodeIp)
                                        .setResponse("Store chunk failed. Please try it again!")
                                        .build();
                    }
                    protoWrapperOut.writeDelimitedTo(socket.getOutputStream());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case "RETRIEVEFILE":
                StorageMessages.RetrieveFile retrieveFileMsgIn
                        = protoWrapperIn.getRetrieveFile();
                String retreiveFuntionType = retrieveFileMsgIn.getFunctionCase().toString();

                if(retreiveFuntionType.equals("ASKCHUNKSPOS")) {
                    askChunksPos(retrieveFileMsgIn);
                }else if(retreiveFuntionType.equals("RETRIEVECHUNK")) {
                    retrieveChunk(retrieveFileMsgIn);
                }
                break;
            case "ASKINFO":
                nodeFilesList();
                break;
            default: break;
        }
    }

    /**
     * Ask position request from client(with chunk name)
     * hash the give chunk name and
     * send back the positioned storage node nodeId and nodeIp
     */
    private void askPosition() {
        try {
            String fileNameWithType = protoWrapperIn.getAskPosition();
            MessageDigest mDigest = MessageDigest.getInstance("SHA1");

            byte[] bytes = mDigest.digest(fileNameWithType.getBytes());
            int hash16bits = bytesToInt(bytes);

            ///////////
            /////
            ArrayList<Integer> nodeIdList = stMetaData.getNodeIdList();
            Collections.sort(nodeIdList);
            int nodesNum = nodeIdList.size();
            int result = hash16bits / ((int)Math.pow(2, 16) / nodesNum);

            int[] nodeIdArray = new int[nodesNum];
            for(int i = 0; i < nodeIdArray.length; i++) {
                nodeIdArray[i] = nodeIdList.get(i);
            }

            int nodeId = nodeIdArray[result];
//            int nodeId = nodeIdList.get(result);
            String positionNodeIp = stMetaData.getPositionNodeIp(nodeId);

            StorageMessages.ReturnPosition returnPositionMsg
                    = StorageMessages.ReturnPosition.newBuilder()
                    .setNodeId(nodeId)
                    .setToStoreNodeIp(positionNodeIp)
                    .build();

            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor(STORAGENODE)
                    .setIp(oriNodeIp)
                    .setReturnPosition(returnPositionMsg)
                    .build();

            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the 16 bits result from the bytes
     * @param bytes
     * @return
     */
    private int bytesToInt(byte[] bytes) {
        int b0 = bytes[0] & 0xFF;
        int b1 = bytes[1] & 0xFF;

        return (b0 << 8) | b1 ;
    }

    /**
     * Store chunk on this storage Node, update the storage Node's chunksList,
     * send copies to the other 2 nodes and update the RoutingTable
     * @return
     */
    private boolean store3Chunks() {
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

            Chunk chunk = new Chunk(fileName, chunkId, fileType, b, numChunks);
            stMetaData.addChunkToChunksList(chunk);
            String inputFileChunk = fileName+ "_" + chunkId + fileType;
            int nodeId = stMetaData.getStorageNodeInfo().getNodeId();
            stMetaData.updateAllFilesPosTable(inputFileChunk, nodeId);
            System.out.println("Store " + fileName + "_" + chunkId + fileType + " Successfully!");

            TreeSet<Integer> nodeIdSetSuccess = new TreeSet<>();

            while(nodeIdSetSuccess.size() < 2) {
                int[] nodeIdArray = stMetaData.get2ChunkCopyNodeId(stMetaData.getStorageNodeInfo().getNodeId());
                for(int i = 0; i < nodeIdArray.length; i++) {



                    //////if socket failed
                    Socket copyChunkSocket = new Socket();
                    String copyChunkNodeIp = stMetaData.getRoutingTable().get(nodeIdArray[i]).getNodeIp();
                    InetAddress serverIP = InetAddress.getByName(copyChunkNodeIp);
                    copyChunkSocket.connect(new InetSocketAddress(serverIP, PORT), 2000);

                    StorageMessages.StoreChunk chunkMsgOut
                            = StorageMessages.StoreChunk.newBuilder()
                            .setFileName(fileName)
                            .setChunkId(chunkId)
                            .setFileType(fileType)
                            .setData(storeChunkMsg.getData())
                            .setNumChunks(numChunks)
                            .build();

                    StorageMessages.ProtoWrapper protoWrapperOut =
                            StorageMessages.ProtoWrapper.newBuilder()
                                    .setRequestor(STORAGENODE)
                                    .setIp(oriNodeIp)
                                    .setStoreChunk(chunkMsgOut)
                                    .build();
                    protoWrapperOut.writeDelimitedTo(copyChunkSocket.getOutputStream());

                    StorageMessages.ProtoWrapper protoWrapperIn =
                            StorageMessages.ProtoWrapper.parseDelimitedFrom(
                                    copyChunkSocket.getInputStream());

                    String response = protoWrapperIn.getResponse();
                        if (response.equals("success") && nodeIdSetSuccess.size() < 2) {
                            nodeIdSetSuccess.add(nodeIdArray[i]);
                            continue;
                        }
                }
            }
            updateOthersAllFilesPosTable(inputFileChunk, nodeId);
//            for(Integer i : nodeIdSetSuccess) {
//                stMetaData.updateAllFilesPosTable(inputFileChunk, i);
//            }

            return true;
        } catch (UnknownHostException e) {

            e.printStackTrace();
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

    /**
     * Given a file name to get the nodeIds list store that all file chunks
     * give the response with nodeId list and nodeIp table
     * @param retrieveFileMsgIn
     */
    private void askChunksPos(StorageMessages.RetrieveFile retrieveFileMsgIn) {
        String fileName;
        String fileType = "";
        String fileNameWithType = retrieveFileMsgIn.getAskChunksPos();

        if(fileNameWithType.contains(".")) {
            fileName = fileNameWithType.split("\\.")[0];
            fileType = "." + fileNameWithType.split("\\.")[1];
        }else {
            fileName = fileNameWithType;
        }
        Hashtable<String, StorageMessages.NodeIdList> retrieveChunksPosTable = stMetaData.getRetrieveChunksPos(fileName, fileType);
        Hashtable<Integer, String> nodeIpTable = stMetaData.getNodeIpTable();

        try {
            StorageMessages.ResChunksPos resChunksPosMsgOut
                    = StorageMessages.ResChunksPos.newBuilder()
                    .putAllChunksPos(retrieveChunksPosTable)
                    .putAllNodeIpTable(nodeIpTable)
                    .build();

            StorageMessages.RetrieveFile retrieveFileMsgOut
                    = StorageMessages.RetrieveFile.newBuilder()
                    .setResChunksPos(resChunksPosMsgOut)
                    .build();

            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor(STORAGENODE)
                    .setIp(oriNodeIp)
                    .setRetrieveFile(retrieveFileMsgOut)
                    .build();

            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Give a chunk info and give back that chunk data to the client
     * @param retrieveFileMsgIn
     */
    private void retrieveChunk(StorageMessages.RetrieveFile retrieveFileMsgIn) {
        StorageMessages.StoreChunk retreiveChunkMsgIn = retrieveFileMsgIn.getRetrieveChunk();
        String fileName = retreiveChunkMsgIn.getFileName();
        int chunkId = retreiveChunkMsgIn.getChunkId();
        String fileType = retreiveChunkMsgIn.getFileType();

        Chunk chunk = stMetaData.getChunk(fileName, chunkId, fileType);

        StorageMessages.RetrieveFile retrieveFileMsgOut;
        if(chunk != null) {
            ByteString data = ByteString.copyFrom(chunk.getData());
            StorageMessages.StoreChunk retrieveChunkMsgOut
                    = StorageMessages.StoreChunk.newBuilder()
                    .setFileName(fileName)
                    .setChunkId(chunkId)
                    .setFileType(fileType)
                    .setData(data)
                    .setNumChunks(chunk.getNumChunks())
                    .build();
            retrieveFileMsgOut =
                    StorageMessages.RetrieveFile.newBuilder()
                    .setRetrieveChunk(retrieveChunkMsgOut)
                    .build();

        }else {
            retrieveFileMsgOut =
                    StorageMessages.RetrieveFile.newBuilder()
                    .setResChunkStatus("false")
                    .build();
        }

        protoWrapperOut =
                StorageMessages.ProtoWrapper.newBuilder()
                .setRequestor(STORAGENODE)
                .setIp(oriNodeIp)
                .setRetrieveFile(retrieveFileMsgOut)
                .build();

        try {
            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get nodeFiles list from chunks list
     */
    private void nodeFilesList() {
        try {
            ArrayList<StorageMessages.StoreChunk> nodeFilesList = stMetaData.getNodeFilesList();
            StorageMessages.NodeFilesList nodeFilesListMsg =
                    StorageMessages.NodeFilesList.newBuilder()
                            .addAllStoreChunk(nodeFilesList)
                            .build();

            StorageMessages.AskInfo askInfoMsgOut
                    = StorageMessages.AskInfo.newBuilder()
                    .setResNodeFilesList(nodeFilesListMsg)
                    .build();

            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                            .setRequestor(STORAGENODE)
                            .setIp(oriNodeIp)
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
        switch(functionType) {
            case "UPDATEALLFILESTABLE":
                updateAllFilesTable();
                break;
            case "STORECHUNK":
                storeChunkCopy();
                break;
            default: break;
        }
    }

    /**
     * Update the all files table in this storage node
     */
    private void updateAllFilesTable() {
        StorageMessages.UpdateAllFilesTable updateAllFilesTableMsg
                = protoWrapperIn.getUpdateAllFilesTable();
        String inputFileChunk = updateAllFilesTableMsg.getInputFileChunk();
        int nodeId = updateAllFilesTableMsg.getNodeId();
        stMetaData.updateAllFilesPosTable(inputFileChunk, nodeId);
    }

    /**
     * Store chunk copy in this storage node from the other storage node sent
     */
    private void storeChunkCopy() {
        if (!createDirectory()) {
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

        try (FileOutputStream fo = new FileOutputStream(file)) {
            fo.write(b);

            Chunk chunk = new Chunk(fileName, chunkId, fileType, b, numChunks);
            stMetaData.addChunkToChunksList(chunk);
            String inputFileChunk = fileName+ "_" + chunkId + fileType;
            int nodeId = stMetaData.getStorageNodeInfo().getNodeId();
            stMetaData.updateAllFilesPosTable(inputFileChunk, nodeId);
            System.out.println("Store " + inputFileChunk + " Successfully!");


            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor(STORAGENODE)
                    .setIp(oriNodeIp)
                    .setResponse("success")
                    .build();

            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());
//            String inputFileChunk = fileName + "_" + chunkId + fileType;
//            int nodeId = stMetaData.getStorageNodeInfo().getNodeId();
//            stMetaData.updateAllFilesPosTable(inputFileChunk, nodeId);
            updateOthersAllFilesPosTable(inputFileChunk, nodeId);
            System.out.println("Store " + fileName + "_" + chunkId + fileType + " Successfully!");
        } catch (IOException e) {
            System.out.println("Store " + fileName + "_" + chunkId + fileType + " Failed!");
            e.printStackTrace();
        }
    }

    /**
     * Update all the other storage nodes' all files position table
     * multi-thread
     * @param inputFileChunk
     * @param nodeId
     */
    private void updateOthersAllFilesPosTable(String inputFileChunk, int nodeId) {
        ExecutorService executorService =
                Executors.newFixedThreadPool(Runtime.getRuntime()
                        .availableProcessors() * 20);
        ArrayList<String> nodeIpList = stMetaData.getNodeIpList();
        for(String desNodeIp : nodeIpList) {

            if(desNodeIp.equals(oriNodeIp)){
                continue;
            }
            executorService.execute(new UpdateFilesTableTask(oriNodeIp, desNodeIp, inputFileChunk, nodeId));
        }

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

    ///what if connect failed
    public class UpdateFilesTableTask implements Runnable {

        private Socket socket;
        private InetAddress serverIP;
        private String oriNodeIp;
        private String desNodeIp;
        private String inputFileChunk;
        private int nodeId;

        public UpdateFilesTableTask(String oriNodeIp, String desNodeIp, String inputFileChunk, int nodeId) {
            this.socket = new Socket();
            this.oriNodeIp = oriNodeIp;
            this.desNodeIp = desNodeIp;
            this.inputFileChunk = inputFileChunk;
            this.nodeId = nodeId;
        }

        public void run() {
            try {
                serverIP = InetAddress.getByName(desNodeIp);
                socket.connect(new InetSocketAddress(serverIP, PORT), 2000);

                StorageMessages.UpdateAllFilesTable updateAllFilesTableMsg
                        = StorageMessages.UpdateAllFilesTable.newBuilder()
                        .setInputFileChunk(inputFileChunk)
                        .setNodeId(nodeId)
                        .build();

                StorageMessages.ProtoWrapper protoWrapperOut =
                        StorageMessages.ProtoWrapper.newBuilder()
                        .setRequestor(STORAGENODE)
                        .setIp(oriNodeIp)
                        .setUpdateAllFilesTable(updateAllFilesTableMsg)
                        .build();

                protoWrapperOut.writeDelimitedTo(socket.getOutputStream());

                socket.close();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

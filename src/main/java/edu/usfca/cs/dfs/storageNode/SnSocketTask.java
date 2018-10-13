package edu.usfca.cs.dfs.storageNode;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.client.Client;
import edu.usfca.cs.dfs.client.RetrieveChunkTask;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SnSocketTask implements Runnable{

    public static final String STORAGENODE = "storageNode";
    public static final String COORDINATOR = "coordinator";
    public static final String CLIENT = "client";
    public static final String DIR = "/bigdata/ssun28/";
    private static final int CHUNKSIZE = 8000000;
    private static final int PORT = 37100;

    private Socket socket;
    private StMetaData stMetaData;
    private StorageMessages.ProtoWrapper protoWrapperIn;
    private StorageMessages.ProtoWrapper protoWrapperOut;
    private String oriNodeIp;
    private static Logger log;

    public SnSocketTask(Socket socket, StMetaData stMetaData) {
        this.socket = socket;
        this.stMetaData = stMetaData;
        this.oriNodeIp = stMetaData.getStorageNodeInfo().getNodeIp();
        log = Logger.getLogger(SnSocketTask.class);
    }

    public void run() {
        try {
            protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    socket.getInputStream());
            String requestor = protoWrapperIn.getRequestor();
            String functionType = protoWrapperIn.getFunctionCase().toString();

            log.info("A " + requestor + "(" + protoWrapperIn.getIp()+ " ) has connected to Storage Node " + stMetaData.getStorageNodeInfo().getNodeId());

            stMetaData.increaseReqNum();
            if(requestor.equals(CLIENT)) {
                clientRequest(functionType);
            }else if(requestor.equals(STORAGENODE)) {
                storageNodeRequest(functionType);
            }else if(requestor.equals(COORDINATOR)) {
                failNodeFilesSearch();
            }
        } catch (IOException e) {
            e.printStackTrace();
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
                quit();
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
                quit();
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
                quit();
                break;
            case "ASKINFO":
                nodeFilesList();
                quit();
                break;
            default: break;
        }
    }

    /**
     * Ask position request from client(with chunk name)
     * checkSum the give chunk name and
     * send back the positioned storage node nodeId and nodeIp
     */
    private void askPosition() {
        try {
            String fileNameWithType = protoWrapperIn.getAskPosition();
            MessageDigest mDigest = MessageDigest.getInstance(StorageNode.HASH_ALGORITHM_SHA1);

            byte[] bytes = mDigest.digest(fileNameWithType.getBytes());
            int hash16bits = bytesToInt(bytes);

//            log.info(fileNameWithType + "'s checkSum code is " + Integer.toHexString(hash16bits));


            ArrayList<Integer> nodeIdList = stMetaData.getNodeIdList();
            Collections.sort(nodeIdList);
            int nodesNum = nodeIdList.size();
            int pieceSize = ((int)Math.pow(2, 16) / nodesNum);
            System.out.println("piece size is" + pieceSize);
            int result = hash16bits / pieceSize;

            int nodeId = nodeIdList.get(result);

            log.info(fileNameWithType +" will store on node " + result + "-----" + nodeId);

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
        int chunkSize = storeChunkMsg.getChunkSize();
        byte[] b = storeChunkMsg.getData().toByteArray();

        String msgInCheckSum = storeChunkMsg.getChunkCheckSum();
        String chunkCheckSum = toHex(b);

        while(!msgInCheckSum.equals(chunkCheckSum)) {
            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                            .setRequestor(STORAGENODE)
                            .setIp(oriNodeIp)
                            .setResponse("CheckSums not equal")
                            .build();
            try {
                protoWrapperOut.writeDelimitedTo(socket.getOutputStream());

                protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                        socket.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }

            storeChunkMsg = protoWrapperIn.getStoreChunk();
            fileName = storeChunkMsg.getFileName();
            chunkId = storeChunkMsg.getChunkId();
            fileType = storeChunkMsg.getFileType();
            numChunks = storeChunkMsg.getNumChunks();
            chunkSize = storeChunkMsg.getChunkSize();
            b = storeChunkMsg.getData().toByteArray();

            msgInCheckSum = storeChunkMsg.getChunkCheckSum();
            chunkCheckSum = toHex(b);
        }

        protoWrapperOut =
                StorageMessages.ProtoWrapper.newBuilder()
                        .setRequestor(STORAGENODE)
                        .setIp(oriNodeIp)
                        .setResponse("CheckSums equal")
                        .build();

        try {
            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

        stMetaData.updateNumOfChunks(fileName, fileType, numChunks);

        File file = new File(DIR + fileName + "_" + chunkId + fileType);

        log.info(fileName + "____"+ chunkId + "____" + fileType);

        try(FileOutputStream fo = new FileOutputStream(file)) {
            fo.write(b);

            Chunk chunk = new Chunk(fileName, chunkId, fileType, numChunks, chunkSize, chunkCheckSum);

            String inputFileChunk = fileName+ "_" + chunkId + fileType;
            stMetaData.addChunkToChunksMap(inputFileChunk, chunk);

            int nodeId = stMetaData.getStorageNodeInfo().getNodeId();
            stMetaData.updateAllFilesPosTable(inputFileChunk, nodeId);
            log.info("Store " + fileName + "_" + chunkId + fileType + " Successfully!");

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
                            .setChunkSize(chunkSize)
                            .setChunkCheckSum(chunkCheckSum)
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
                    }
                }
            }

            updateOthersAllFilesPosTable(inputFileChunk, nodeId);

            for(int i : nodeIdSetSuccess){
                log.info("one of the back up is stored on " + i);
            }
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

    /**
     * Create a directory if it doesn't exist
     * @return
     */
    private boolean createDirectory() {
        File dirFile = new File(DIR);
        if(!dirFile.exists()){
            return dirFile.mkdir();
        }
        return true;
    }

    /**
     * Bytes to hex String
     * @param bytes
     * @return
     */
    private String toHex(byte[] bytes) {
        byte[] hash = checkSum(bytes);
        if(hash == null) {
            System.err.println("chunk checkSum is null");
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /***
     * Compute the SHA-1 hash of the given byte array
     * @param hashBytes
     * @return
     */
    private byte[] checkSum(byte[] hashBytes) {
        try {
            MessageDigest md = MessageDigest.getInstance(StorageNode.HASH_ALGORITHM_SHA1);
            return md.digest(hashBytes);
        } catch (NoSuchAlgorithmException n) {
            System.err.println("SHA-1 algorithm is not available...");
        }
        return null;
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

        log.info("Client is asking the chunk pos of "+ fileNameWithType);

        int numOfChunks = stMetaData.getNumOfChunksTable().get(fileNameWithType);
        Hashtable<String, StorageMessages.NodeIdList> retrieveChunksPosTable = new Hashtable<>();
        for(int i = 0; i < numOfChunks; i++){
            String chunkName = fileName+"_"+i+fileType;
            StorageMessages.NodeIdList list = stMetaData.getRetrieveChunksPos(chunkName);
            retrieveChunksPosTable.put(chunkName, list);
        }


//        Hashtable<String, StorageMessages.NodeIdList> retrieveChunksPosTable = stMetaData.getRetrieveChunksPos(fileName, fileType);
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
    private synchronized void retrieveChunk(StorageMessages.RetrieveFile retrieveFileMsgIn) {
        StorageMessages.StoreChunk retreiveChunkMsgIn = retrieveFileMsgIn.getRetrieveChunk();
        String chunkName = retreiveChunkMsgIn.getFileName();

        Chunk chunk = stMetaData.getChunk(chunkName);

        StorageMessages.RetrieveFile retrieveFileMsgOut = null;
        StorageMessages.StoreChunk retrieveChunkMsgOut = null;
        try {
    //        if(chunk != null) {
    //            byte[] bytes= readFromDisk(fileName, chunkId, fileType, chunk.getSize());
            byte[] bytes= readFromDisk(chunkName, Client.CHUNKSIZE);
            if(bytes != null) {

                String checkSum = toHex(bytes);
                if(checkSum.equals(chunk.getCheckSum())) {
                    ByteString data = ByteString.copyFrom(bytes);
                    retrieveChunkMsgOut(chunk, data);
                    protoWrapperIn =
                            StorageMessages.ProtoWrapper.parseDelimitedFrom(
                                    socket.getInputStream());

                    while(!protoWrapperIn.getResponse().equals("CheckSums equal")) {
                        retrieveChunkMsgOut(chunk, data);
                        protoWrapperIn =
                                StorageMessages.ProtoWrapper.parseDelimitedFrom(
                                        socket.getInputStream());
                        System.out.println("protoWrapperMsgIn.getResponse() = " + protoWrapperIn.getResponse());
                    }
                }else {
                    log.error("checkSum is not equal to the correct one!!!");
                    getCopy(chunkName, chunk, retrieveFileMsgIn);

                }
            }else {

                log.error("The original file has been deleted!!");
                getCopy(chunkName, chunk, retrieveFileMsgIn);
//                retrieveFileMsgOut =
//                        StorageMessages.RetrieveFile.newBuilder()
//                        .setResChunkStatus("false")
//                        .build();
//                protoWrapperOut =
//                        StorageMessages.ProtoWrapper.newBuilder()
//                                .setRequestor(STORAGENODE)
//                                .setIp(oriNodeIp)
//                                .setRetrieveFile(retrieveFileMsgOut)
//                                .build();
//                protoWrapperOut.writeDelimitedTo(socket.getOutputStream());

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getCopy(String chunkName, Chunk chunk, StorageMessages.RetrieveFile retrieveFileMsgIn) {


        int nodeId = getNextNode(chunkName, stMetaData.getStorageNodeInfo().getNodeId());
        String nodeIp = stMetaData.getRoutingTable().get(nodeId).getNodeIp();
        Hashtable<Integer, byte[]> chunkTable = new Hashtable<>();

        Runnable r = new RetrieveChunkTask(chunkName,
                nodeId,
                nodeIp,
                oriNodeIp,
                chunkTable);
        r.run();

        setNewFile(chunkTable, chunk.getChunkId(), chunkName, chunk.getSize());
        log.info("Has reset the local file to the correct one!!!");
        retrieveChunk(retrieveFileMsgIn);
    }

    private void setNewFile(Hashtable<Integer, byte[]> table, int chunkId, String chunkName, int size){
        File file = new File(DIR + chunkName);

        log.info("Replace the corrupted file chunk " + chunkName);

        byte[] data = table.get(chunkId);
        data = Arrays.copyOfRange(data, 0, size);

        try(FileOutputStream fo = new FileOutputStream(file)) {
            fo.write(data);
        } catch(IOException e){
            e.printStackTrace();
        }

    }


    private int getNextNode(String chunkName, int nodeId) {
        ArrayList<Integer> idList = stMetaData.getFilePos(chunkName);
        int nextId = nodeId;
        int i = 0;
        while(nodeId == nextId){
            nextId = idList.get(i);
            i = (i + 1) % idList.size();
        }
        return nextId;
    }
    /**
     * Construct a retrieveChunkMsgOut protoWrapper
     * @param c
     * @param data
     */
    private void retrieveChunkMsgOut(Chunk c, ByteString data) {
        StorageMessages.StoreChunk retrieveChunkMsgOut
                = StorageMessages.StoreChunk.newBuilder()
                .setFileName(c.getFileName())
                .setChunkId(c.getChunkId())
                .setFileType(c.getFileType())
                .setData(data)
                .setNumChunks(c.getNumChunks())
                .setChunkSize(c.getSize())
                .setChunkCheckSum(c.getCheckSum())
                .build();

        StorageMessages.RetrieveFile retrieveFileMsgOut =
                StorageMessages.RetrieveFile.newBuilder()
                        .setRetrieveChunk(retrieveChunkMsgOut)
                        .build();

        protoWrapperOut =
                StorageMessages.ProtoWrapper.newBuilder()
                        .setRequestor(STORAGENODE)
                        .setIp(oriNodeIp)
                        .setRetrieveFile(retrieveFileMsgOut)
                        .build();
        try {
            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[] readFromDisk(String inputFile, int chunkSize) {
        File f = new File(DIR + inputFile);
        byte[] data = new byte[chunkSize];
        try (FileInputStream fs = new FileInputStream(f)){
            fs.read(data);
            return data;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
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
        quit();
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
        int chunkSize = storeChunkMsg.getChunkSize();
        String checkSum = storeChunkMsg.getChunkCheckSum();

        stMetaData.updateNumOfChunks(fileName, fileType, numChunks);

        byte[] b = storeChunkMsg.getData().toByteArray();
        File file = new File(DIR + fileName + "_" + chunkId + fileType);

        try (FileOutputStream fo = new FileOutputStream(file)) {
            fo.write(b);

            Chunk chunk = new Chunk(fileName, chunkId, fileType, numChunks, chunkSize, checkSum);
            String inputFileChunk = fileName+ "_" + chunkId + fileType;
            stMetaData.addChunkToChunksMap(inputFileChunk, chunk);

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

    private void failNodeFilesSearch() {
        StorageMessages.RemoveNode removeNodeMsgIn = protoWrapperIn.getRemoveNode();
        int failNodeId = removeNodeMsgIn.getFailNodeId();
        Hashtable<String, Hashtable<String, String>> filesOnFailNodeTable = stMetaData.filesOnFailNode(failNodeId);
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

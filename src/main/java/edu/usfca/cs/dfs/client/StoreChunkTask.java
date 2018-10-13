package edu.usfca.cs.dfs.client;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.storageNode.StorageNode;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * StoreChunkTask class: parallel store one chunk of file data 
 */
public class StoreChunkTask implements Runnable {

    private ClientMetaData clientMetaData;
    private int chunkId;
    private byte[] bytes;
    private ByteString data;
    private int chunkSize;
    private String positionNodeIp;
    private static Logger log;

    public StoreChunkTask(ClientMetaData clientMetaData, int chunkId, byte[] bytes, int chunkSize) {
        this.clientMetaData = clientMetaData;
        this.chunkId = chunkId;
        this.bytes = bytes;
//        this.data = data;
        this.chunkSize = chunkSize;
        log = Logger.getLogger(StoreChunkTask.class);
    }

    /**
     * Main run method
     */
    public void run() {
        askPosition();
        storeChunkData();
    }

    /**
     * Construct a fileNameWithType : fileName + chunkId + fileType, and
     * ask that chunk's position from storage node
     * get back a position nodeIp
     */
    private void askPosition() {
        try {
            Socket socket = new Socket();
            System.out.println("ServerIp = " + clientMetaData.getServerIP().toString());
//            socket.connect(new InetSocketAddress(clientMetaData.getServerIP(), Coordinator.PORT), 2000);
            socket.connect(new InetSocketAddress(clientMetaData.getServerIP(), StorageNode.PORT), 2000);

            String fileNameWithType = clientMetaData.getStoreFileName() + "_" + chunkId + clientMetaData.getStoreFileType();
            StorageMessages.ProtoWrapper protoWrapperMsgOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor(Client.CLIENT)
                    .setIp(clientMetaData.getClientIp())
                    .setAskPosition(fileNameWithType)
                    .build();

            protoWrapperMsgOut.writeDelimitedTo(socket.getOutputStream());
            StorageMessages.ProtoWrapper protoWrapperMsgIn =
                    StorageMessages.ProtoWrapper.parseDelimitedFrom(
                            socket.getInputStream());
            StorageMessages.ReturnPosition returnPositionMsgIn = protoWrapperMsgIn.getReturnPosition();

            int nodeId = returnPositionMsgIn.getNodeId();
            positionNodeIp = returnPositionMsgIn.getToStoreNodeIp();

            log.info(chunkId + " should be store on node"+ nodeId + " with IP " + positionNodeIp);

            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Store that chunk data to the position storage node
     */
    private void storeChunkData() {
        try {
            Socket socket = new Socket();
            InetAddress toStoreSnIP = InetAddress.getByName(positionNodeIp);
//            socket.connect(new InetSocketAddress(toStoreSnIP, Coordinator.PORT), 2000);
            socket.connect(new InetSocketAddress(toStoreSnIP, StorageNode.PORT), 2000);

            String chunkCheckSum = toHex(bytes);

//            ByteString data = ByteString.copyFrom(bytes, 0, chunkSize);
            ByteString data = ByteString.copyFrom(bytes, 0, Client.CHUNKSIZE);

            StorageMessages.StoreChunk storeChunkMsgOut
                = StorageMessages.StoreChunk.newBuilder()
                .setFileName(clientMetaData.getStoreFileName())
                .setFileType(clientMetaData.getStoreFileType())
                .setChunkId(chunkId)
                .setData(data)
                .setNumChunks(clientMetaData.getNumChunks())
                .setChunkSize(chunkSize)
                .setChunkCheckSum(chunkCheckSum)
                .build();

            StorageMessages.ProtoWrapper protoWrapperMsgOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                            .setRequestor(Client.CLIENT)
                            .setIp(clientMetaData.getClientIp())
                            .setStoreChunk(storeChunkMsgOut)
                            .build();
            protoWrapperMsgOut.writeDelimitedTo(socket.getOutputStream());

            StorageMessages.ProtoWrapper protoWrapperMsgIn =
                    StorageMessages.ProtoWrapper.parseDelimitedFrom(
                            socket.getInputStream());

//            socketCheckSumUntilSuccess(protoWrapperMsgIn, protoWrapperMsgOut, storeChunkMsgOut);
            while(!protoWrapperMsgIn.getResponse().equals("CheckSums equal")) {
                storeChunkMsgOut
                        = StorageMessages.StoreChunk.newBuilder()
                        .setFileName(clientMetaData.getStoreFileName())
                        .setFileType(clientMetaData.getStoreFileType())
                        .setChunkId(chunkId)
                        .setData(data)
                        .setNumChunks(clientMetaData.getNumChunks())
                        .setChunkSize(chunkSize)
                        .setChunkCheckSum(chunkCheckSum)
                        .build();

                protoWrapperMsgOut =
                        StorageMessages.ProtoWrapper.newBuilder()
                                .setRequestor(Client.CLIENT)
                                .setIp(clientMetaData.getClientIp())
                                .setStoreChunk(storeChunkMsgOut)
                                .build();

                protoWrapperMsgOut.writeDelimitedTo(socket.getOutputStream());

                protoWrapperMsgIn =
                        StorageMessages.ProtoWrapper.parseDelimitedFrom(
                                socket.getInputStream());
                System.out.println("protoWrapperMsgIn.getResponse() = " + protoWrapperMsgIn.getResponse());

            }
            ///store successful or fail
            protoWrapperMsgIn =
                    StorageMessages.ProtoWrapper.parseDelimitedFrom(
                            socket.getInputStream());
            String response = protoWrapperMsgIn.getResponse();
            String nodeIp = protoWrapperMsgIn.getIp();
            log.info(nodeIp  + " " + response);

            socket.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Bytes to hex String
     * @param bytes
     * @return
     */
    private String toHex(byte[] bytes) {
        byte[] hash = checkSum(bytes);
        if(hash == null) {
            System.err.println("one of the chunk checkSum is null");
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
}

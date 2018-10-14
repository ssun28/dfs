package edu.usfca.cs.dfs.client;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.storageNode.Chunk;
import edu.usfca.cs.dfs.storageNode.StorageNode;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Hashtable;

/**
 * RetrieveChunkTask: parallel retrieve each chunk from storage node
 */
public class RetrieveChunkTask implements Runnable {

    private String retrieveChunkName;
    private int nodeId;
    private String nodeIp;
    private String clientIp;
    private StorageMessages.ProtoWrapper protoWrapperIn;
    private StorageMessages.ProtoWrapper protoWrapperOut;
    private Hashtable<Integer, byte[]> chunkTable;
    private Chunk chunk;
    private static Logger log;

    public RetrieveChunkTask(String retrieveChunkName,
                             int nodeId,
                             String nodeIp,
                             String clientIp,
                             Hashtable<Integer, byte[]> chunkTable) {
        this.retrieveChunkName = retrieveChunkName;
        this.nodeId = nodeId;
        this.nodeIp = nodeIp;
        this.clientIp = clientIp;
        this.chunkTable = chunkTable;
        this.log = Logger.getLogger(RetrieveChunkTask.class);
    }

    /**
     * Main run:
     * send the retrieve chunkName to the storage node and get
     * back that chunk's info including the data
     */
    @Override
    public void run() {
        Socket socket = new Socket();
        try {
            InetAddress serverIP = InetAddress.getByName(nodeIp);
            socket.connect(new InetSocketAddress(serverIP, StorageNode.PORT), 2000);

            StorageMessages.StoreChunk storeChunkMsgOut =
                    StorageMessages.StoreChunk.newBuilder()
                    .setFileName(retrieveChunkName)
                    .build();

            StorageMessages.RetrieveFile retrieveFileMsgOut =
                    StorageMessages.RetrieveFile.newBuilder()
                    .setRetrieveChunk(storeChunkMsgOut)
                    .build();

            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor(Client.CLIENT)
                    .setIp(clientIp)
                    .setRetrieveFile(retrieveFileMsgOut)
                    .build();

            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());

            protoWrapperIn =
                    StorageMessages.ProtoWrapper.parseDelimitedFrom(
                                socket.getInputStream());

            StorageMessages.RetrieveFile retrieveFileMsgIn = protoWrapperIn.getRetrieveFile();
            StorageMessages.StoreChunk storeChunkMsgIn = retrieveFileMsgIn.getRetrieveChunk();
            String fileName = storeChunkMsgIn.getFileName();
            int chunkId = storeChunkMsgIn.getChunkId();
            int size = storeChunkMsgIn.getChunkSize();
            int numChunks = storeChunkMsgIn.getNumChunks();
            String fileType = storeChunkMsgIn.getFileType();
            String msgInCheckSum = storeChunkMsgIn.getChunkCheckSum();

            chunk = new Chunk(fileName, chunkId, fileType, numChunks, size, msgInCheckSum);

            ByteString bytes = storeChunkMsgIn.getData();
            byte[] data= bytes.toByteArray();
            String checkSum = toHex(data);
            while(!msgInCheckSum.equals(checkSum)) {
                protoWrapperOut =
                        StorageMessages.ProtoWrapper.newBuilder()
                                .setRequestor(Client.CLIENT)
                                .setIp(clientIp)
                                .setResponse("CheckSums not equal")
                                .build();
                try {
                    protoWrapperOut.writeDelimitedTo(socket.getOutputStream());

                    protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                            socket.getInputStream());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                protoWrapperIn =
                        StorageMessages.ProtoWrapper.parseDelimitedFrom(
                                socket.getInputStream());

                retrieveFileMsgIn = protoWrapperIn.getRetrieveFile();
                storeChunkMsgIn = retrieveFileMsgIn.getRetrieveChunk();

                bytes = storeChunkMsgIn.getData();
                data = bytes.toByteArray();
                checkSum = toHex(data);
                log.info(chunkId + " check sum is equal");
            }
            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                            .setRequestor(Client.CLIENT)
                            .setIp(clientIp)
                            .setResponse("CheckSums equal")
                            .build();

            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());

            log.info(fileName + " has been got from the storageNode successfully");
            data = Arrays.copyOfRange(data, 0, size);

            chunkTable.put(chunkId, data);
            System.out.println(chunkId + " :" + chunkTable.size());

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            quit(socket);
        }
    }

    /**
     * Close the socket
     * @param socket
     */
    private void quit(Socket socket){
        try {
            socket.close();
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

    public Chunk getChunk() {
        return chunk;
    }
}

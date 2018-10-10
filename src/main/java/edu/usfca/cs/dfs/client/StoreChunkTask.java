package edu.usfca.cs.dfs.client;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.coordinator.Coordinator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class StoreChunkTask implements Runnable {

    private ClientMetaData clientMetaData;
    private int chunkId;
    private ByteString data;
    private String positionNodeIp;
    private int chunkSize;

    public StoreChunkTask(ClientMetaData clientMetaData, int chunkId, ByteString data, int chunkSize) {
        this.clientMetaData = clientMetaData;
        this.chunkId = chunkId;
        this.data = data;
        this.chunkSize = chunkSize;
    }

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
            socket.connect(new InetSocketAddress(clientMetaData.getServerIP(), Coordinator.PORT), 2000);
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
            socket.connect(new InetSocketAddress(toStoreSnIP, Coordinator.PORT), 2000);

            StorageMessages.StoreChunk storeChunkMsgOut
                = StorageMessages.StoreChunk.newBuilder()
                .setFileName(clientMetaData.getStoreFileName())
                .setFileType(clientMetaData.getStoreFileType())
                .setChunkId(chunkId)
                .setData(data)
                .setNumChunks(clientMetaData.getNumChunks())
                .setChunkSize(chunkSize)
                .build();

            StorageMessages.ProtoWrapper protoWrapper =
                    StorageMessages.ProtoWrapper.newBuilder()
                            .setRequestor(Client.CLIENT)
                            .setIp(clientMetaData.getClientIp())
                            .setStoreChunk(storeChunkMsgOut)
                            .build();
            protoWrapper.writeDelimitedTo(socket.getOutputStream());

            StorageMessages.ProtoWrapper protoWrapperMsgIn =
                    StorageMessages.ProtoWrapper.parseDelimitedFrom(
                            socket.getInputStream());

            ///store successful or fail
            String response = protoWrapperMsgIn.getResponse();
            String nodeIp = protoWrapperMsgIn.getIp();
            System.out.println("nodeIp = " + nodeIp + response);

            socket.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class MoveFileTask implements Runnable {

    private String chunkName;
    private String destIp;
    private String sourceIp;
    private int failNodeId;


    public MoveFileTask(String chunkName, String destIp, String sourceIp, int failNodeId) {
        this.chunkName = chunkName;
        this.destIp = destIp;
        this.sourceIp = sourceIp;
        this.failNodeId = failNodeId;
    }

    /**
     *
     */
    @Override
    public void run() {
        Socket socket = new Socket();
        try {
            InetAddress serverIP = InetAddress.getByName(destIp);
            socket.connect(new InetSocketAddress(serverIP, StorageNode.PORT), 2000);

            StorageMessages.RemoveNode removeNodeMsgOut =
                    StorageMessages.RemoveNode.newBuilder()
                    .setFailNodeId(failNodeId)
                    .setChunkName(chunkName)
                    .setSouceNodeIp(sourceIp)
                    .build();

            StorageMessages.ProtoWrapper protoWrapperOut
                    = StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor(SnSocketTask.STORAGENODE)
                    .setRemoveNode(removeNodeMsgOut)
                    .build();

            protoWrapperOut.writeDelimitedTo(socket.getOutputStream());

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            quit(socket);
        }
    }

    private void quit(Socket socket){
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
package edu.usfca.cs.dfs.coordinator;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.storageNode.SnSocketTask;
import edu.usfca.cs.dfs.storageNode.StorageNode;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class MoveFileTask extends Thread {

    private String nodeIp;
    private int failNodeId;

    public MoveFileTask(String nodeIp, int failNodeId) {
        this.nodeIp = nodeIp;
        this.failNodeId = failNodeId;
    }

    public void run() {
        Socket socket = new Socket();
        try {
            InetAddress serverIP = InetAddress.getByName(nodeIp);
            socket.connect(new InetSocketAddress(serverIP, StorageNode.PORT), 2000);

            StorageMessages.RemoveNode removeNodeMsgOut =
                    StorageMessages.RemoveNode.newBuilder()
                    .setFailNodeId(failNodeId)
                    .build();

            StorageMessages.ProtoWrapper protoWrapperOut
                    = StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor(SnSocketTask.COORDINATOR)
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

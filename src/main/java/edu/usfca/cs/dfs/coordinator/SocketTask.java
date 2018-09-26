package edu.usfca.cs.dfs.coordinator;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.net.Socket;
import java.util.Hashtable;

public class SocketTask extends Thread {

    private Socket socket;
    private Hashtable routingTable;
    private StorageMessages.ProtoWrapper protoWrapper;

    public SocketTask(Socket socket, Hashtable routingTable) {
        this.socket = socket;
        this.routingTable = routingTable;
        try {
            this.protoWrapper = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        String requestor = protoWrapper.getRequestor();
        String functionType = protoWrapper.getFunctionCase().toString();

        if (requestor.equals("client") && functionType.equals("ASKINFO")) {
            clientRequest();
        } else if (requestor.equals("storageNode")) {
            storageNodeRequest(functionType);
        }

    }

    private void clientRequest() {
        StorageMessages.AskInfo AskInfo
                = protoWrapper.getAskInfo();
        String askInfoType = AskInfo.getFunctionCase().toString();

        switch (askInfoType) {
            case "ACTIVENODESLIST":
                getActiveNodesList();

                break;
            case "TOTALDISKSPACE":
                getTotalDiskSpace();
                break;
            case "REQUESTSNUM":
                getRequestsNum();
                break;
            default: break;
        }

    }

    private void getActiveNodesList() {

    }

    private void getTotalDiskSpace() {

    }

    private void getRequestsNum() {

    }

    private void storageNodeRequest(String functionType) {
        switch (functionType) {
            case "ADDNODE":
                addNodeRequest();
                break;
            case "REMOVENODE":
                removeNodeRequest();
                break;
            case "":
                break;
            default: break;
        }
    }

    private void addNodeRequest() {
        System.out.println("Node"+"is allowed to add into the hash space!");
    }

    private void removeNodeRequest() {

    }
}

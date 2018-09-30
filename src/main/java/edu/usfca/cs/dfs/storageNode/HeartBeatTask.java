package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class HeartBeatTask implements Runnable{

    public static final int PORT = 37000;

    private Socket hbSocket;
    private InetAddress serverIP;
    private StMetaData stMetaData;

    private boolean isConnectedCoor;

    public HeartBeatTask(StMetaData stMetaData) {
        this.hbSocket = new Socket();
        this.stMetaData = stMetaData;
        this.isConnectedCoor = false;
    }

    public void run() {
        connectCoordinator();
        addNodeToCor();
    }

    private void connectCoordinator() {
        while(!isConnectedCoor) {
            System.out.print("Enter the coordinator's IP address : ");
            Scanner scanner = new Scanner(System.in);
            try {
                serverIP = InetAddress.getByName(scanner.nextLine());
                hbSocket.connect(new InetSocketAddress(serverIP, PORT), 2000);
                isConnectedCoor = true;
                System.out.println("Successfully connecting with the coordinator !");

            } catch (UnknownHostException e) {
                System.out.println("Please enter the coordinator's IP address correctly!");
                connectCoordinator();
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("Please enter the coordinator's IP address correctly!");
                connectCoordinator();
                e.printStackTrace();
            }
        }
    }

    private void addNodeToCor() {
        try {
            StorageMessages.ProtoWrapper protoWrapperOut
                    = StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor("storageNode")
                    .setIp(this.stMetaData.getNodeIp())
                    .setAddNode("true")
                    .build();

            protoWrapperOut.writeDelimitedTo(hbSocket.getOutputStream());

            StorageMessages.ProtoWrapper protoWrapperIn
                    = StorageMessages.ProtoWrapper.parseDelimitedFrom(hbSocket.getInputStream());
            this.stMetaData.setNodeId(Integer.parseInt(protoWrapperIn.getAddNode()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}

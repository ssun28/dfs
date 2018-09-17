package edu.usfca.cs.dfs.client;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;

public class Client {

    public static final int PORT = 37000;

    private Socket client;
    private InetAddress serverIP;
    private InetAddress inetAddress;
    private OutputStream os;


    private boolean isConnectedServer = false;

    public Client() {
        this.client = new Socket();
        try {
            inetAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        System.out.println(getLocalDataTime() + " Starting client...");
        connectServer();
        sendData();
    }

    public void connectServer() {
        while (!isConnectedServer) {
            System.out.print("Enter the coordinator's IP address : ");
            Scanner scanner = new Scanner(System.in);
            try {
                serverIP = InetAddress.getByName(scanner.nextLine());
                client.connect(new InetSocketAddress(serverIP, PORT), 2000);

                isConnectedServer = true;
                System.out.println("Successfully connecting with the coordinator !");

            } catch (UnknownHostException e) {
                System.out.println("Please enter the coordinator's IP address correctly!");
                connectServer();
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("Please enter the coordinator's IP address correctly!");
                connectServer();
                e.printStackTrace();
            }
        }
    }

    private void sendData() {
        try {
            StorageMessages.ProtoWrapper protoWrapper =
                    StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor("client")
                    .setIp(inetAddress.getHostAddress())
                    .setAskInfo("true")
                    .build();
            protoWrapper.writeDelimitedTo(client.getOutputStream());
//            os = client.getOutputStream();
//            os.write(1);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private String getLocalDataTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    public void quit() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Client client = new Client();
        client.start();
    }

}

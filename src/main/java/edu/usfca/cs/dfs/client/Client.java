package edu.usfca.cs.dfs.client;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;

public class Client {

    public static final int PORT = 37000;

    private Socket client;
    private InetAddress serverIP;
    private OutputStream os;

    public void start() {
        System.out.println(getLocalDataTime() + " Starting client...");
        connectServer();
    }

    public void connectServer() {
        System.out.print("Enter the coordinator's IP address : ");
        Scanner scanner = new Scanner(System.in);
        try {
            serverIP = InetAddress.getByName(scanner.nextLine());

            client = new Socket(serverIP, PORT);
            if (client != null) {
                System.out.println("Successfully connecting with the coordinator !");
            }

            os = client.getOutputStream();
            os.write(1);

            client.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getLocalDataTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    public static void main(String[] args) {
        Client client = new Client();
        client.start();
    }

}

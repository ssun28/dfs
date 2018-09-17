package edu.usfca.cs.dfs.coordinator;

import edu.usfca.cs.dfs.StorageMessages;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Coordinator {

    public static final int PORT = 37000;

    private ExecutorService executorService;
    private ServerSocket serverSocket = null;
    private Socket socket;
    private InetAddress inetAddress;
    private boolean isStarted = true;

    public Coordinator() {
        try {
            executorService = Executors.newFixedThreadPool(20);
            serverSocket = new ServerSocket(PORT);
        } catch (IOException e) {
            e.printStackTrace();
            //quit();
        }
    }

    public void start() {
        System.out.println(getLocalDataTime() + " Starting coordinator...");

        try {
//            InputStream dataIn;
            while (isStarted) {
                socket = serverSocket.accept();
                System.out.println(getLocalDataTime() + " New connection from " + socket.getRemoteSocketAddress()+ " is connected! ");
                StorageMessages.ProtoWrapper protoWrapper =
                        StorageMessages.ProtoWrapper.parseDelimitedFrom(
                                socket.getInputStream());
                System.out.println("requestor is "+ protoWrapper.getRequestor());
                System.out.println("IP is "+ protoWrapper.getIp());

//                System.out.println("Function is "+ protoWrapper.getFunctionCase());
                // executorService.execute(new SocketTask(socket));
//                dataIn = socket.getInputStream();
//                System.out.println(dataIn.read());

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private String getLocalDataTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    public void getIpAddress() {
        try {

            inetAddress = InetAddress.getLocalHost();
            //String hostname = ip.getHostName();
            System.out.println("Coordinator IP address : " + inetAddress.getHostAddress());
            //System.out.println("Coordinator hostname : " + hostname);

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public void quit() {
        try {
            this.isStarted = false;
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //System.out.println("Starting coordinator...");
        Coordinator cd = new Coordinator();
        cd.getIpAddress();
        cd.start();

    }

}

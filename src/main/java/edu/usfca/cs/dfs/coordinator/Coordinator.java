package edu.usfca.cs.dfs.coordinator;

import java.io.IOException;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Coordinator {

    public static final int PORT = 37000;
    public static final int NTHREADS = 20;

    private ExecutorService executorService;
    private ServerSocket serverSocket = null;
    private Socket socket;
    private InetAddress inetAddress;
    private CoorMetaData coorMetaData;

    private boolean isStarted = true;
    private Hashtable<Integer, StorageNodeHashSpace> routingTable;
    private Hashtable<Integer, StorageNodeInfo> metaDataTable;
//    private int nodeId;
//    private double timeStamp;


    public Coordinator() {
        String coorIp = getIpAddress();
        this.routingTable = new Hashtable<>();
        this.metaDataTable = new Hashtable<>();
        this.coorMetaData = new CoorMetaData(routingTable, metaDataTable, -1, 0.0, coorIp);
//        this.nodeId = 0;
//        this.timeStamp = 0.00;
        try {
            executorService = Executors.newFixedThreadPool(NTHREADS);
            serverSocket = new ServerSocket(PORT);
        } catch (IOException e) {
            e.printStackTrace();
            //quit();
        }
    }

    public void start() {
        try {
//            InputStream dataIn;
            while(isStarted) {
                socket = serverSocket.accept();
//                System.out.println(getLocalDataTime() + " New connection from " + socket.getRemoteSocketAddress()+ " is connected! ");
//                StorageMessages.ProtoWrapper protoWrapper =
//                        StorageMessages.ProtoWrapper.parseDelimitedFrom(
//                                socket.getInputStream());
//                System.out.println("requestor is "+ protoWrapper.getRequestor());
//                System.out.println("IP is "+ protoWrapper.getIp());

//                System.out.println("Function is "+ protoWrapper.getFunctionCase());
                SocketTask socketTask = new SocketTask(socket, coorMetaData);
                executorService.execute(socketTask);
                System.out.println("Test coordinator begin !!!!!!");
                for(Map.Entry<Integer, StorageNodeHashSpace> e : routingTable.entrySet()){
                    System.out.println("e.getKey() = " + e.getKey());
                    System.out.println("e.getValue().getPositionNodeIp() = " + e.getValue().getNodeIp());
                    System.out.println("e.getValue().getSpaceRange(0) = " + e.getValue().getSpaceRange()[0]);
                    System.out.println("e.getValue().getSpaceRange(1) = " + e.getValue().getSpaceRange()[1]);

                }
//                executorService.execute(new SocketTask(socket, routingTable, nodeId));

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

    private String getIpAddress() {
        try {
            System.out.println(getLocalDataTime() + " Starting coordinator...");
            inetAddress = InetAddress.getLocalHost();
            //String hostname = ip.getHostName();
            System.out.println("Coordinator IP address : " + inetAddress.getHostAddress());
            return inetAddress.getHostAddress();
            //System.out.println("Coordinator hostname : " + hostname);

        } catch (Throwable t) {
            t.printStackTrace();
        }
        return null;
    }

    public void quit() {
        try {
            this.isStarted = false;
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public class SocketTask implements Runnable {
//
//        private Socket socket;
//        private SocketTask inThread;
//        private OutputThread outThread;
//
//        public SocketTask(Socket socket, Hashtable r) {
//            this.socket = socket;
//            this.outThread = new OutputThread(socket);
//            this.inThread = new SocketTask(socket, outThread);
//        }
//
//        @Override
//        public void run() {
//            inThread.start();
//        }
//    }

    public static void main(String[] args) {
        //System.out.println("Starting coordinator...");
        Coordinator cd = new Coordinator();
        cd.start();

    }

}

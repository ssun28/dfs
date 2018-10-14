package edu.usfca.cs.dfs.coordinator;

import edu.usfca.cs.dfs.storageNode.StorageNodeInfo;

import java.io.IOException;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Hashtable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Coordinator class: the entrance for the coordinator
 * coordinator will receive different requests from client
 * storage nodes and give back information
 */
public class Coordinator {

    public static final int PORT = 37000;

    private static final int NTHREADS = 20;

    private ExecutorService executorService;
    private ServerSocket serverSocket = null;
    private CoorMetaData coorMetaData;
    private boolean isStarted = true;
    private Hashtable<Integer, StorageNodeHashSpace> routingTable;
    private Hashtable<Integer, StorageNodeInfo> metaDataTable;

    public Coordinator() {
        String coorIp = getIpAddress();
        this.routingTable = new Hashtable<>();
        this.metaDataTable = new Hashtable<>();
        this.coorMetaData = new CoorMetaData(routingTable, metaDataTable, -1, 0.0, coorIp);
        try {
            executorService = Executors.newFixedThreadPool(NTHREADS);
            serverSocket = new ServerSocket(PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Main start: as the server, receive a socket and pick a thread
     * to do that socket task
     *
     */
    public void start() {
        try {
            while(isStarted) {
                Socket socket = serverSocket.accept();

                CoorSocketTask coorSocketTask = new CoorSocketTask(socket, coorMetaData);
                executorService.execute(coorSocketTask);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the local time
     * @return
     */
    private String getLocalDataTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    /**
     * Get the ip address of the current host.
     * @return
     */
    private String getIpAddress() {
        InetAddress inetAddress;
        try {
            System.out.println(getLocalDataTime() + " Starting coordinator...");
            inetAddress = InetAddress.getLocalHost();
            System.out.println("Coordinator IP address : " + inetAddress.getHostAddress());
            return inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Close the socket
     */
    public void quit() {
        try {
            this.isStarted = false;
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String filePath = System.getProperty("user.dir")
                + "/log4j.properties";
        PropertyConfigurator.configure(filePath);
        Coordinator cd = new Coordinator();
        cd.start();
    }

}

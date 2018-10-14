package edu.usfca.cs.dfs.client;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.coordinator.CoorMetaData;
import edu.usfca.cs.dfs.coordinator.Coordinator;
import edu.usfca.cs.dfs.storageNode.StorageNode;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * Client class: the entrance for the client
 * client will send different requests to coordinator or
 * storage node to get the information
 */
public class Client {

    public static final int CHUNKSIZE = 8000000;
    private static final int NTHREADS = 20;
    public static final String CLIENT = "client";
    private static final String STORAGENODE = "storageNode";
    private static final String COORDINATOR = "coordinator";
    private static DecimalFormat df2 = new DecimalFormat(".##");

    private Socket client;
    private InetAddress serverIP;
    private InetAddress inetAddress;
    private String clientIp;
    private boolean isConnectedCoor = false;
    private StorageMessages.ProtoWrapper protoWrapperIn;
    private StorageMessages.ProtoWrapper protoWrapperOut;
    private static Logger log;

    public Client() {
        this.client = new Socket();
        log = Logger.getLogger(Client.class);
        try {
            inetAddress = InetAddress.getLocalHost();
            this.clientIp = inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * Main start method
     */
    public void start() {
        int clientOption = clientMenu();

        if(clientOption == 7) {
            quit();
            return;
        }else if (clientOption >= 1 && clientOption <= 3) {
            connectServer(COORDINATOR);
            switch (clientOption) {
                case 1:
                    getActiveNodesList();
                    break;
                case 2:
                    getTotalDiskSpace();
                    break;
                case 3:
                    getRequestsNum();
                    break;
                default: break;
            }
            quit();
        }else if (clientOption >=4 && clientOption <= 6){
            connectServer(STORAGENODE);
            switch (clientOption) {
                case 4:
                    getStorageNodeFilesList();
                    quit();
                    break;
                case 5:
                    storeFile();
                    System.out.println("finish store");
                    ////// no quit need !!!!
                    break;
                case 6:
                    retrieveFile();
                    quit();
                    break;
                default: break;
            }
        }else if (clientOption == 8) {
            connectServer(COORDINATOR);
            removeNodeOperation();
            quit();
        }else if (clientOption == 9) {
            connectServer(STORAGENODE);
            printAllfilesTable();
            quit();
        }

        start();
    }


    /**
     * Print all files table for test
     */
    private void printAllfilesTable() {
        try {
            System.out.println("ServerIp = " + serverIP);
            client = new Socket();
            client.connect(new InetSocketAddress(serverIP, StorageNode.PORT), 2000);

            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor(CLIENT)
                    .setIp(clientIp)
                    .setPrintAllfilesTable("true")
                    .build();

            protoWrapperOut.writeDelimitedTo(client.getOutputStream());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Remove the node for test
     */
    private void removeNodeOperation() {
        System.out.print("Enter the node id for removing: ");
        Scanner scanner = new Scanner(System.in);
        int removeNodeId = scanner.nextInt();
        protoWrapperOut =
                StorageMessages.ProtoWrapper.newBuilder()
                .setRequestor(CLIENT)
                .setIp(clientIp)
                .setRemoveNodeId(removeNodeId)
                .build();
        try {
            protoWrapperOut.writeDelimitedTo(client.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Simple text-based command entry interface for user to select
     * the option
     * @return
     */
    private int clientMenu() {
        int userOption = 0;
        boolean isRightOption = false;
        while(!isRightOption) {
            System.out.println("Enter the option you want: ");
            System.out.println("1. Print out a list of active nodes from coordinator");
            System.out.println("2. The total disk space available in the cluster (in GB) from coordinator");
            System.out.println("3. Number of requests handled by each node from coordinator");
            System.out.println("4. Given a specific storage node(retrieve a list of files stored there)");
            System.out.println("5. Store file");
            System.out.println("6. Retrieve file");
            System.out.println("7. Exit");

//            System.out.println("8. Remove Node");
//            System.out.println("9. print all files table");

            System.out.print("Your option is: ");
            Scanner scanner = new Scanner(System.in);
            userOption = scanner.nextInt();
            if (userOption > 0 && userOption < 10) {
                isRightOption = true;
            }
        }

        return userOption;
    }

    /**
     * Connect to the coordinator
     * @param serverType
     */
    public void connectServer(String serverType) {
        while(!isConnectedCoor) {
            System.out.print("Enter the " + serverType + "'s IP address : ");
            Scanner scanner = new Scanner(System.in);
            try {

                serverIP = InetAddress.getByName(scanner.nextLine());
                System.out.println("serverIP = " + serverIP);
                if(serverType.equals(STORAGENODE)){
                    isConnectedCoor = false;
                    return;
                }
                client = new Socket();
                client.connect(new InetSocketAddress(serverIP, Coordinator.PORT), 2000);

                isConnectedCoor = true;
                System.out.println("Successfully connecting with the " + serverType + " !");

            } catch (UnknownHostException e) {
                System.out.println("Please enter the " + serverType + "'s IP address correctly!");
                connectServer(serverType);
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("Please enter the " + serverType + "'s IP address correctly!");
                connectServer(serverType);
                e.printStackTrace();
            }
        }
        isConnectedCoor = false;
    }

    /**
     * Send request to coordinator and get the active nodes list
     * from coordinator
     */
    private void getActiveNodesList() {
        try {
            StorageMessages.AskInfo askInfoMsgOut
                    = StorageMessages.AskInfo.newBuilder()
                    .setActiveNodesList(true)
                    .build();

            askInfoRequestWapperOut(askInfoMsgOut);

            protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    client.getInputStream());

            StorageMessages.AskInfo askInfoMsgIn = protoWrapperIn.getAskInfo();
            StorageMessages.ActiveNodesList activeNodesList = askInfoMsgIn.getResActiveNodesList();

            System.out.println("Here is the list of active nodes from coordinator: ");

            for(StorageMessages.ActiveNode node : activeNodesList.getActiveNodeList()) {
                System.out.println("NodeId: " + node.getNodeId() + "  , active  " + ", NodeIp: " + node.getNodeIp());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send request to coordinator and get the total disk space
     * from coordinator
     */
    private void getTotalDiskSpace() {
        try {
            StorageMessages.AskInfo askInfoMsgOut
                    = StorageMessages.AskInfo.newBuilder()
                    .setTotalDiskSpace(true)
                    .build();

            askInfoRequestWapperOut(askInfoMsgOut);

            protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    client.getInputStream());

            StorageMessages.AskInfo askInfoMsgIn = protoWrapperIn.getAskInfo();
            StorageMessages.TotalDiskSpace totalDiskSpace = askInfoMsgIn.getResTotalDiskSpace();

            double total = 0.0;
            System.out.println("Here is the list of nodes' disk space available in the cluster (in GB) from coordinator: ");
            for(StorageMessages.DiskSpace node : totalDiskSpace.getDiskSpaceList()) {
                System.out.println("NodeId: " + node.getNodeId() + ", NodeIp: " + node.getNodeIp() + ", disk space available: " + node.getSpace());
                total+= node.getSpace();
            }

            total = Double.parseDouble(df2.format(total));
            System.out.println("The total disk space available in the cluster (in GB) from coordinator is " + total);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send request to coordinator and get the nodes' requests number list
     * from coordinator
     */
    private void getRequestsNum() {
        try {
            StorageMessages.AskInfo askInfoMsgOut
                    = StorageMessages.AskInfo.newBuilder()
                    .setRequestsNum(true)
                    .build();

            askInfoRequestWapperOut(askInfoMsgOut);

            protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    client.getInputStream());

            StorageMessages.AskInfo askInfoMsgIn = protoWrapperIn.getAskInfo();
            StorageMessages.RequestsNum requestsNum = askInfoMsgIn.getResRequestsNum();

            System.out.println("Here is the list of number of requests handled by each node: ");
            for(StorageMessages.NodeRequestsNum node : requestsNum.getNodeRequestsNumList()) {
                System.out.println("NodeId: " + node.getNodeId() + "  , number of requests: " + node.getRequestsNum() +  " , NodeIp: " + node.getNodeIp());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * A basic protoWapperOut for askInfo requests
     * @param askInfoMsgOut
     */
    private void askInfoRequestWapperOut(StorageMessages.AskInfo askInfoMsgOut) {
        try {
            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                            .setRequestor(CLIENT)
                            .setIp(clientIp)
                            .setAskInfo(askInfoMsgOut)
                            .build();

            protoWrapperOut.writeDelimitedTo(client.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send request to a specific storage node and get the node files list
     * from that storage node
     */
    private void getStorageNodeFilesList() {
        try {
            System.out.println("ServerIp = " + serverIP);
            client = new Socket();
//            client.connect(new InetSocketAddress(serverIP, Coordinator.PORT), 2000);
            client.connect(new InetSocketAddress(serverIP, StorageNode.PORT), 2000);

            StorageMessages.AskInfo askInfoMsgOut
                    = StorageMessages.AskInfo.newBuilder()
                    .setNodeFilesList(true)
                    .build();

            askInfoRequestWapperOut(askInfoMsgOut);

            protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    client.getInputStream());

            StorageMessages.AskInfo askInfoMsgIn = protoWrapperIn.getAskInfo();
            StorageMessages.NodeFilesList nodeFilesList = askInfoMsgIn.getResNodeFilesList();

            System.out.println("Here is the list of files stored on the given storage node: ");
            for(StorageMessages.StoreChunk c : nodeFilesList.getStoreChunkList()) {
                System.out.println(c.getFileName() + "_" + c.getChunkId() + c.getFileType() + "  " + c.getChunkSize() +"  " + c.getNumChunks());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Store a file: break the whole file into multiple chunks
     * send each chunk name to a storage node to get the every positioned storage nodes
     * multi-thread to send each chunk to the positioned storage node
     */
    private void storeFile() {
        String fileName;
        String fileType = "";

        System.out.println("Enter the file you want to store: ");
        System.out.print("File path:");
        Scanner scanner = new Scanner(System.in);
        String inputFilePath = scanner.nextLine();
        File f = new File(inputFilePath);

        String file = f.getName();

        if(file.contains(".")) {
            fileName = file.split("\\.")[0];
            fileType = "." + file.split("\\.")[1];
        }else {
            fileName = file;
        }

        long fileSize = f.length();

        long numChunks = (fileSize/CHUNKSIZE) + (fileSize % CHUNKSIZE == 0 ? 0 : 1);

        log.info(fileName + " has " + numChunks + " chunks with chunk size :" + CHUNKSIZE + " mb");

        try(FileInputStream fs = new FileInputStream(f)) {
            ExecutorService executorService = Executors.newFixedThreadPool(NTHREADS);

            int size;
            int chunkId = 0;
            byte[] b = new byte[CHUNKSIZE];
            ClientMetaData clientMetaData = new ClientMetaData(fileName, fileType, (int)numChunks, clientIp, serverIP);
            while((size = fs.read(b)) != -1) {
//                log.info(chunkId + "'s size = " + size + " with byte[] size:" + b.length);
//                ByteString data = ByteString.copyFrom(b, 0, size);
                StoreChunkTask storeChunkTask = new StoreChunkTask(clientMetaData, chunkId, b, size);
                executorService.execute(storeChunkTask);

                b = new byte[CHUNKSIZE];
                chunkId++;
            }
            executorService.shutdown();
            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("All finish");
        } catch (FileNotFoundException fds) {
            System.out.println(fds.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }

    /**
     * Retrieve a file: ask a storage node for all the file chunks' position in
     * the system, and parallel retrieve each chunk and reconstruct the file
     */
    private void retrieveFile() {
        try {
            System.out.println("Enter the file you want to retrieve: ");
            System.out.print("File:");

            Scanner scanner = new Scanner(System.in);
            String inputFile = scanner.nextLine();

            client = new Socket();
            client.connect(new InetSocketAddress(serverIP, StorageNode.PORT), 2000);

            StorageMessages.RetrieveFile retrieveFileMsgOut
                    = StorageMessages.RetrieveFile.newBuilder()
                    .setAskChunksPos(inputFile)
                    .build();

            protoWrapperOut =
                    StorageMessages.ProtoWrapper.newBuilder()
                    .setRequestor(CLIENT)
                    .setIp(clientIp)
                    .setRetrieveFile(retrieveFileMsgOut)
                    .build();

            protoWrapperOut.writeDelimitedTo(client.getOutputStream());
            // get list
            protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    client.getInputStream());

            StorageMessages.RetrieveFile retrieveFileMsgIn = protoWrapperIn.getRetrieveFile();
            StorageMessages.ResChunksPos resChunksPos = retrieveFileMsgIn.getResChunksPos();

            Map<String, StorageMessages.NodeIdList> retrieveChunksPosTableMap = resChunksPos.getChunksPosMap();
            Map<Integer, String> nodeIpTableMap = resChunksPos.getNodeIpTableMap();

            if(retrieveChunksPosTableMap.size() != 0) {
                ExecutorService executorService = Executors.newFixedThreadPool(NTHREADS);
                Hashtable<Integer, byte[]> chunkTable = new Hashtable<>();
                for(Map.Entry<String, StorageMessages.NodeIdList> c : retrieveChunksPosTableMap.entrySet()) {
                    String retrieveChunkName = c.getKey();
                    StorageMessages.NodeIdList nodeIdList = c.getValue();

                    ///
                    int nodeId = nodeIdList.getNodeId(0);
                    String nodeIp = nodeIpTableMap.get(nodeId);

                    RetrieveChunkTask retrieveChunkTask = new RetrieveChunkTask(retrieveChunkName, nodeId, nodeIp,
                            clientIp, chunkTable);
                    executorService.execute(retrieveChunkTask);
                }
                executorService.shutdown();

                try {
                    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                merge(chunkTable, inputFile);
            }else {
                System.out.println("There is no such file store in the file system!");
                System.out.println("Please make sure you type the correct file name.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Close the socket
     */
    public void quit() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Merge the chunks to the whole file
     * @param chunkTable
     * @param fileName
     */
    private void merge(Hashtable<Integer, byte[]> chunkTable, String fileName){
        File file = new File(StorageNode.DIR + fileName);
        System.out.println("chunkTable = " + chunkTable.size());
        log.info(fileName +" is merging!");
        int numOfChunks = chunkTable.size();
        try{
            FileOutputStream fo = new FileOutputStream(file);
            for(int i = 0 ; i < numOfChunks;i++){
                byte[] data = chunkTable.get(i);
                int size = data.length;
                fo.write(data, 0, size);
                chunkTable.remove(i);
            }
            fo.flush();
            fo.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String filePath = System.getProperty("user.dir")
                + "/log4j.properties";
        PropertyConfigurator.configure(filePath);

        Client client = new Client();
        client.start();
    }

}

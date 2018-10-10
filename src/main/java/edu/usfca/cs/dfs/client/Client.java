package edu.usfca.cs.dfs.client;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.coordinator.Coordinator;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Client {

//    public static final int PORT = 37000;
    private static final int CHUNKSIZE = 8000000;
    private static final int NTHREADS = 20;
    public static final String CLIENT = "client";
    private static final String STORAGENODE = "storageNode";
    private static final String COORDINATOR = "coordinator";

    public enum functionType {STORE_CHUNK,ASK_INFO};

    private Socket client;
    private InetAddress serverIP;
    private InetAddress inetAddress;
    private String clientIp;
    private boolean isConnectedCoor = false;
    private StorageMessages.ProtoWrapper protoWrapperIn;
    private StorageMessages.ProtoWrapper protoWrapperOut;

    public Client() {
        this.client = new Socket();
        try {
            inetAddress = InetAddress.getLocalHost();
            this.clientIp = inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

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
        }else if (clientOption >=4 && clientOption <= 6){
            connectServer(STORAGENODE);
            switch (clientOption) {
                case 4:
                    getStorageNodeFilesList();
                    break;
                case 5:
                    storeFile();
                    break;
                case 6:
                    retrieveFile();
                    break;
                default: break;
            }
        }

        start();

        //sendData();
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

            System.out.print("Your option is: ");
            Scanner scanner = new Scanner(System.in);
            userOption = scanner.nextInt();
            if (userOption > 0 && userOption < 8) {
                isRightOption = true;
            }
        }
        return userOption;
    }

    public void connectServer(String serverType) {
        while(!isConnectedCoor) {
            System.out.print("Enter the " + serverType + "'s IP address : ");
            Scanner scanner = new Scanner(System.in);
            try {
                serverIP = InetAddress.getByName(scanner.nextLine());
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
                System.out.println(c.getFileName() + "_" + c.getChunkId() + c.getFileType());
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
//        System.out.print("./input/");
        System.out.print("File path:");
        Scanner scanner = new Scanner(System.in);
        String inputFilePath = scanner.nextLine();
        File f = new File(inputFilePath);

        String file = f.getName();

        System.out.println("inputFilePath = " + inputFilePath);
        System.out.println("file = " + file);
        if(file.contains(".")) {
            fileName = file.split("\\.")[0];
            fileType = "." + file.split("\\.")[1];
        }else {
            fileName = file;
        }

//        System.out.println("fileName = " + fileName);
//        System.out.println("fileType = " + fileType);
//        File file = new File("./input/"+inputFilePath);

        long fileSize = f.length();

//        System.out.println("fileSize" + fileSize+"_# of chunks :"+(fileSize/CHUNKSIZE));
        long numChunks = (fileSize/CHUNKSIZE) + (fileSize % CHUNKSIZE == 0 ? 0 : 1);

//        System.out.println("numChunks = " + numChunks);
        try(FileInputStream fs = new FileInputStream(f)) {
            ExecutorService executorService = Executors.newFixedThreadPool(NTHREADS);

            int size;
            int chunkId = 0;
            byte[] b = new byte[CHUNKSIZE];
            ClientMetaData clientMetaData = new ClientMetaData(fileName, fileType, (int)numChunks, clientIp, serverIP);
            while((size = fs.read(b)) != -1) {
                System.out.println("size = " + size +"_"+"byte[] size:" + b.length);
                ByteString data = ByteString.copyFrom(b, 0, size);

                StoreChunkTask storeChunkTask = new StoreChunkTask(clientMetaData, chunkId, data);
                executorService.execute(storeChunkTask);


//                StorageMessages.StoreChunk storeChunkMsg
//                        = StorageMessages.StoreChunk.newBuilder()
//                        .setFileName(fileName)
//                        .setFileType(fileType)
//                        .setChunkId(chunkId)
//                        .setData(data)
//                        .setNumChunks((int)numChunks)
//                        .build();
//
//                StorageMessages.ProtoWrapper protoWrapper =
//                        StorageMessages.ProtoWrapper.newBuilder()
//                                .setRequestor("client")
//                                .setIp(inetAddress.getHostAddress())
//                                .setStoreChunk(storeChunkMsg)
//                                .build();
//                protoWrapper.writeDelimitedTo(client.getOutputStream());
                b = new byte[CHUNKSIZE];
                chunkId++;
            }
            Thread.currentThread().join();

        } catch (FileNotFoundException fds) {
            System.out.println(fds.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void retrieveFile() {
        try {
            System.out.println("Enter the file you want to retrieve: ");
            System.out.print("File:");

            Scanner scanner = new Scanner(System.in);
            String inputFile = scanner.nextLine();

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

            protoWrapperIn = StorageMessages.ProtoWrapper.parseDelimitedFrom(
                    client.getInputStream());

            StorageMessages.RetrieveFile retrieveFileMsgIn = protoWrapperIn.getRetrieveFile();
            StorageMessages.ResChunksPos resChunksPos = retrieveFileMsgIn.getResChunksPos();

            Map<String, StorageMessages.NodeIdList> retrieveChunksPosTableMap = resChunksPos.getChunksPosMap();
            Map<Integer, String> nodeIpTableMap = resChunksPos.getNodeIpTableMap();

            if(retrieveChunksPosTableMap.size() != 0) {


            }else {
                System.out.println("There is no such file store in the file system!");
                System.out.println("Please make sure you type the correct file name.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    private void sendData(String requestor, functionType type) {
//        try {
//            byte[] b = new byte[2];
//            StorageMessages.ProtoWrapper.Builder builder =
//                    StorageMessages.ProtoWrapper.newBuilder()
//                    .setRequestor(requestor)
//                    .setIp(inetAddress.getHostAddress());
//
//            switch(type){
//                case STORE_CHUNK:
//                    StorageMessages.StoreChunk chunk =
//                            StorageMessages.StoreChunk.newBuilder()
//                            .setChunkId(1)
//                            .setFileName("")
//                            .setData(b).build();
//
//                    builder.setStoreChunk(chunk);
//                    break;
//                case ASK_INFO:
//                    builder.setAskInfo(true);
//            }
//
//
//            StorageMessages.ProtoWrapper protoWrapper = builder.build();
//
//            protoWrapper.writeDelimitedTo(client.getOutputStream());
////            os = client.getOutputStream();
////            os.write(1);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }

    public void quit() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get local date time
     * @return
     */
    public String getLocalDataTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }

    public static void main(String[] args) {
//        double  MEGABYTE = 1024 * 1024 * 1024;
//        DecimalFormat df2 = new DecimalFormat(".##");
//
//
//        long a = System.currentTimeMillis();
//        File file = new File("/");
//        double b = (double)file.getUsableSpace()/ (1024.0 * 1024.0 * 1024.0);
//        System.out.println("b = " + b);
//
//        File[] roots = File.listRoots();
//        for(File file1 : roots) {
//            System.out.println(file1.getPath());
//            System.out.println("Free Space  = " + (double)file1.getFreeSpace()/(1024 * 1024 * 1024));
//            System.out.println("Usable Space = " + (double)file1.getUsableSpace()/(1024 * 1024 * 1024));
//            System.out.println("Total space = " + (double)file1.getTotalSpace()/(1024 * 1024 * 1024));
//        }
//
//        System.out.println(df2.format(new File("/").getUsableSpace()/MEGABYTE)); //in GB

//        Client client = new Client();
//        System.out.println(client.getLocalDataTime() + " Starting client...");
//        client.start();


        ArrayList<Integer> nodeIdList = new ArrayList<>();
        nodeIdList.add(0);
        nodeIdList.add(1);
        nodeIdList.add(3);
        nodeIdList.add(5);
        Collections.sort(nodeIdList);
        int nodesNum = nodeIdList.size();
        int test = (int)Math.pow(2, 14) - 1;
        int result =  test/ ((int)Math.pow(2, 16) / nodesNum);

        int[] nodeIdArray = new int[nodeIdList.size()];
        for(int i = 0; i < nodeIdArray.length; i++) {
            nodeIdArray[i] = nodeIdList.get(i);
        }

        int nodeId = nodeIdArray[result];
        System.out.println("nodeId = " + nodeId);
    }

}

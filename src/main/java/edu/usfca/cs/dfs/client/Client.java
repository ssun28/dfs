package edu.usfca.cs.dfs.client;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.coordinator.Coordinator;
import edu.usfca.cs.dfs.storageNode.StorageNodeInfo;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Scanner;

public class Client {

//    public static final int PORT = 37000;
    private static final int CHUNKSIZE = 8000000;
    private static final String CLIENT = "client";
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

            storeFile();
        }

        start();

        //sendData();
    }

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

    private void getActiveNodesList() {
        try {
            StorageMessages.AskInfo askInfoMsgOut
                    = StorageMessages.AskInfo.newBuilder()
                    .setActiveNodesList(true)
                    .build();

            toCoorRequestWapperOut(askInfoMsgOut);

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

    private void getTotalDiskSpace() {
        try {
            StorageMessages.AskInfo askInfoMsgOut
                    = StorageMessages.AskInfo.newBuilder()
                    .setTotalDiskSpace(true)
                    .build();

            toCoorRequestWapperOut(askInfoMsgOut);

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

    private void getRequestsNum() {
        try {
            StorageMessages.AskInfo askInfoMsgOut
                    = StorageMessages.AskInfo.newBuilder()
                    .setRequestsNum(true)
                    .build();

            toCoorRequestWapperOut(askInfoMsgOut);

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

    private void toCoorRequestWapperOut(StorageMessages.AskInfo askInfoMsgOut) {
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

        System.out.println("fileName = " + fileName);
        System.out.println("fileType = " + fileType);
//        File file = new File("./input/"+inputFilePath);

        long fileSize = f.length();

//        System.out.println("fileSize" + fileSize+"_# of chunks :"+(fileSize/CHUNKSIZE));
        long numChunks = (fileSize/CHUNKSIZE) + (fileSize % CHUNKSIZE == 0 ? 0 : 1);

//        System.out.println("numChunks = " + numChunks);
        try(FileInputStream fs = new FileInputStream(f)) {
            int size;
            int chunkId = 0;
            byte[] b = new byte[CHUNKSIZE];
            while((size = fs.read(b)) != -1) {
                System.out.println("size = " + size +"_"+"byte[] size:" + b.length);
                ByteString data = ByteString.copyFrom(b, 0, size);
                StorageMessages.StoreChunk storeChunkMsg
                        = StorageMessages.StoreChunk.newBuilder()
                        .setFileName(fileName)
                        .setFileType(fileType)
                        .setChunkId(chunkId)
                        .setData(data)
                        .setNumChunks((int)numChunks)
                        .build();

                StorageMessages.ProtoWrapper protoWrapper =
                        StorageMessages.ProtoWrapper.newBuilder()
                                .setRequestor("client")
                                .setIp(inetAddress.getHostAddress())
                                .setStoreChunk(storeChunkMsg)
                                .build();
                protoWrapper.writeDelimitedTo(client.getOutputStream());
                b = new byte[CHUNKSIZE];
                chunkId++;
            }
        } catch (FileNotFoundException fds) {
            System.out.println(fds.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
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



//        String file = "file_8.s";
//        String fileName;
//        String fileType = "";
//        String[] strings = new String[]{"file_8file_0.json", "file_1.json","file_2","file_8_2"};
//
//        if(file.contains(".")) {
//            fileName = file.split("\\.")[0];
//            fileType = "." + file.split("\\.")[1];
//        }else {
//            fileName = file;
//        }
//
//        for(String s : strings) {
//            String sfileName;
//            String sfileType = "";
//            if(s.contains(".")) {
//                sfileName = s.split("\\.")[0];
//                sfileType = "." + s.split("\\.")[1];
//            }else {
//                sfileName = s;
//            }
//
//            int index = sfileName.lastIndexOf("_");
//            String fileNamePre = sfileName.substring(0, index);
//            if(fileName.equals(fileNamePre) && sfileType.equals(fileType)) {
//                System.out.println("s = " + s);
//            }
//        }

    }

}

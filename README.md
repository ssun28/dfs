# P1
Project 1 - Distributed File System

This repository includes starter files and a sample directory structure. You are welcome to use it or come up with your own project structure.

Project Specification: https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-1.html


# Documentation

## Coordinator
  1. Add/Remove nodes:
     * can't: when the coordinator goes down
     * can:
       * hash ring re-arrange(add or remove proportion)
       * if remove(keep the id in a list, the next new node may take that id)
       * update the routing table

  2. Monitor the health of the cluster:
     * heartbeat and long connection socket
     * detect storage node failures:
       * a total of 3 duplicate chunks
       * a backup copy

  3. Coordinator recovery

  4. Routing table:

         * Node ID
         * position
         * hostname/ IP
         * status: active or inactive
         * free space available
         * the total number of requests processed


##  Storage Node
  1. Route client requests

  2. Store/Retrieve
     * update the routing table
     * update the all storage nodes map table(for loop all the nodes to maintain this table)
     * node info list itself

  3. Heartbeat with coordinator

     * update the routing table
     * messages in the storage node:

           * Store chunk [File name, Chunk Number, Chunk Data]
           * Get number of chunks [File name]
           * Get chunk location [File name, Chunk Number]
           * Retrieve chunk [File name, Chunk Number]
           * List chunks and file names [No input]
           * Get copy of current hash space [No input]

  4. Startup
     * a storage directory and the hostname/IP


## Client
  1. Break files

  2. Ask position

  3. Upload/Download(parallel)

  4. Pipeline fashion

  5. Get routing table from coordinator
     * a list of active nodes
     * the total disk space available in the cluster (in GB)
     * number of requests
     * a specific storage node(retrieve a list of files stored there)


### Design decisions
   * Protocols
     * Client/Storage node -------> Coordinator

         | Parameters         | Data Type     | Description                                               |
         | ------------------ |-------------  |:---------------------------------------------------------:|
         | requestor          | string        | the request is from client or storage node                |
         | IP                 | string        | the IP address                                            |
         | function           | string        | such as adding node, removing node, asking info, heartbeat|


     * Client -------> Storage node

         | Parameters         | Data Type     | Description                                              |
         | ------------------ |-------------  |:--------------------------------------------------------:|
         | requestor          | string        | the request is from client                               |
         | IP                 | string        | the IP address                                           |
         | failure            | string        | the failure in the storage node                          |
         | function           | string        | such as asking position, storing, retrieving, asking info|

   * Chunk size: 128MB

     If we make the chunk size in a very small size, most of the data will spilt into many chunks. When we do the reading operation, it will spend a lot of time looking up the address of the chunks. Also this will let storage node RAM under a heavy load. If we make the chunk size in a large size, it takes more time when the storage node have to restart.

   * Node distribute algorithm: how nodes are positioned within the hash space

     The idea is the hash ring will be divided into 16 pieces(0-15), so if there is one node, range 0-15 will all go to node1.

     * Case add node: there are four nodes and 16/4 = 4. Node0: 0-3, Node1: 4-7, Node2: 8-11, Node3: 12-15. If node4 will be added in, then 16/5 = 3. Node0: 0-2, Node1: 3-5, Node2: 6-8, Node3: 9-11, Node4: 12-15(we decide to make the remaining to the last node). Then the every origin Node0 - Node3 will move their chunksSizes * 1/5 chunk to the new Node4.

     * Case remove node: similarly, let's assum there are four nodes now and we want to remove node2 and 16/3 = 5. Node0: 0-4, Node1: 5 - 9, Node3: 10-15(we decide to make the remaining to the last node). Then node2 will move every chunksSizes * 1/3 to other three nodes. The Node ID 2 will be stored and when next new node comes in, it will take ID 2 first.

   * File distribute algorithm: how files are positioned in which storage node
     The idea is we will hash the fileName into 32 bits, and then do a 16 bits right shift, then we will have a 16 bits number let's called hash(fileName_16bits).

         result = hash(fileName_16bits) % (2^16 / numberOfNodes);
         if result == 0 then nodeNum = result;
         if result != 0 then nodeNum = result + 1;

### Basic components flow
  * ![basic components flow](flow.jpg)
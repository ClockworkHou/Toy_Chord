package com.houchen;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javafx.util.Pair;
import sun.awt.Mutex;

public class ChordServer implements Runnable{
    // node info
    private Integer hashID;
    private Integer port;
    final static int FINGER_SIZE = 13;

    // structural info
    private Mutex joinLock = new Mutex();
    private Mutex joinSucLock = new Mutex();

    private Integer successorPort;
    public Integer getSuccessorPort() {
        return successorPort;
    }
    private Integer predecessorPort;
    public Integer getPredecessorPort() {
        return predecessorPort;
    }
    private Integer successorID;
    public Integer getSuccessorID() {
        return successorID;
    }
    private Integer predecessorID;
    public Integer getPredecessorID() {
        return predecessorID;
    }
    private Integer preprePort;
    public Integer getPrePre() {
        return preprePort;
    }

    private List<Integer> fingerPort = new CopyOnWriteArrayList<Integer>();
    private List<Integer> fingerID = new CopyOnWriteArrayList<Integer>();
    List<Integer> getFingerID() {return fingerID;}

    // data
    Map<Integer,String> data = new ConcurrentHashMap<>();
    Map<Integer,String> backup = new ConcurrentHashMap<>();

    private Thread t;

    ChordServer(int _hashID, int _port, boolean superNode) {
        this.hashID = _hashID;
        this.port = _port;
        for(int i = 0; i < FINGER_SIZE; i++) {
            fingerPort.add(this.port);
            fingerID.add(this.hashID);
        }
        if(superNode) {
            this.predecessorPort = this.port;
            this.successorPort = this.port;
            this.successorID = this.hashID;
            this.predecessorID = this.hashID;
            this.preprePort = this.port;
        }
    }

    // Commmunication
    void listen() throws IOException {
        ServerSocket serverSocket =new ServerSocket(this.port);
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        while (true) {
            Socket socket = serverSocket.accept();
            Runnable runnable = () -> {
                BufferedReader bufferedReader =null;
                BufferedWriter bufferedWriter =null;
                try {
                    // read msg
                    bufferedReader =new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
                    String str;
                    List<String> msg = new LinkedList<String>();
                    while (!(str = bufferedReader.readLine()).equals("END")) {
                        msg.add(str);
                    }
                    //System.out.println("Receive：" + msg);
                    Iterator<String> msgIt = msg.iterator();

                    // respond according to msg
                    bufferedWriter =new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    switch(msgIt.next()) {
                        case "lookup": {
                            Integer tarHashID = Integer.valueOf(msgIt.next());
                            Integer ansPort = -1;
                            Integer ansID = -1;
                            // only one node in Chord || ID in interval
                            if ((this.successorID == this.hashID) ||
                                    (tarHashID <= this.hashID
                                            && (tarHashID > this.predecessorID || this.predecessorID == 8191))
                            ){
                                ansPort = this.port;
                                ansID = this.hashID;
                            } else {
                                Iterator<Integer> hit = fingerID.iterator();
                                Iterator<Integer> pit = fingerPort.iterator();
                                /*while(hit.hasNext()){
                                    Integer nodePort = pit.next();
                                    if(hit.next() >= tarHashID) {
                                        List<String> sendTmp = new LinkedList<String>();
                                        sendTmp.add("lookup");
                                        sendTmp.add(tarHashID.toString());
                                        // ask next node
                                        Socket socketSend = ChordClient.connect(nodePort);
                                        ChordClient.send(socketSend,sendTmp);
                                        List<String> recursiveRspn = ChordClient.receive(socketSend);
                                        ansID = Integer.valueOf(recursiveRspn.get(0));
                                        ansPort = Integer.valueOf(recursiveRspn.get(1));
                                        ChordClient.close(socketSend);
                                        break;
                                    }
                                }*/
                                List<String> recurSiveRes = ChordClient.lookup(tarHashID,this.successorPort);
                                ansID = Integer.valueOf(recurSiveRes.get(0));
                                ansPort = Integer.valueOf(recurSiveRes.get(1));
                            }
                            bufferedWriter.write(ansID.toString()+"\n");
                            bufferedWriter.write(ansPort.toString()+"\n");
                            //bufferedWriter.flush();
                            break;
                        }
                        case "getAll": {
                            bufferedWriter.write(this.hashID.toString()+"\n");
                            bufferedWriter.write(this.successorPort.toString()+"\n");
                            bufferedWriter.write(this.successorID.toString()+"\n");
                            bufferedWriter.write(this.predecessorID.toString()+"\n");
                            bufferedWriter.write(this.preprePort.toString()+"\n");
                            bufferedWriter.write(this.fingerID+"\n");
                            bufferedWriter.write(this.data+"\n");
                            bufferedWriter.write(this.backup+"\n");
                            break;
                        }
                        case "join": {
                            joinLock.lock();
                            Integer nodeHashID = Integer.valueOf(msgIt.next());
                            Integer nodePort = Integer.valueOf(msgIt.next());
                            // only one node in Chord || ID in interval
                            if (this.successorID == this.hashID) {
                                bufferedWriter.write("Accept\n");
                            } else if(nodeHashID >= this.hashID
                                    || (nodeHashID <= this.predecessorID && this.predecessorID != 8191)
                            ) {
                                bufferedWriter.write("Reject\n");
                                joinLock.unlock();
                                break;
                            } else {
                                // join
                                bufferedWriter.write("Accept\n");
                            }
                            Integer rollBackprepre = this.preprePort;
                            Integer rollBackprePort = this.predecessorPort;
                            Integer rollBackpreID = this.predecessorID;
                            this.preprePort = this.predecessorPort;
                            this.predecessorID = nodeHashID;
                            this.predecessorPort = nodePort;
                            bufferedWriter.write(rollBackpreID+"\n");
                            bufferedWriter.write(rollBackprePort+"\n");
                            Set<Integer> checkSet = this.data.keySet();
                            for(Integer key : checkSet) {
                                if(key <= nodeHashID) {
                                    bufferedWriter.write(key+" "+this.data.get(key)+"\n");
                                }
                            }
                            bufferedWriter.write("END\n");
                            bufferedWriter.flush();

                            str = bufferedReader.readLine();
                            if(!str.equals("OK")) {
                                // roll back
                                this.preprePort = rollBackprepre;
                                this.predecessorID = rollBackpreID;
                                this.predecessorPort = rollBackprePort;
                            } else {
                                this.updateFinger(false);
                                for(Integer key : checkSet) {
                                    if(key <= nodeHashID) {
                                        this.data.remove(key);
                                    }
                                }
                            }
                            joinLock.unlock();
                            break;
                        }
                        case "newSuccessor": {
                            joinSucLock.lock();
                            Integer nodeHashID = Integer.valueOf(msgIt.next());
                            Integer nodePort = Integer.valueOf(msgIt.next());
                            this.successorID = nodeHashID;
                            this.successorPort = nodePort;
                            if(this.predecessorID != this.hashID && this.preprePort == this.preprePort) {
                                this.preprePort = nodePort;
                            }
                            Set<Integer> checkSet = this.backup.keySet();
                            bufferedWriter.write(this.predecessorPort+"\n");
                            for(Integer key : checkSet) {
                                if(key > nodeHashID) {
                                    bufferedWriter.write( key+" "+this.backup.get(key)+"\n");
                                    this.backup.remove(key);
                                }
                            }
                            while(msgIt.hasNext()) {
                                String newData = msgIt.next();
                                String[] datapair = newData.split(" ");
                                this.backup.put(Integer.valueOf(datapair[0]),datapair[1]);
                            }
                            joinSucLock.unlock();
                            break;
                        }
                        case "insert": {
                            Integer dataID = Integer.valueOf(msgIt.next());
                            String data = msgIt.next();
                            this.data.put(dataID,data);
                            ChordClient.insertBackup(data,this.predecessorPort);
                            break;
                        }
                        case "backup": {
                            Integer dataID = Integer.valueOf(msgIt.next());
                            String data = msgIt.next();
                            this.backup.put(dataID,data);
                            break;
                        }
                        case "leave": {
                            joinSucLock.lock();
                            joinLock.lock();

                            Socket socketUpdateSucc = ChordClient.connect(this.preprePort);
                            List<String> sendPackage = new LinkedList<>();
                            sendPackage.add("newSuccessor");
                            sendPackage.add(this.hashID.toString());
                            sendPackage.add(this.port.toString());
                            for(Integer key:this.data.keySet()) {
                                String content = this.data.get(key);
                                sendPackage.add(key+" "+content);
                            }
                            ChordClient.send(socketUpdateSucc,sendPackage);
                            List<String> rspnUpdateSucc = ChordClient.receive(socketUpdateSucc);
                            Integer newPrePre = Integer.valueOf(rspnUpdateSucc.get(0));
                            socketUpdateSucc.close();

                            Socket socketGetall = ChordClient.connect(this.preprePort);
                            sendPackage.clear();
                            sendPackage.add("getAll");
                            ChordClient.send(socketGetall,sendPackage);
                            List<String> rspnGetall = ChordClient.receive(socketGetall);
                            socketGetall.close();

                            this.predecessorPort = this.preprePort;
                            this.predecessorID = Integer.valueOf(rspnGetall.get(0));
                            this.preprePort = newPrePre;

                            String tmp = rspnGetall.get(7);
                            String[] updateDate = tmp.substring(1,tmp.length()-1).split(", ");
                            System.out.println(updateDate);
                            for(int i = 0; i < updateDate.length; i++) {
                                String[] cur = updateDate[i].split("=");
                                if(!this.data.containsKey(Integer.valueOf(cur[0]))) {
                                    this.data.put(Integer.valueOf(cur[0]),cur[1]);
                                }

                            }
                            joinLock.unlock();
                            joinSucLock.unlock();
                            break;
                        }
                        default: bufferedWriter.write("END\n");
                    }
                    bufferedWriter.write("END\n");
                    bufferedWriter.flush();
                }catch (IOException e) {
                    e.printStackTrace();
                }
            };
            executorService.submit(runnable);
        }
    }
    public void run(){
        try {
            this.listen();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void start () {
        System.out.println("ChordServer Started......");
        if (t == null) {
            t = new Thread (this, "Chord Server "+this.hashID.toString());
            t.start ();
        }
    }

    public boolean join() {

        List<String> sendPackage = new LinkedList<>();
        // 1. lookup the successor to join
        List<String> joinNode = ChordClient.lookup(this.hashID,1110);
        Integer newSuccessorID = Integer.valueOf(joinNode.get(0));
        Integer newSuccessorPort = Integer.valueOf(joinNode.get(1));

        // 2. contact successor
        sendPackage.add("join");
        sendPackage.add(this.hashID.toString());
        sendPackage.add(this.port.toString());
        Socket socketJoin = ChordClient.connect(newSuccessorPort);
        ChordClient.send(socketJoin,sendPackage);

        // 3. receive pre info
        List<String> rspn = ChordClient.receive(socketJoin);
        Iterator<String> rspnIt = rspn.iterator();
        if(rspnIt.next().equals("Reject")) {
            ChordClient.close(socketJoin);
            return false;
        } else {
            Integer preID = Integer.valueOf(rspnIt.next());
            Integer prePort = Integer.valueOf(rspnIt.next());
            while(rspnIt.hasNext()) {
                String data = rspnIt.next();
                String[] datapair = data.split(" ");
                this.data.put(Integer.valueOf(datapair[0]),datapair[1]);
            }
            Socket socketPre = ChordClient.connect(prePort);
            sendPackage.clear();
            sendPackage.add("newSuccessor");
            sendPackage.add(this.hashID.toString());
            sendPackage.add(this.port.toString());
            ChordClient.send(socketPre,sendPackage);
            rspn = ChordClient.receive(socketPre);
            ChordClient.close(socketPre);

            rspnIt = rspn.iterator();
            this.preprePort = Integer.valueOf(rspnIt.next());
            this.predecessorID = preID;
            this.predecessorPort = prePort;
            this.successorID = newSuccessorID;
            this.successorPort = newSuccessorPort;
            while(rspnIt.hasNext()) {
                String data = rspnIt.next();
                String[] datapair = data.split(" ");
                this.backup.put(Integer.valueOf(datapair[0]),datapair[1]);
            }
            this.updateFinger(false);
            sendPackage.clear();
            sendPackage.add("OK");
            ChordClient.send(socketJoin,sendPackage);
            ChordClient.close(socketJoin);
        }
        return true;
    }

    void updateFinger(boolean global) {
        //Iterator<Integer> fingerIDIt = fingerID.iterator();
        //Iterator<Integer> fingerPortIt = fingerPort.iterator();
        for(int i = 0; i < 13; i++) {
            Integer tmpID = (this.hashID + Integer.valueOf(1<<i))%(1<<13);
            if(tmpID <= this.successorID && (tmpID > this.hashID||this.hashID==8191)) {
                fingerID.set(i, this.successorID);
                fingerPort.set(i, this.successorPort);
            } else if (global){
                List<String> lookupRes = ChordClient.lookup(tmpID,this.successorPort);
                fingerID.set(i,Integer.valueOf(lookupRes.get(0)));
                fingerPort.set(i,Integer.valueOf(lookupRes.get(1)));
            }
        }
    }

    void checkFinger(){
        Runnable runnable = () -> {
            try {
                while (true) {
                    Thread.sleep(500);
                    this.updateFinger(true);
                }
            } catch (Exception e) {
                return;
            }
        };
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.submit(runnable);
    }


}

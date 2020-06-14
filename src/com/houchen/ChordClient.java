package com.houchen;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Map;

public class ChordClient {


    static Socket connect (int port) {
        Socket socket = null;
        try {
            socket = new Socket("127.0.0.1", port);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return socket;
    }
    static void close (Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static List<String> receive(Socket socket) {
        List<String> rspn = new LinkedList<>();
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
            String str;
            while (!(str = bufferedReader.readLine()).equals("END")) {
                rspn.add(str);
            }
            //System.out.println("Receive：" + rspn);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rspn;
    }
    static void send (Socket socket, List<String> msg) {
        List<String> rspn = new LinkedList<String>();
        //System.out.println("Send "+msg+" to "+socket.getPort());
        try {
            BufferedWriter bufferedWriter =new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            Iterator<String> sendIt = msg.iterator();
            while(sendIt.hasNext())
                bufferedWriter.write(sendIt.next()+"\n");
            bufferedWriter.write("END\n");
            bufferedWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static public List<String> lookup(Integer tarHashID, Integer lookUpPort) {
        List<String> sendPackage = new LinkedList<>();
        sendPackage.add("lookup");
        sendPackage.add(tarHashID.toString());
        Socket socketLookUp = ChordClient.connect(lookUpPort);
        ChordClient.send(socketLookUp,sendPackage);
        List<String> res = ChordClient.receive(socketLookUp);
        ChordClient.close(socketLookUp);
        return res;
    }

    static void insert(String content, Integer port) {

        Integer dataID = HashUtil.hash(content);
        List<String> lookupRes = ChordClient.lookup(dataID,port);

        List<String> sendPackage = new LinkedList<>();
        sendPackage.add("insert");
        sendPackage.add(dataID.toString());
        sendPackage.add(content);
        Integer insertPort = Integer.valueOf(lookupRes.get(1));
        Socket socketLookUp = ChordClient.connect(insertPort);
        ChordClient.send(socketLookUp,sendPackage);
        ChordClient.close(socketLookUp);
    }
    static void insertBackup(String content, Integer port) {

        List<String> sendPackage = new LinkedList<>();
        sendPackage.add("backup");
        sendPackage.add(HashUtil.hash(content).toString());
        sendPackage.add(content);
        Socket socketLookUp = ChordClient.connect(port);
        ChordClient.send(socketLookUp,sendPackage);
        ChordClient.close(socketLookUp);
    }

    static void leave(Integer port) {
        List<String> sendPackage = new LinkedList<>();
        sendPackage.add("leave");
        Socket socketLookUp = ChordClient.connect(port);
        ChordClient.send(socketLookUp,sendPackage);
        ChordClient.close(socketLookUp);
    }

}

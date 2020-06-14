package com.houchen;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.beans.Encoder;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Node {
    private Integer hashID;
    private Integer port;

    private ChordServer chordServer;

    Node(int _port) {
        this.port = _port;
    }
    private void init () {
        Random rand = new Random();
        hashID = HashUtil.hash(Integer.valueOf(rand.nextInt()).toString());
        System.out.println("Node "+hashID.toString()+" starts successfully, port: "+port.toString());
        chordServer = new ChordServer(this.hashID,this.port,false);
        chordServer.start();
        chordServer.join();
        chordServer.checkFinger();

    }
    public void start() {

        this.init();

        Scanner sc = new Scanner(System.in);

        while(true) {

            int op = sc.nextInt();

            switch(op) {
                case 1: {
                    String name = sc.next();
                    ChordClient.insert(name,this.port);
                    break;
                }
            }

        }

    }

}

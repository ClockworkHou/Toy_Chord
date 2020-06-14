package com.houchen;

import java.lang.System;
import java.util.Random;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

        boolean superNode = false;
        //superNode = true;
        int id = 4500;
        int port = 1114;

        if (superNode) {
            SuperNode node = new SuperNode();
            node.init(1110);
            node.repaint();
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    return;
                }
                node.repaint();
            }

        }
        else {
            Random rand = new Random();
            int newID = rand.nextInt(100)+id;
            Node node = new Node(port,newID);
            node.start();
        }

    }
}

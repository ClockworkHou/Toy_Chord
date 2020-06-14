package com.houchen;

import java.lang.System;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

        boolean superNode = false;
        //superNode = true;

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
            Node node = new Node(1111);
            node.start();
        }

    }
}

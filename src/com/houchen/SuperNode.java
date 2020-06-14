package com.houchen;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import java.lang.Math;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.net.Socket;


class SuperNode extends JFrame{

    // ---------------Chord--------------------
    private Integer hashID;
    private Integer port;

    private ChordServer chordServer;

    private static boolean flag = false;


    void init (int _port) {
        this.hashID = 8191;
        this.port = _port;
        System.out.println("Node "+hashID.toString()+" starts successfully, port: "+port.toString());
        chordServer = new ChordServer(this.hashID,this.port,true);
        chordServer.start();
        chordServer.checkFinger();
    }


    // ---------------GUI----------------------
    JLabel out = new JLabel("窗口");
    final int RADIUS = 200;
    final int C_X = 500;
    final int C_Y = 400;

    public SuperNode()
    {
        out.setText("Chord Golbal Info");
        setLayout(new FlowLayout());
        setSize(1000,800);
        setLocation(800,0);
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);
        setVisible(true);
    }
    // paint every time interval, update global info
    public void paint(Graphics g) {
        // draw basic chord circle
        Graphics2D g2=(Graphics2D)g;
        g2.clearRect(0, 0, 1000, 800);
        g2.setStroke(new BasicStroke(3.0f));
        g.setColor(Color.BLUE);
        g.drawOval(C_X-RADIUS, C_Y-RADIUS, RADIUS*2, RADIUS*2);
        Integer ID;
        for(ID = 0;ID < 8192; ID += 2048) {
            double angel = (Double.valueOf(ID) / 8192.0) * Math.PI * 2;
            Integer x = Double.valueOf(Math.sin(angel) * RADIUS + C_X).intValue();
            Integer y = Double.valueOf(Math.cos(angel) * RADIUS + C_Y).intValue();
            Integer xb = Double.valueOf(Math.sin(angel) * (0.90*Double.valueOf(RADIUS)) + C_X).intValue();
            Integer yb = Double.valueOf(Math.cos(angel) * (0.90*Double.valueOf(RADIUS)) + C_Y).intValue();
            g.drawOval(x - 5, y - 5, 10, 10);
            g.drawString(ID.toString(),xb-12,yb);
        }
        // draw nodes on chord
        if(flag) {
            ID = this.hashID;
            double angle = (Double.valueOf(ID) / 8192.0) * Math.PI * 2;
            //System.out.println(angel/Math.PI * 180.0);
            Integer x = Double.valueOf(Math.sin(angle) * RADIUS + C_X).intValue();
            Integer y = Double.valueOf(Math.cos(angle) * RADIUS + C_Y).intValue();
            Integer xb = Double.valueOf(Math.sin(angle) * (0.80*Double.valueOf(RADIUS)) + C_X).intValue();
            Integer yb = Double.valueOf(Math.cos(angle) * (0.80*Double.valueOf(RADIUS)) + C_Y).intValue();
            Integer xc = Double.valueOf(Math.sin(angle) * (1.3*Double.valueOf(RADIUS)) + C_X).intValue();
            Integer yc = Double.valueOf(Math.cos(angle) * (1.3*Double.valueOf(RADIUS)) + C_Y).intValue();
            g.setColor(Color.RED);
            g.drawRect(x - 8, y - 8, 16, 16);
            g.setColor(Color.BLACK);
            g.drawString(ID.toString(),xb,yb);
            g.drawString(
                    "succ "+chordServer.getSuccessorID()
                            +" pre "+chordServer.getPredecessorID()
                            +" prepre "+chordServer.getPrePre(),xc,yc);
            g.drawString(chordServer.getFingerID().toString(),xc,yc+20);
            g.drawString(chordServer.data.toString(), xc,yc+40);
            g.drawString(chordServer.backup.toString(), xc,yc+60);

            int nxtPort = chordServer.getSuccessorPort();
            while (nxtPort != this.port) {

                List<String> sendPackage = new LinkedList<>();
                sendPackage.add("getAll");
                Socket socket = ChordClient.connect(nxtPort);
                ChordClient.send(socket,sendPackage);
                List<String> rspn = ChordClient.receive(socket);
                ChordClient.close(socket);
                Iterator<String> it = rspn.iterator();
                ID = Integer.valueOf(it.next());
                nxtPort = Integer.valueOf(it.next());
                String succID = it.next();
                String preID = it.next();
                String prepre = it.next();
                //System.out.println("Paint: "+ID.toString());
                //draw nxt node
                angle = (Double.valueOf(ID) / 8192.0) * Math.PI * 2;
                //System.out.println(angel/Math.PI * 180.0);
                x = Double.valueOf(Math.sin(angle) * RADIUS + C_X).intValue();
                y = Double.valueOf(Math.cos(angle) * RADIUS + C_Y).intValue();
                xb = Double.valueOf(Math.sin(angle) * (0.80*Double.valueOf(RADIUS)) + C_X).intValue();
                yb = Double.valueOf(Math.cos(angle) * (0.80*Double.valueOf(RADIUS)) + C_Y).intValue();
                xc = Double.valueOf(Math.sin(angle) * (1.3*Double.valueOf(RADIUS)) + C_X).intValue();
                yc = Double.valueOf(Math.cos(angle) * (1.3*Double.valueOf(RADIUS)) + C_Y).intValue();
                g.setColor(Color.RED);
                g.drawRect(x - 8, y - 8, 16, 16);
                g.setColor(Color.BLACK);
                g.drawString(ID.toString(),xb,yb);
                g.drawString("succ "+succID+"\npre "+preID+"\nprepre "+prepre,xc,yc);
                g.drawString(it.next(),xc,yc+20);
                g.drawString(it.next(), xc,yc+40);
                g.drawString(it.next(), xc,yc+60);

            };
        }
        flag = true;
    }
};

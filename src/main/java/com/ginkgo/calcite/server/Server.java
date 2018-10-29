package com.ginkgo.calcite.server;

import com.thrift.calciteserver.CalciteServer;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

public class Server {
    public final static int SERVER_PORT = 8099;
    private static String SERVER_IP = "localhost";

    public void startServer() {
        try {
            System.out.println("HelloWorld Server start...");
            // user specific
            TProcessor tprocessor = new CalciteServer.Processor(new CalciteServerHandler());
            CalciteServerHandler.createSchema();
            TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(SERVER_PORT);
            TThreadedSelectorServer.Args tArgs = new TThreadedSelectorServer.Args(serverTransport);
            tArgs.processor(tprocessor);
            tArgs.transportFactory(new TFramedTransport.Factory());
            tArgs.protocolFactory(new TBinaryProtocol.Factory());
            TServer server = new TThreadedSelectorServer(tArgs);
            System.out.println("HelloTThreadedSelectorServer start....");
            server.serve();

        } catch (Exception e) {
            System.out.println("Server start error");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Server server = new Server();
        server.startServer();
    }
}
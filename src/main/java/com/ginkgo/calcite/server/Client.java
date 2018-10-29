package com.ginkgo.calcite.server;

import com.thrift.calciteserver.CalciteServer;
import com.thrift.calciteserver.TPlanResult;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Client {
    public static final int SERVER_PORT = 8099;
    public static final String SERVER_IP = "localhost";

    public void startClient() {
        TTransport tTransport = null;
        try {
            tTransport = new TSocket(SERVER_IP, SERVER_PORT);
            //the same with those of the server
            TTransport transport = new TFramedTransport(tTransport);
            TProtocol protocol = new TBinaryProtocol(transport);
            // user specific
            CalciteServer.Client client = new CalciteServer.Client(protocol);

            transport.open();
            // user processing
            for(int i=0;i<10;++i) {
                TPlanResult result = client.sql2Plan("fzh","123456","default","select * from MYTABLE",true,false);
                System.out.println("Thrift client result=" + result.plan_result);
            }

            transport.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Client client = new Client();
        client.startClient();
    }
}
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.net.InetAddress;


public class FENode {
    static Logger log;

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java FENode FE_port");
            System.exit(-1);
        }

        // initialize log4j
        BasicConfigurator.configure();
        log = Logger.getLogger(FENode.class.getName());

        int portFE = Integer.parseInt(args[0]);
        log.info("Launching FE node on port " + portFE);

        // add FE to WorkerNodePool
        System.out.println("Adding FE into WorkerNode");
        WorkerNodePool.workers.put(WorkerNode.FEWORKERNODE, 0);

        // launch Thrift server
        BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler());
        TNonblockingServerSocket socket = new TNonblockingServerSocket(portFE);
        THsHaServer.Args sargs = new THsHaServer.Args(socket);
        sargs.protocolFactory(new TBinaryProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
        sargs.processorFactory(new TProcessorFactory(processor));
        THsHaServer server = new THsHaServer(sargs);
        server.serve();




    }
    static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }
}

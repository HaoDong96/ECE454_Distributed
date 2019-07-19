import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;

import java.util.*;

public class StorageNode implements CuratorWatcher {
    static Logger log;
    private String host;
    private int port;
    private KeyValueHandler keyValueHandler;
    private CuratorFramework curClient;
    private String zkName;
    private String serverString;

    public StorageNode( KeyValueHandler keyValueHandler, CuratorFramework curClient, String serverString, String zkName) {
        this.keyValueHandler = keyValueHandler;
        this.curClient = curClient;
        this.zkName = zkName;
        this.serverString = serverString;
        this.host = serverString.split(":")[0];
        this.port = Integer.parseInt(serverString.split(":")[1]);
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        log = Logger.getLogger(StorageNode.class.getName());

        if (args.length != 4) {
            System.err.println("Usage: java StorageNode 0:host 1:port 2:zkconnectstring 3:zknode");
            System.exit(-1);
        }

        CuratorFramework curClient =
                CuratorFrameworkFactory.builder()
                        .connectString(args[2])
                        .retryPolicy(new RetryNTimes(10, 1000))
                        .connectionTimeoutMs(1000)
                        .sessionTimeoutMs(10000)
                        .build();

        curClient.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                curClient.close();
            }
        });


        KeyValueHandler keyValueHandler = new KeyValueHandler(curClient, args[3]);
        KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(keyValueHandler);
        TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
        TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
        sargs.protocolFactory(new TBinaryProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
        sargs.processorFactory(new TProcessorFactory(processor));
        sargs.maxWorkerThreads(64);
        TServer server = new TThreadPoolServer(sargs);
        log.info("Launching server");

        new Thread(new Runnable() {
            public void run() {
                server.serve();
            }
        }).start();

        String serverString = args[0] + ":" + args[1];
        curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(args[3] + "/", serverString.getBytes());

        StorageNode watcher = new StorageNode(keyValueHandler, curClient, serverString,args[3]);
        List<String> children = curClient.getChildren().usingWatcher(watcher).forPath(args[3]);

        watcher.identifyRole(children);
        watcher.setUpConnection(children);

    }

    void identifyRole(List<String> children) {
        Role original = keyValueHandler.getRole();
        if (keyValueHandler.getRole() != Role.PRIMARY)
            keyValueHandler.setRole(children.size() == 1 ? Role.PRIMARY : Role.BACKUP);
        System.out.println("Role change for node " + serverString + " from " + original + " to " + keyValueHandler.getRole());
    }

    void setUpConnection(List<String> children) throws Exception {
        Collections.sort(children);
        List<String> nodes = new LinkedList<>();
        for (String child : children) {
            byte[] data = curClient.getData().forPath(zkName + "/" + child);
            String strData = new String(data);
            nodes.add(strData);
        }
        System.out.println(nodes);
        Optional<SiblingNode> siblingNodeOption = SiblingNode.querySibling(nodes, keyValueHandler.getRole());
        System.out.println("Setting up connection to siblingOption:");
        System.out.println(siblingNodeOption);

        keyValueHandler.setSiblingNode(siblingNodeOption);
    }

    @Override
    public void process(WatchedEvent event) throws Exception {
        List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkName);
        identifyRole(children);
        setUpConnection(children);
    }


}

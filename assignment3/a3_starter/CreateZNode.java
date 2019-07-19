import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class CreateZNode {
    static Logger log;

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        log = Logger.getLogger(StorageNode.class.getName());

        if (args.length != 2) {
            System.err.println("Usage: java CreateZKNode zkconnectstring zknode");
            System.exit(-1);
        }

        CuratorFramework curClient =
                CuratorFrameworkFactory.builder()
                        .connectString(args[0])
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

        ZKPaths.mkdirs(curClient.getZookeeperClient().getZooKeeper(), args[1]);
    }
}

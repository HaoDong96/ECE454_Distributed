import org.apache.curator.framework.CuratorFramework;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class KeyValueHandler implements KeyValueService.Iface {
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();
    }

    public String get(String key) throws org.apache.thrift.TException {
        String ret = myMap.get(key);
        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        myMap.put(key, value);
    }
}

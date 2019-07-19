import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

enum Role {
    INIT,
    PRIMARY,
    BACKUP
}

public class KeyValueHandler implements KeyValueService.Iface {
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private Role role;
    private Map<String, Integer> backupOpsMap;
    private AtomicInteger primaryOps;
    private Optional<ThriftConnection> connectionToSibling;

    public KeyValueHandler(CuratorFramework curClient, String zkNode) {
        this.curClient = curClient;
        this.zkNode = zkNode;
        this.role = Role.INIT;
        myMap = new ConcurrentHashMap<>();
        backupOpsMap = new ConcurrentHashMap<>();
        primaryOps = new AtomicInteger(0);
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public Role getRole() {
        return role;
    }

    public void setConnectionToSibling(ThriftConnection connection) {
        this.connectionToSibling = Optional.ofNullable(connection);
    }

    @Override
    public String get(String key) throws TException {
        String ret = myMap.get(key);
        if (ret == null)
            return "";
        else
            return ret;
    }

    @Override
    public void put(String key, String value) throws org.apache.thrift.TException {
        myMap.put(key, value);
        if (role == Role.PRIMARY && connectionToSibling.isPresent()) {
            replicateCore(key, value, primaryOps.addAndGet(1));
        }
    }

    @Override
    public void replicate(String key, String value, int primaryOps) throws TException {
        if (backupOpsMap.containsKey(key)) {
            if (primaryOps >= backupOpsMap.get(key)) {
                myMap.put(key, value);
                backupOpsMap.put(key, primaryOps);
            }
        } else {
            myMap.put(key, value);
            backupOpsMap.put(key, primaryOps);
        }
    }

    public void replicateCore(String key, String value, int primaryOps) {
        try {
            if (connectionToSibling.isPresent()) {
                KeyValueService.Client client = connectionToSibling.get().getClient();
                connectionToSibling.get().openTransport();
                client.replicate(key, value, primaryOps);
            }
        } catch (TException e) {
            e.printStackTrace();
        }
    }

}

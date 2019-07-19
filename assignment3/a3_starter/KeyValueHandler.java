import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;

import java.util.*;
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
    private Optional<SiblingNode> siblingNode;

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

    public void setSiblingNode(Optional<SiblingNode> siblingNode) {
        this.siblingNode = siblingNode;
    }

    public Optional<SiblingNode> getSiblingNode() {
        return siblingNode;
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
        if (role == Role.PRIMARY && siblingNode.isPresent()) {
            replicateCaller(key, value, primaryOps.addAndGet(1));
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

    @Override
    public void transfer(List<String> keys, List<String> values) throws TException {
        System.out.println("Receiving k-v map from primary...");
        for (int i = 0; i < keys.size(); i++) {
            myMap.putIfAbsent(keys.get(i), values.get(i));
        }
    }

    public void replicateCaller(String key, String value, int primaryOps) {
        if (siblingNode.isPresent()) {
            ThriftConnection connection = siblingNode.get().getNewConnection();
            KeyValueService.Client client = connection.getClient();
            try {
                connection.openConnection();
                client.replicate(key, value, primaryOps);
            } catch (TException e) {
                e.printStackTrace();
            } finally {
                connection.closeConnection();
            }
        }
    }

    public void transferCaller() {
        System.out.println("Transferring k-v map to backup...");
        List<String> keys = new LinkedList<>();
        List<String> values = new LinkedList<>();

        for (String key : myMap.keySet()) {
            keys.add(key);
            values.add(myMap.get(key));
        }

        if (siblingNode.isPresent()) {
            ThriftConnection connection = siblingNode.get().getNewConnection();
            KeyValueService.Client client = connection.getClient();
            try {
                connection.openConnection();
                client.transfer(keys, values);
            } catch (TException e) {
                e.printStackTrace();
            } finally {
                connection.closeConnection();
            }
        }
    }
}

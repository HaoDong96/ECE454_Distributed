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
    private Map<Integer, Boolean> ackMap;

    public KeyValueHandler(CuratorFramework curClient, String zkNode) {
        this.curClient = curClient;
        this.zkNode = zkNode;
        this.role = Role.INIT;
        myMap = new ConcurrentHashMap<>();
        backupOpsMap = new ConcurrentHashMap<>();
        ackMap = new ConcurrentHashMap<>();
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
        ackCaller(primaryOps);
    }

    @Override
    public void transfer(List<String> keys, List<String> values, int primaryOps) throws TException {
        System.out.println("Receiving k-v map from primary...");
        for (int i = 0; i < keys.size(); i++) {
            myMap.putIfAbsent(keys.get(i), values.get(i));
        }
        ackCaller(primaryOps);
    }

    @Override
    public void ack(int ackOps) {
        ackMap.put(ackOps, true);
    }

    public void replicateCaller(String key, String value, int primaryOps) {
        if (siblingNode.isPresent()) {
            ThriftConnection connection = siblingNode.get().getNewConnection();
            KeyValueService.Client client = connection.getClient();
            try {
                ackMap.put(primaryOps, false);
                connection.openConnection();
                client.replicate(key, value, primaryOps);
                while (!ackMap.get(primaryOps)) {
                    Thread.sleep(1);
                }
            } catch (TException | InterruptedException e) {
                System.out.println(e.getClass() + " Exception thrown");
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
                ackMap.put(primaryOps.get(), false);
                connection.openConnection();
                client.transfer(keys, values, primaryOps.get());
                while (!ackMap.get(primaryOps.get())) {
                    Thread.sleep(1);
                }
            } catch (TException | InterruptedException e) {
                System.out.println(e.getClass() + " Exception thrown");
            } finally {
                connection.closeConnection();
            }
        }
    }

    public void ackCaller(int ackOps) {
        while (siblingNode == null) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                System.out.println(e.getClass() + " Exception thrown");
            }
        }
        if (siblingNode.isPresent()) {
            ThriftConnection connection = siblingNode.get().getNewConnection();
            KeyValueService.Client client = connection.getClient();
            try {
                connection.openConnection();
                client.ack(ackOps);
            } catch (TException e) {
                System.out.println(e.getClass() + " Exception thrown");
            } finally {
                connection.closeConnection();
            }
        }
    }
}

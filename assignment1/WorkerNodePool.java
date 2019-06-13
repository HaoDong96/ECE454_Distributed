import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerNodePool {
    // workers: <WorkNode, workload>
    public static ConcurrentHashMap<WorkerNode, Integer> workers = new ConcurrentHashMap<>();

    public WorkerNodePool() {
    }

    public static HashMap<WorkerNode, int[]> distributeLoad(int totalLoad) {
        HashMap<WorkerNode, int[]> res = new HashMap<>();
        if (workers.size() == 0) { // No worker available, return empty distribution map
            return res;
        } else if (workers.size() == 1) {
            res.put(workers.keySet().iterator().next(), new int[] {0, totalLoad - 1}); // Only one worker available, assign all work to it
        }

        HashMap<WorkerNode, Integer> workload = new HashMap<>();
        int loadToDistribute = totalLoad;

        for (WorkerNode wn : workers.keySet()) {
            workload.put(wn, 0);
        }

        int minLoad = Collections.min(workers.values());

        while (loadToDistribute > 0) {
            for (WorkerNode wn : workload.keySet()) {
                if (workload.get(wn) + workers.get(wn) <= minLoad) {
                    minLoad = workload.get(wn) + workers.get(wn) + 1;
                    workload.put(wn, workload.get(wn) + 1);
                    loadToDistribute--;
                }
            }
        }

        System.out.println(workload);

        int i = 0;
        for (WorkerNode wn : workers.keySet()) {
            res.put(wn, new int[] {i, i + workload.get(wn) - 1});
            i = i + workload.get(wn);
        }

        assert i == totalLoad - 1;
        return res;
    }

    public static WorkerNode getAvailableWorker() {
        WorkerNode slackNode = null;
        for (WorkerNode wn : workers.keySet()) {
            if (slackNode == null || workers.get(wn) <= workers.get(slackNode)) {
                slackNode = wn;
            }
        }
//        System.out.println(WorkerNodePool.workers);
//        System.out.println("Returned node: " + slackNode);
        return slackNode;
    }

}

class WorkerNode {
    private String host;
    private short port;

    class JobConnection {
        private BcryptService.Client clientToWorker;
        private TSocket sock;
        private TTransport transport;
        private TProtocol protocol;

        public JobConnection() {
            sock = new TSocket(host, port);
            transport = new TFramedTransport(sock);
            protocol = new TBinaryProtocol(transport);
            clientToWorker = new BcryptService.Client(protocol);
            int start;
            int end;
        }

        public BcryptService.Client getClientToWorker() {
            return clientToWorker;
        }

        public TTransport getTransport() {
            return transport;
        }
    }

    public WorkerNode(String host, short port) {
        this.host = host;
        this.port = port;
    }

    JobConnection getNewConnection() {
        return new JobConnection();
    }

     List<String> assignHashPassword(List<String> password, short logRounds) throws TException {
        JobConnection jobConnection = getNewConnection();
        TTransport tTransport = jobConnection.getTransport();
        List<String> res = null;
        try {
            if (!tTransport.isOpen())
                tTransport.open();
            int load = password.size();
            WorkerNodePool.workers.put(this, WorkerNodePool.workers.get(this) + load);
            res = jobConnection.getClientToWorker().BEhashPassword(password, logRounds);
            WorkerNodePool.workers.put(this, WorkerNodePool.workers.get(this) - load);
        } catch (Exception e) {
            if (e.getClass() == TTransportException.class) {
                System.out.println(this + " died for hashPassword.");
                e.getStackTrace();
                WorkerNodePool.workers.remove(this);
                res = null;
            } else {
                e.printStackTrace();
            }
        } finally {
            if (tTransport != null && tTransport.isOpen()) {
                tTransport.close();
            }
        }
        return res;
    }

    List<Boolean> assignCheckPassword(List<String> password, List<String> hash) throws TException {
        JobConnection jobConnection = getNewConnection();
        TTransport tTransport = jobConnection.getTransport();
        List<Boolean> res = null;
        try {
            if (!tTransport.isOpen())
                tTransport.open();
            int load = password.size();
            WorkerNodePool.workers.put(this, WorkerNodePool.workers.get(this) + load);
            res = jobConnection.getClientToWorker().BEcheckPassword(password, hash);
            WorkerNodePool.workers.put(this, WorkerNodePool.workers.get(this) - load);
        } catch (Exception e) {
            if (e.getClass() == TTransportException.class) {
                System.out.println(this + " died for checkPassword.");
                e.getStackTrace();
                WorkerNodePool.workers.remove(this);
                res = null;
            } else if (e.getClass() == IllegalArgument.class)
                System.out.println("Catch exception from checkPassword.");
            else
                e.printStackTrace();

        } finally {
            if (tTransport != null && tTransport.isOpen()) {
                tTransport.close();
            }
        }
        return res;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public int hashCode() {
        return host.hashCode() + port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != WorkerNode.class)
            return false;
        return host.equals(((WorkerNode) obj).host) && port == ((WorkerNode) obj).port;
    }
}

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerNodePool {
    // workers: <WorkNode, workload>
    public static ConcurrentHashMap<WorkerNode, Integer> workers = new ConcurrentHashMap<>();

    public WorkerNodePool() {
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

    class Connection {
        private BcryptService.Client clientToWorker;
        private TSocket sock;
        private TTransport transport;
        private TProtocol protocol;

        public Connection() {
            sock = new TSocket(host, port);
            transport = new TFramedTransport(sock);
            protocol = new TBinaryProtocol(transport);
            clientToWorker = new BcryptService.Client(protocol);
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

    WorkerNode.Connection getNewConnection() {
//        return new BcryptService.Client(new TBinaryProtocol(new TFramedTransport(sock)));
        return new Connection();
    }

    List<String> assignHashPassword(List<String> password, short logRounds) throws TException {
        WorkerNode.Connection connection = getNewConnection();
        TTransport tTransport = connection.getTransport();
        List<String> res = null;
        try {
            if (!tTransport.isOpen())
                tTransport.open();
            int load = password.size() * (int) Math.pow(2, logRounds);
            WorkerNodePool.workers.put(this, WorkerNodePool.workers.get(this) + load);
            res = connection.getClientToWorker().BEhashPassword(password, logRounds);
            WorkerNodePool.workers.put(this, WorkerNodePool.workers.get(this) - load);
        } catch (Exception e) {
            if (e.getClass() == TTransportException.class) {
                System.out.println(this + " died for hashPassword.");
                e.getStackTrace();
                WorkerNodePool.workers.remove(this);
                res = null;
            } else {
                System.out.println("Other kinds of exception, need attention.");
            }
        } finally {
            if (tTransport != null && tTransport.isOpen()) {
                tTransport.close();
            }
        }
        return res;
    }

    List<Boolean> assignCheckPassword(List<String> password, List<String> hash) throws TException {
        WorkerNode.Connection connection = getNewConnection();
        TTransport tTransport = connection.getTransport();
        List<Boolean> res = null;
        try {
            if (!tTransport.isOpen())
                tTransport.open();
            int load = password.size() * (int) Math.pow(2, 10);
            WorkerNodePool.workers.put(this, WorkerNodePool.workers.get(this) + load);
            res = connection.getClientToWorker().BEcheckPassword(password, hash);
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
                System.out.println("Other kinds of exception, need attention.");

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

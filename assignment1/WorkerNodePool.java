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
        System.out.println("########################\n"+workers.keySet()+"\n########################");
        WorkerNode slackNode = null;
        for (WorkerNode wn : workers.keySet()) {
            if (slackNode == null || workers.get(wn) <= workers.get(slackNode)) {
                slackNode = wn;
            }
        }
        return slackNode;
    }

}

class WorkerNode {
    private String host;
    private short port;
    private BcryptService.Client clientToWorker;
    private TSocket sock;
    private TTransport tTransport;
    private TProtocol tProtocol;

    public WorkerNode(String host, short port) {
        this.host = host;
        this.port = port;
        this.sock = new TSocket(host, port);
        this.tTransport = new TFramedTransport(sock);
        this.tProtocol = new TBinaryProtocol(tTransport);
        this.clientToWorker = new BcryptService.Client(tProtocol);
    }

    BcryptService.Client getNewClient() {
//        return new BcryptService.Client(new TBinaryProtocol(new TFramedTransport(sock)));
        return new BcryptService.Client(tProtocol);
    }

    List<String> assignHashPassword(List<String> password, short logRounds) throws TException {
        List<String> res = null;
        try {
            if (!tTransport.isOpen())
                tTransport.open();
            int load = password.size() * (int) Math.pow(2, logRounds);
            WorkerNodePool.workers.put(this, WorkerNodePool.workers.get(this) + load);
            res = clientToWorker.BEhashPassword(password, logRounds);
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

        List<Boolean> res = null;
        try {
            if (!tTransport.isOpen())
                tTransport.open();
            int load = password.size();
            WorkerNodePool.workers.put(this, WorkerNodePool.workers.get(this) + load);
            res = clientToWorker.BEcheckPassword(password, hash);
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

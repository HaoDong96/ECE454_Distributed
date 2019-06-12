import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerNodePool {
    // transport.open();
    public static ConcurrentHashMap<WorkerNode, Boolean> workers = new ConcurrentHashMap<>();

    public WorkerNodePool() {
    }

    public static WorkerNode getAvailableWorker() {
        for (WorkerNode wn : workers.keySet()) {
            if (workers.get(wn)) return wn;
        }
        return null;
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

    List<String> hashPassword(List<String> password, short logRounds) throws TException {
        if (!tTransport.isOpen())
            tTransport.open();
        List<String> res = clientToWorker.BEhashPassword(password, logRounds);
        tTransport.close();
        return res;
    }

    List<Boolean> checkPassword(List<String> password, List<String> hash) throws TException {
        if (!tTransport.isOpen()) {
            tTransport.open();
        }
        List<Boolean> res = clientToWorker.BEcheckPassword(password, hash);
        tTransport.close();
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

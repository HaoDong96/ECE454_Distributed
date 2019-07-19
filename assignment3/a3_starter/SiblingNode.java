import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.Optional;
import java.util.*;

public class SiblingNode {
    String host;
    int port;

    static Optional<SiblingNode> querySibling(List<String> nodes, Role role) {

        assert nodes.size() <= 2;
        if (nodes.size() == 1)
            return Optional.empty();
        Collections.sort(nodes);
        String sibling;
        sibling = role == Role.PRIMARY ? nodes.get(1) : nodes.get(0);
        String host = sibling.split(":")[0];
        int port = Integer.parseInt(sibling.split(":")[1]);
        return Optional.of(new SiblingNode(host, port));
    }

    public SiblingNode(String host, int port) {
        this.host = host;
        this.port = port;
    }

    ThriftConnection getNewConnection() {
        return new ThriftConnection(host, port);
    }

}

class ThriftConnection {
    private TSocket sock;
    private TTransport transport;
    private TProtocol protocol;
    private KeyValueService.Client client;


    public ThriftConnection(String host, int port) {
        this.sock = new TSocket(host, port);
        this.transport = new TFramedTransport(sock);
        this.protocol = new TBinaryProtocol(transport);
        this.client = new KeyValueService.Client(protocol);
    }

    public KeyValueService.Client getClient() {
        return client;
    }

    public void openTransport() throws TTransportException {
        transport.open();
    }

    void closeConnection() {
        transport.close();
    }
}
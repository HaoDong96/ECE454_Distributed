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
    private TSocket sock;
    private TTransport transport;

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
        sock = new TSocket(host, port);
        transport = new TFramedTransport(sock);
    }

    ThriftConnection getNewConnection() {
        return new ThriftConnection(transport);
    }

}

class ThriftConnection {
    private KeyValueService.Client client;
    private TTransport transport;

    public ThriftConnection(TTransport transport) {
        this.transport = transport;
        TProtocol protocol = new TBinaryProtocol(transport);
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
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;

public class ClientA {
    static void work(String... args) {
        if (args.length != 3) {
            System.err.println("Usage: java Client FE_host FE_port password");
            System.exit(-1);
        }

        try {
            TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
            TTransport transport = new TFramedTransport(sock);
            TProtocol protocol = new TBinaryProtocol(transport);
            BcryptService.Client client = new BcryptService.Client(protocol);
            transport.open();

            List<String> password = new ArrayList<>();
            for(int i = 0; i < 16; i ++)
                password.add("faldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurvcvmvcmdoiZZ");
            List<String> hash = client.hashPassword(password, (short) 10);
            transport.close();
        } catch (TException x) {
            x.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("#######TEST START##########");
        long initTime = System.currentTimeMillis();
        for (int i = 0; i < 20; i++) {
            long t1 = System.currentTimeMillis();
            work(args);
            long t2 = System.currentTimeMillis();
            System.out.println("16 passwords processed in " + (t2-t1) + " ms.");
        }
        long afterHashTime = System.currentTimeMillis();
        System.out.println("320 passwords processed in " + (afterHashTime-initTime) + " ms.");
        System.out.println("Throughput (hashpw/s) is " + (320.0 / (afterHashTime - initTime) * 1000));
    }
}

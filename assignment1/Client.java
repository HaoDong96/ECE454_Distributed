import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;

public class Client {
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

            long init_time = System.currentTimeMillis();
            List<String> password = new ArrayList<>();
            for(int i = 0; i < 100; i ++)
                password.add("faldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurvcvmvcmdoiZZ");
            List<String> hash = client.hashPassword(password, (short) 10);
            long after_hash_time = System.currentTimeMillis();
            System.out.println(after_hash_time-init_time);

            for (int i = 0; i < password.size(); i++) {
                System.out.println("Password: " + password.get(i));
                System.out.println("Hash: " + hash.get(i));
            }
//            System.out.println("Positive check: " + client.checkPassword(password, hash));

//            hash.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
//            System.out.println("Negative check: " + client.checkPassword(password, hash));
//            try {
//                hash.set(0, "too short");
//                List<Boolean> rets = client.checkPassword(password, hash);
//                System.out.println("Exception check: no exception thrown");
//            } catch (Exception e) {
//                System.out.println("Exception check: exception thrown");
//            }

            transport.close();
        } catch (TException x) {
            x.printStackTrace();
        }
    }
    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            work(args);
//            Thread t = new Thread(() -> work(args));
//            t.start();
        }
    }
}

import org.apache.thrift.TException;
import org.mindrot.jbcrypt.BCrypt;

import java.util.ArrayList;
import java.util.List;

public class BcryptServiceHandler implements BcryptService.Iface {
    @Override
    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, TException {
        WorkerNode wn = WorkerNodePool.getAvailableWorker();
        List<String> res;
        if (wn != null) {
            System.out.println("Found available worker " + wn);
            res = wn.hashPassword(password, logRounds);
        } else {
            System.out.println("No worker available. Processing by FE...");
            res = hashPasswordCore(password, logRounds);
        }
        return res;
    }

    @Override
    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, TException {
        WorkerNode wn = WorkerNodePool.getAvailableWorker();
        List<Boolean> res;
        if (wn != null) {
            System.out.println("Found available worker " + wn);
            res = wn.checkPassword(password, hash);
        } else {
            System.out.println("No worker available. Processing by FE...");
            res = checkPasswordCore(password, hash);
        }
        return res;
    }

    @Override
    public List<String> BEhashPassword(List<String> password, short logRounds) throws IllegalArgument, TException {
        System.out.println("Finished a hashing job. Returning result.");
        return hashPasswordCore(password, logRounds);
    }

    @Override
    public List<Boolean> BEcheckPassword(List<String> password, List<String> hash) throws IllegalArgument, TException {
        System.out.println("Finished a checking job. Returning result.");
        return checkPasswordCore(password, hash);
    }

    @Override
    public void addBE(String BEhost, short BEport) throws IllegalArgument, TException {
        System.out.printf("Adding Backend Server: %s:%d\n", BEhost, BEport);
        WorkerNodePool.workers.put(new WorkerNode(BEhost, BEport), true);
    }

    public List<String> hashPasswordCore(List<String> password, short logRounds) throws IllegalArgument, TException {
        try {
            List<String> ret = new ArrayList<>();
            for (String passwd : password) {
                String hash = BCrypt.hashpw(passwd, BCrypt.gensalt(logRounds));
                ret.add(hash);
            }
            return ret;
        } catch (Exception e) {
            throw new IllegalArgument(e.getMessage());
        }
    }

    public List<Boolean> checkPasswordCore(List<String> password, List<String> hash) throws IllegalArgument, TException {
        try {
            if (password.size() != hash.size())
                throw new IllegalArgument();
            List<Boolean> ret = new ArrayList<>();
            for (int i = 0; i < password.size(); i++) {
                ret.add(BCrypt.checkpw(password.get(i), hash.get(i)));
            }
            return ret;
        } catch (Exception e) {
            throw new IllegalArgument(e.getMessage());
        }
    }
}

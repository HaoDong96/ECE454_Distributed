import org.apache.thrift.TException;
import org.mindrot.jbcrypt.BCrypt;

import java.util.ArrayList;
import java.util.List;

public class BcryptServiceHandler implements BcryptService.Iface {
    @Override
    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, TException {
        if (password.isEmpty()) {
            throw new IllegalArgument("Password list empty");
        }

        if (logRounds < 4 || logRounds > 30) {
            throw new IllegalArgument("Illegal logRound argument. BCrypt supports rounds between 4 and 30 (inclusive).");
        }

        WorkerNode wn = WorkerNodePool.getAvailableWorker();
        List<String> res;
        if (wn != null) {
            System.out.println("Found available worker for hashPassword " + wn);
            res = wn.assignHashPassword(password, logRounds);
            // keep getting available workers and try hashPassword until res is not null
            while (res == null) {
                System.out.println("Looking for another one...");
                wn = WorkerNodePool.getAvailableWorker();
                if (wn != null) {
                    System.out.println("Found available worker " + wn);
                    res = wn.assignHashPassword(password, logRounds);
                }
                // there is no available worker, jump out while and FEHashPassword
                else
                    break;
            }
            if (res != null)
                return res;
        }
        // if there is no worker nodes
        System.out.println("No worker available. HashPassword by FE...");
        res = hashPasswordCore(password, logRounds);
        return res;
    }

    @Override
    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, TException {
        if (password.isEmpty() || hash.isEmpty() || password.size() != hash.size()) {
            throw new IllegalArgument("One or more lists are empty or lengths mismatch");
        }

        WorkerNode wn = WorkerNodePool.getAvailableWorker();
        List<Boolean> res;
        if (wn != null) {
            System.out.println("Found available worker for checkPassword " + wn);
            res = wn.assignCheckPassword(password, hash);
            // keep getting available workers and try checkPassword until res is not null
            while (res == null) {
                System.out.println("worker " + wn + " died, Looking for another one...");
                wn = WorkerNodePool.getAvailableWorker();
                if (wn != null) {
                    System.out.println("Found available worker " + wn);
                    res = wn.assignCheckPassword(password, hash);
                }
                // there is no available worker, jump out while and FEHashPassword
                else
                    break;
            }
            if (res != null)
                return res;
        }
        // if there is no worker nodes
        System.out.println("No worker available. CheckPassword by FE...");
        res = checkPasswordCore(password, hash);
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
        WorkerNodePool.workers.put(new WorkerNode(BEhost, BEport), 0);
    }

    @Override
    public void pingBE() throws TException {

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

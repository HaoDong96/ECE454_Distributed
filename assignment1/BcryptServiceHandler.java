import org.apache.thrift.TException;
import org.mindrot.jbcrypt.BCrypt;

import java.awt.SystemTray;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BcryptServiceHandler implements BcryptService.Iface {
    @Override
    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, TException {
        if (password.isEmpty()) {
            throw new IllegalArgument("Password list empty");
        }

        if (logRounds < 4 || logRounds > 30) {
            throw new IllegalArgument("Illegal logRound argument. BCrypt supports rounds between 4 and 30 (inclusive).");
        }

        String[] res = new String[password.size()];

        HashMap<WorkerNode, int[]> loads = WorkerNodePool.distributeLoad(password.size());
        for (WorkerNode e : loads.keySet()) {
            System.out.println("" + e + "---[" + loads.get(e)[0] + "," + loads.get(e)[1] + "]");
        }

        // If no BE node at all
        if (loads.isEmpty()) {
            System.out.println("No worker available. HashPassword by FE...");
            return hashPasswordCore(password, logRounds);
        }

        // If some BE nodes exist
        for (WorkerNode wn : loads.keySet()) {
            System.out.println("Found available worker for hashPassword " + wn);
            if (wn != null) {
                int start = loads.get(wn)[0];
                int end = loads.get(wn)[1];
                List<String> r = wn.assignHashPassword(password.subList(start, end + 1), logRounds);

                // If node is down, perform calculation by FE
                if (r == null)
                    r = hashPasswordCore(password.subList(start, end + 1), logRounds);

                // Store partial result into the global result array
                for (int i = start; i <= end; i++) {
                    res[i] = r.get(i - start);
                }
            }
        }

        return Arrays.asList(res);
    }

    @Override
    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, TException {
        if (password.isEmpty() || hash.isEmpty() || password.size() != hash.size()) {
            throw new IllegalArgument("One or more lists are empty or lengths mismatch");
        }

        Boolean[] res = new Boolean[password.size()];

        HashMap<WorkerNode, int[]> loads = WorkerNodePool.distributeLoad(password.size());
        for (WorkerNode e : loads.keySet()) {
            System.out.println("" + e + "---[" + loads.get(e)[0] + "," + loads.get(e)[1] + "]");
        }

        // If no BE node at all
        if (loads.isEmpty()) {
            System.out.println("No worker available. Checking by FE...");
            return checkPasswordCore(password, hash);
        }

        // If some BE nodes exist
        for (WorkerNode wn : loads.keySet()) {
            System.out.println("Found available worker for Checking " + wn);
            if (wn != null) {
                int start = loads.get(wn)[0];
                int end = loads.get(wn)[1];
                List<Boolean> r = wn.assignCheckPassword(password.subList(start, end + 1), hash.subList(start, end + 1));

                // If node is down, perform calculation by FE
                if (r == null)
                    r = checkPasswordCore(password.subList(start, end + 1), hash.subList(start, end + 1));

                // Store partial result into the global result array
                for (int i = start; i <= end; i++) {
                    res[i] = r.get(i - start);
                }
            }
        }

        return Arrays.asList(res);
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

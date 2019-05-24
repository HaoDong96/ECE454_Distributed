import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

class CCServer {
	static HashMap<Integer, int[]> hm = new HashMap<>();

	static int rank(int n) {
		return hm.get(n)[0];
	}

	static int parent(int n) {
		return hm.get(n)[1];
	}

	static void setRank(int n, int r) {
		hm.get(n)[0] = r;
	}

	static void setParent(int n, int p) {
		if (p != -1)
			hm.get(n)[1] = p;
	}

	static int find(int n) {
		while (parent(n) != -1) {
			setParent(n, parent(parent(n)));
			n = parent(n);
		}
		return n;
	}

	static void union(int a, int b) {
		if (!hm.containsKey(a))
			hm.put(a, new int[] {0, -1});
		if (!hm.containsKey(b))
			hm.put(b, new int[] {0, -1});

		int rootA = find(a);
		int rootB = find(b);

		if (rootA != rootB) {
            if (rank(rootA) > rank(rootB)) {
                setParent(rootB, rootA);
            } else if (rank(rootA) < rank(rootB)) {
                setParent(rootA, rootB);
            } else {
                setParent(rootB, rootA);
                setRank(rootA, rank(rootA) + 1);
            }
        }
	}

    public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: java CCServer port");
			System.exit(-1);
		}
		int port = Integer.parseInt(args[0]);

		ServerSocket ssock = new ServerSocket(port);
		System.out.println("listening on port " + port);
		//noinspection InfiniteLoopStatement
		while (true) {
			try {
				/*
                YOUR CODE GOES HERE
                - accept connection from server socket
                - read requests from connection repeatedly
                - for each request, compute an output and send a response
                - each message has a 4-byte header followed by a payload
                - the header is the length of the payload
                    (signed, two's complement, big-endian)
                - the payload is a string
                    (UTF-8, big-endian)
                */

				// Receive from client
			    Socket csock = ssock.accept();

				long t0 = System.currentTimeMillis();
				DataInputStream din = new DataInputStream(csock.getInputStream());
				int inDataLen = din.readInt();
				BufferedReader bf = new BufferedReader(new InputStreamReader(din));

				long t1 = System.currentTimeMillis();
				System.out.printf("%d ms spent in receiving data\n", t1 - t0);




				String temp;
				System.out.println(inDataLen);
				while(inDataLen > 0){
					temp = bf.readLine();
					inDataLen -= temp.getBytes().length;
					inDataLen --;

					int i = Integer.valueOf(temp.split(" ")[0]);
					int j = Integer.valueOf(temp.split(" ")[1]);
					union(i, j);
				}

				long tReadString = System.currentTimeMillis();
				System.out.printf("%d ms spent in reading string\n", tReadString - t1);


				long t2 = System.currentTimeMillis();
				System.out.printf("%d ms spent in performing union\n", t2 - tReadString);

				// Response to client
				DataOutputStream dout = new DataOutputStream(csock.getOutputStream());
				StringBuilder output = new StringBuilder();
				for (int node: hm.keySet()){
					output.append(node).append(" ").append(find(node)).append("\n");
				}

				long t3 = System.currentTimeMillis();
				System.out.printf("%d ms spent in making string\n", t3 - t2);

				byte[] resp = output.toString().getBytes();
				dout.writeInt(resp.length);
				dout.write(resp);
				dout.flush();
				dout.close();

				long endTime = System.currentTimeMillis();
				System.out.printf("%d ms spent in response\n", endTime - t3);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
    }
}

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.swing.plaf.synth.SynthTextAreaUI;

class CCServer {

    // key: nodeNum, value:  rank,parent

    static HashMap<Integer, int[]> hs = new HashMap<>();

    static int rank(int n) {
        return hs.get(n)[0];
    }

    static int parent(int n) {
        return hs.get(n)[1];
    }

    static void setRank(int n, int r) {
        hs.get(n)[0] = r;
    }

    static void setParent(int n, int p) {
        hs.get(n)[1] = p;
    }

    static int root(int n) {
        while (parent(n) != -1) {
            n = parent(n);
        }
        return n;
    }

    static void union(int a, int b) {
        if (!hs.containsKey(a))
            hs.put(a, new int[] {0, -1});
        if (!hs.containsKey(b))
            hs.put(b, new int[] {0, -1});

        int rootA = root(a);
        int rootB = root(b);

        if (rootA == rootB)
            return;

        if (rank(rootA) > rank(rootB)) {
            setParent(rootB, rootA);
        } else if (rank(rootA) < rank(rootB)) {
            setParent(rootA, rootB);
        } else {
            setParent(rootB, rootA);
            setRank(rootA, rank(rootA)+1);
        }
    }




    public static void main(String args[]) throws Exception {
        if (args.length != 1) {
            System.out.println("usage: java CCServer port");
            System.exit(-1);
        }
        int port = Integer.parseInt(args[0]);

        ServerSocket ssock = new ServerSocket(port);
        System.out.println("listening on port " + port);
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
                // accept connection from server socket
                Socket acceptSock = ssock.accept();
                DataInputStream din = new DataInputStream(acceptSock.getInputStream());
                int acceptDataLen = din.readInt();
                byte[] bytes = new byte[acceptDataLen];
                din.readFully(bytes);
                String input = new String(bytes, StandardCharsets.UTF_8);

                //parse the input numbers, save them into HashMap and union them
                String[] lines = input.split("\n");
                int i;
                int j;

                for (String line : lines) {
                    i = Integer.valueOf(line.split(" ")[0]);
                    j = Integer.valueOf(line.split(" ")[1]);

                    union(i,j);

                    System.out.println(i);
                    System.out.println(j);
                    System.out.println("#############");
                }
                //output
                DataOutputStream dout = new DataOutputStream(acceptSock.getOutputStream());
                StringBuilder output = new StringBuilder();
                System.out.println(hs.size());
                for (int node: hs.keySet()){
                    output.append(String.format("%d %d\n", node, root(node)));
                }
                System.out.println(output);
                byte[] resp = output.toString().getBytes();
                dout.writeInt(resp.length);
                dout.write(resp);
                dout.flush();


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}







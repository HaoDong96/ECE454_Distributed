import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Properties;

class Pair<T1, T2> {
    T1 v1;
    T2 v2;

    Pair(T1 v1, T2 v2) {
        this.v1 = v1;
        this.v2 = v2;
    }
}

public class A4Application {
    private static Integer getOrElse(Integer n, Integer e) {
        if (n != null)
            return n;
        else
            return e;
    }

    static int makeInt(int i, int j) {
        return i * 40000 + j;
    }

    static Pair<Integer, Integer> readInt(int n) {
        int[] res = new int[2];
        if (n >= 0) {
            res[1] = n % 40000;
            if (res[1] > 20000) res[1] -= 40000;
            res[0] = (n - res[1]) / 40000;
        } else {
            n = -n;
            res[1] = n % 40000;
            if (res[1] > 20000) res[1] -= 40000;
            res[0] = (n - res[1]) / 40000;
            res[0] = -res[0];
            res[1] = -res[1];
        }
        return new Pair<>(res[0], res[1]);
    }

    public static void main(String[] args) throws Exception {
        // do not modify the structure of the command line
        String bootstrapServers = args[0];
        String appName = args[1];
        String studentTopic = args[2];
        String classroomTopic = args[3];
        String outputTopic = args[4];
        String stateStoreDir = args[5];

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

        // add code here if you need any additional configuration options

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Integer> studentKTable = builder.stream(studentTopic)
                                                    .map((k, v) -> KeyValue.pair(k.toString(), v.toString()))
                                                    .groupByKey()
                                                    .reduce((x, y) -> y)
                                                    .groupBy((studentID, roomID) -> KeyValue.pair(roomID, studentID))
                                                    .count()
                                                    .mapValues(Long::intValue);

        KTable<String, Integer> classroomKTable = builder.stream(classroomTopic)
                                                    .map((k, v) -> KeyValue.pair(k.toString(), v.toString()))
                                                    .groupByKey()
                                                    .reduce((n1, n2) -> n2)
                                                    .mapValues((ValueMapper<String, Integer>) Integer::valueOf);

        KTable<String, Pair<Integer, Integer>> allEntryKTable = studentKTable.outerJoin(classroomKTable, (v1, v2) ->
                new Pair<>(getOrElse(v1, 0), getOrElse(v2, Integer.MAX_VALUE)));

        KStream<String, String> result = allEntryKTable
                .mapValues(pair -> pair.v1 - pair.v2)
                .toStream()
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce((v1, v2) -> {
                    Pair<Integer, Integer> prev = readInt(v1);
                    return makeInt(prev.v2, v2);
                })
                .toStream()
                .join(allEntryKTable, Pair::new)
                // (RoomID, (lastTwo, (Occupied, Capacity)))
                .filter((k, v) -> {
                    Pair<Integer, Integer> lastTwo = readInt(v.v1);
                    return v.v2.v1 - v.v2.v2 > 0 || (v.v2.v1.equals(v.v2.v2) && lastTwo.v1 > lastTwo.v2);
                })
                .map((k, v) -> {
                    String res = v.v2.v1.equals(v.v2.v2) ? "OK" : v.v2.v1.toString();
                    return KeyValue.pair(k, res);
                });

        result.to(outputTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // this line initiates processing
        streams.start();

        // shutdown hook for Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

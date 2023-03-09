package Stream.CEPFunctions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple7;

public class KeySelectore implements KeySelector<Tuple7<String, String, Double, Double, Long, Long, String>, String> {
    @Override
    public String getKey(Tuple7<String, String, Double, Double, Long, Long, String> o) throws Exception {
        return o.f0;
    }

}

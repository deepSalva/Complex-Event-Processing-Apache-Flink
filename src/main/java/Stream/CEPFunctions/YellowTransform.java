package Stream.CEPFunctions;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class YellowTransform implements
        PatternSelectFunction<Tuple7<String, String, Double, Double, Long, Long, String>, Tuple7<String, String, Double, Double, Long, Long, String>> {

    Random RANDOM = new Random();
    @Override
    public Tuple7<String, String, Double, Double, Long, Long, String> select(Map<String, List<Tuple7<String, String, Double, Double, Long, Long, String>>> pattern)
            throws Exception {

        Tuple7<String, String, Double, Double, Long, Long, String> flag = pattern.get("flag").get(0);
        return flag;
    }
}

package Stream.CEPFunctions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;

public class JoinSelectore implements JoinFunction<Tuple7<String, String, Double, Double, Long, Long, String>,
        Tuple7<String, String, Double, Double, Long, Long, String>,
        Tuple8<String, String, Double, Double, Long, Long, String, Integer>> {
    @Override
    public Tuple8<String, String, Double, Double, Long, Long, String, Integer>
    join(Tuple7<String, String, Double, Double, Long, Long, String> first,
         Tuple7<String, String, Double, Double, Long, Long, String> second) {
        //System.out.println(first.toString()+"COMA"+second.toString());
        if (first.f1.equals(second.f1)){
            return new Tuple8<>(second.f0, second.f1, second.f2, second.f3, second.f4, second.f5, second.f6, 3);
        }
        return new Tuple8<>(first.f0, first.f1, first.f2, first.f3, first.f4, first.f5, first.f6, 2);
    }

}

package Stream.CEPpattern;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

public class BodyTemperaturePattern {
    public static class GreenPattern extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>> {
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"BT\"")){
                return false;
            }
            return value.f2 <= 37;
        }
    }
    public static class YellowPattern extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>>{
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"BT\"")){
                return false;
            }
            return value.f2 > 37 && value.f2 <= 38;
        }
    }
    public static class RedPattern extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>>{
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"BT\"")){
                return false;
            }
            return value.f2 >= 38;
        }
    }
}

package Stream.CEPpattern;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

public class HeartRatePattern {
    public static class GreenPattern extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>> {
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"HR\"")){
                return false;
            }
            return value.f2 >= 60 && value.f2 <= 90;
        }
    }
    public static class YellowPattern extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>>{
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"HR\"")){
                return false;
            }
            return value.f2 > 90 && value.f2 <= 120 || value.f2 > 50 && value.f2 < 60;
        }
    }
    public static class RedPattern extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>>{
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"HR\"")){
                return false;
            }
            return value.f2 > 120 || value.f2 <= 50;
        }
    }
}

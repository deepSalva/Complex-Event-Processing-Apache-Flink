package Stream.CEPpattern;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

public class BloodPressurePattern {
    public static class GreenPatternSys extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>> {
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"BP\"")){
                return false;
            }
            return value.f2 >= 110 && value.f2 <= 140;
        }
    }
    public static class GreenPatternDia extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>> {
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"BP\"")){
                return false;
            }
            return value.f3 >= 60 && value.f3 <= 90;
        }
    }
    public static class YellowPatternSys extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>>{
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"BP\"")){
                return false;
            }
            return value.f2 > 140 && value.f2 < 180 || value.f2 < 110 && value.f2 > 90;
        }
    }
    public static class YellowPatternDia extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>>{
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"BP\"")){
                return false;
            }
            return value.f3 > 90 && value.f3 < 110 || value.f3 > 50 && value.f3 < 60;
        }
    }
    public static class RedPatternSys extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>>{
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"BP\"")){
                return false;
            }
            return value.f2 >= 180 || value.f2 <= 90;
        }
    }
    public static class RedPatternDia extends IterativeCondition<Tuple7<String, String, Double, Double, Long, Long, String>>{
        @Override
        public boolean filter(Tuple7<String, String, Double, Double, Long, Long, String> value,
                              Context<Tuple7<String, String, Double, Double, Long, Long, String>> ctx) throws Exception {
            if (!value.f6.equals("\"BP\"")){
                return false;
            }
            return value.f3 >= 110 || value.f3 <= 50;
        }
    }
}

package Stream.CEPFunctions;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.cep.PatternSelectFunction;
import java.util.List;
import java.util.Map;

public class GenerateMessageFlag implements PatternSelectFunction<Tuple7<String, String, Double, Double, Long, Long, String>, String> {
    int FLAG_VAL;
    public GenerateMessageFlag(int flag){
        this.FLAG_VAL = flag;
    }
    @Override
    public String select(Map<String, List<Tuple7<String, String, Double, Double, Long, Long, String>>> pattern) throws Exception {

        Tuple7<String, String, Double, Double, Long, Long, String> flag = pattern.get("flag").get(0);

        String s1 = "MACSSDownload";
        String s2 = flag.f6.substring(1,3);
        String s3 = "ID";
        String concat = s1 + s2 + s3;
        String msg = "{\"MACSSID\": "+flag.f0+", \""+ concat +"\": "+flag.f1+", \"Value\": "+ flag.f2+", \"Flag\": "+ FLAG_VAL + ",\"Date\": "+flag.f5+",\"Type\": "+flag.f6+"}";
        return msg;
    }
}

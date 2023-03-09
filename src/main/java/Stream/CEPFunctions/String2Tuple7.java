package Stream.CEPFunctions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class String2Tuple7 implements MapFunction<String, Tuple7<String, String, Double, Double, Long, Long, String>> {
    private static final long serialVersionUID = 1L;
    private ObjectMapper mapper = new ObjectMapper();
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    Tuple7<String, String, Double, Double, Long, Long, String> streamtuple = new Tuple7<>();
    @Override
    public Tuple7<String, String, Double, Double, Long, Long, String> map(String str) throws IOException, ParseException {
        JsonNode jsonStream;
        try {
            jsonStream = mapper.readTree(str);
            String DateString = jsonStream.get("Date").asText();
            Date Date = df.parse(DateString);
            streamtuple.f0 = jsonStream.get("MACSSID").toString();
            streamtuple.f4 = Date.getTime();
            streamtuple.f5 = jsonStream.get("IngestionTime").asLong();
            streamtuple.f6 = jsonStream.get("Type").toString();
            if (jsonStream.get("Type").asText().equals("BP")) {
                streamtuple.f1 = jsonStream.get("MACSSDownloadBloodPressureID").toString();
                streamtuple.f2 = jsonStream.get("AmountSystolic").asDouble();
                streamtuple.f3 = jsonStream.get("AmountDiastolic").asDouble();
            }else {
                streamtuple.f2 = jsonStream.get("Amount").asDouble();
                if (jsonStream.get("Type").asText().equals("BW")) {
                    streamtuple.f3 = 1.0;
                    streamtuple.f1 = jsonStream.get("MACSSDownloadBodyWeightID").toString();
                }
                if (jsonStream.get("Type").asText().equals("HR")) {
                    streamtuple.f3 = 1.0;
                    streamtuple.f1 = jsonStream.get("MACSSDownloadHeartRateID").toString();
                }
                if (jsonStream.get("Type").asText().equals("BT")) {
                    streamtuple.f3 = 1.0;
                    streamtuple.f1 = jsonStream.get("MACSSDownloadBodyTempID").toString();
                }
                if (jsonStream.get("Type").asText().equals("MD")) {
                    streamtuple.f3 = jsonStream.get("MedicamentType").asDouble();
                    streamtuple.f1 = jsonStream.get("MACSSDownloadMedicationID").toString();
                }
            }
        } catch (Exception e){
            System.out.println(e);
        }
        return streamtuple;
    }
}
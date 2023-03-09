package Stream;

import Stream.CEPFunctions.*;
import Stream.CEPpattern.*;
import Stream.Influx.InfluxDBSink;
import Stream.Influx.InfluxDBSinkCEP;
import Stream.data.KeyedDataPoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Prototype of a system to monitor adherence from patients inside the project MACSS and with the application
 * "MyTherapy".
 * This prototype consumes data from Kafka and apply a stream data processing using Flink and FlinkCEP.
 *
 *
 * Note that the Kafka source is expecting the following parameters to be set
 *
 *  - "bootstrap.servers" (comma separated list of kafka brokers)
 *  - "zookeeper.connect" (comma separated list of zookeeper servers)
 *  - "group.id" the id of the consumer group
 *  - "topic" the name of the topic to read data from.
 *
 * --topic
 * Hello-Medizine
 * --outTopic
 * Bye-Medizine
 * --bootstrap.servers
 * localhost:9092
 * --zookeeper.connect
 * localhost:2181
 * --group.id
 * myGroup
 * --output
 * /home/savi01/dfki/bigMedalitics/teleMedizine/data/outputTest.txt
 *
 * Kafka output:
 *     Bye-Medizine
 *
 * Test Input:
 *     {"MACSSID": "39","MACSSDownloadBodyWeightID": "25","MACSSObservationID": "30","Amount": 58.0,
 *     "Date": "2020-01-01T17:41:54","Source": "135","IngestionTime": 1542465270522,"Type": "BW"}
 *
 *     {"MACSSID": "39","MACSSDownloadBloodPressureID": "25","MACSSObservationID": "30","AmountSystolic": 58.0,
 *     "AmountDiastolic": 78.0,"Date": "2020-01-01T17:41:54","Source": "135","IngestionTime": 1542465270522,"Type": "BP"}
 *
 * TestOutput:
 *    {"MACSSID":"12", "MACSSDownloadAdherenceID":"2", "Date":"2019-11-27T14:00:00", "Type":"AD", "Flag":1.0}
 *
 */

public class MainStream {
    private static final int GREEN_FLAG = 1;
    private static final int YELLOW_FLAG = 2;
    private static final int RED_FLAG = 3;
    private static final int TIME_RED_FLAG = 48;
    private static final String DATABASE_INPUT = "testCEPNew";
    private static final String DATABASE_CEP = "testCEP";
    private static long millis = System.currentTimeMillis() % 1000;
    public static void main(final String[] ARGS) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);
        ParameterTool parameterTool = ParameterTool.fromArgs(ARGS);
        DataStreamSource<String> myConsumer = env.addSource(new FlinkKafkaConsumer011<>(
                parameterTool.getRequired("topic"),
                new SimpleStringSchema(),
                parameterTool.getProperties()));

        DataStream<Tuple7<String, String, Double, Double, Long, Long, String>> tupleStream7 = myConsumer
                .map(new String2Tuple7())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple7<String, String, Double, Double, Long, Long, String>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple7<String, String, Double, Double, Long, Long, String> element) {
                        return element.f5;
            }}).keyBy(0);

        //KAFKA PRODUCER
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(
                "localhost:9092",            // broker list
                parameterTool.get("outTopic"),                  // target topic
                new SimpleStringSchema());   // serialization schema
        // this method is not available for earlier Kafka versions
        myProducer.setWriteTimestampToKafka(true);

        //////////INFLUXdb SINKING//////////////////
        tupleStream7.map(new Tuple2KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSink<>(DATABASE_INPUT));

        //////////INDIVIDUAL PATTERN LAYER//////////
        ///PATTERN MEDICAMENT///
        //GREEN PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> MDPatternGreen =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new MedicationPatterns.GreenPattern());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> MDMatchGreen = CEP
                .pattern(tupleStream7, MDPatternGreen);
        //YELLOW PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> MDPatternYellow =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new MedicationPatterns.YellowPattern());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> MDMatchYellow = CEP
                .pattern(tupleStream7, MDPatternYellow);
        DataStream<Tuple7<String, String, Double, Double, Long, Long, String>> MDYellow = MDMatchYellow
                .select(new YellowTransform());
        //RED PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> MDPatternRed =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("first")
                        .where(new MedicationPatterns.YellowPattern()).next("secondMatch")
                        .where(new MedicationPatterns.YellowPattern()).next("flag")
                        .where(new MedicationPatterns.YellowPattern()).within(Time.hours(TIME_RED_FLAG));
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> MDMatchRed = CEP
                .pattern(MDYellow, MDPatternRed);


        ///PATTERN HEART RATE///
        //GREEN PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> HRPatternGreen =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new HeartRatePattern.GreenPattern());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> HRMatchGreen = CEP
                .pattern(tupleStream7, HRPatternGreen);
        //YELLOW PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> HRPatternYellow =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new HeartRatePattern.YellowPattern());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> HRMatchYellow = CEP
                .pattern(tupleStream7, HRPatternYellow);
        //RED PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> HRPatternRed =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new HeartRatePattern.RedPattern());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> HRMatchRed = CEP
                .pattern(tupleStream7, HRPatternRed);


        ///PATTERN BLOOD PRESSURE///
        //GREEN PATTERN SYSTOLIC
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BPPatternGreenSys =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BloodPressurePattern.GreenPatternSys());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BPMatchGreenSys = CEP
                .pattern(tupleStream7, BPPatternGreenSys);
        //GREEN PATTERN DIASTOLIC
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BPPatternGreenDia =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BloodPressurePattern.GreenPatternDia());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BPMatchGreenDia = CEP
                .pattern(tupleStream7, BPPatternGreenDia);
        //YELLOW PATTERN SYSTOLIC
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BPPatternYellowSys =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BloodPressurePattern.YellowPatternSys());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BPMatchYellowSys = CEP
                .pattern(tupleStream7, BPPatternYellowSys);
        //YELLOW PATTERN DIASTOLIC
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BPPatternYellowDia =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BloodPressurePattern.YellowPatternDia());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BPMatchYellowDia = CEP
                .pattern(tupleStream7, BPPatternYellowDia);
        //RED PATTERN SYSTOLIC
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BPPatternRedSys =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BloodPressurePattern.RedPatternSys());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BPMatchRedSys = CEP
                .pattern(tupleStream7, BPPatternRedSys);
        //RED PATTERN DIASTOLIC
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BPPatternRedDia =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BloodPressurePattern.RedPatternDia());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BPMatchRedDia = CEP
                .pattern(tupleStream7, BPPatternRedDia);

        ///PATTERN BODY TEMPERATURE///
        //GREEN PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BTPatternGreen =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BodyTemperaturePattern.GreenPattern());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BTMatchGreen = CEP
                .pattern(tupleStream7, BTPatternGreen);
        //YELLOW PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BTPatternYellow =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BodyTemperaturePattern.YellowPattern());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BTMatchYellow = CEP
                .pattern(tupleStream7, BTPatternYellow);
        //RED PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BTPatternRed =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BodyTemperaturePattern.RedPattern());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BTMatchRed = CEP
                .pattern(tupleStream7, BTPatternRed);

        ///PATTERN BODY TEMPERATURE///
        //GREEN PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BWPatternGreen =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BodyWeightPattern.GreenPattern());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BWMatchGreen = CEP
                .pattern(tupleStream7, BWPatternGreen);
        //YELLOW PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BWPatternYellow =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BodyWeightPattern.YellowPattern());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BWMatchYellow = CEP
                .pattern(tupleStream7, BWPatternYellow);
        //RED PATTERN
        Pattern<Tuple7<String, String, Double, Double, Long, Long, String>, ?> BWPatternRed =
                Pattern.<Tuple7<String, String, Double, Double, Long, Long, String>>begin("flag")
                        .where(new BodyWeightPattern.RedPattern());
        PatternStream<Tuple7<String, String, Double, Double, Long, Long, String>> BWMatchRed = CEP
                .pattern(tupleStream7, BWPatternRed);

        GenerateMessageFlag generateGreen = new GenerateMessageFlag(GREEN_FLAG);
        GenerateMessageFlag generateYellow = new GenerateMessageFlag(YELLOW_FLAG);
        GenerateMessageFlag generateRed = new GenerateMessageFlag(RED_FLAG);
        GenerateMessageFlagSys generateSysGreen = new GenerateMessageFlagSys(GREEN_FLAG);
        GenerateMessageFlagSys generateSysYellow = new GenerateMessageFlagSys(YELLOW_FLAG);
        GenerateMessageFlagSys generateSysRed = new GenerateMessageFlagSys(RED_FLAG);
        GenerateMessageFlagDia generateDiaGreen = new GenerateMessageFlagDia(GREEN_FLAG);
        GenerateMessageFlagDia generateDiaYellow = new GenerateMessageFlagDia(YELLOW_FLAG);
        GenerateMessageFlagDia generateDiaRed = new GenerateMessageFlagDia(RED_FLAG);
        //TRANSFORM TO KFK MSG. MEDICATION
        DataStream<String> MDGreenKafkaMsg = MDMatchGreen
                .select(generateGreen);
        DataStream<String> MDYellowKafkaMsg = MDMatchYellow
                .select(generateYellow);
        DataStream<String> MDRedKafkaMsg = MDMatchRed
                .select(generateRed);
        //TRANSFORM TO KFK MSG. HEART RATE
        DataStream<String> HRGreenKafkaMsg = HRMatchGreen
                .select(generateGreen);
        DataStream<String> HRYellowKafkaMsg = HRMatchYellow
                .select(generateYellow);
        DataStream<String> HRRedKafkaMsg = HRMatchRed
                .select(generateRed);
        //TRANSFORM TO KFK MSG. BLOOD PRESSURE SYS
        DataStream<String> BPSysGreenKafkaMsg = BPMatchGreenSys
                .select(generateSysGreen);
        DataStream<String> BPSysYellowKafkaMsg = BPMatchYellowSys
                .select(generateSysYellow);
        DataStream<String> BPSysRedKafkaMsg = BPMatchRedSys
                .select(generateSysRed);
        //TRANSFORM TO KFK MSG. BLOOD PRESSURE DIA
        DataStream<String> BPDiaGreenKafkaMsg = BPMatchGreenDia
                .select(generateDiaGreen);
        DataStream<String> BPDiaYellowKafkaMsg = BPMatchYellowDia
                .select(generateDiaYellow);
        DataStream<String> BPDiaRedKafkaMsg = BPMatchRedDia
                .select(generateDiaRed);
        //TRANSFORM TO KFK MSG. BODY TEMPERATURE
        DataStream<String> BTGreenKafkaMsg = BTMatchGreen
                .select(generateGreen);
        DataStream<String> BTYellowKafkaMsg = BTMatchYellow
                .select(generateYellow);
        DataStream<String> BTRedKafkaMsg = BTMatchRed
                .select(generateRed);
        //TRANSFORM TO KFK MSG. BODY WEIGHT
        DataStream<String> BWGreenKafkaMsg = BWMatchGreen
                .select(generateGreen);
        DataStream<String> BWYellowKafkaMsg = BWMatchYellow
                .select(generateYellow);
        DataStream<String> BWRedKafkaMsg = BWMatchRed
                .select(generateRed);
/*
        ///OUPUT SINK TO INFLUXDB///
        //MEDICAMENT TO INFLUX
        MDMatchGreen.select(new pattern2Tuple9(1.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        MDMatchYellow.select(new pattern2Tuple9(2.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        MDMatchRed.select(new pattern2Tuple9(3.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        //HEART RATE TO INFLUX
        HRMatchGreen.select(new pattern2Tuple9(1.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        HRMatchYellow.select(new pattern2Tuple9(2.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        HRMatchRed.select(new pattern2Tuple9(3.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        //BLOOD PRESSURE SYS TO INFLUX
        BPMatchGreenSys.select(new pattern2Tuple9(1.0, "SYS"))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        BPMatchYellowSys.select(new pattern2Tuple9(2.0, "SYS"))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        BPMatchRedSys.select(new pattern2Tuple9(3.0, "SYS"))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        //BLOOD PRESSURE DIA TO INFLUX
        BPMatchGreenDia.select(new pattern2Tuple9(1.0, "DIA"))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        BPMatchYellowDia.select(new pattern2Tuple9(2.0, "DIA"))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        BPMatchRedDia.select(new pattern2Tuple9(3.0, "DIA"))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        //BODY TEMPERATURE TO INFLUX
        BTMatchGreen.select(new pattern2Tuple9(1.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        BTMatchYellow.select(new pattern2Tuple9(2.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        BTMatchRed.select(new pattern2Tuple9(3.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        //BODY WEIGHT TO INFLUX
        BWMatchGreen.select(new pattern2Tuple9(1.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        BWMatchYellow.select(new pattern2Tuple9(2.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
        BWMatchRed.select(new pattern2Tuple9(3.0))
                .map(new Tuple92KeyedDataPoint())
                .flatMap(new AsKeyedDataPoint())
                .addSink(new InfluxDBSinkCEP<>(DATABASE_CEP));
*/

        ///SINK TO KAFKA///
        //MEDICATION SINK
        MDGreenKafkaMsg.addSink(myProducer);
        MDYellowKafkaMsg.addSink(myProducer);
        MDRedKafkaMsg.addSink(myProducer);
        //HEART RATE SINK
        HRGreenKafkaMsg.addSink(myProducer);
        HRYellowKafkaMsg.addSink(myProducer);
        HRRedKafkaMsg.addSink(myProducer);
        //BLOOD PRESSURE SYS SINK
        BPSysGreenKafkaMsg.addSink(myProducer);
        BPSysYellowKafkaMsg.addSink(myProducer);
        BPSysRedKafkaMsg.addSink(myProducer);
        //BLOOD PRESSURE DIA SINK
        BPDiaGreenKafkaMsg.addSink(myProducer);
        BPDiaYellowKafkaMsg.addSink(myProducer);
        BPDiaRedKafkaMsg.addSink(myProducer);
        //BODY TEMPERATURE SINK
        BTGreenKafkaMsg.addSink(myProducer);
        BTYellowKafkaMsg.addSink(myProducer);
        BTRedKafkaMsg.addSink(myProducer);
        //BODY WEIGHT SINK
        BWGreenKafkaMsg.addSink(myProducer);
        BWYellowKafkaMsg.addSink(myProducer);
        BWRedKafkaMsg.addSink(myProducer);

        ///PRINTING RESULTS///
        //MEDICATION PRINT
        MDGreenKafkaMsg.print("MD Green");
        MDYellowKafkaMsg.print("MD Warning");
        MDRedKafkaMsg.print("MD Alarm");

        //HEARTRATE PRINT
        HRGreenKafkaMsg.print("HR Green");
        HRYellowKafkaMsg.print("HR Warning");
        HRRedKafkaMsg.print("HR Alarm");
        //BPSYS PRINT
        BPSysGreenKafkaMsg.print("BPSYS Green");
        BPSysYellowKafkaMsg.print("BPSYS Warning");
        BPSysRedKafkaMsg.print("BPSYS Alarm");
        //BPDIA PRINT
        BPDiaGreenKafkaMsg.print("BPDIA Green");
        BPDiaYellowKafkaMsg.print("BPDIA Warning");
        BPDiaRedKafkaMsg.print("BPDIA Alarm");
        //BODY TEMPERATURE PRINT
        BTGreenKafkaMsg.print("BT Green");
        BTYellowKafkaMsg.print("BT Warning");
        BTRedKafkaMsg.print("BT Alarm");
        //BODY WEIGHT PRINT
        BWGreenKafkaMsg.print("BW Green");
        BWYellowKafkaMsg.print("BW Warning");
        BWRedKafkaMsg.print("BW Alarm");

        env.execute("CEP monitoring job");

        /*
        //YELLOW RED WINDOW JOIN
        DataStream<Tuple8<String, String, Double, Double, Long, Long, String, Integer>> MDYRJoin =
                MDYellow.join(MDRed).where(new KeySelectore())
                        .equalTo(new KeySelectore())
                        .window(EventTimeSessionWindows.withGap(Time.milliseconds(1)))
                        .apply(new JoinSelectore());
        */
    }
    private static class Tuple2KeyedDataPoint implements
            MapFunction<Tuple7<String, String, Double, Double, Long, Long, String>, KeyedDataPoint<Double>> {
        @Override
        public KeyedDataPoint<Double> map(Tuple7<String, String, Double, Double, Long, Long, String> point)
                throws Exception {
            return new KeyedDataPoint<>(point.f0, point.f5, point.f2, point.f3, point.f6);
        }
    }
    private static class Tuple92KeyedDataPoint implements
            MapFunction<Tuple9<String, String, Double, Double, Long, Long, String, Double, String>, KeyedDataPoint<Double>> {
        @Override
        public KeyedDataPoint<Double> map(Tuple9<String, String, Double, Double, Long, Long, String, Double, String> point)
                throws Exception {
            Double VALUE = point.f2;
            String TYPE = point.f6;
            if (point.f8.equals("SYS")){
                VALUE = point.f2;
                TYPE = "BPSYS";
            }
            if (point.f8.equals("DIA")){
                VALUE = point.f3;
                TYPE = "BPDIA";
            }
            return new KeyedDataPoint<>(point.f0, point.f5, point.f7, VALUE, TYPE);
        }
    }
    private static class AsKeyedDataPoint implements FlatMapFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>> {
        private int index_val;
        @Override
        public void flatMap(KeyedDataPoint<Double> point, Collector<KeyedDataPoint<Double>> collector)
                throws Exception {
            collector.collect(new KeyedDataPoint<>(point.getKey(), point.getTimeStampMs(), point.getValue(), point.getValue2(), point.getType()));
        }
    }
    private static class pattern2Tuple9 implements
            PatternSelectFunction<Tuple7<String, String, Double, Double, Long, Long, String>, Tuple9<String, String, Double, Double, Long, Long, String, Double, String>> {
        private Double flag;
        private String SYS_DIAS = "None";
        public pattern2Tuple9(Double flag){
            this.flag = flag;
        }
        public pattern2Tuple9(Double flag, String SYS_DIAS){
            this.flag = flag;
            this.SYS_DIAS = SYS_DIAS;
        }
        @Override
        public Tuple9<String, String, Double, Double, Long, Long, String, Double, String> select(Map<String, List<Tuple7<String, String, Double, Double, Long, Long, String>>> pattern) throws Exception {
            Tuple7<String, String, Double, Double, Long, Long, String> first = pattern.get("flag").get(0);
            Tuple9<String, String, Double, Double, Long, Long, String, Double, String> out = new Tuple9<>(first.f0, first.f1, first.f2, first.f3, first.f4, first.f5, first.f6, flag, SYS_DIAS);
            return out;
        }
    }

}

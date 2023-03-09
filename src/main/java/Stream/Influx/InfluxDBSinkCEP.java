/**
 * (c) dataartisans
 */

package Stream.Influx;

import Stream.data.DataPoint;
import Stream.data.KeyedDataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class InfluxDBSinkCEP<T extends DataPoint<? extends Number>> extends RichSinkFunction<T> {

    private transient InfluxDB influxDB = null;
    private static String dataBaseName;
    private static String fieldName = "flag";
    private static String fieldName2 = "value";
    private String type;

    /**
     * Provide Database name and measurement (series) name
     * @param dbName
     */
    public InfluxDBSinkCEP(String dbName){
        this.dataBaseName = dbName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        influxDB = InfluxDBFactory.connect("http://localhost:8086", "admin", "admin");
        influxDB.setDatabase(dataBaseName);
        influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(T dataPoint) throws Exception {
        String type;
        if (dataPoint.getType().equals("BPSYS") || dataPoint.getType().equals("BPDIA")) {
            type = dataPoint.getType();
        }else {
            type=dataPoint.getType().substring(1,3);
        }
        Point.Builder builder = Point.measurement(type)
                .time(dataPoint.getTimeStampMs(), TimeUnit.MILLISECONDS)
                .addField(fieldName, dataPoint.getValue()).addField(fieldName2, dataPoint.getValue2());

        if(dataPoint instanceof KeyedDataPoint){
            builder.tag("key", ((KeyedDataPoint) dataPoint).getKey());
        }

        Point p = builder.build();

        influxDB.write(dataBaseName, "autogen", p);
    }
}

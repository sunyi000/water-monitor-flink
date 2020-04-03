package yi.sun.water.monitor.utils;

import yi.sun.water.monitor.datatypes.WaterEvent;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class WaterBase {

    public static SourceFunction<WaterEvent> waterEvent = null;
    public static SourceFunction<String> strings = null;
    public static SinkFunction out = null;
    public static int parallelism = 4;

    public final static String pathToWaterData = "C:\\Users\\ysun\\water-data\\data-all-1.csv.gz";

    public static SourceFunction<WaterEvent> waterSourceOrTest(SourceFunction<WaterEvent> source) {
        if (waterEvent == null) {
            return source;
        }
        return waterEvent;
    }

    public static SourceFunction<String> stringSourceOrTest(SourceFunction<String> source) {
        if (strings == null) {
            return source;
        }
        return strings;
    }

    public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
        if (out == null) {
            ds.print();
        } else {
            ds.addSink(out);
        }
    }

    public static void printOrTest(org.apache.flink.streaming.api.scala.DataStream<?> ds) {
        if (out == null) {
            ds.print();
        } else {
            ds.addSink(out);
        }
    }
}
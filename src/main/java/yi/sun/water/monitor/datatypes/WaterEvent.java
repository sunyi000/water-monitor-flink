package yi.sun.water.monitor.datatypes;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;


/**
 * A WaterEvent is a Water event.
 *consists of
 * id,timestamp,disOxy,temp,ph,conductivity,turbidity,orp,barrierid,siteid
 *
 */
public class WaterEvent implements Serializable {

    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    public WaterEvent() {
        this.timestamp = new DateTime();
    }

    public WaterEvent(String eventId, DateTime timestamp, float temperature, float disOxy, float conductivity, float ph,
                      float turbidity, float orp, long minutes, String barrierId, String siteId) {

        this.eventId = eventId;
        this.timestamp = timestamp;
        this.temperature = temperature;
        this.disOxy = disOxy;
        this.conductivity = conductivity;
        this.ph = ph;
        this.turbidity = turbidity;
        this.orp = orp;
        this.minutes = minutes;
        this.barrierId = barrierId;
        this.siteId = siteId;
    }

    public String eventId;
    public DateTime timestamp;
    public float temperature;
    public float disOxy;
    public float conductivity;
    public float ph;
    public float turbidity;
    public float orp;
    public long minutes;
    public String barrierId;
    public String siteId;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(eventId).append(",");
        sb.append(timestamp.toString(timeFormatter)).append(",");
        sb.append(temperature).append(",");
        sb.append(disOxy).append(",");
        sb.append(conductivity).append(",");
        sb.append(ph).append(",");
        sb.append(turbidity).append(",");
        sb.append(orp).append(",");
        sb.append(barrierId).append(",");
        sb.append(siteId);

        return sb.toString();
    }

    public static WaterEvent fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 10) {
            throw new RuntimeException("Invalid record: " + line);
        }

        WaterEvent waterEvent = new WaterEvent();

        try {
            waterEvent.eventId = tokens[0];
            waterEvent.timestamp = DateTime.parse(tokens[1], timeFormatter);
            waterEvent.temperature = tokens[2].equals("NULL") ? 0.0f : Float.parseFloat(tokens[2]);
            waterEvent.disOxy =  Float.parseFloat(tokens[3]);
            waterEvent.conductivity = Float.parseFloat(tokens[4]) ;
            waterEvent.ph =  Float.parseFloat(tokens[5]);
            waterEvent.turbidity = Float.parseFloat(tokens[6]) ;
            waterEvent.orp = Float.parseFloat(tokens[7]) ;
         //   waterEvent.orp = StringUtils.isNumeric(tokens[7]) ? Float.parseFloat(tokens[7]) : 0.0f;
            waterEvent.siteId = tokens[8];
            waterEvent.barrierId = tokens[9];


        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return waterEvent;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof WaterEvent &&
                this.eventId == ((WaterEvent) other).eventId;
    }

    @Override
    public int hashCode() {
        return this.eventId.hashCode();
    }

    public long getEventTime() {
        return timestamp.getMillis();
    }
}

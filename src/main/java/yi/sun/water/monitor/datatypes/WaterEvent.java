package yi.sun.water.monitor.datatypes;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;


/**
 * A TaxiFare is a taxi fare event.
 *
 * A TaxiFare consists of
 * - the rideId of the event
 * - the time of the event
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
        if (tokens.length != 8) {
            throw new RuntimeException("Invalid record: " + line);
        }

        WaterEvent waterEvent = new WaterEvent();

        try {
            waterEvent.eventId = tokens[0];
            waterEvent.timestamp = DateTime.parse(tokens[1], timeFormatter);
            waterEvent.temperature = StringUtils.isNumeric(tokens[2]) ? Float.parseFloat(tokens[2]) : 0.0f;
            waterEvent.disOxy = StringUtils.isNumeric(tokens[3]) ? Float.parseFloat(tokens[3]) : 0.0f;
            waterEvent.conductivity = StringUtils.isNumeric(tokens[4]) ? Float.parseFloat(tokens[4]) : 0.0f;
            waterEvent.ph = StringUtils.isNumeric(tokens[5]) ? Float.parseFloat(tokens[5]) : 0.0f;
            waterEvent.turbidity = StringUtils.isNumeric(tokens[6]) ? Float.parseFloat(tokens[6]) : 0.0f;
            waterEvent.orp = StringUtils.isNumeric(tokens[7]) ? Float.parseFloat(tokens[7]) : 0.0f;
            waterEvent.barrierId = tokens[8];
            waterEvent.siteId = tokens[9];

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

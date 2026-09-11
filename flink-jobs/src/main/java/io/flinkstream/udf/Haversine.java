package io.flinkstream.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * <b>Scalar UDF.</b> Great-circle distance in kilometres.
 *
 * <p>Used by the impossible-travel query: two card-present transactions far apart in space but close in time.
 * Doing this in a UDF rather than inline SQL keeps the query readable and the trigonometry testable.
 */
public class Haversine extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    private static final double EARTH_RADIUS_KM = 6371.0088d;

    public Double eval(Double lat1, Double lon1, Double lat2, Double lon2) {
        if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) {
            return null;
        }
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return EARTH_RADIUS_KM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}

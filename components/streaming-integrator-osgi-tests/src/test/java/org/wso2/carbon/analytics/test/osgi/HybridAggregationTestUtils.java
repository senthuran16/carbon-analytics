package org.wso2.carbon.analytics.test.osgi;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HybridAggregationTestUtils {

    public static String getCreationQuery(List<Object[]> list) {
        StringBuilder sb = new StringBuilder("Arrays.asList(\n");
        for (int i = 0; i < list.size(); i++) {
            Object[] objArr = list.get(i);
            sb.append("new Object[]{");
            for (int j = 0; j < objArr.length; j++) {
                if (j == 0 || j == 1) {
                    sb.append(objArr[j] + "L, ");
                } else if (j == objArr.length - 1) {
                    sb.append(objArr[j]);
                } else {
                    sb.append(objArr[j] + ",");
                }
            }
            sb.append("}");
            if (i < list.size() - 1) {
                sb.append(",\n");
            }
        }
        sb.append("\n);");
        return sb.toString();
    }

    public static long convertToEpoch(String dateAsString) {
        try {
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
//            format.setTimeZone(TimeZone.getTimeZone("GMT+5:30"));
            return format.parse(dateAsString).getTime();
//            return new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss").parse(dateAsString).getTime();
        } catch (ParseException e) {
            return -1L;
        }
    }

    public static String convertToReadableDate(long timestamp) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z"); // TODO Takes system timezone?
        return formatter.format(new Date(timestamp));
    }

    public static String getReadableNextYearStart() {
//        long currentTime = System.currentTimeMillis();
//        String date = conver // TODO hamawelema future ekak athi
        return "";
    }

    public static void main(String[] args) {
        List<String> test = new ArrayList<>();
        System.out.println(convertToReadableDate(convertToEpoch("2019-06-01 4:00:00 GMT+5:30")));
    }
}

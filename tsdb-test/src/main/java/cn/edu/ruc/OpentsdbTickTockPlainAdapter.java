package cn.edu.ruc;


import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;
import cn.edu.ruc.utils.Pair;
import okhttp3.*;

public class OpentsdbTickTockPlainAdapter extends OpentsdbAdapter {
    //执行insert
    public Pair<Long, Integer> insertData(String data) {
        String[] rows = data.split(TSBM.LINE_SEPARATOR);
        StringBuilder putReqSB = new StringBuilder();

        int totalRespTimeMs = 0;
        int totalDataPoints = 0;

        int currDataPoints = 0;
        int maxDataPointsPerReq = 50*50;

        int rowIndex = 0;
        for (String row : rows) {
            rowIndex++;
            String[] sensors = row.split(TSBM.SEPARATOR);
            if (sensors.length < 3) {//过滤空行
                continue;
            }
            Long timestamp = Long.parseLong(sensors[0]);
            String farmId = sensors[1];
            String deviceId = sensors[2];
            int length = sensors.length;
            for (int index = 3; index < length; index++) {
                currDataPoints ++;

                Double value = Double.parseDouble(sensors[index]);
                String sensorName = "s" + (index - 2);

                putReqSB.append("put " + METRIC);
                putReqSB.append(" "+timestamp);
                putReqSB.append(" "+value);
                putReqSB.append(" "+FARM_TAG+"="+farmId);
                putReqSB.append(" "+DEVICE_TAG+"="+deviceId);
                putReqSB.append(" "+SENSOR_TAG+"="+sensorName);
                putReqSB.append(System.lineSeparator());
            }

            // Split all rows into several small requests.
            if (currDataPoints >= maxDataPointsPerReq || rowIndex == rows.length) {
                Request request = new Request.Builder()
                    .url(writeURL)
                    .post(RequestBody.create(MEDIA_TYPE_TEXT, putReqSB.toString()))
                    .build();
                long timeMs = exeOkHttpRequest(request);
                if (timeMs != BaseAdapter.FAILURE) {
                    totalRespTimeMs += timeMs;
                    totalDataPoints += currDataPoints;
                }

                currDataPoints = 0;
                try{
                    Thread.sleep(10);
                } catch(InterruptedException e){
                    e.printStackTrace();
                }
            }
        }

        return (totalDataPoints == 0) ?
            null :
            new Pair(totalRespTimeMs, totalDataPoints);
    }
}

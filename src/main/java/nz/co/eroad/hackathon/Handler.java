package nz.co.eroad.hackathon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.google.gson.Gson;
import io.vavr.control.Try;
import nz.co.eroad.hackathon.model.SensorTelemetry;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

public class Handler implements RequestStreamHandler {

    private static final Gson GSON = new Gson();

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        try (Reader reader = new InputStreamReader(inputStream);
             CSVPrinter csvPrinter = CSVFormat.DEFAULT.print(new OutputStreamWriter(outputStream))) {
            SensorTelemetry sensorTelemetry = GSON.fromJson(reader, SensorTelemetry.class);
            Instant start = Instant.ofEpochMilli(sensorTelemetry.getStart().longValue());

            csvPrinter.printRecord("time (ms)", "accel x (m/s²)", "accel y (m/s²)", "accel z (m/s²)");
            sensorTelemetry.getSensorData()
                    .forEach(sensorData ->{
                        Instant timestamp = Instant.ofEpochMilli(sensorData.getT_sec().longValue());
                        long time = Duration.between(start, timestamp).toMillis();
                        BigDecimal xAcceleration = sensorData.getX_acc().negate();
                        BigDecimal yAcceleration = sensorData.getY_acc();
                        BigDecimal zAcceleration = sensorData.getZ_acc();

                        Try.run(() -> csvPrinter.printRecord(time, xAcceleration, yAcceleration, zAcceleration));
                    });
        }
    }
}

package nz.co.eroad.hackathon.model;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class SensorData {

    private BigDecimal t_sec;
    private BigDecimal x_acc;
    private BigDecimal y_acc;
    private BigDecimal z_acc;
}

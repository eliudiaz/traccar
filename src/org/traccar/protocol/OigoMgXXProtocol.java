package org.traccar.protocol;

import org.jboss.netty.channel.Channel;
import org.joda.time.DateTime;
import org.traccar.BaseProtocolDecoder;
import org.traccar.Protocol;
import org.traccar.helper.Log;

import java.math.BigInteger;
import java.net.SocketAddress;
import java.util.TimeZone;

/**
 * Created by edcracken on 01/08/2017.
 */
public class OigoMgXXProtocol extends BaseProtocolDecoder {

    public OigoMgXXProtocol(Protocol protocol) {
        super(protocol);
    }

    @Override
    protected Object decode(Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        return null;
    }

    public byte[] getHandlePacket(byte pktBytes[]) {
        if (pktBytes != null && pktBytes.length > 0) {
            String hex = StringTools.toHexString(pktBytes);
            Log.info((new StringBuilder()).append("Recv[HEX]: ").append(hex).toString(), new Object[0]);
            Log.info((new StringBuilder()).append("Recv[Length]: ").append(pktBytes.length).toString(), new Object[0]);
            byte rtn[] = null;
            if (pktBytes.length > 14)
                rtn = parseInsertRecord_MGXX(hex);
            return rtn;
        } else {
            Log.info("Empty packet received ...", new Object[0]);
            return null;
        }
    }

    public byte[] getFinalPacket(boolean hasError)
            throws Exception {
        return null;
    }

    private String hex2DecimalStr(String hexadecimal) {
        BigInteger decimal = new BigInteger(hexadecimal, 16);
        return String.valueOf(decimal);
    }

    private int hex2DecimalInt(String hexadecimal) {
        BigInteger decimal = new BigInteger(hexadecimal, 16);
        return decimal.intValue();
    }

    private long hex2DecimalLong(String hexadecimal) {
        BigInteger decimal = new BigInteger(hexadecimal, 16);
        return decimal.longValue();
    }

    private String hex2BinStr(String hexadecimal) {
        BigInteger decimal = new BigInteger(hexadecimal, 16);
        int numHex = decimal.intValue();
        String binary = Integer.toBinaryString(numHex);
        return binary;
    }

    private int bin2DecimalInt(String binary) {
        int decimalValue = Integer.parseInt(binary, 2);
        return decimalValue;
    }

    private double _parseLatitudeOigo(String gps_latitude) {
        double latitude = 0.0D;
        long long_latitude = hex2DecimalLong(gps_latitude);
        long FFFFFFFF_value = 0xffffffffL;
        if (long_latitude > 0x7fffffffL) {
            double pre_calculo = ((double) FFFFFFFF_value - (double) long_latitude) + 1.0D;
            String s_pre_calculo = String.valueOf(pre_calculo);
            String degree = s_pre_calculo.substring(0, 2);
            String minutes = s_pre_calculo.substring(2, 4);
            String minutes_fraction = s_pre_calculo.substring(4, 7);
            int _degree = StringTools.parseInt(degree, 0);
            double _mm_mf = StringTools.parseDouble((new StringBuilder()).append(minutes).append(".").append(minutes_fraction).toString(), 0.0D);
            latitude = ((double) _degree + _mm_mf / 60D) * -1D;
        } else {
            if (long_latitude == 0L)
                return 90D;
            String decimal_latitude = hex2DecimalStr(gps_latitude);
            String degree = decimal_latitude.substring(0, 2);
            String minutes = decimal_latitude.substring(2, 4);
            String minutes_fraction = decimal_latitude.substring(4, 7);
            int _degree = StringTools.parseInt(degree, 0);
            double _mm_mf = StringTools.parseDouble((new StringBuilder()).append(minutes).append(".").append(minutes_fraction).toString(), 0.0D);
            latitude = (double) _degree + _mm_mf / 60D;
        }
        return latitude;
    }

    private double _parseLongitudeOigo(String gps_longitude) {
        double longitude = 0.0D;
        long long_longitude = hex2DecimalLong(gps_longitude);
        long FFFFFFFF_value = 0xffffffffL;
        if (long_longitude > 0x7fffffffL) {
            double pre_calculo = ((double) FFFFFFFF_value - (double) long_longitude) + 1.0D;
            String s_pre_calculo = String.valueOf(pre_calculo);
            String degree = s_pre_calculo.substring(0, 2);
            String minutes = s_pre_calculo.substring(2, 4);
            String minutes_fraction = s_pre_calculo.substring(4, 7);
            int _degree = StringTools.parseInt(degree, 0);
            double _mm_mf = StringTools.parseDouble((new StringBuilder()).append(minutes).append(".").append(minutes_fraction).toString(), 0.0D);
            longitude = ((double) _degree + _mm_mf / 60D) * -1D;
        } else {
            if (long_longitude == 0L)
                return 180D;
            String decimal_longitude = hex2DecimalStr(gps_longitude);
            String degree = decimal_longitude.substring(0, 2);
            String minutes = decimal_longitude.substring(2, 4);
            String minutes_fraction = decimal_longitude.substring(4, 7);
            int _degree = StringTools.parseInt(degree, 0);
            double _mm_mf = StringTools.parseDouble((new StringBuilder()).append(minutes).append(".").append(minutes_fraction).toString(), 0.0D);
            longitude = (double) _degree + _mm_mf / 60D;
        }
        return longitude;
    }

    private double _parseAltitudeOigo(String gps_altitude) {
        double altitude = 0.0D;
        int int_altitude = hex2DecimalInt(gps_altitude);
        int FFFF_value = 65535;
        if (int_altitude > 32767) {
            int pre_calculo = (FFFF_value - int_altitude) + 1;
            String s_pre_calculo = String.valueOf(pre_calculo);
            altitude = StringTools.parseInt(s_pre_calculo, 0);
        } else {
            String s_pre_calculo = hex2DecimalStr(gps_altitude);
            altitude = StringTools.parseInt(s_pre_calculo, 0);
        }
        return altitude;
    }

    private byte[] parseInsertRecord_MGXX(String hex) {
        if (hex == null) {
            Log.error("String is null", new Object[0]);
            return null;
        }
        Log.info("Parsing(Oigo MG-XX Series)", new Object[0]);
        int data_ext = 0;
        String startOfFrame = "";
        String gpsRssi = "00";
        String sequenceCounter = "00";
        String versionId = "00";
        String gps_satellites_status = "00";
        String virtual_odometer = "00000000";
        boolean need_ack = false;
        if (hex.length() >= 74) {
            String report_tag = hex.substring(0 + data_ext, 2 + data_ext);
            String flag = hex.substring(2 + data_ext, 3 + data_ext);
            String identification = hex.substring(2 + data_ext, 18 + data_ext);
            String imsi = hex.substring(18 + data_ext, 34 + data_ext);
            String utcTime = hex.substring(34 + data_ext, 42 + data_ext);
            String gpsLatitude = hex.substring(42 + data_ext, 50 + data_ext);
            String gps_longitude = hex.substring(50 + data_ext, 58 + data_ext);
            String gps_altitude = hex.substring(58 + data_ext, 62 + data_ext);
            String gps_heading = hex.substring(62 + data_ext, 66 + data_ext);
            String gps_speed = hex.substring(66 + data_ext, 68 + data_ext);
            String power_input_voltage = hex.substring(68 + data_ext, 70 + data_ext);
            String gpio_direction = hex.substring(70 + data_ext, 72 + data_ext);
            String time_seconds = hex.substring(72 + data_ext, 74 + data_ext);
            if (hex.length() >= 76)
                gpsRssi = hex.substring(74 + data_ext, 76 + data_ext);
            if (hex.length() >= 78) {
                sequenceCounter = hex.substring(76 + data_ext, 78 + data_ext);
                versionId = hex.substring(78 + data_ext, 80 + data_ext);
            }
            if (hex.length() >= 82)
                gps_satellites_status = hex.substring(80 + data_ext, 82 + data_ext);
            if (hex.length() >= 90)
                virtual_odometer = hex.substring(82 + data_ext, 90 + data_ext);
            if (gpsLatitude.equalsIgnoreCase("00000000")) {
                Log.info("Latitud Invalida, Se Interrumpe Trama", new Object[0]);
                return null;
            }
            if (gps_longitude.equalsIgnoreCase("00000000")) {
                Log.info("Longitud Invalida, Se Interrumpe Trama", new Object[0]);
                return null;
            }
            String flag_bin = hex2BinStr(flag);
            String flag_inputs = "0000";
            if (flag_bin.length() == 1)
                flag_inputs = (new StringBuilder()).append("000").append(flag_bin).toString();
            else if (flag_bin.length() == 2)
                flag_inputs = (new StringBuilder()).append("00").append(flag_bin).toString();
            else if (flag_bin.length() == 3)
                flag_inputs = (new StringBuilder()).append("0").append(flag_bin).toString();
            else if (flag_bin.length() == 4)
                flag_inputs = flag_bin;
            if (flag_inputs.substring(0, 1) == "1")
                need_ack = true;
            String gpioDirectionBin = hex2BinStr(gpio_direction);
            String gpioDirectionInputs = "00000000";
            if (gpioDirectionBin.length() == 1)
                gpioDirectionInputs = (new StringBuilder()).append("0000000").append(gpioDirectionBin).toString();
            else if (gpioDirectionBin.length() == 2)
                gpioDirectionInputs = (new StringBuilder()).append("000000").append(gpioDirectionBin).toString();
            else if (gpioDirectionBin.length() == 3)
                gpioDirectionInputs = (new StringBuilder()).append("00000").append(gpioDirectionBin).toString();
            else if (gpioDirectionBin.length() == 4)
                gpioDirectionInputs = (new StringBuilder()).append("0000").append(gpioDirectionBin).toString();
            else if (gpioDirectionBin.length() == 5)
                gpioDirectionInputs = (new StringBuilder()).append("000").append(gpioDirectionBin).toString();
            else if (gpioDirectionBin.length() == 6)
                gpioDirectionInputs = (new StringBuilder()).append("00").append(gpioDirectionBin).toString();
            else if (gpioDirectionBin.length() == 7)
                gpioDirectionInputs = (new StringBuilder()).append("0").append(gpioDirectionBin).toString();
            else if (gpioDirectionBin.length() == 8)
                gpioDirectionInputs = gpioDirectionBin;
            String gpsSatellitesStatusBin = hex2BinStr(gps_satellites_status);
            String gpsSatellitesStatusInputs = "00000000";
            if (gpsSatellitesStatusBin.length() == 1)
                gpsSatellitesStatusInputs = (new StringBuilder()).append("0000000").append(gpsSatellitesStatusBin).toString();
            else if (gpsSatellitesStatusBin.length() == 2)
                gpsSatellitesStatusInputs = (new StringBuilder()).append("000000").append(gpsSatellitesStatusBin).toString();
            else if (gpsSatellitesStatusBin.length() == 3)
                gpsSatellitesStatusInputs = (new StringBuilder()).append("00000").append(gpsSatellitesStatusBin).toString();
            else if (gpsSatellitesStatusBin.length() == 4)
                gpsSatellitesStatusInputs = (new StringBuilder()).append("0000").append(gpsSatellitesStatusBin).toString();
            else if (gpsSatellitesStatusBin.length() == 5)
                gpsSatellitesStatusInputs = (new StringBuilder()).append("000").append(gpsSatellitesStatusBin).toString();
            else if (gpsSatellitesStatusBin.length() == 6)
                gpsSatellitesStatusInputs = (new StringBuilder()).append("00").append(gpsSatellitesStatusBin).toString();
            else if (gpsSatellitesStatusBin.length() == 7)
                gpsSatellitesStatusInputs = (new StringBuilder()).append("0").append(gpsSatellitesStatusBin).toString();
            else if (gpsSatellitesStatusBin.length() == 8)
                gpsSatellitesStatusInputs = gpsSatellitesStatusBin;
            gps_heading = hex2DecimalStr(gps_heading);
            gps_speed = hex2DecimalStr(gps_speed);
            gpsRssi = hex2DecimalStr(gpsRssi);
            String modemID = identification.substring(1);
            if (StringTools.isBlank(modemID)) {
                Log.error("'IMEI' value is missing", new Object[0]);
                return null;
            }
            gpsEvent = createGPSEvent(modemID);
            if (gpsEvent == null)
                return null;
            Device device = gpsEvent.getDevice();
            if (device == null)
                return null;
            String accountID = device.getAccountID();
            String deviceID = device.getDeviceID();
            String uniqueID = device.getUniqueID();
            int statusCode = 61472;
            long fixtime = _parseDateOigo(utcTime, time_seconds);
            double latitude = _parseLatitudeOigo(gpsLatitude);
            double longitude = _parseLongitudeOigo(gps_longitude);
            double speedMPH = StringTools.parseDouble(gps_speed, 0.0D);
            double heading = StringTools.parseDouble(gps_heading, 0.0D);
            double altitudeF = _parseAltitudeOigo(gps_altitude);
            double rssi = StringTools.parseDouble(gpsRssi, 0.0D);
            int satsCount = bin2DecimalInt(gpsSatellitesStatusInputs.substring(4, 8));
            double speedKPH = speedMPH * 1.6093440000000001D;
            double altitudeM = altitudeF * 0.30480000000000002D;
            double odometer = hex2DecimalInt(virtual_odometer);
            Log.info((new StringBuilder()).append("IMEI\t\t: ").append(modemID).toString(), new Object[0]);
            Log.info((new StringBuilder()).append("Timestamp\t: ").append(fixtime).append(" [").append(new DateTime(fixtime)).append("]").append("\n").toString(), new Object[0]);
            Log.info((new StringBuilder()).append("UniqueID\t: ").append(uniqueID).toString(), new Object[0]);
            Log.info((new StringBuilder()).append("DeviceID\t: ").append(accountID).append("/").append(deviceID).toString(), new Object[0]);
            Log.info((new StringBuilder()).append("Evento\t: ").append(report_tag).toString(), new Object[0]);
            Log.info((new StringBuilder()).append("Secuencia\t: ").append(hex2DecimalStr(sequenceCounter)).toString(), new Object[0]);
            if (report_tag.equalsIgnoreCase("01"))
                statusCode = 64793;
            else if (!report_tag.equalsIgnoreCase("02"))
                if (report_tag.equalsIgnoreCase("03"))
                    statusCode = 61722;
                else if (report_tag.equalsIgnoreCase("09"))
                    statusCode = 64787;
                else if (report_tag.equalsIgnoreCase("0F"))
                    statusCode = 64789;
                else if (report_tag.equalsIgnoreCase("12"))
                    statusCode = 62465;
                else if (report_tag.equalsIgnoreCase("13"))
                    statusCode = 62467;
                else if (!report_tag.equalsIgnoreCase("14"))
                    if (report_tag.equalsIgnoreCase("15"))
                        statusCode = 61727;
                    else if (report_tag.equalsIgnoreCase("19"))
                        statusCode = 61714;
                    else if (report_tag.equalsIgnoreCase("1A"))
                        statusCode = 61715;
                    else if (report_tag.equalsIgnoreCase("1C"))
                        statusCode = 61720;
            if (statusCode == 61472) {
                double maxSpeed = device.getSpeedLimitKPH();
                if (speedKPH > MINIMUM_SPEED_KPH && speedKPH < maxSpeed) {
                    if (device.getLastValidSpeedKPH() <= MINIMUM_SPEED_KPH)
                        statusCode = 61713;
                    else
                        statusCode = 61714;
                } else if (speedKPH >= maxSpeed)
                    statusCode = 61722;
                else if (speedKPH <= MINIMUM_SPEED_KPH)
                    statusCode = 61715;
            }
            gpsEvent.setTimestamp(fixtime);
            gpsEvent.setStatusCode(statusCode);
            gpsEvent.setLatitude(latitude);
            gpsEvent.setLongitude(longitude);
            gpsEvent.setSpeedKPH(speedKPH);
            gpsEvent.setHeading(heading);
            gpsEvent.setAltitude(altitudeM);
            gpsEvent.setSatelliteCount(satsCount);
            gpsEvent.setSignalStrength(rssi);
            gpsEvent.setOdometerKM(odometer);
            if (parseInsertRecord_Common(gpsEvent)) {
                if (need_ack) {
                    String ack_start = "E0";
                    String ack_counter = sequenceCounter;
                    String ack = (new StringBuilder()).append(ack_start).append(ack_counter).toString();
                    Log.info((new StringBuilder()).append("Respuesta ACK [").append(modemID).append("]\t: ").append(ack).toString(), new Object[0]);
                    return ack.getBytes();
                } else {
                    Log.info((new StringBuilder()).append("Sin ACK [").append(modemID).append("]").toString(), new Object[0]);
                    return null;
                }
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private boolean parseInsertRecord_Common(GPSEvent gpsEv) {
        long fixtime = gpsEv.getTimestamp();
        int statusCode = gpsEv.getStatusCode();
        Device dev = gpsEv.getDevice();
        if (fixtime <= 0L) {
            Print.logWarn("Invalid date/time", new Object[0]);
            fixtime = DateTime.getCurrentTimeSec();
            gpsEv.setTimestamp(fixtime);
        }
        if (!gpsEv.isValidGeoPoint()) {
            Print.logWarn((new StringBuilder()).append("Invalid lat/lon: ").append(gpsEv.getLatitude()).append("/").append(gpsEv.getLongitude()).toString(), new Object[0]);
            gpsEv.setLatitude(0.0D);
            gpsEv.setLongitude(0.0D);
        }
        GeoPoint geoPoint = gpsEv.getGeoPoint();
        Log.info((new StringBuilder()).append("GPS      \t: ").append(geoPoint).toString(), new Object[0]);
        if (gpsEv.getSpeedKPH() < MINIMUM_SPEED_KPH) {
            gpsEv.setSpeedKPH(0.0D);
            gpsEv.setHeading(0.0D);
        }
        double odomKM = 0.0D;
        if (odomKM <= 0.0D)
            odomKM = !ESTIMATE_ODOMETER || !geoPoint.isValid() ? gpsDevice.getLastOdometerKM() : gpsDevice.getNextOdometerKM(geoPoint);
        else
            odomKM = dev.adjustOdometerKM(odomKM);
        Log.info((new StringBuilder()).append("Odometer KM: ").append(odomKM).toString(), new Object[0]);
        gpsEv.setOdometerKM(odomKM);
        if (SIMEVENT_GEOZONES && geoPoint.isValid()) {
            List zone = dev.checkGeozoneTransitions(fixtime, geoPoint);
            if (zone != null) {
                org.opengts.db.tables.Device.GeozoneTransition z;
                for (Iterator i$ = zone.iterator(); i$.hasNext(); Log.info((new StringBuilder()).append("Geozone    : ").append(z).toString(), new Object[0])) {
                    z = (org.opengts.db.tables.Device.GeozoneTransition) i$.next();
                    gpsEv.insertEventData(z.getTimestamp(), z.getStatusCode(), z.getGeozone());
                }

            }
        }
        if (gpsEv.hasInputMask() && gpsEv.getInputMask() >= 0L) {
            long gpioInput = gpsEv.getInputMask();
            if (SIMEVENT_DIGITAL_INPUTS > 0L) {
                long chgMask = (dev.getLastInputState() ^ gpioInput) & SIMEVENT_DIGITAL_INPUTS;
                if (chgMask != 0L) {
                    for (int b = 0; b <= 15; b++) {
                        long m = 1L << b;
                        if ((chgMask & m) != 0L) {
                            int inpCode = (gpioInput & m) == 0L ? InputStatusCodes_OFF[b] : InputStatusCodes_ON[b];
                            long inpTime = fixtime;
                            gpsEv.insertEventData(inpTime, inpCode);
                            Log.info((new StringBuilder()).append("GPIO : ").append(StatusCodes.GetDescription(inpCode, null)).toString(), new Object[0]);
                        }
                    }

                }
            }
            dev.setLastInputState(gpioInput & 65535L);
        }
        gpsEv.insertEventData(fixtime, statusCode);
        gpsEv.updateDevice();
        return true;
    }

    private long _parseDateOigo(String utc_time, String time_second) {
        String year = utc_time.substring(0, 1);
        String month = utc_time.substring(1, 2);
        String day = utc_time.substring(2, 4);
        String hour = utc_time.substring(4, 6);
        String minute = utc_time.substring(6, 8);
        String second = time_second.substring(0, 2);
        int YY = StringTools.parseInt(hex2DecimalStr(year), 0);
        int MM = StringTools.parseInt(hex2DecimalStr(month), 0);
        int DD = StringTools.parseInt(hex2DecimalStr(day), 0);
        int hh = StringTools.parseInt(hex2DecimalStr(hour), 0);
        int mm = StringTools.parseInt(hex2DecimalStr(minute), 0);
        int ss = StringTools.parseInt(hex2DecimalStr(second), 0);
        YY += 2010;
        DateTime dt = new DateTime(gmtTimezone, YY, MM, DD, hh, mm, ss);
        return dt.getTimeSec();
    }

    public static void configInit() {
        DCServerConfig dcsc = Main.getServerConfig(null);
        if (dcsc == null) {
            Print.logWarn((new StringBuilder()).append("DCServer not found: ").append(Main.getServerName()).toString(), new Object[0]);
            return;
        } else {
            DATA_FORMAT_OPTION = dcsc.getIntProperty(Main.ARG_FORMAT, DATA_FORMAT_OPTION);
            MINIMUM_SPEED_KPH = dcsc.getMinimumSpeedKPH(MINIMUM_SPEED_KPH);
            ESTIMATE_ODOMETER = dcsc.getEstimateOdometer(ESTIMATE_ODOMETER);
            SIMEVENT_GEOZONES = dcsc.getSimulateGeozones(SIMEVENT_GEOZONES);
            SIMEVENT_DIGITAL_INPUTS = dcsc.getSimulateDigitalInputs(SIMEVENT_DIGITAL_INPUTS) & 65535L;
            PACKET_LEN_END_OF_STREAM = dcsc.getBooleanProperty("oigo_mgxx.packetLenEndOfStream", PACKET_LEN_END_OF_STREAM);
            return;
        }
    }


    public static int DATA_FORMAT_OPTION = 1;
    public static boolean ESTIMATE_ODOMETER = false;
    public static boolean SIMEVENT_GEOZONES = false;
    public static long SIMEVENT_DIGITAL_INPUTS = 0L;
    private static boolean DFT_INSERT_EVENT;
    private static boolean INSERT_EVENT;
    public static double MINIMUM_SPEED_KPH = 0.0D;
    public static final double KILOMETERS_PER_KNOT = 1.8520000000000001D;
    public static final double KNOTS_PER_KILOMETER = 0.5399568034557235D;
    public static final double KILOMETERS_PER_MPH = 1.6093440000000001D;
    public static boolean PACKET_LEN_END_OF_STREAM = true;
    private static boolean IGNORE_NMEA_CHECKSUM = false;
    private static final TimeZone gmtTimezone = DateTime.getGMTTimeZone();
    private static final int InputStatusCodes_ON[] = {
            62496, 62497, 62498, 62499, 62500, 62501, 62502, 62503, 62504, 62505,
            62506, 62507, 62508, 62509, 62510, 62511
    };
    private static final int InputStatusCodes_OFF[] = {
            62528, 62529, 62530, 62531, 62532, 62533, 62534, 62535, 62536, 62537,
            62538, 62539, 62540, 62541, 62542, 62543
    };
    private String sessionID;
    private GPSEvent gpsEvent;
    private Device gpsDevice;
    private String lastModemID;
    private boolean terminate;
    private String ipAddress;
    private int clientPort;
    public static final boolean USE_STANDARD_TCP_SESSION_ID = true;

    static {
        DFT_INSERT_EVENT = true;
        INSERT_EVENT = DFT_INSERT_EVENT;
    }

}

/*
 * Copyright 2016 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.protocol;

import org.apache.commons.collections.map.LinkedMap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.traccar.BaseProtocolDecoder;
import org.traccar.DeviceSession;
import org.traccar.helper.BitUtil;
import org.traccar.helper.DateBuilder;
import org.traccar.helper.Log;
import org.traccar.helper.UnitsConverter;
import org.traccar.model.Position;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.text.FieldPosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.Vector;

import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;
import static org.jboss.netty.buffer.ChannelBuffers.hexDump;

public class OigoMSXXProtocolDecoder extends BaseProtocolDecoder {

    public static final int MSG_AR_LOCATION = 0x00;
    public static final int MSG_AR_REMOTE_START = 0x10;
    public static final int MSG_ACKNOWLEDGEMENT = 0xE0;
    private static final TimeZone gmtTimezone = DateTime.getGMTTimeZone();

    public OigoMSXXProtocolDecoder(OigoMSXXProtocol protocol) {
        super(protocol);
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

    private Position decodeMSXXMessage(Channel channel, SocketAddress remoteAddress, ChannelBuffer buf) {
        byte[] pktBytes = new byte[buf.capacity()];
        buf.readBytes(pktBytes);
        String hex = hexDump(buf);
        Log.info((new StringBuilder()).append("Recv[HEX]: ").append(hex).toString() + new Object[0]);
        Log.info((new StringBuilder()).append("Recv[Length]: ").append(hex.length()).toString() + new Object[0]);
        Position position = null;
        if (pktBytes.length > 14)
            position = parseInsertRecord_MGXX(hex);
        return position;
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

    private Position parseInsertRecord_MGXX(String hex) {
        if (hex == null) {
            Log.error("String is null" + new Object[0]);
            return null;
        }
        Log.info("Parsing(Oigo MG-XX Series)" + new Object[0]);
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
                Log.info("Latitud Invalida, Se Interrumpe Trama" + new Object[0]);
                return null;
            }
            if (gps_longitude.equalsIgnoreCase("00000000")) {
                Log.info("Longitud Invalida, Se Interrumpe Trama" + new Object[0]);
                return null;
            }
//            String flag_bin = hex2BinStr(flag);
//            String flag_inputs = "0000";
//            if (flag_bin.length() == 1)
//                flag_inputs = (new StringBuilder()).append("000").append(flag_bin).toString();
//            else if (flag_bin.length() == 2)
//                flag_inputs = (new StringBuilder()).append("00").append(flag_bin).toString();
//            else if (flag_bin.length() == 3)
//                flag_inputs = (new StringBuilder()).append("0").append(flag_bin).toString();
//            else if (flag_bin.length() == 4)
//                flag_inputs = flag_bin;
//            if (flag_inputs.substring(0, 1) == "1")
//                need_ack = true;
//            String gpioDirectionBin = hex2BinStr(gpio_direction);
//            String gpioDirectionInputs = "00000000";
//            if (gpioDirectionBin.length() == 1)
//                gpioDirectionInputs = (new StringBuilder()).append("0000000").append(gpioDirectionBin).toString();
//            else if (gpioDirectionBin.length() == 2)
//                gpioDirectionInputs = (new StringBuilder()).append("000000").append(gpioDirectionBin).toString();
//            else if (gpioDirectionBin.length() == 3)
//                gpioDirectionInputs = (new StringBuilder()).append("00000").append(gpioDirectionBin).toString();
//            else if (gpioDirectionBin.length() == 4)
//                gpioDirectionInputs = (new StringBuilder()).append("0000").append(gpioDirectionBin).toString();
//            else if (gpioDirectionBin.length() == 5)
//                gpioDirectionInputs = (new StringBuilder()).append("000").append(gpioDirectionBin).toString();
//            else if (gpioDirectionBin.length() == 6)
//                gpioDirectionInputs = (new StringBuilder()).append("00").append(gpioDirectionBin).toString();
//            else if (gpioDirectionBin.length() == 7)
//                gpioDirectionInputs = (new StringBuilder()).append("0").append(gpioDirectionBin).toString();
//            else if (gpioDirectionBin.length() == 8)
//                gpioDirectionInputs = gpioDirectionBin;
//            String gpsSatellitesStatusBin = hex2BinStr(gps_satellites_status);
//            String gpsSatellitesStatusInputs = "00000000";
//            if (gpsSatellitesStatusBin.length() == 1)
//                gpsSatellitesStatusInputs = (new StringBuilder()).append("0000000").append(gpsSatellitesStatusBin).toString();
//            else if (gpsSatellitesStatusBin.length() == 2)
//                gpsSatellitesStatusInputs = (new StringBuilder()).append("000000").append(gpsSatellitesStatusBin).toString();
//            else if (gpsSatellitesStatusBin.length() == 3)
//                gpsSatellitesStatusInputs = (new StringBuilder()).append("00000").append(gpsSatellitesStatusBin).toString();
//            else if (gpsSatellitesStatusBin.length() == 4)
//                gpsSatellitesStatusInputs = (new StringBuilder()).append("0000").append(gpsSatellitesStatusBin).toString();
//            else if (gpsSatellitesStatusBin.length() == 5)
//                gpsSatellitesStatusInputs = (new StringBuilder()).append("000").append(gpsSatellitesStatusBin).toString();
//            else if (gpsSatellitesStatusBin.length() == 6)
//                gpsSatellitesStatusInputs = (new StringBuilder()).append("00").append(gpsSatellitesStatusBin).toString();
//            else if (gpsSatellitesStatusBin.length() == 7)
//                gpsSatellitesStatusInputs = (new StringBuilder()).append("0").append(gpsSatellitesStatusBin).toString();
//            else if (gpsSatellitesStatusBin.length() == 8)
//                gpsSatellitesStatusInputs = gpsSatellitesStatusBin;
//            gps_heading = hex2DecimalStr(gps_heading);
//            gps_speed = hex2DecimalStr(gps_speed);
//            gpsRssi = hex2DecimalStr(gpsRssi);
//            String modemID = identification.substring(1);
//            if (StringTools.isBlank(modemID)) {
//                Log.error("'IMEI' value is missing", new Object[0]);
//                return null;
//            }
//            gpsEvent = createGPSEvent(modemID);
//            if (gpsEvent == null)
//                return null;
//            Device device = gpsEvent.getDevice();
//            if (device == null)
//                return null;
//            String accountID = device.getAccountID();
//            String deviceID = device.getDeviceID();
//            String uniqueID = device.getUniqueID();
//            int statusCode = 61472;
            long fixtime = _parseDateOigo(utcTime, time_seconds);
            double latitude = _parseLatitudeOigo(gpsLatitude);
            double longitude = _parseLongitudeOigo(gps_longitude);
            double speedMPH = StringTools.parseDouble(gps_speed, 0.0D);
            double heading = StringTools.parseDouble(gps_heading, 0.0D);
            double altitudeF = _parseAltitudeOigo(gps_altitude);
            double rssi = StringTools.parseDouble(gpsRssi, 0.0D);

            Position position = new Position();
            position.setLatitude(latitude);
            position.setLongitude(longitude);

//            int satsCount = bin2DecimalInt(gpsSatellitesStatusInputs.substring(4, 8));
//            double speedKPH = speedMPH * 1.6093440000000001D;
//            double altitudeM = altitudeF * 0.30480000000000002D;
//            double odometer = hex2DecimalInt(virtual_odometer);
//            Log.info((new StringBuilder()).append("IMEI\t\t: ").append(modemID).toString(), new Object[0]);
//            Log.info((new StringBuilder()).append("Timestamp\t: ").append(fixtime).append(" [").append(new DateTime(fixtime)).append("]").append("\n").toString(), new Object[0]);
//            Log.info((new StringBuilder()).append("UniqueID\t: ").append(uniqueID).toString(), new Object[0]);
//            Log.info((new StringBuilder()).append("DeviceID\t: ").append(accountID).append("/").append(deviceID).toString(), new Object[0]);
//            Log.info((new StringBuilder()).append("Evento\t: ").append(report_tag).toString(), new Object[0]);
//            Log.info((new StringBuilder()).append("Secuencia\t: ").append(hex2DecimalStr(sequenceCounter)).toString(), new Object[0]);
//            if (report_tag.equalsIgnoreCase("01"))
//                statusCode = 64793;
//            else if (!report_tag.equalsIgnoreCase("02"))
//                if (report_tag.equalsIgnoreCase("03"))
//                    statusCode = 61722;
//                else if (report_tag.equalsIgnoreCase("09"))
//                    statusCode = 64787;
//                else if (report_tag.equalsIgnoreCase("0F"))
//                    statusCode = 64789;
//                else if (report_tag.equalsIgnoreCase("12"))
//                    statusCode = 62465;
//                else if (report_tag.equalsIgnoreCase("13"))
//                    statusCode = 62467;
//                else if (!report_tag.equalsIgnoreCase("14"))
//                    if (report_tag.equalsIgnoreCase("15"))
//                        statusCode = 61727;
//                    else if (report_tag.equalsIgnoreCase("19"))
//                        statusCode = 61714;
//                    else if (report_tag.equalsIgnoreCase("1A"))
//                        statusCode = 61715;
//                    else if (report_tag.equalsIgnoreCase("1C"))
//                        statusCode = 61720;
//            if (statusCode == 61472) {
//                double maxSpeed = device.getSpeedLimitKPH();
//                if (speedKPH > MINIMUM_SPEED_KPH && speedKPH < maxSpeed) {
//                    if (device.getLastValidSpeedKPH() <= MINIMUM_SPEED_KPH)
//                        statusCode = 61713;
//                    else
//                        statusCode = 61714;
//                } else if (speedKPH >= maxSpeed)
//                    statusCode = 61722;
//                else if (speedKPH <= MINIMUM_SPEED_KPH)
//                    statusCode = 61715;
//            }
//            gpsEvent.setTimestamp(fixtime);
//            gpsEvent.setStatusCode(statusCode);
//            gpsEvent.setLatitude(latitude);
//            gpsEvent.setLongitude(longitude);
//            gpsEvent.setSpeedKPH(speedKPH);
//            gpsEvent.setHeading(heading);
//            gpsEvent.setAltitude(altitudeM);
//            gpsEvent.setSatelliteCount(satsCount);
//            gpsEvent.setSignalStrength(rssi);
//            gpsEvent.setOdometerKM(odometer);
//            if (parseInsertRecord_Common(gpsEvent)) {
//                if (need_ack) {
//                    String ack_start = "E0";
//                    String ack_counter = sequenceCounter;
//                    String ack = (new StringBuilder()).append(ack_start).append(ack_counter).toString();
//                    Log.info((new StringBuilder()).append("Respuesta ACK [").append(modemID).append("]\t: ").append(ack).toString(), new Object[0]);
//                    return ack.getBytes();
//                } else {
//                    Log.info((new StringBuilder()).append("Sin ACK [").append(modemID).append("]").toString(), new Object[0]);
//                    return null;
//                }
//            } else {
//                return null;
//            }
        }
        return null;


    }

    private Position decodeArMessage(Channel channel, SocketAddress remoteAddress, ChannelBuffer buf) {

        buf.skipBytes(1); // header
        buf.readUnsignedShort(); // length

        int type = buf.readUnsignedByte();

        int tag = buf.readUnsignedByte();

        DeviceSession deviceSession;
        switch (BitUtil.to(tag, 3)) {
            case 0:
                String imei = hexDump(buf.readBytes(8)).substring(1);
                deviceSession = getDeviceSession(channel, remoteAddress, imei);
                break;
            case 1:
                buf.skipBytes(1);
                String meid = buf.readBytes(14).toString(StandardCharsets.US_ASCII);
                deviceSession = getDeviceSession(channel, remoteAddress, meid);
                break;
            default:
                deviceSession = getDeviceSession(channel, remoteAddress);
                break;
        }

        if (deviceSession == null || type != MSG_AR_LOCATION) {
            return null;
        }

        Position position = new Position();
        position.setProtocol(getProtocolName());
        position.setDeviceId(deviceSession.getDeviceId());

        position.set(Position.KEY_EVENT, buf.readUnsignedByte());

        int mask = buf.readInt();

        if (BitUtil.check(mask, 0)) {
            position.set(Position.KEY_INDEX, buf.readUnsignedShort());
        }

        if (BitUtil.check(mask, 1)) {
            int date = buf.readUnsignedByte();
            DateBuilder dateBuilder = new DateBuilder()
                    .setDate(BitUtil.between(date, 4, 8) + 2010, BitUtil.to(date, 4), buf.readUnsignedByte())
                    .setTime(buf.readUnsignedByte(), buf.readUnsignedByte(), buf.readUnsignedByte());
            position.setTime(dateBuilder.getDate());
        }

        if (BitUtil.check(mask, 2)) {
            buf.skipBytes(5); // device time
        }

        if (BitUtil.check(mask, 3)) {
            position.setLatitude(buf.readUnsignedInt() * 0.000001 - 90);
            position.setLongitude(buf.readUnsignedInt() * 0.000001 - 180.0);
        }

        if (BitUtil.check(mask, 4)) {
            int status = buf.readUnsignedByte();
            position.setValid(BitUtil.between(status, 4, 8) != 0);
            position.set(Position.KEY_SATELLITES, BitUtil.to(status, 4));
            position.set(Position.KEY_HDOP, buf.readUnsignedByte() * 0.1);
        }

        if (BitUtil.check(mask, 5)) {
            position.setSpeed(UnitsConverter.knotsFromKph(buf.readUnsignedByte()));
        }

        if (BitUtil.check(mask, 6)) {
            position.setCourse(buf.readUnsignedShort());
        }

        if (BitUtil.check(mask, 7)) {
            position.setAltitude(buf.readShort());
        }

        if (BitUtil.check(mask, 8)) {
            position.set(Position.KEY_RSSI, buf.readUnsignedByte());
        }

        if (BitUtil.check(mask, 9)) {
            position.set(Position.KEY_POWER, buf.readUnsignedShort() * 0.001);
        }

        if (BitUtil.check(mask, 10)) {
            position.set(Position.KEY_BATTERY, buf.readUnsignedShort() * 0.001);
        }

        if (BitUtil.check(mask, 11)) {
            buf.skipBytes(2); // gpio
        }

        if (BitUtil.check(mask, 12)) {
            position.set(Position.KEY_ODOMETER, buf.readUnsignedInt() * 1000);
        }

        if (BitUtil.check(mask, 13)) {
            buf.skipBytes(6); // software version
        }

        if (BitUtil.check(mask, 14)) {
            buf.skipBytes(5); // hardware version
        }

        if (BitUtil.check(mask, 15)) {
            buf.readUnsignedShort(); // device config
        }

        return position;
    }

    private double convertCoordinate(long value) {
        boolean negative = value < 0;
        value = Math.abs(value);
        double minutes = (value % 100000) * 0.001;
        value /= 100000;
        double degrees = value + minutes / 60;
        return negative ? -degrees : degrees;
    }

    private Position decodeMgMessage(Channel channel, SocketAddress remoteAddress, ChannelBuffer buf) {

        buf.readUnsignedByte(); // tag
        int flags = buf.getUnsignedByte(buf.readerIndex());

        DeviceSession deviceSession;
        if (BitUtil.check(flags, 6)) {
            buf.readUnsignedByte(); // flags
            deviceSession = getDeviceSession(channel, remoteAddress);
        } else {
            String imei = hexDump(buf.readBytes(8)).substring(1);
            deviceSession = getDeviceSession(channel, remoteAddress, imei);
        }

        if (deviceSession == null) {
            return null;
        }

        Position position = new Position();
        position.setProtocol(getProtocolName());
        position.setDeviceId(deviceSession.getDeviceId());

        buf.skipBytes(8); // imsi

        int date = buf.readUnsignedShort();

        DateBuilder dateBuilder = new DateBuilder()
                .setDate(2010 + BitUtil.from(date, 12), BitUtil.between(date, 8, 12), BitUtil.to(date, 8))
                .setTime(buf.readUnsignedByte(), buf.readUnsignedByte(), 0);

        position.setValid(true);
        position.setLatitude(convertCoordinate(buf.readInt()));
        position.setLongitude(convertCoordinate(buf.readInt()));

        position.setAltitude(UnitsConverter.metersFromFeet(buf.readShort()));
        position.setCourse(buf.readUnsignedShort());
        position.setSpeed(UnitsConverter.knotsFromMph(buf.readUnsignedByte()));

        position.set(Position.KEY_POWER, buf.readUnsignedByte() * 0.1);
        position.set(Position.PREFIX_IO + 1, buf.readUnsignedByte());

        dateBuilder.setSecond(buf.readUnsignedByte());
        position.setTime(dateBuilder.getDate());

        position.set(Position.KEY_RSSI, buf.readUnsignedByte());

        int index = buf.readUnsignedByte();

        position.set(Position.KEY_VERSION_FW, buf.readUnsignedByte());
        position.set(Position.KEY_SATELLITES, buf.readUnsignedByte());
        position.set(Position.KEY_ODOMETER, (long) (buf.readUnsignedInt() * 1609.34));

        if (channel != null && BitUtil.check(flags, 7)) {
            ChannelBuffer response = dynamicBuffer();
            response.writeByte(MSG_ACKNOWLEDGEMENT);
            response.writeByte(index);
            response.writeByte(0x00);
            channel.write(response, remoteAddress);
        }

        return position;
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

        ChannelBuffer buf = (ChannelBuffer) msg;

//        if (buf.getUnsignedByte(buf.readerIndex()) == 0x7e) {
        return decodeArMessage(channel, remoteAddress, buf);
//        } else {
//            return decodeMgMessage(channel, remoteAddress, buf);
//        }
    }

    private static final class StringTools {

        public static boolean isLong(Object data, boolean strict) {
            if (data == null) {
                return false;
            } else
                return data instanceof Number;
        }

        /**
         * ** Parse the specified Object array into a long array
         * ** @param data  The Object array to parse
         * ** @param dft   The default values used if unable to parse a specific entry in the Object array
         * ** @return The parsed long array
         **/
        public static long[] parseLong(Object data[], long dft) {
            if (data == null) {
                return new long[0];
            } else {
                long valList[] = new long[data.length];
                for (int i = 0; i < data.length; i++) {
                    valList[i] = StringTools.parseLong(data[i], dft);
                }
                return valList;
            }
        }

        /**
         * ** Parse the specified object into a long value
         * ** @param data  The object to parse
         * ** @param dft   The default long value if unable to parse the specified object
         * ** @return The parsed long value
         **/
        public static long parseLong(Object data, long dft) {
            if (data == null) {
                return dft;
            } else if (data instanceof Number) {
                return ((Number) data).longValue();
            } else if (data instanceof DateTime) {
                return ((DateTime) data).getTimeSec();
            } else {
                return StringTools.parseLong(data.toString(), dft);
            }
        }

        public static boolean isInt(Object data, boolean strict) {
            if (data == null) {
                return false;
            }
            return data instanceof Number;
        }

        public static String[] parseString(String value, char delim) {
            return StringTools.parseString(value, String.valueOf(delim), true);
        }

        public static String[] parseString(String value, String sdelim) {
            return StringTools.parseString(value, sdelim, true);
        }

        public static String[] parseString(StringBuffer value, char delim) {
            if (value == null) {
                return new String[0];
            } else {
                return StringTools.parseString(value.toString(), String.valueOf(delim), true);
            }
        }

        public static String[] parseString(StringBuffer value, String sdelim) {
            if (value == null) {
                return new String[0];
            } else {
                return StringTools.parseString(value.toString(), sdelim, true);
            }
        }

        public static String[] parseString(StringBuffer value, String sdelim, boolean trim) {
            if (value == null) {
                return new String[0];
            } else {
                return StringTools.parseString(value.toString(), sdelim, trim);
            }
        }

        /**
         * ** Parse the character delimited input String into an array of Strings (similar to Perl's "split" function).<br>
         * ** Quoted Strings are parsed literally.<br>
         * ** If the input String is non-null, this method returns an array with at-least one entry.
         * ** If the input String is null, an empty (non-null) String array is returned.<br>
         * ** @param value  The String to parse
         * ** @param sdelim The character delimiters (all characters in this String are considered
         * **               individual delimiter candidates)
         * ** @param trim   True to trim leading/trailing spaces
         * ** @return The array of parse Strings
         **/
        public static String[] parseString(String value, String sdelim, boolean trim) {
            if (value != null) {
                boolean skipNL = sdelim.equals("\r\n"); // special case

            /* parse */
                List<String> v1 = new Vector<String>();
                Arrays.asList(new StringTokenizer(value, sdelim, true), v1);

            /* examine all tokens to make sure we include blank items */
                int dupDelim = 1; // assume we've started with a delimiter
                boolean consumeNextNL = false;
                java.util.List<String> v2 = new Vector<String>();
                for (Iterator i = v1.iterator(); i.hasNext(); ) {
                    String s = (String) i.next();
                    if ((s.length() == 1) && (sdelim.indexOf(s) >= 0)) {
                        // found a delimiter
                        if (skipNL) {
                            char ch = s.charAt(0);
                            if (consumeNextNL && (ch == '\n')) {
                                consumeNextNL = false;
                            } else {
                                consumeNextNL = (ch == '\r');
                                if (dupDelim > 0) {
                                    v2.add("");
                                } // blank item
                                dupDelim++;
                            }
                        } else {
                            if (dupDelim > 0) {
                                v2.add("");
                            } // blank item
                            dupDelim++;
                        }
                    } else {
                        v2.add(trim ? s.trim() : s);
                        dupDelim = 0;
                        consumeNextNL = false;
                    }
                }
                if (dupDelim > 0) {
                    v2.add("");
                } // final blank item

            /* return parsed array */
                return v2.toArray(new String[v2.size()]);

            } else {

            /* nothing parsed */
                return new String[0];

            }
        }

        public static int[] parseInt(Object data[], int dft) {
            if (data == null) {
                return new int[0];
            } else {
                int valList[] = new int[data.length];
                for (int i = 0; i < data.length; i++) {
                    valList[i] = StringTools.parseInt(data[i], dft);
                }
                return valList;
            }
        }

        public static int[] parseInt(Object data[], int dftList[]) {
            if (Arrays.asList(data).isEmpty()) {
                return dftList;
            } else {
                int dftLast = ((dftList != null) && (dftList.length > 0)) ? dftList[dftList.length - 1] : 0;
                int intList[] = new int[data.length];
                for (int i = 0; i < data.length; i++) {
                    int d = ((dftList != null) && (dftList.length > i)) ? dftList[i] : dftLast;
                    intList[i] = StringTools.parseInt(data[i], d);
                }
                return intList;
            }
        }


        public static int parseInt(Object data, int dft) {
            if (data == null) {
                return dft;
            } else if (data instanceof Number) {
                return ((Number) data).intValue();
            } else {
                return StringTools.parseInt(data.toString(), dft);
            }
        }

        public static double parseDouble(byte b[], int ofs, boolean isBigEndian, double dft) {

        /* valid byte array */
            if ((b == null) || ((ofs + 8) > b.length)) {
                return dft;
            }

        /* parse IEEE 754 double */
            int i = ofs;
            long doubleLong = 0L;
            if (isBigEndian) {
                doubleLong =
                        (((long) b[i + 0] & 0xFF) << (7 * 8)) +
                                (((long) b[i + 1] & 0xFF) << (6 * 8)) +
                                (((long) b[i + 2] & 0xFF) << (5 * 8)) +
                                (((long) b[i + 3] & 0xFF) << (4 * 8)) +
                                (((long) b[i + 4] & 0xFF) << (3 * 8)) +
                                (((long) b[i + 5] & 0xFF) << (2 * 8)) +
                                (((long) b[i + 6] & 0xFF) << (1 * 8)) +
                                ((long) b[i + 7] & 0xFF);
            } else {
                doubleLong =
                        (((long) b[i + 7] & 0xFF) << (7 * 8)) +
                                (((long) b[i + 6] & 0xFF) << (6 * 8)) +
                                (((long) b[i + 5] & 0xFF) << (5 * 8)) +
                                (((long) b[i + 4] & 0xFF) << (4 * 8)) +
                                (((long) b[i + 3] & 0xFF) << (3 * 8)) +
                                (((long) b[i + 2] & 0xFF) << (2 * 8)) +
                                (((long) b[i + 1] & 0xFF) << (1 * 8)) +
                                ((long) b[i + 0] & 0xFF);
            }
            return Double.longBitsToDouble(doubleLong);

        }


        public static double[] parseDouble(Object data[], double dft) {
            if (data == null) {
                return new double[0];
            } else {
                double valList[] = new double[data.length];
                for (int i = 0; i < data.length; i++) {
                    valList[i] = StringTools.parseDouble(data[i], dft);
                }
                return valList;
            }
        }


        public static double parseDouble(Object data, double dft) {
            if (data == null) {
                return dft;
            } else if (data instanceof Number) {
                return ((Number) data).doubleValue();
            } else {
                return StringTools.parseDouble(data.toString(), dft);
            }
        }


    }

    public static class DateTime implements Comparable, Cloneable {
        public static final String GMT_TIMEZONE = "GMT";
        public static final TimeZone GMT = DateTime.getGMTTimeZone();

        // ------------------------------------------------------------------------

        public static final String DEFAULT_DATE_FORMAT = "yyyy/MM/dd";
        public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";
        public static final String DEFAULT_TIMEZONE_FORMAT = "zzz"; // short format

        public static final String DEFAULT_DATETIME_FORMAT = DEFAULT_DATE_FORMAT + " " + DEFAULT_TIME_FORMAT;
        public static final String DEFAULT_DATETIME_TZ_FORMAT = DEFAULT_DATETIME_FORMAT + " " + DEFAULT_TIMEZONE_FORMAT;
        public static final DateTime INVALID_DATETIME = new DateTime(0L);
        public static final DateTime MIN_DATETIME = DateTime.getMinDate();

        // ------------------------------------------------------------------------
        public static final DateTime MAX_DATETIME = DateTime.getMaxDate();
        public static final long MIN_TIMESEC = MIN_DATETIME.getTimeSec();
        public static final long MAX_TIMESEC = MAX_DATETIME.getTimeSec();
        public static final long HOURS_PER_DAY = 24L;
        public static final long SECONDS_PER_MINUTE = 60L;

        // ------------------------------------------------------------------------
        // DateParseException
        public static final long MINUTES_PER_HOUR = 60L;

        // ------------------------------------------------------------------------
        public static final long DAYS_PER_WEEK = 7L;

        // ------------------------------------------------------------------------
        public static final long SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR;
        public static final long MINUTES_PER_DAY = HOURS_PER_DAY * MINUTES_PER_HOUR;
        public static final long SECONDS_PER_DAY = MINUTES_PER_DAY * SECONDS_PER_MINUTE;
        public static final long MINUTES_PER_WEEK = DAYS_PER_WEEK * MINUTES_PER_DAY;
        public static final long SECONDS_PER_WEEK = MINUTES_PER_WEEK * SECONDS_PER_MINUTE;
        public static final int JAN = 0;
        public static final int FEB = 1;
        public static final int MAR = 2;
        public static final int APR = 3;
        public static final int MAY = 4;
        public static final int JUN = 5;
        public static final int JUL = 6;
        public static final int AUG = 7;

        // ------------------------------------------------------------------------
        public static final int SEP = 8;
        public static final int OCT = 9;
        public static final int NOV = 10;
        public static final int DEC = 11;
        public static final int JANUARY = JAN;
        public static final int FEBRUARY = FEB;
        public static final int MARCH = MAR;
        public static final int APRIL = APR;
        //public static final int MAY       = MAY;
        public static final int JUNE = JUN;
        public static final int JULY = JUL;
        public static final int AUGUST = AUG;
        public static final int SEPTEMBER = SEP;
        public static final int OCTOBER = OCT;
        public static final int NOVEMBER = NOV;
        public static final int DECEMBER = DEC;
        public static final int SUN = 0;
        public static final int MON = 1;
        public static final int TUE = 2;
        public static final int WED = 3;
        public static final int THU = 4;
        public static final int FRI = 5;
        public static final int SAT = 6;
        public static final int SUNDAY = SUN;
        public static final int MONDAY = MON;
        public static final int TUESDAY = TUE;
        public static final int WEDNESDAY = WED;
        public static final int THURSDAY = THU;
        public static final int FRIDAY = FRI;
        public static final int SATURDAY = SAT;

        // ------------------------------------------------------------------------
        // I18N?
        private static final String MONTH_NAME[][] = {
                {"January", "Jan"},
                {"February", "Feb"},
                {"March", "Mar"},
                {"April", "Apr"},
                {"May", "May"},
                {"June", "Jun"},
                {"July", "Jul"},
                {"August", "Aug"},
                {"September", "Sep"},
                {"October", "Oct"},
                {"November", "Nov"},
                {"December", "Dec"},
        };
        private static final int MONTH_DAYS[] = {
                31, // Jan
                29, // Feb
                31, // Mar
                30, // Apr
                31, // May
                30, // Jun
                31, // Jul
                31, // Aug
                30, // Sep
                31, // Oct
                30, // Nov
                31, // Dec
        };
        // I18N?
        private static final String DAY_NAME[][] = {
                // 0            1      2
                {"Sunday", "Sun", "Su"},
                {"Monday", "Mon", "Mo"},
                {"Tuesday", "Tue", "Tu"},
                {"Wednesday", "Wed", "We"},
                {"Thursday", "Thu", "Th"},
                {"Friday", "Fri", "Fr"},
                {"Saturday", "Sat", "Sa"},
        };
        private static final String ARG_NOW_SEC = "-now_sec";

        // ------------------------------------------------------------------------
        private static final String ARG_NOW_MS = "-now_ms";
        private static final String ARG_COMPILE_TIME = "-compileTime";
        private static final String ARG_TIMEZONE_LIST[] = new String[]{"tzlist", "timezones"};
        private static final String ARG_TIMEZONE_FILE[] = new String[]{"tmzfile"};
        private static final String ARG_TIMEZONE_TIME[] = new String[]{"tmztime"};
        private static final String ARG_TIMEZONE[] = new String[]{"tmz", "tz"};
        private static final String ARG_TIME[] = new String[]{"time"};
        private static final String ARG_PLUS[] = new String[]{"plus"};
        private static final String ARG_FORMAT[] = new String[]{"format"};
        private static Object TimeZoneDSTMap_initLock = new Object();
        private static Map<String, String> TimeZoneDSTMap = null;
        private static SimpleDateFormat simpleFormatter = null;
        private static String mainTMZ[] = {"US/Pacific", "GMT", "GMT-00:00", "GMT-01:00"};
        private TimeZone timeZone = null;
        private long timeMillis = 0L; // ms since January 1, 1970, 00:00:00 GMT

        /**
         * ** Default constructor.
         * ** Initialize with the current Epoch time.
         **/
        public DateTime() {
            this.setTimeMillis(getCurrentTimeMillis());
        }

        /**
         * ** Constructor.
         * ** Initialize with the current Epoch time.
         * ** @param tz  The TimeZone
         **/
        public DateTime(TimeZone tz) {
            this();
            this.setTimeZone(tz);
        }

        /**
         * ** Constructor.
         * ** @param date  The Date object used to initialize this DateTime
         **/
        public DateTime(Date date) {
            this.setTimeMillis((date != null) ? date.getTime() : 0L);
        }

        /**
         * ** Constructor.
         * ** @param date  The Date object used to initialize this DateTime
         * ** @param tz  The TimeZone
         **/
        public DateTime(Date date, TimeZone tz) {
            this.setTimeMillis((date != null) ? date.getTime() : 0L);
            this.setTimeZone(tz);
        }

        /**
         * ** Constructor (time is set to Noon)
         * ** @param tz      The TimeZone
         * ** @param year    The year
         * ** @param month1  The 1-based month index [1..12]
         * ** @param day     The day
         **/
        public DateTime(TimeZone tz, int year, int month1, int day) {
            this(tz, year, month1, day, 12, 0, 0); // Noon
        }

        /**
         * ** Constructor.
         * ** @param tz      The TimeZone
         * ** @param year    The year
         * ** @param month1  The 1-based month index [1..12]
         * ** @param day     The day
         * ** @param hour24  The hour of the day [24 hour clock]
         * ** @param minute  The minute of the hour
         * ** @param second  The second of the minute
         **/
        public DateTime(TimeZone tz, int year, int month1, int day, int hour24, int minute, int second) {
            this.setDate(tz, year, month1, day, hour24, minute, second);
        }

        /**
         * ** Constructor.
         * ** @param timeSec  The Epoch time in seconds
         **/
        public DateTime(long timeSec) {
            if (timeSec > 9000000000L) {
                this.setTimeMillis(timeSec);
            } else {
                this.setTimeSec(timeSec);
            }
        }

        /**
         * ** Constructor.
         * ** @param timeSec  The Epoch time in seconds
         * ** @param tz       The TimeZone
         **/
        public DateTime(long timeSec, TimeZone tz) {
            this(timeSec);
            this.setTimeZone(tz);
        }

        /**
         * ** Constructor.
         * ** @param d  A date/time String representation
         * ** @throws DateParseException if an invalid date/time format was specified.
         **/
        public DateTime(String d)
                throws DateParseException {
            this.setDate(d);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Copy constructor
         * ** @param dt  Another DateTime instance
         **/
        public DateTime(DateTime dt) {
            this.timeMillis = dt.timeMillis;
            this.timeZone = dt.timeZone;
        }

        /**
         * ** Copy constructor with delta offset time
         * ** @param dt  Another DateTime instance
         * ** @param deltaOffsetSec  +/- offset from time specified in DateTime instance
         **/
        public DateTime(DateTime dt, long deltaOffsetSec) {
            this.timeMillis = dt.timeMillis + (deltaOffsetSec * 1000L);
            this.timeZone = dt.timeZone;
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns a compact date/time format<br>
         * ** This format can be parsed by the "parseArgumentDate" method if the specified
         * ** separator is one of ",", " ", or "|".
         * ** @parm sep The date|time|zone separator String.
         * ** @return The date|time|zone format string
         **/
        public static String CompactDateTimeFormat(String sep) {
            // ie. "yyyy/MM/dd|HH:mm:ss|zzz"
            String s = (sep != null) ? sep : "|";
            StringBuffer sb = new StringBuffer();
            sb.append("yyyy/MM/dd");
            sb.append(sep);
            sb.append("HH:mm:ss");
            sb.append(sep);
            sb.append("zzz");
            return sb.toString();
        }

        /**
         * ** Returns a compact date/time format<br>
         * ** This format can be parsed by the "parseArgumentDate" method if the specified
         * ** separator is one of ",", " ", or "|".
         * ** @parm sep The date|time|zone separator character.
         * ** @return The date|time|zone format string
         **/
        public static String CompactDateTimeFormat(char sep) {
            return CompactDateTimeFormat(String.valueOf(sep));
        }

        /**
         * ** Analyzes the specified date format to determine the date separator characters
         * ** @param dateFormat The date format to analyze
         * ** @return The date separator characters
         **/
        public static char[] GetDateSeparatorChars(String dateFormat) {
            char sep[] = new char[]{'/', '/'};
            if (dateFormat != null) {
                for (int i = 0, c = 0; (i < dateFormat.length()) && (c < sep.length); i++) {
                    char ch = dateFormat.charAt(i);
                    if (!Character.isLetterOrDigit(ch)) {
                        sep[c++] = ch;
                    }
                }
            }
            return sep;
        }

        /**
         * ** Returns the number of seconds in the specified number of days
         * ** @param days  The number of days to convert to seconds
         * ** @return The number of seconds
         **/
        public static long DaySeconds(long days) {
            return days * SECONDS_PER_DAY;
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns the number of seconds in the specified number of days
         * ** @param days  The number of days to convert to seconds
         * ** @return The number of seconds
         **/
        public static long DaySeconds(double days) {
            return Math.round(days * (double) SECONDS_PER_DAY);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns the number of seconds in the specified number of hours
         * ** @param hours  The number of hours to convert to seconds
         * ** @return The number of seconds
         **/
        public static long HourSeconds(long hours) {
            return hours * SECONDS_PER_HOUR;
        }

        /**
         * ** Returns the number of seconds in the specified number of minutes
         * ** @param minutes  The number of minutes to convert to seconds
         * ** @return The number of seconds
         **/
        public static long MinuteSeconds(long minutes) {
            return minutes * SECONDS_PER_MINUTE;
        }

        /**
         * ** Gets the 0-based index for the specified month abbreviation
         * ** @param month  The month abbreviation
         * ** @return The 0-based index [0..11] of the specified month appreviation
         **/
        public static int getMonthIndex0(String month, int dft) {
            String m = (month != null) ? month.toLowerCase().trim() : null;
            if ((m != null) && !m.equals("")) {
                if (Character.isDigit(m.charAt(0))) {
                    int v = StringTools.parseInt(m, -1);
                    if ((v >= 0) && (v < 12)) {
                        return v;
                    }
                } else {
                    for (int i = 0; i < MONTH_NAME.length; i++) {
                        if (m.startsWith(MONTH_NAME[i][1].toLowerCase())) {
                            return i;
                        }
                    }
                }
            }
            return dft;
        }

        /**
         * ** Gets the 1-based index for the specified month abbreviation
         * ** @param month  The month abbreviation
         * ** @return The 1-based index [1..12] of the specified month appreviation
         **/
        public static int getMonthIndex1(String month, int dft) {
            return DateTime.getMonthIndex0(month, dft - 1) + 1;
        }

        // ------------------------------------------------------------------------

        /**
         * ** Gets the month name/abbreviation for the specified 1-based month index
         * ** @param mon1  A 1-based month index [1..12]
         * ** @param abbrev  True to return the month abbreviation, false to return the name
         * ** @return The month name/abbreviation
         **/
        public static String getMonthName(int mon1, boolean abbrev) {
            int mon0 = mon1 - 1;
            if ((mon0 >= JANUARY) && (mon0 <= DECEMBER)) {
                return abbrev ? MONTH_NAME[mon0][1] : MONTH_NAME[mon0][0];
            } else {
                return "";
            }
        }

        ;

        /**
         * ** Returns all month names/appreviations
         * ** @param abbrev  True to return month abbreviations, false to return month names
         * ** @return  An array of month names/abbreviations
         **/
        public static String[] getMonthNames(boolean abbrev) {
            String mo[] = new String[MONTH_NAME.length];
            for (int i = 0; i < MONTH_NAME.length; i++) {
                mo[i] = DateTime.getMonthName(i, abbrev);
            }
            return mo;
        }

        /**
         * ** Returns a Map object containing a map of month names/abbreviations and it's
         * ** 0-based month index [0..11]
         * ** @param abbrev  True to create the Map object with abbreviations, false for names
         * ** @return The Map object
         **/
        public static Map<String, Integer> getMonthNameMap(boolean abbrev) {
            Map<String, Integer> map = new LinkedMap();
            for (int i = 0; i < MONTH_NAME.length; i++) {
                map.put(DateTime.getMonthName(i, abbrev), new Integer(i));
            }
            return map;
        }

        /**
         * ** Gets the number of days in the specified month
         * ** @param tz   The TimeZone
         * ** @param mon1 The 1-based month index [1..12]
         * ** @param year The year [valid for all AD years]
         * ** @return The number of days in the specified month
         **/
        public static int getDaysInMonth(TimeZone tz, int mon1, int year) {
            int yy = (year > mon1) ? year : mon1; // larger of the two
            int mm = (year > mon1) ? mon1 : year; // smaller of the two
            return DateTime.getMaxMonthDayCount(mm, DateTime.isLeapYear(yy));
        }

        /**
         * ** Gets the maximum number of days in the specified month
         * ** @param mon1  The 1-based month index [1..12]
         * ** @param isLeapYear  True for leap-year, false otherwise
         * ** @return The maximum number of days in the specified month
         **/
        public static int getMaxMonthDayCount(int mon1, boolean isLeapYear) {
            int m0 = mon1 - 1;
            int d = ((m0 >= 0) && (m0 < DateTime.MONTH_DAYS.length)) ? DateTime.MONTH_DAYS[m0] : 31;
            return ((m0 != FEBRUARY) || isLeapYear) ? d : 28;
        }

        /**
         * ** Returns true if the specified year represents a leap-year
         * ** @param year  The year [valid for all AD years]
         * ** @return True if the year is a leap-year, false otherwise
         **/
        public static boolean isLeapYear(int year) {
            return (new GregorianCalendar()).isLeapYear(year);
        }

        /**
         * ** Gets the day-of-week number for the specified day short abbreviation
         * ** @param day  The day short abbreviation
         * ** @return The 0-based day index [0..6]
         **/
        public static int getDayIndex(String day, int dft) {
            String d = (day != null) ? day.toLowerCase().trim() : null;
            if (!d.isEmpty()) {
                if (Character.isDigit(d.charAt(0))) {
                    int v = StringTools.parseInt(d, -1);
                    if ((v >= 0) && (v < 7)) {
                        return v;
                    }
                } else {
                    for (int i = 0; i < DAY_NAME.length; i++) {
                        if (d.startsWith(DAY_NAME[i][2].toLowerCase())) {
                            return i;
                        }
                    }
                }
            }
            return dft;
        }

        // ------------------------------------------------------------------------

        /**
         * ** Gets the day-of-week name for the specified day number/index
         * ** @param day  A 0-based day number/index [0..6]
         * ** @param abbrev  0 for full name, 1 for abbreviation, 2 for short abbreviation
         * ** @return  The day-of-week name/abbreviation
         **/
        public static String getDayName(int day, int abbrev) {
            if ((day >= SUNDAY) && (day <= SATURDAY) && (abbrev >= 0) && (abbrev <= 2)) {
                return DAY_NAME[day][abbrev];
            } else {
                return "";
            }
        }

        /**
         * ** Gets the day-of-week name for the specified day number/index
         * ** @param day  A 0-based day number/index [0..6]
         * ** @param abbrev  True for abbreviation, false for full name
         * ** @return  The day-of-week name/abbreviation
         **/
        public static String getDayName(int day, boolean abbrev) {
            return DateTime.getDayName(day, abbrev ? 1 : 0);
        }

        /**
         * ** Returns an array of day-of-week names
         * ** @param abbrev  0 for full name, 1 for abbreviation, 2 for short abbreviation
         * ** @return An array of day-of-week names/abbreviations
         **/
        public static String[] getDayNames(int abbrev) {
            String dy[] = new String[DAY_NAME.length];
            for (int i = 0; i < DAY_NAME.length; i++) {
                dy[i] = DateTime.getDayName(i, abbrev);
            }
            return dy;
        }

        /**
         * ** Returns an array of day-of-week names
         * ** @param abbrev  True abbreviations, false for full names
         * ** @return An array of day-of-week names/abbreviations
         **/
        public static String[] getDayNames(boolean abbrev) {
            return DateTime.getDayNames(abbrev ? 1 : 0);
        }

        /**
         * ** Returns a Map object of day-of-week names to their 0-based number/index
         * ** (used as a VComboBox item list)
         * ** @param abbrev 0 for full name, 1 for abbreviation, 2 for short abbreviation
         * ** @return The Map object
         **/
        public static Map<String, Integer> getDayNameMap(int abbrev) {
            Map<String, Integer> map = new LinkedMap();
            for (int i = 0; i < DAY_NAME.length; i++) {
                map.put(DateTime.getDayName(i, abbrev), new Integer(i));
            }
            return map;
        }

        /**
         * ** Returns a Map object of day-of-week names to their 0-based number/index
         * ** (used as a VComboBox item list)
         * ** @param abbrev True for abbreviations, false for full names
         * ** @return The Map object
         **/
        public static Map<String, Integer> getDayNameMap(boolean abbrev) {
            return DateTime.getDayNameMap(abbrev ? 1 : 0);
        }

        // ------------------------------------------------------------------------
        // ------------------------------------------------------------------------

        /**
         * ** Returns the day of the week for the specified year/month/day
         * ** @param year  The year [valid for dates after 1582/10/15]
         * ** @param mon1  The month [1..12]
         * ** @param day   The day [1..31]
         * ** @return The day of the week (0=Sunday, 6=Saturday)
         **/
        public static int getDayOfWeek(int year, int mon1, int day) {
            int mon0 = mon1 - 1;
            GregorianCalendar cal = new GregorianCalendar(year, mon0, day);
            return cal.get(Calendar.DAY_OF_WEEK) - Calendar.SUNDAY;
        }

        /**
         * ** Returns the day of the year for the specified year/month/day
         * ** @param year  The year [valid for dates after 1582/10/15]
         * ** @param mon1  The month [1..12]
         * ** @param day   The day [1..31]
         * ** @return The day of the year
         **/
        public static int getDayOfYear(int year, int mon1, int day) {
            int mon0 = mon1 - 1;
            GregorianCalendar cal = new GregorianCalendar(year, mon0, day);
            return cal.get(Calendar.DAY_OF_YEAR);
        }

        /**
         * ** Gets a String array of hours in a day
         * ** (used as a VComboBox item list)
         * ** @param hr24  True for 24 hour clock, false for 12 hour clock
         * ** @return A String array of hours in a day
         **/
        public static String[] getHours(boolean hr24) {
            String hrs[] = new String[hr24 ? 24 : 12];
            for (int i = 0; i < hrs.length; i++) {
                hrs[i] = String.valueOf(i);
            }
            return hrs;
        }

        /**
         * ** Gets a String array of minutes in a day
         * ** (used as a VComboBox item list)
         * ** @return A String array of minutes in a day
         **/
        public static String[] getMinutes() {
            String min[] = new String[60];
            for (int i = 0; i < min.length; i++) {
                min[i] = String.valueOf(i);
            }
            return min;
        }

        /**
         * ** Returns a String representation of the specified epoch time seconds
         * ** @param timeSec  The Epoch time seconds
         * ** @return The String representation
         **/
        public static String toString(long timeSec) {
            return (new java.util.Date(timeSec * 1000L)).toString();
        }

        /**
         * ** Returns the current Epoch time in seconds since January 1, 1970 00:00:00 GMT
         * ** @return The current Epoch time in seconds
         **/
        public static long getCurrentTimeSec() {
            // Number of seconds since the 'epoch' January 1, 1970, 00:00:00 GMT
            return getCurrentTimeMillis() / 1000L;
        }

        /**
         * ** Returns the current Epoch time in milliseconds since January 1, 1970 00:00:00 GMT
         * ** @return The current Epoch time in milliseconds
         **/
        public static long getCurrentTimeMillis() {
            // Number of milliseconds since the 'epoch' January 1, 1970, 00:00:00 GMT
            return System.currentTimeMillis();
        }


        /**
         * ** Returns true if the specified date is valid
         * ** @param dt   The DateTime to check
         * ** @return True if the specified date is valid
         **/
        public static boolean isValid(DateTime dt) {
            long tm = (dt != null) ? dt.getTimeSec() : -1L;
            if (tm < MIN_TIMESEC) {
                return false;
            } else if (tm > MAX_TIMESEC) {
                return false;
            } else {
                return true;
            }
        }

        /**
         * ** Gets the minimum acceptable DateTime
         * ** @return The minimum acceptable DateTime
         **/
        public static DateTime getMinDate() {
            return DateTime.getMinDate(null);
        }

        /**
         * ** Gets the minimum acceptable DateTime
         * ** @param tz  The TimeZone (not that it really matters)
         * ** @return The minimum acceptable DateTime
         **/
        public static DateTime getMinDate(TimeZone tz) {
            return new DateTime(1L, tz);
        }

        /**
         * ** Gets the maximum acceptable DateTime
         * ** @return The maximum acceptable DateTime
         **/
        public static DateTime getMaxDate() {
            return DateTime.getMaxDate(null);
        }

        /**
         * ** Gets the maximum acceptable DateTime
         * ** @param tz  The TimeZone (not that it really matters)
         * ** @return The maximum acceptable DateTime
         **/
        public static DateTime getMaxDate(TimeZone tz) {
            return new DateTime(tz, 2050, 12, 31);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Parse specified String into a ParsedDateTime instance.
         * ** This method parses a date which has been provided as an argument to a command-line tool
         * ** or in a URL request string.
         * ** Allowable Date Formats:<br>
         * **   YYYY-MM-DDThh:mm:ssZ  (GPX date format)<br>
         * **   YYYY/MM[/DD[/hh[:mm[:ss]]]][,ZZZ]  (ie. "2010/07/04/12:57:32,PST")<br>
         * **   YYYY/MM/DD,hh[:mm[:ss]]]][,ZZZ]    (ie. "2010/07/04,12:57:32,PST")<br>
         * **   EEEEEEEE[,ZZZ]                     (ie. "1277704868,PST")<br>
         * **   -Dd[,hh[:mm[:ss]]][,ZZZ]           (ie. "-3d,PST" => "3 days ago")<br>
         * **   +Dd[,hh[:mm[:ss]]][,ZZZ]           (ie. "+4d,PST" => "4 days from now")<br>
         * ** Examples:
         * **   "2010/07/04/12:57:32,PST"
         * **   "2010/07/04,12:57,EST"
         * **   "2010/07/04|12:57|EST"
         * **   "1277704868,PST"
         * **   "-1d,PST"     (yesterday)
         * **   "-3d,PST"     (3 days ago)
         * **   "+4d,PST"     (4 days from now)
         * ** @param dateStr  The date String to parse
         * ** @param dftTZ    The default TimeZone
         * ** @param dftTime  Enum indicating what 'time' to use if a time is not specified
         * ** @return The parsed ParsedDateTime instance, or null if unable to parse the specified date/time string
         * ** @throws DateParseException if an invalid date/time format was specified.
         **/
        public static ParsedDateTime parseDateTime(String dateStr, TimeZone dftTZ, DateTime.DefaultParsedTime dftTime)
                throws DateParseException {

        /* default DefaultParsedTime [isToDate] */
            if (dftTime == null) {
                dftTime = DefaultParsedTime.CurrentTime;
            }
            boolean useDayStart = DefaultParsedTime.DayStart.equals(dftTime);
            boolean useCurrTime = DefaultParsedTime.CurrentTime.equals(dftTime);
            boolean useDayEnd = DefaultParsedTime.DayEnd.equals(dftTime);

        /* no date string specified? */
            dateStr = dateStr.trim();
            if (dateStr.isEmpty()) {
                throw new DateParseException("'dateStr' argument is null/empty");
            }

        /* parse fields */
            String dateGrp[] = StringTools.parseString(dateStr, ", |"); // ", T"
            if ((dateGrp.length == 1) && (dateStr.length() == 20) && (dateStr.charAt(10) == 'T')) {
                // GPX format "YYYY-MM-DDThh:mm:ssZ"
                dateGrp = new String[]{dateStr.substring(0, 10), dateStr.substring(11)};
            }

        /* validate fields */
            int grpLen = dateGrp.length;
            if ((dateGrp.length < 1) || (dateGrp.length > 3)) {
                throw new DateParseException("Invalid number of ',' separated groups: " + dateStr);
            } else if (dateGrp[0].length() == 0) {
                throw new DateParseException("Invalid Date/Time specification[#1]: " + dateStr);
            }

        /* extract timezone (if any) */
            TimeZone timeZone = dftTZ;
            if ((dateGrp[grpLen - 1].length() > 0) && Character.isLetter(dateGrp[grpLen - 1].charAt(0))) {
                timeZone = DateTime.getTimeZone(dateGrp[grpLen - 1], null);
                if (timeZone == null) {
                    throw new DateParseException("Invalid TimeZone[#2]: " + dateStr);
                }
                grpLen--;
            } else if ((dateGrp.length == 2) && dateGrp[1].endsWith("Z")) { // GPX format
                timeZone = DateTime.getGMTTimeZone();
                dateGrp[1] = dateGrp[1].substring(0, dateGrp[1].length() - 1);
            }
            DateTime now = new DateTime(timeZone);

        /* parse date/time fields */
            char s = dateGrp[0].charAt(0);
            String dateFld[] = null;
            if (grpLen == 1) {
                // EEEEEEEE
                // YYYY/MM[/DD[/hh[:mm[:ss]]]]
                // -Dd
                if ((s == '+') || (s == '-')) {
                    // [+|-]####d
                    String D = dateGrp[0];
                    char lastCh = Character.toLowerCase(D.charAt(D.length() - 1));
                    if (lastCh == 'd') {
                        // days
                        D = D.substring(0, D.length() - 1);
                    } else {
                        // seconds
                    }
                    if (!StringTools.isInt(D, true)) {
                        throw new DateParseException("Non-numeric Delta time: " + dateStr);
                    }
                    long delta = StringTools.parseLong(D, 0);
                    long ofs = (lastCh == 'd') ? DateTime.DaySeconds(delta) : delta; // days/seconds
                    DateTime dt = new DateTime(now.getTimeSec() + ofs, timeZone);
                    int YY = dt.getYear();
                    int MM = dt.getMonth1();
                    int DD = dt.getDayOfMonth();
                    dateFld = new String[]{
                            String.valueOf(YY),
                            String.valueOf(MM),
                            String.valueOf(DD)
                    };
                } else {
                    String d[] = StringTools.parseString(dateGrp[0], "/:-");
                    if ((d.length < 1) || (d.length > 6)) {
                        throw new DateParseException("Invalid number of Date/Time fields: " + dateStr);
                    } else if (d.length == 1) {
                        // EEEEEEEE
                        if (!StringTools.isLong(d[0], true)) {
                            throw new DateParseException("Non-numeric Epoch time: " + dateStr);
                        }
                        long epoch = StringTools.parseLong(d[0], DateTime.getCurrentTimeSec());
                        return new ParsedDateTime(timeZone, epoch);
                    } else {
                        dateFld = d; // (length >= 2) && (length <= 6)
                    }
                }
            } else if (grpLen == 2) {
                // YYYY/MM/DD,hh[:mm[:ss]]]]
                // -Dd,hh[:mm[:ss]]]]
                String d[];
                if ((s == '+') || (s == '-')) {
                    String D = dateGrp[0];
                    char lastCh = Character.toLowerCase(D.charAt(D.length() - 1));
                    if (lastCh == 'd') {
                        // days
                        D = D.substring(0, D.length() - 1);
                    } else {
                        // seconds
                        throw new DateParseException("Missing 'd' on delta days: " + dateStr);
                    }
                    if (!StringTools.isInt(D, true)) {
                        throw new DateParseException("Non-numeric Delta time: " + dateStr);
                    }
                    long delta = StringTools.parseLong(D, 0);
                    long ofs = (lastCh == 'd') ? DateTime.DaySeconds(delta) : delta; // days/seconds
                    DateTime dt = new DateTime(now.getTimeSec() + ofs, timeZone);
                    int YY = dt.getYear();
                    int MM = dt.getMonth1();
                    int DD = dt.getDayOfMonth();
                    d = new String[]{
                            String.valueOf(YY),
                            String.valueOf(MM),
                            String.valueOf(DD)
                    };
                } else {
                    d = StringTools.parseString(dateGrp[0], "/:-");
                    if (d.length != 3) {
                        throw new DateParseException("Invalid number of Date fields: " + dateStr);
                    }
                    for (String n : d) {
                        if (!StringTools.isInt(n, true)) {
                            throw new DateParseException("Non-numeric Date specification: " + dateStr);
                        }
                    }
                }
                String t[] = StringTools.parseString(dateGrp[1], "/:-");
                if ((t.length < 1) || (t.length > 3)) {
                    throw new DateParseException("Invalid number of Time fields: " + dateStr);
                }
                for (String n : t) {
                    if (!StringTools.isInt(n, true)) {
                        throw new DateParseException("Non-numeric Time specification: " + dateStr);
                    }
                }
                dateFld = new String[d.length + t.length];  // (length >= 3) && (length <= 6)
                System.arraycopy(d, 0, dateFld, 0, d.length);
                System.arraycopy(t, 0, dateFld, d.length, t.length);
            } else {
                throw new DateParseException("Invalid number of Date/Time/TMZ groups: " + dateStr);
            }

        /* evaluate date/time fields (dateFld.length >= 2) */
            int YY = StringTools.parseInt(dateFld[0], now.getYear());
            int MM = StringTools.parseInt(dateFld[1], now.getMonth1());
            int maxDD = DateTime.getDaysInMonth(timeZone, MM, YY);
            int DD = 0; // day set below
            int hh = useDayStart ? 0 : useDayEnd ? 23 : now.getHour24();
            int mm = useDayStart ? 0 : useDayEnd ? 59 : now.getMinute();
            int ss = useDayStart ? 0 : useDayEnd ? 59 : now.getSecond();
            if (dateFld.length >= 3) {
                // at least YYYY/MM/DD provided
                DD = StringTools.parseInt(dateFld[2], now.getDayOfMonth());
                if (DD > maxDD) {
                    DD = maxDD;
                }
                if (dateFld.length >= 4) {
                    hh = StringTools.parseInt(dateFld[3], hh);
                }
                if (dateFld.length >= 5) {
                    mm = StringTools.parseInt(dateFld[4], mm);
                }
                if (dateFld.length >= 6) {
                    ss = StringTools.parseInt(dateFld[5], ss);
                }
            } else {
                // only YYYY/MM provided
                DD = useDayEnd ? maxDD : useDayStart ? 1 : now.getDayOfMonth();
                if (DD > maxDD) {
                    DD = maxDD;
                }
            }

        /* return new ParsedDateTime instance */
            return new ParsedDateTime(timeZone, YY, MM, DD, hh, mm, ss);

        }

        /**
         * ** Parse specified String into a DateTime instance.
         * ** This method parses a date which has been provided as an argument to a command-line tool
         * ** or in a URL request string.<br>
         * ** (see "parseDateTime" for allowable Date Formats)
         * ** @param dateStr  The date String to parse
         * ** @param dftTZ    The default TimeZone
         * ** @param dftTime  Enum indicating what 'time' to use if a time is not specified
         * ** @return The parsed DateTime instance
         * ** @throws DateParseException if an invalid date/time format was specified.
         **/
        public static DateTime parseArgumentDate(String dateStr, TimeZone dftTZ, DateTime.DefaultParsedTime dftTime)
                throws DateParseException {
            return DateTime.parseDateTime(dateStr, dftTZ, dftTime).createDateTime();
        }

        /**
         * ** Parse specified String into a DateTime instance.
         * ** This method parses a date which has been provided as an argument to a command-line tool
         * ** or in a URL request string.<br>
         * ** (see "parseDateTime" for allowable Date Formats)
         * ** @param dateStr  The date String to parse
         * ** @param dftTZ    The default TimeZone
         * ** @param isToDate True to default to an end-of-day time if the time is not specified
         * ** @return The parsed DateTime instance
         * ** @throws DateParseException if an invalid date/time format was specified.
         **/
        public static DateTime parseArgumentDate(String dateStr, TimeZone dftTZ, boolean isToDate)
                throws DateParseException {
            DefaultParsedTime dftTime = isToDate ? DefaultParsedTime.DayEnd : DefaultParsedTime.DayStart;
            return DateTime.parseDateTime(dateStr, dftTZ, dftTime).createDateTime();
        }

        /**
         * ** Parse specified String into a DateTime instance.
         * ** This method parses a date which has been provided as an argument to a command-line tool
         * ** or in a URL request string.
         * ** (see "parseDateTime" for allowable Date Formats)
         * ** @param dateStr  The date String to parse
         * ** @param dftTZ    The default TimeZone
         * ** @return The parsed DateTime instance
         * ** @throws DateParseException if an invalid date/time format was specified.
         **/
        public static DateTime parseArgumentDate(String dateStr, TimeZone dftTZ)
                throws DateParseException {
            DefaultParsedTime dftTime = DefaultParsedTime.DayStart;
            return DateTime.parseDateTime(dateStr, dftTZ, dftTime).createDateTime();
        }

        /**
         * ** Parse specified String into a DateTime instance.
         * ** This method parses a date which has been provided as an argument to a command-line tool
         * ** or in a URL request string.
         * ** (see "parseDateTime" for allowable Date Formats)
         * ** @param dateStr  The date String to parse
         * ** @return The parsed DateTime instance
         * ** @throws DateParseException if an invalid date/time format was specified.
         **/
        public static DateTime parseArgumentDate(String dateStr)
                throws DateParseException {
            DefaultParsedTime dftTime = DefaultParsedTime.DayStart;
            return DateTime.parseDateTime(dateStr, DateTime.getGMTTimeZone(), dftTime).createDateTime();
        }

        /**
         * ** Returns the day number of days since October 15, 1582 (the first day of the Gregorian Calendar)
         * ** @param year   The year [1582..4000]
         * ** @param month1 The month [1..12]
         * ** @param day    The day [1..31]
         * ** @return The number of days since October 15, 1582.
         **/
        public static long getDayNumberFromDate(int year, int month1, int day) {
            long yr = ((long) year * 1000L) + (long) (((month1 - 3) * 1000) / 12);
            return ((367L * yr + 625L) / 1000L) - (2L * (yr / 1000L))
                    + (yr / 4000L) - (yr / 100000L) + (yr / 400000L)
                    + (long) day - 578042L; // October 15, 1582, beginning of Gregorian Calendar
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns the day number of days since October 15, 1582 (the first day of the Gregorian Calendar)
         **/
        public static long getDayNumberFromDate(ParsedDateTime pdt) {
            if (pdt == null) {
                return 0L;
            } else if (pdt.epoch >= 0L) {
                DateTime dt = pdt.createDateTime();
                return DateTime.getDayNumberFromDate(dt.getYear(), dt.getMonth1(), dt.getDayOfMonth());
            } else {
                return DateTime.getDayNumberFromDate(pdt.year, pdt.month1, pdt.day);
            }
        }

        /**
         * ** Returns the day number of days since October 15, 1582 (the first day of the Gregorian Calendar)
         **/
        public static long getDayNumberFromDate(DateTime dt, TimeZone tz) {
            if (dt == null) {
                return 0L;
            } else {
                return DateTime.getDayNumberFromDate(dt.getYear(tz), dt.getMonth1(tz), dt.getDayOfMonth(tz));
            }
        }

        /**
         * ** Returns the day number of days since October 15, 1582 (the first day of the Gregorian Calendar)
         **/
        public static long getDayNumberFromDate(DateTime dt) {
            if (dt == null) {
                return 0L;
            } else {
                return DateTime.getDayNumberFromDate(dt.getYear(), dt.getMonth1(), dt.getDayOfMonth());
            }
        }

        /**
         * ** Returns the year/month/day from the day number (days since October 15, 1582)
         * ** @param dayNumber  The number of days since October 15, 1582
         * ** @return ParsedDateTime structure containing the year/month/day.
         **/
        public static ParsedDateTime getDateFromDayNumber(long dayNumber) {
            return DateTime.getDateFromDayNumber(dayNumber, null/*TimeZone*/);
        }

        /**
         * ** Returns the year/month/day from the day number (days since October 15, 1582)
         * ** @param dayNumber  The number of days since October 15, 1582
         * ** @return ParsedDateTime structure containing the year/month/day.
         **/
        public static ParsedDateTime getDateFromDayNumber(long dayNumber, TimeZone tmz) {
            long N = dayNumber + 578042L;
            long C = ((N * 1000L) - 200L) / 36524250L;
            long N1 = N + C - (C / 4L);
            long Y1 = ((N1 * 1000L) - 200L) / 365250L;
            long N2 = N1 - ((365250L * Y1) / 1000L);
            long M1 = ((N2 * 1000L) - 500L) / 30600L;
            int day = (int) (((N2 * 1000L) - (30600L * M1) + 500L) / 1000L);
            int month1 = (int) ((M1 <= 9L) ? (M1 + 3L) : (M1 - 9L));
            int year = (int) ((M1 <= 9L) ? Y1 : (Y1 + 1));
            return new ParsedDateTime(tmz, year, month1, day);
        }

        /**
         * ** Reads the available TimeZone IDs from the specified file.
         * ** @param tmzFile  The file from which TimeZone IDs are read.
         * ** @return A String array of read TimeZone IDs.
         **/
        public static String[] readTimeZones(File tmzFile) {
            if ((tmzFile != null) && tmzFile.exists()) {
                java.util.List<String> tzList = new Vector<String>();
                BufferedReader br = null;
                try {
                    br = new BufferedReader(new FileReader(tmzFile));
                    for (; ; ) {
                        String rline = br.readLine();
                        if (rline == null) {
                            break;
                        }
                        String tline = rline.trim();
                        if (!tline.equals("") && !tline.startsWith("#")) {
                            tzList.add(tline);
                        }
                    }
                    return tzList.toArray(new String[tzList.size()]);
                } catch (IOException ioe) {
                    Log.error("Unable to read file: " + tmzFile + " [" + ioe + "]");
                    return null;
                } finally {
                    if (br != null) {
                        try {
                            br.close();
                        } catch (IOException ioe) {/*ignore*/}
                    }
                }
            }
            return null;
        }

        private static Map<String, String> _GetTimeZoneDSTMap() {
            if (TimeZoneDSTMap == null) {
                // -- Lazy init of the DST TimeZone lookup table---
                synchronized (TimeZoneDSTMap_initLock) {
                    if (TimeZoneDSTMap == null) { // try again after lock
                        HashMap<String, String> dstMap = new HashMap<String, String>();
                        for (String tzi : TimeZone.getAvailableIDs()) {
                            TimeZone tz = TimeZone.getTimeZone(tzi);
                            String dst = tz.getDisplayName(true, TimeZone.SHORT);
                            if (!dst.isEmpty() && !dstMap.containsKey(dst)) {
                                dstMap.put(dst, tzi);
                            }
                        }
                        TimeZoneDSTMap = dstMap;
                    }
                }
            }
            return TimeZoneDSTMap;
        }

        private static TimeZone _lookupTimeZone(String tzid, TimeZone dft) {

        /* blank TimeZone ID */
            if (tzid.isEmpty()) {
                return dft;
            }

        /* GMT/UTC/Zulu timezones */
            if (tzid.equalsIgnoreCase("GMT") ||
                    tzid.equalsIgnoreCase("UTC") ||
                    tzid.equalsIgnoreCase("Zulu")) {
                return TimeZone.getTimeZone(tzid);
            }

        /* lookup TimeZone ID/name */
            // "TimeZone.getTimeZone" returns GMT for invalid timezones
            TimeZone tmz = TimeZone.getTimeZone(tzid);
            if (tmz.getRawOffset() != 0) { // ie. !(GMT+0)
                return tmz; // must be a valid time-zone
            }

        /* ID/name lookup failed, try looking up the DST name */
            // If the above specified TimeZone ID/name is a short DST name (such
            // as "PDT", "CDT", etc), the lookup will likely fail.  This section
            // attempts to create a cross referenced map of DST names to the
            // non-DST ID.
            String dstTmzID = _GetTimeZoneDSTMap().get(tzid);
            TimeZone dstTmz = (dstTmzID != null) ? TimeZone.getTimeZone(dstTmzID) : null;
            return ((dstTmz != null) && (dstTmz.getRawOffset() != 0)) ? dstTmz : dft;

        }

        /**
         * ** Returns true if the specified TimeZone is valid
         * ** @param tzid  The String representation of a TimeZone
         * ** @return True if the TimeZone is valid, false otherwise
         **/
        public static boolean isValidTimeZone(String tzid) {
            if (tzid.isEmpty()) {
                return false;
            } else if (tzid.equalsIgnoreCase("GMT") ||
                    tzid.equalsIgnoreCase("UTC") ||
                    tzid.equalsIgnoreCase("Zulu")) {
                return true;
            } else {
                TimeZone tmz = DateTime._lookupTimeZone(tzid, null);
                return (tmz != null) ? true : false;
            }
        }

        /**
         * ** Gets the TimeZone instance for the specified TimeZone id
         * ** @param tzid  The TimeZone id
         * ** @param dft   The default TimeZone to return if the specified TimeZone id
         * **              does not exist.
         * ** @return The TimZone
         **/
        public static TimeZone getTimeZone(String tzid, TimeZone dft) {
            if (tzid.isEmpty()) {
                return dft;
            } else if (tzid.equalsIgnoreCase("GMT") ||
                    tzid.equalsIgnoreCase("UTC") ||
                    tzid.equalsIgnoreCase("Zulu")) {
                return TimeZone.getTimeZone(tzid);
            } else {
                return DateTime._lookupTimeZone(tzid, dft);
            }
        }

        /**
         * ** Gets the TimeZone instance for the specified TimeZone id
         * ** @param tzid  The TimeZone id
         * ** @return The TimeZone
         **/
        public static TimeZone getTimeZone(String tzid) {
            if (tzid.isEmpty()) {
                return TimeZone.getDefault(); // local system default time-zone
            } else {
                TimeZone tmz = DateTime._lookupTimeZone(tzid, null);
                return (tmz != null) ? tmz : TimeZone.getDefault(); // DateTime.getGMTTimeZone();
            }
        }

        /**
         * ** Returns the default TimeZone
         * ** @return The default TimeZone
         **/
        public static TimeZone getDefaultTimeZone() {
            return DateTime.getTimeZone(null);
        }

        /**
         * ** Returns the GMT TimeZone
         * ** @return The GMT TimeZone
         **/
        public static TimeZone getGMTTimeZone() {
            return TimeZone.getTimeZone(GMT_TIMEZONE);
        }

        /**
         * ** Formats the specified Date instance.
         * ** @param date The Date instance
         * ** @param tz   The TimeZone
         * ** @param dtFmt  The Date/Time format
         * ** @return The String representation of the StringBuffer destination.
         **/
        public static String format(java.util.Date date, TimeZone tz, String dtFmt) {
            StringBuffer sb = new StringBuffer();
            SimpleDateFormat sdf = null;
            try {
                String f = (dtFmt != null) ? dtFmt : DEFAULT_DATETIME_FORMAT;
                sdf = new SimpleDateFormat(f);
            } catch (IllegalArgumentException iae) {
                Log.error("Invalid date/time format: " + dtFmt + iae);
                sdf = new SimpleDateFormat(DEFAULT_DATETIME_FORMAT); // assumed to be valid
            }
            sdf.setTimeZone((tz != null) ? tz : DateTime.getDefaultTimeZone());
            sdf.format(date, sb, new FieldPosition(0));
            return sb.toString();
        }


        /**
         * ** Parses the specified "hhhh:mm:ss" String into the number of represented seconds
         * ** @param hms  The String containing the "hhhh:mm:ss" to parse
         * ** @param dft  The default seconds returned if unable to parse the specified String.
         * ** @return The number of seconds represented by the specified String format
         **/
        public static int parseHourMinuteSecond(String hms, int dft) {
            String a[] = StringTools.parseString(hms, ":");
            if (a.length <= 1) {
                // assume all seconds
                return StringTools.parseInt(hms, dft);
            } else if (a.length == 2) {
                // assume "hhhh:mm"
                int h = StringTools.parseInt(a[0], -1);
                int m = StringTools.parseInt(a[1], -1);
                return ((h >= 0) && (m >= 0)) ? (((h * 60) + m) * 60) : dft;
            } else { // (a.length >= 3)
                // assume "hhhh:mm:ss"
                int h = StringTools.parseInt(a[0], -1);
                int m = StringTools.parseInt(a[1], -1);
                int s = StringTools.parseInt(a[2], -1);
                return ((h >= 0) && (m >= 0) && (s >= 0)) ? ((((h * 60) + m) * 60) + s) : dft;
            }
        }

        /**
         * ** Gets a Date object based on the time in this DateTime object
         * ** @return A Date instance
         **/
        public java.util.Date getDate() {
            return new java.util.Date(this.getTimeMillis());
        }

        /**
         * ** Sets the current time of this instance
         * ** @param d  The String representation of a date/time
         * ** @throws DateParseException if an invalid date/time format was specified.
         **/
        public void setDate(String d)
                throws DateParseException {
            this.setDate(d, (TimeZone) null);
        }

        /**
         * ** Sets the current date of this instance (time is left unchanged)
         * ** @param tz      The TimeZone
         * ** @param year    The year
         * ** @param month1  The 1-based month index [1..12]
         * ** @param day     The day
         **/
        public void setDate(TimeZone tz, int year, int month1, int day) {
            int hour24 = this.getHour24(tz);
            int minute = this.getMinute(tz);
            int second = this.getSecond(tz);
            this.setDate(tz, year, month1, day, hour24, minute, second);
        }

        /**
         * ** Sets the current time of this instance
         * ** @param tz      The TimeZone
         * ** @param year    The year (1970+)
         * ** @param month1  The 1-based month index [1..12]
         * ** @param day     The day of the month
         * ** @param hour24  The hour of the day [24 hour clock]
         * ** @param minute  The minute of the hour
         * ** @param second  The second of the minute
         **/
        public void setDate(TimeZone tz, int year, int month1, int day, int hour24, int minute, int second) {
            int month0 = month1 - 1;

        /* quick validate year */
            if (year < 0) {
                year = 1970;
            }

        /* quick validate month */
            if (month0 < 0) {
                month0 = 0;
            }

        /* quick validate day */
            if (day < 1) {
                day = 1;
            }

        /* quick validate time */
            if (hour24 < 0) {
                hour24 = 0;
                minute = 0;
                second = 0;
            } else if (minute < 0) {
                minute = 0;
                second = 0;
            } else if (second < 0) {
                second = 0;
            }

        /* set */
            this.setTimeZone(tz);
            Calendar cal = new GregorianCalendar(this._timeZone(tz));
            cal.set(year, month0, day, hour24, minute, second);
            Date date = cal.getTime();
            this.setTimeMillis(date.getTime());

        }

        /**
         * ** Sets the current time of this instance
         * ** @param d  The String representation of a date/time
         * ** @param dftTMZ The TimeZone used if no timezone was parsed from the String
         * ** @throws DateParseException if an invalid date/time format was specified.
         **/
        public void setDate(String d, TimeZone dftTMZ)
                throws DateParseException {
            String ds[] = StringTools.parseString(d, " _");
            String dt = (ds.length > 0) ? ds[0] : "";
            String tm = (ds.length > 1) ? ds[1] : "";
            String tz = (ds.length > 2) ? ds[2] : "";
            // Valid format: "YYYY/MM/DD [HH:MM:SS] [PST]"
            //Print.logInfo("Parsing " + dt + " " + tm + " " + tz);

        /* time-zone */
            TimeZone timeZone = null;
            if (ds.length > 2) {
                if (tz.equals("+0000") || tz.equals("-0000")) {
                    timeZone = DateTime.getGMTTimeZone();
                } else if (DateTime.isValidTimeZone(tz)) {
                    timeZone = DateTime.getTimeZone(tz);
                } else {
                    throw new DateParseException("Invalid TimeZone[#1]: " + tz);
                }
            } else if ((ds.length > 1) && DateTime.isValidTimeZone(tm)) {
                tz = tm;
                tm = "";
                timeZone = DateTime.getTimeZone(tz);
            } else {
                timeZone = (dftTMZ != null) ? dftTMZ : this.getTimeZone();
            }
            //Print.logInfo("Timezone = " + timeZone);
            this.setTimeZone(timeZone);
            Calendar calendar = new GregorianCalendar(timeZone);

        /* date */
            int yr = -1, mo = -1, dy = -1;
            int d1 = dt.indexOf('/'), d2 = (d1 > 0) ? dt.indexOf('/', d1 + 1) : -1;
            if ((d1 > 0) && (d2 > d1)) {

            /* year */
                String YR = dt.substring(0, d1);
                yr = StringTools.parseInt(YR, -1);
                if ((yr >= 0) && (yr <= 49)) {
                    yr += 2000;
                } else if ((yr >= 50) && (yr <= 99)) {
                    yr += 1900;
                }
                if ((yr < 1900) || (yr > 2100)) {
                    throw new DateParseException("Date/Year out of range: " + YR);
                }

            /* month */
                String MO = dt.substring(d1 + 1, d2);
                mo = StringTools.parseInt(MO, -1) - 1; // 0 indexed
                if ((mo < 0) || (mo > 11)) {
                    throw new DateParseException("Date/Month out of range: " + MO);
                }

            /* seconds */
                String DY = dt.substring(d2 + 1);
                dy = StringTools.parseInt(DY, -1);
                if ((dy < 1) || (dy > 31)) {
                    throw new DateParseException("Date/Day out of range: " + DY);
                }

            } else {

                throw new DateParseException("Invalid date format (Y/M/D): " + dt);

            }

        /* time */
            if (tm.equals("")) {

                //Print.logInfo("Just using YMD:" + yr + "/" + mo + "/" + dy);
                calendar.set(yr, mo, dy);

            } else {

                int hr = -1, mn = -1, sc = -1;
                int t1 = tm.indexOf(':'), t2 = (t1 > 0) ? tm.indexOf(':', t1 + 1) : -1;
                if (t1 > 0) {

                /* hour */
                    String HR = tm.substring(0, t1);
                    hr = StringTools.parseInt(HR, -1);
                    if ((hr < 0) || (hr > 23)) {
                        throw new DateParseException("Time/Hour out of range: " + HR);
                    }
                    if (t2 > t1) {

                    /* minute */
                        String MN = tm.substring(t1 + 1, t2);
                        mn = StringTools.parseInt(MN, -1);
                        if ((mn < 0) || (mn > 59)) {
                            throw new DateParseException("Time/Minute out of range: " + MN);
                        }

                    /* second */
                        String SC = tm.substring(t2 + 1);
                        sc = StringTools.parseInt(SC, -1);
                        if ((sc < 0) || (sc > 59)) {
                            throw new DateParseException("Time/Second out of range: " + SC);
                        }

                        //Print.logInfo("Setting YMDHMS:" + yr + "/" + mo + "/" + dy+ " " + hr + ":" + mn + ":" + sc);
                        calendar.set(yr, mo, dy, hr, mn, sc);

                    } else {

                    /* minute */
                        String MN = tm.substring(t1 + 1);
                        mn = StringTools.parseInt(MN, -1);
                        if ((mn < 0) || (mn > 59)) {
                            throw new DateParseException("Time/Minute out of range: " + MN);
                        }

                        //Print.logInfo("Setting YMDHM:" + yr + "/" + mo + "/" + dy+ " " + hr + ":" + mn);
                        calendar.set(yr, mo, dy, hr, mn);

                    }
                } else {

                    throw new DateParseException("Invalid time format (H:M:S): " + tm);

                }
            }

        /* ok */
            this.setTimeMillis(calendar.getTime().getTime());

        }


        /**
         * ** Gets a value from a GregorianCalendar based on the Epoch time of this instance
         * ** @param tz    The TimeZone
         * ** @param value The value number to return
         **/
        private int _get(TimeZone tz, int value) {
            return this.getCalendar(tz).get(value);
        }

        /**
         * ** Gets a GregorianCalendar calendar instance
         * ** @param tz  The TimeZone
         * ** @return The Calendar object
         **/
        public Calendar getCalendar(TimeZone tz) {
            Calendar c = new GregorianCalendar(this._timeZone(tz));
            c.setTimeInMillis(this.getTimeMillis());
            return c;
        }

        /**
         * ** Gets a GregorianCalendar calendar instance
         * ** @return The Calendar object
         **/
        public Calendar getCalendar() {
            return this.getCalendar(null);
        }

        /**
         * ** Gets the 0-based index of the month represented by this DateTime instance
         * ** @param tz  The TimeZone used when calculating the 0-based month index
         * ** @return The 0-based month index
         **/
        public int getMonth0(TimeZone tz) {
            // return 0..11
            return this._get(tz, Calendar.MONTH); // 0 indexed
        }

        /**
         * ** Gets the 0-based index of the month represented by this DateTime instance
         * ** @return The 0-based month index
         **/
        public int getMonth0() {
            return this.getMonth0(null);
        }

        /**
         * ** Gets the 1-based index of the month represented by this DateTime instance
         * ** @param tz  The TimeZone used when calculating the 0-based month index
         * ** @return The 1-based month index
         **/
        public int getMonth1(TimeZone tz) {
            // return 1..12
            return this.getMonth0(tz) + 1;
        }

        /**
         * ** Gets the 1-based index of the month represented by this DateTime instance
         * ** @return The 1-based month index
         **/
        public int getMonth1() {
            return this.getMonth1(null);
        }

        /**
         * ** Gets the day of the month represented by this DateTime instance
         * ** @param tz  The TimeZone used when calculating the day of the month
         * ** @return The day of the month
         **/
        public int getDayOfMonth(TimeZone tz) {
            // return 1..31
            return this._get(tz, Calendar.DAY_OF_MONTH);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Gets the day of the month represented by this DateTime instance
         * ** @return The day of the month
         **/
        public int getDayOfMonth() {
            return this.getDayOfMonth(null);
        }

        /**
         * ** Gets the number of days in the month represented by this DateTime instance
         * ** @param tz  The TimeZone used when calculating the number of days in the month
         * ** @return The number of days in the month
         **/
        public int getDaysInMonth(TimeZone tz) {
            return DateTime.getDaysInMonth(tz, this.getMonth1(tz), this.getYear(tz));
        }

        // ------------------------------------------------------------------------

        /**
         * ** Gets the number of days in the month represented by this DateTime instance
         * ** @return The number of days in the month
         **/
        public int getDaysInMonth() {
            return this.getDaysInMonth(null);
        }

        /**
         * ** Returns the day of week for this DateTime instance
         * ** @param tz  The TimeZone used when calculating the day of week
         * ** @return The day of week (0=Sunday ... 6=Saturday)
         **/
        public int getDayOfWeek(TimeZone tz) {
            // return 0..6
            return this._get(tz, Calendar.DAY_OF_WEEK) - Calendar.SUNDAY;
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns the day of week for this DateTime instance
         * ** @return The day of week (0=Sunday ... 6=Saturday)
         **/
        public int getDayOfWeek() {
            return this.getDayOfWeek(null);
        }

        /**
         * ** Returns the year for this DataTime instance
         * ** @param tz  The TimeZone used when calculating the year
         * ** @return The year.
         **/
        public int getYear(TimeZone tz) {
            return this._get(tz, Calendar.YEAR);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns the year for this DataTime instance
         * ** @return The year.
         **/
        public int getYear() {
            return this.getYear(null);
        }

        /**
         * ** Returns true if this DateTime instance represents a leap-year
         * ** @param tz  The TimeZone used when calculating the leap-year
         * ** @return True if the year is a leap-year, false otherwise
         **/
        public boolean isLeapYear(TimeZone tz) {
            GregorianCalendar gc = (GregorianCalendar) this.getCalendar(tz);
            return gc.isLeapYear(gc.get(Calendar.YEAR));
        }

        /**
         * ** Returns true if this DateTime instance represents a leap-year
         * ** @return True if the year is a leap-year, false otherwise
         **/
        public boolean isLeapYear() {
            return this.isLeapYear(null);
        }

        /**
         * ** Returns the hour of the day (24-hour clock)
         * ** @param tz  The TimeZone used when calculating the hour of the day
         * ** @return The hour of the day
         **/
        public int getHour24(TimeZone tz) {
            return this._get(tz, Calendar.HOUR_OF_DAY);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns the hour of the day (24-hour clock)
         * ** @return The hour of the day
         **/
        public int getHour24() {
            return this.getHour24(null);
        }

        /**
         * ** Returns the hour of the day (12-hour clock)
         * ** @param tz  The TimeZone used when calculating the hour of the day
         * ** @return The hour of the day
         **/
        public int getHour12(TimeZone tz) {
            return this._get(tz, Calendar.HOUR);
        }

        /**
         * ** Returns the hour of the day (12-hour clock)
         * ** @return The hour of the day
         **/
        public int getHour12() {
            return this.getHour12(null);
        }

        /**
         * ** Returns true if AM, false if PM
         * ** @param tz  The TimeZone used when calculating AM/PM
         * ** @return True if AM, false if PM
         **/
        public boolean isAM(TimeZone tz) {
            return this._get(tz, Calendar.AM_PM) == Calendar.AM;
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns true if AM, false if PM
         * ** @return True if AM, false if PM
         **/
        public boolean isAM() {
            return this.isAM(null);
        }

        /**
         * ** Returns true if PM, false if AM
         * ** @param tz  The TimeZone used when calculating AM/PM
         * ** @return True if PM, false if AM
         **/
        public boolean isPM(TimeZone tz) {
            return this._get(tz, Calendar.AM_PM) == Calendar.PM;
        }

        /**
         * ** Returns true if PM, false if AM
         * ** @return True if PM, false if AM
         **/
        public boolean isPM() {
            return this.isPM(null);
        }

        /**
         * ** Returns the minute of the hour
         * ** @param tz  The TimeZone used when calculating the minute
         * ** @return The minute of the hour
         **/
        public int getMinute(TimeZone tz) {
            return this._get(tz, Calendar.MINUTE);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns the minute of the hour
         * ** @return The minute of the hour
         **/
        public int getMinute() {
            return this.getMinute(null);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns the second of the minute
         * ** @param tz  The TimeZone used when calculating the second
         * ** @return The second of the minute
         **/
        public int getSecond(TimeZone tz) {
            return this._get(tz, Calendar.SECOND);
        }

        /**
         * ** Returns the second of the minute
         * ** @return The second of the minute
         **/
        public int getSecond() {
            return this.getSecond(null);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns true if this DateTime instance currently represents a daylight savings time.
         * ** @param tz  The TimeZone used when calculating daylight savings time.
         * ** @return True if this DateTime instance currently represents a daylight savings time.
         **/
        public boolean isDaylightSavings(TimeZone tz) {
            return _timeZone(tz).inDaylightTime(this.getDate());
        }

        /**
         * ** Returns true if this DateTime instance currently represents a daylight savings time.
         * ** @return True if this DateTime instance currently represents a daylight savings time.
         **/
        public boolean isDaylightSavings() {
            return this.isDaylightSavings(null);
        }

        /**
         * ** Returns the DayNumber long, for this DateTime instance
         * ** @param tz the Timezone
         * ** @return The long day number
         **/
        public long getDayNumber(TimeZone tz) {
            return DateTime.getDayNumberFromDate(this, tz);
        }

        /**
         * ** Returns the DayNumber long, for this DateTime instance
         * ** @param tz the Timezone
         * ** @return The long day number
         **/
        public long getDayNumber() {
            return this.getDayNumber(null);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Gets the Epoch time in seconds represented by this instance
         * ** @return The Epoch time in seconds
         **/
        public long getTimeSec() {
            return this.getTimeMillis() / 1000L;
        }

        /**
         * ** Sets the Epoch time in seconds represented by this instance
         * ** @param timeSec  Epoch time in seconds
         **/
        public void setTimeSec(long timeSec) {
            this.timeMillis = timeSec * 1000L;
        }

        /**
         * ** Gets the Epoch time in milliseconds represented by this instance
         * ** @return The Epoch time in milliseconds
         **/
        public long getTimeMillis() {
            return this.timeMillis;
        }

        /**
         * ** Sets the Epoch time in milliseconds represented by this instance
         * ** @param timeMillis  Epoch time in milliseconds
         **/
        public void setTimeMillis(long timeMillis) {
            this.timeMillis = timeMillis;
        }

        /**
         * ** Returns an Epoch time in seconds which is the beginning of the day
         * ** represented by this instance.
         * ** @param tz  The TimeZone
         * ** @return An Epoch time which is at the beginning of the day represented
         * **         by this instance.
         **/
        public long getDayStart(TimeZone tz) {
            if (tz == null) {
                tz = _timeZone(tz);
            }
            Calendar c = this.getCalendar(tz);
            Calendar nc = new GregorianCalendar(tz);
            nc.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
            return nc.getTime().getTime() / 1000L;
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns an Epoch time in seconds which is the beginning of the day
         * ** represented by this instance.
         * ** @return An Epoch time which is at the beginning of the day represented
         * **         by this instance.
         **/
        public long getDayStart() {
            return this.getDayStart(null);
        }

        /**
         * ** Returns an Epoch time in seconds which is the end of the day
         * ** represented by this instance.
         * ** @param tz  The TimeZone
         * ** @return An Epoch time which is at the end of the day represented
         * **         by this instance.
         **/
        public long getDayEnd(TimeZone tz) {
            if (tz == null) {
                tz = _timeZone(tz);
            }
            Calendar c = this.getCalendar(tz);
            Calendar nc = new GregorianCalendar(tz);
            nc.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH), 23, 59, 59);
            return nc.getTime().getTime() / 1000L;
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns an Epoch time in seconds which is the end of the day
         * ** represented by this instance.
         * ** @return An Epoch time which is at the end of the day represented
         * **         by this instance.
         **/
        public long getDayEnd() {
            return this.getDayEnd(null);
        }

        /**
         * ** Returns the Epoch timestamp representing the beginning of the month
         * ** represented by this DateTime instance, plus the specified delta month offset.
         * ** @param tz  The overriding TimeZone
         * ** @param deltaMo  The delta monnths (added to the month represented by this DateTime instance)
         * ** @return The Epoch timestamp
         **/
        public long getMonthStart(TimeZone tz, int deltaMo) {
            if (tz == null) {
                tz = _timeZone(tz);
            }
            Calendar c = this.getCalendar(tz);
            int YY = c.get(Calendar.YEAR);
            int MM = c.get(Calendar.MONTH); // 0..11
            if (deltaMo != 0) {
                MM += deltaMo;
                if (MM < 0) {
                    YY += (MM - 11) / 12;
                    MM %= 12;
                    if (MM < 0) {
                        MM += 12;
                    }
                } else {
                    YY += MM / 12;
                    MM %= 12;
                }
            }
            int DD = 1;
            Calendar nc = new GregorianCalendar(tz);
            nc.set(YY, MM, DD, 0, 0, 0);
            return nc.getTime().getTime() / 1000L;
        }

        /**
         * ** Returns the Epoch timestamp representing the beginning of the month
         * ** represented by this DateTime instance.
         * ** @param tz  The overriding TimeZone
         * ** @return The Epoch timestamp
         **/
        public long getMonthStart(TimeZone tz) {
            return this.getMonthStart(tz, 0);
        }

        /**
         * ** Returns the Epoch timestamp representing the beginning of the month
         * ** represented by this DateTime instance, plus the specified delta month offset.
         * ** @param deltaMo  The delta monnths (added to the month represented by this DateTime instance)
         * ** @return The Epoch timestamp
         **/
        public long getMonthStart(int deltaMo) {
            return this.getMonthStart(null, deltaMo);
        }

        /**
         * ** Returns the Epoch timestamp representing the beginning of the month
         * ** represented by this DateTime instance.
         * ** @return The Epoch timestamp
         **/
        public long getMonthStart() {
            return this.getMonthStart(null, 0);
        }

        /**
         * ** Returns the Epoch timestamp representing the end of the month
         * ** represented by this DateTime instance, plus the specified delta month offset.
         * ** @param tz  The overriding TimeZone
         * ** @param deltaMo  The delta monnths (added to the month represented by this DateTime instance)
         * ** @return The Epoch timestamp
         **/
        public long getMonthEnd(TimeZone tz, int deltaMo) {
            if (tz == null) {
                tz = _timeZone(tz);
            }
            Calendar c = this.getCalendar(tz);
            int YY = c.get(Calendar.YEAR);
            int MM = c.get(Calendar.MONTH); // 0..11
            if (deltaMo != 0) {
                MM += deltaMo;
                if (MM < 0) {
                    YY += (MM - 11) / 12;
                    MM %= 12;
                    if (MM < 0) {
                        MM += 12;
                    }
                } else {
                    YY += MM / 12;
                    MM %= 12;
                }
            }
            int DD = DateTime.getDaysInMonth(tz, MM + 1, YY);
            Calendar nc = new GregorianCalendar(tz);
            nc.set(YY, MM, DD, 23, 59, 59);
            return nc.getTime().getTime() / 1000L;
        }

        /**
         * ** Returns the Epoch timestamp representing the end of the month
         * ** represented by this DateTime instance.
         * ** @param tz  The overriding TimeZone
         * ** @return The Epoch timestamp
         **/
        public long getMonthEnd(TimeZone tz) {
            return this.getMonthEnd(tz, 0);
        }

        /**
         * ** Returns the Epoch timestamp representing the end of the month
         * ** represented by this DateTime instance, plus the specified delta month offset.
         * ** @param deltaMo  The delta monnths (added to the month represented by this DateTime instance)
         * ** @return The Epoch timestamp
         **/
        public long getMonthEnd(int deltaMo) {
            return this.getMonthEnd(null, deltaMo);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns the Epoch timestamp representing the end of the month
         * ** represented by this DateTime instance.
         * ** @return The Epoch timestamp
         **/
        public long getMonthEnd() {
            return this.getMonthEnd(null, 0);
        }

        /**
         * ** Returns the Epoch timestamp representing the current date, plus the number of
         * ** months offset.
         * ** @param tz       The overriding TimeZone
         * ** @param deltaMo  The delta monnths (added to the month represented by this DateTime instance)
         * ** @return The Epoch timestamp
         **/
        public long getMonthDelta(TimeZone tz, int deltaMo) {
            if (tz == null) {
                tz = _timeZone(tz);
            }
            Calendar c = this.getCalendar(tz);
            int YY = c.get(Calendar.YEAR);
            int MM = c.get(Calendar.MONTH); // 0..11
            if (deltaMo != 0) {
                MM += deltaMo;
                if (MM < 0) {
                    YY += (MM - 11) / 12;
                    MM %= 12;
                    if (MM < 0) {
                        MM += 12;
                    }
                } else {
                    YY += MM / 12;
                    MM %= 12;
                }
            }
            int DD = c.get(Calendar.DAY_OF_MONTH); // 1..31
            int maxDays = DateTime.getDaysInMonth(tz, MM + 1, YY);
            if (DD > maxDays) {
                DD = maxDays;
            }
            Calendar nc = new GregorianCalendar(tz);
            if (deltaMo <= 0) {
                nc.set(YY, MM, DD, 0, 0, 0);
            } else {
                nc.set(YY, MM, DD, 23, 59, 59);
            }
            return nc.getTime().getTime() / 1000L;
        }

        /**
         * ** Returns the Epoch timestamp representing the start of the current day represented
         * ** by this DateTime instance, based on the GMT TimeZone.
         * ** @return The Epoch timestamp
         **/
        public long getDayStartGMT() {
            // GMT TimeZone
            return (this.getTimeSec() / SECONDS_PER_DAY) * SECONDS_PER_DAY;
        }

        /**
         * ** Returns the Epoch timestamp representing the end of the current day represented
         * ** by this DateTime instance, based on the GMT TimeZone.
         * ** @return The Epoch timestamp
         **/
        public long getDayEndGMT() {
            // GMT TimeZone
            return this.getDayStartGMT() + SECONDS_PER_DAY - 1L;
        }

        /**
         * ** Returns true if the specified DateTime is <b>after</b> this DateTime instance
         * ** @param dt the other DateTime instance
         * ** @return True if the specified DateTime is <b>after</b> this DateTime instance
         **/
        public boolean after(DateTime dt) {
            return this.after(dt, false);
        }

        /**
         * ** Returns true if the specified DateTime is <b>after</b> this DateTime instance
         * ** @param dt the other DateTime instance
         * ** @param inclusive  True to test for "equal-to or after", false to test only for "after".
         * ** @return True if the specified DateTime is after this DateTime instance (or "equal-to" if
         * **         'inclusive' is true).
         **/
        public boolean after(DateTime dt, boolean inclusive) {
            if (dt == null) {
                return true; // arbitrary
            } else if (inclusive) {
                return (this.getTimeMillis() >= dt.getTimeMillis());
            } else {
                return (this.getTimeMillis() > dt.getTimeMillis());
            }
        }

        /**
         * ** Returns true if the specified DateTime is <b>before</b> this DateTime instance
         * ** @param dt the other DateTime instance
         * ** @return True if the specified DateTime is <b>before</b> this DateTime instance
         **/
        public boolean before(DateTime dt) {
            return this.before(dt, false);
        }

        /**
         * ** Returns true if the specified DateTime is <b>before</b> this DateTime instance
         * ** @param dt the other DateTime instance
         * ** @param inclusive  True to test for "equal-to or before", false to test only for "before".
         * ** @return True if the specified DateTime is before this DateTime instance (or "equal-to" if
         * **         'inclusive' is true).
         **/
        public boolean before(DateTime dt, boolean inclusive) {
            if (dt == null) {
                return false; // arbitrary
            } else if (inclusive) {
                return (this.getTimeMillis() <= dt.getTimeMillis());
            } else {
                return (this.getTimeMillis() < dt.getTimeMillis());
            }
        }

        /**
         * ** Return the number of years since specified date (year/month/day).
         * ** @param priorDate  Prior date
         * ** @return Number of years (and fractional years)
         **/
        public double getYearsSince(DateTime priorDate) {
            return this.getYearsSince(priorDate, null);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Return the number of years since specified date (year/month/day).
         * ** @param priorDate  Prior date
         * ** @return Number of years (and fractional years)
         **/
        public double getYearsSince(DateTime priorDate, TimeZone tz) {
            long priorDayNumber = DateTime.getDayNumberFromDate(priorDate, tz);
            return this.getYearsSince(priorDayNumber);
        }

        /**
         * ** Return the number of years since specified date (year/month/day).
         * ** @param year   Prior year
         * ** @param month1 Month of prior year (1..12)
         * ** @param day    Day of month
         * ** @return Number of years (and fractional years)
         **/
        public double getYearsSince(int year, int month1, int day) {
            long priorDayNumber = DateTime.getDayNumberFromDate(year, month1, day);
            return this.getYearsSince(priorDayNumber, null);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Return the number of years since specified date (year/month/day).
         * ** @param priorDayNumber  Prior day-number
         * ** @return Number of years (and fractional years)
         **/
        public double getYearsSince(long priorDayNumber) {
            return this.getYearsSince(priorDayNumber, null);
        }

        /**
         * ** Return the number of years since specified date (year/month/day).
         * ** @param priorDayNumber  Prior day-number
         * ** @param tz              TimeZone
         * ** @return Number of years (and fractional years)
         **/
        public double getYearsSince(long priorDayNumber, TimeZone tz) {

        /* get day numbers */
            boolean reverse = false;
            long thisDayNumber = DateTime.getDayNumberFromDate(this, tz);
            long thatDayNumber = priorDayNumber;
            if (thisDayNumber == thatDayNumber) {
                return 0.0;
            } else if (thisDayNumber < thatDayNumber) {
                long x = thisDayNumber;
                thisDayNumber = thatDayNumber;
                thatDayNumber = x;
                reverse = true;
            }
            ParsedDateTime thisPDT = DateTime.getDateFromDayNumber(thisDayNumber, tz);
            ParsedDateTime thatPDT = DateTime.getDateFromDayNumber(thatDayNumber, tz);

        /* days in year */
            int daysInYear = 365;
            int deltaYear = thisPDT.year - thatPDT.year;
            int thisDOY = DateTime.getDayOfYear(thisPDT.year, thisPDT.month1, thisPDT.day);
            int thatDOY = DateTime.getDayOfYear(thatPDT.year, thatPDT.month1, thatPDT.day);
            boolean thisLeap = DateTime.isLeapYear(thisPDT.year);
            boolean thatLeap = DateTime.isLeapYear(thatPDT.year);
            if (thisLeap && thatLeap) {
                daysInYear = 366; // 366 days in both years
            } else if (thisLeap && !thatLeap) {
                if (thatDOY > (31 + 28)) { // Jan + Feb
                    thatDOY++; // adjust non-leap year
                    daysInYear = 366;
                }
            } else if (!thisLeap && thatLeap) {
                if (thisDOY > (31 + 28)) { // Jan + Feb
                    thisDOY++; // adjust non-leap year
                    daysInYear = 366;
                }
            }

        /* age in years */
            double ageYears = (thisDOY >= thatDOY) ?
                    ((double) deltaYear + ((double) (thisDOY - thatDOY) / (double) daysInYear)) :
                    (((double) deltaYear - 1.0) + (((double) thisDOY + daysInYear) - (double) thatDOY) / (double) daysInYear);
            return reverse ? -ageYears : ageYears;

        }

        /**
         * ** Returns true if the specified DateTime is <b>equal-to</b> this DateTime instance
         * ** @param obj the other DateTime instance
         * ** @return True if the specified DateTime is <b>equal-to</b> this DateTime instance
         **/
        public boolean equals(Object obj) {
            if (obj instanceof DateTime) {
                return (this.getTimeMillis() == ((DateTime) obj).getTimeMillis());
            } else {
                return false;
            }
        }

        /**
         * ** Compares another DateTime instance to this instance.
         * ** @param other  The other DateTime instance.
         * ** @return &lt;0 of the other DateTime instance is before this instance, 0 if the other DateTime
         * **         instance is equal to this instance, and &gt;0 if the other DateTime instance is
         * **         after this instance.
         **/
        public int compareTo(Object other) {
            if (other instanceof DateTime) {
                long otherTime = ((DateTime) other).getTimeMillis();
                long thisTime = this.getTimeMillis();
                if (thisTime < otherTime) {
                    return -1;
                }
                if (thisTime > otherTime) {
                    return 1;
                }
                return 0;
            } else {
                return -1;
            }
        }

        /**
         * ** Returns a non-null TimeZone
         * ** @param tz The TimeZone returned (if non-null)
         * ** @return The specified TimeZone, or the default TimeZone if the specified TimeZone is null
         **/
        protected TimeZone _timeZone(TimeZone tz) {
            return (tz != null) ? tz : this.getTimeZone();
        }

        /**
         * ** Returns the default (or current) TimeZone
         * ** @return The default (or current) TimeZone
         **/
        public TimeZone getTimeZone() {
            return (this.timeZone != null) ? this.timeZone : DateTime.getDefaultTimeZone();
        }

        /**
         * ** Sets the current TimeZone
         * ** @param tz  The TimeZone ID to set
         **/
        public void setTimeZone(String tz) {
            this.setTimeZone(DateTime.getTimeZone(tz, null));
        }

        /**
         * ** Sets the current TimeZone
         * ** @param tz  The TimeZone to set
         **/
        public void setTimeZone(TimeZone tz) {
            this.timeZone = tz;
        }

        /**
         * ** Returns the ID of the current TimeZone
         * ** @return The ID of the current TimeZone
         **/
        public String getTimeZoneID() {
            return this.getTimeZone().getID();
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns the short-name of the current TimeZone
         * ** @return The short-name of the current TimeZone
         **/
        public String getTimeZoneShortName() {
            boolean dst = this.isDaylightSavings();
            return this.getTimeZone().getDisplayName(dst, TimeZone.SHORT);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Returns a String representation of this DateTime instance
         **/
        public String toString() {
            if (simpleFormatter == null) {
                // eg. "Sun Mar 26 12:38:12 PST 2006"
                simpleFormatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
            }
            synchronized (simpleFormatter) {
                simpleFormatter.setTimeZone(this.getTimeZone());
                return simpleFormatter.format(this.getDate());
            }
        }

        /**
         * ** Formats the current DateTime instance.
         * ** @param dtFmt  The Date/Time format
         * ** @param tz   The overriding TimeZone
         * ** @param sb   The StringBuffer where the formatted Date/Time is placed (may be null)
         * ** @return The String representation of the StringBuffer destination.
         **/
        public String format(String dtFmt, TimeZone tz, StringBuffer sb) {
            if (sb == null) {
                sb = new StringBuffer();
            }
            SimpleDateFormat sdf = null;
            try {
                String f = (dtFmt != null) ? dtFmt : DEFAULT_DATETIME_FORMAT;
                sdf = new SimpleDateFormat(f);
            } catch (IllegalArgumentException iae) {
                Log.error("Invalid date/time format: " + dtFmt + iae);
                sdf = new SimpleDateFormat(DEFAULT_DATETIME_FORMAT); // assumed to be valid
            }
            sdf.setTimeZone(this._timeZone(tz));
            sdf.format(this.getDate(), sb, new FieldPosition(0));
            return sb.toString();
        }

        /**
         * ** Formats the current DateTime instance.
         * ** @param fmt  The Date/Time format
         * ** @param tz   The overriding TimeZone
         * ** @return The formatted Date/Time String
         **/
        public String format(String fmt, TimeZone tz) {
            return this.format(fmt, tz, null);
        }

        // ------------------------------------------------------------------------

        /**
         * ** Formats the current DateTime instance (using format "MMM dd, yyyy HH:mm:ss z")
         * ** @param tz   The overriding TimeZone
         * ** @return The formatted Date/Time String
         **/
        public String shortFormat(TimeZone tz) {   // ie. "2003/10/23 7:23:18 PST"
            return this.format("yyyy/MM/dd HH:mm:ss zzz", tz, null);
        }

        /**
         * ** Formats the current DateTime instance (using format "MMM dd, yyyy HH:mm:ss z")
         * ** @param tz   The overriding TimeZone
         * ** @return The formatted Date/Time String
         **/
        public String format(TimeZone tz) {
            return this.format("MMM dd, yyyy HH:mm:ss z", tz, null);
            //DateFormat dateFmt = DateFormat.getDateTimeInstance();
            //dateFmt.setTimeZone(tz);
            //return dateFmt.format(new java.util.Date(this.getTimeMillis()));
        }

        /**
         * ** Formats the current DateTime instance.
         * ** @param fmt  The Date/Time format
         * ** @return The formatted Date/Time String
         **/
        public String format(String fmt) {
            return this.format(fmt, null, null);
        }

        /**
         * ** Formats the current DateTime instance, based on the GMT TimeZone.
         * ** @param fmt  The Date/Time format
         * ** @return The formatted Date/Time String
         **/
        public String gmtFormat(String fmt) {
            return this.format(fmt, DateTime.getGMTTimeZone(), null);
        }

        /**
         * ** Formats the current DateTime instance (using format "yyyy/MM/dd HH:mm:ss 'GMT'"), based on
         * ** the GMT TimeZone.
         * ** @return The formatted Date/Time String
         **/
        public String gmtFormat() {
            return this.gmtFormat("yyyy/MM/dd HH:mm:ss 'GMT'");
        }

        /**
         * ** Formats the current DateTime instance, based on the GMT TimeZone.
         * ** @param fmt  The Date/Time format
         * ** @param sb   The StringBuffer where the formatted Date/Time is placed (may be null)
         * ** @return The String representation of the StringBuffer destination.
         **/
        public String gmtFormat(String fmt, StringBuffer sb) {
            return this.format(fmt, DateTime.getGMTTimeZone(), sb);
        }

        /**
         * ** Returns a clone of this DateTime instance
         * ** @return A clone of this DateTime instance
         **/
        public Object clone() {
            return new DateTime(this);
        }

        /**
         * ** Enum indicating how to handle parsed dates when no time is specified
         **/
        public enum DefaultParsedTime {
            DayStart,
            DayEnd,
            CurrentTime
        }

        /**
         * ** TimeZoneProvider interface
         **/
        public interface TimeZoneProvider {
            public TimeZone getTimeZone();
        }

        /**
         * ** DateParseException class
         **/
        public static class DateParseException
                extends Exception {
            public DateParseException(String msg) {
                super(msg);
            }

            public DateParseException(String msg, Throwable cause) {
                super(msg, cause);
            }
        }

        /**
         * ** Class/Structure which holds date/time information
         **/
        public static class ParsedDateTime {
            public TimeZone timeZone = null;
            public long epoch = -1L;
            public int year = -1;
            public int month1 = -1;
            public int day = -1;
            public int hour24 = -1;
            public int minute = -1;
            public int second = -1;

            public ParsedDateTime(TimeZone tz, long epoch) {
                this.timeZone = tz;
                this.epoch = epoch;
            }

            public ParsedDateTime(TimeZone tz, int year, int month1, int day) {
                this.timeZone = tz;
                this.year = year;
                this.month1 = month1;
                this.day = day;
            }

            public ParsedDateTime(TimeZone tz, int year, int month1, int day, int hour, int minute, int second) {
                this(tz, year, month1, day);
                this.hour24 = hour;
                this.minute = minute;
                this.second = second;
            }

            public int getYear() {
                return this.year;
            }

            public int getMonth1() {
                return this.month1;
            }

            public int getDayOfMonth() {
                return this.day;
            }

            public int getHour24() {
                return this.hour24;
            }

            public int getMinute() {
                return this.minute;
            }

            public int getSecond() {
                return this.second;
            }

//            public String toString() {
//                StringBuffer sb = new StringBuffer();
//                if (this.epoch >= 0L) {
//                    sb.append(this.epoch);
//                } else {
//                    sb.append(StringTools.format(this.year, "%04d")).append("/");
//                    sb.append(StringTools.format(this.month1, "%02d")).append("/");
//                    sb.append(StringTools.format(this.day, "%02d"));
//                    if (this.hour24 >= 0) {
//                        sb.append(",");
//                        sb.append(StringTools.format(this.hour24, "%02d")).append(":");
//                        sb.append(StringTools.format(this.minute, "%02d"));
//                        if (this.second >= 0) {
//                            sb.append(":");
//                            sb.append(StringTools.format(this.second, "%02d"));
//                        }
//                    }
//                }
//                return sb.toString();
//            }

            public DateTime createDateTime(TimeZone tzone) {
                // only valid for years beyond 1970
                TimeZone tmz = (tzone != null) ? tzone : this.timeZone;
                if (this.epoch >= 0L) {
                    return new DateTime(this.epoch, tmz);
                } else {
                    return new DateTime(tmz, this.year, this.month1, this.day, this.hour24, this.minute, this.second);
                }
            }

            public DateTime createDateTime() {
                // only valid for years beyond 1970
                return this.createDateTime(null);
            }

            public long getEpochTime() {
                if (this.epoch >= 0L) {
                    return this.epoch;
                } else {
                    return this.createDateTime().getTimeSec();
                }
            }

            public long getDayNumber() {
                return DateTime.getDayNumberFromDate(this);
            }
        }


    }

}

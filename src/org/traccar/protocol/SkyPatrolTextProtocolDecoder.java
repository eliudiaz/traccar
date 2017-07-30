/*
 * Copyright 2012 - 2017 Anton Tananaev (anton@traccar.org)
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

import org.jboss.netty.channel.Channel;
import org.traccar.BaseProtocolDecoder;
import org.traccar.DeviceSession;
import org.traccar.helper.DateBuilder;
import org.traccar.helper.Log;
import org.traccar.helper.Parser;
import org.traccar.helper.PatternBuilder;
import org.traccar.model.Position;

import java.net.SocketAddress;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SkyPatrolTextProtocolDecoder extends BaseProtocolDecoder {

    private static final Pattern PATTERN_GPGGA = new PatternBuilder()
            .text("$GPGGA,")
            .number("(dd)(dd)(dd).?d*,")         // time (hhmmss)
            .number("(d+)(dd.d+),")              // latitude
            .expression("([NS]),")
            .number("(d+)(dd.d+),")              // longitude
            .expression("([EW]),")
            .any()
            .compile();

    public SkyPatrolTextProtocolDecoder(SkypatrolTextProtocol protocol) {
        super(protocol);
    }


    @Override
    protected Object decode(Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        final String input = msg.toString();
        Log.debug(">>> processing >> " + msg.toString());
        List<String> sanitizedInput = Stream.of(input.split(" "))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
        if (sanitizedInput.isEmpty() || sanitizedInput.size() <= 1) {
            return null;
        }
        String id = sanitizedInput.get(1);
        String sentence = sanitizedInput.stream()
                .filter(s -> s.startsWith("GPGGA"))
                .findFirst()
                .orElse(null);
        if (sentence != null) {
            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, id);
            return parseGpgga(deviceSession, "$".concat(sentence));
        }
        return null;
    }

    private Position parseGpgga(DeviceSession deviceSession, String sentence) {
        Parser parser = new Parser(PATTERN_GPGGA, sentence);
        if (!parser.matches()) {
            return null;
        }

        Position position = new Position();
        position.setProtocol(getProtocolName());
        position.setDeviceId(deviceSession.getDeviceId());

        DateBuilder dateBuilder = new DateBuilder()
                .setCurrentDate()
                .setTime(parser.nextInt(0), parser.nextInt(0), parser.nextInt(0));
        position.setTime(dateBuilder.getDate());

        position.setValid(true);
        position.setLatitude(parser.nextCoordinate());
        position.setLongitude(parser.nextCoordinate());

        return position;
    }

}

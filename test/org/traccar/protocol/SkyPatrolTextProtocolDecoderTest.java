package org.traccar.protocol;

import org.junit.Test;
import org.traccar.ProtocolTest;

public class SkyPatrolTextProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        SkyPatrolTextProtocolDecoder decoder = new SkyPatrolTextProtocolDecoder(new SkypatrolTextProtocol());
        verifyNull(decoder, text(
                "086415031C20"));
        verifyNull(decoder, text(
                "358244017671308"));
        verifyPosition(decoder, text(
                "         29      863286020431558      75  14 GPGGA,045039.00,1436.16226,N,09032.78579,W,1,03,13.24,1525.8,M,-4.8,M,,*55 GPRMC,045037.00,9,1436.16226,N,09032.78579,W,0.000,0.0,280717,,,A*4F  28    20942395  4257"));

    }

}

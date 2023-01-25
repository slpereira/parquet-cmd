package com.silvio.log.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ApacheAccessLogTest {

    @Test
    void testParser() {

        var sample = "37.129.59.160 - - [26/Jan/2019:20:29:13 +0330] \"GET /basket/view HTTP/1.1\" 200 17299 \"https://www-zanbil-ir.cdn.ampproject.org/v/s/www.zanbil.ir/m/product/32148/%DA%AF%D9%88%D8%B4%DB%8C-%D8%AA%D9%84%D9%81%D9%86-%D8%A8%DB%8C-%D8%B3%DB%8C%D9%85-%D9%BE%D8%A7%D9%86%D8%A7%D8%B3%D9%88%D9%86%DB%8C%DA%A9-%D9%85%D8%AF%D9%84-Panasonic-Cordless-Telephone-KX-TGC412?amp_js_v=0.1&usqp=mq331AQECAEoAQ%3D%3D\" \"Mozilla/5.0 (Linux; Android 6.0.1; D6633 Build/23.5.A.1.291) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Mobile Safari/537.36\" \"-\"\n";

        var log = ApacheAccessLog.parse(sample);
        // validate all log fields
        Assertions.assertEquals(log.getIpAddress(), "37.129.59.160");
        Assertions.assertEquals(log.getUserId(), "-");
        Assertions.assertEquals(log.getDateTimeString(), "26/Jan/2019:20:29:13 +0330");
        Assertions.assertEquals(log.getRequest(), "GET /basket/view HTTP/1.1");
        Assertions.assertEquals(log.getResponse(), "200");
        Assertions.assertEquals(log.getBytesSent(), "17299");
        Assertions.assertEquals(log.getReferer(), "https://www-zanbil-ir.cdn.ampproject.org/v/s/www.zanbil.ir/m/product/32148/%DA%AF%D9%88%D8%B4%DB%8C-%D8%AA%D9%84%D9%81%D9%86-%D8%A8%DB%8C-%D8%B3%DB%8C%D9%85-%D9%BE%D8%A7%D9%86%D8%A7%D8%B3%D9%88%D9%86%DB%8C%DA%A9-%D9%85%D8%AF%D9%84-Panasonic-Cordless-Telephone-KX-TGC412?amp_js_v=0.1&usqp=mq331AQECAEoAQ%3D%3D");
        Assertions.assertEquals(log.getUserAgent(), "Mozilla/5.0 (Linux; Android 6.0.1; D6633 Build/23.5.A.1.291) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Mobile Safari/537.36");

    }

}
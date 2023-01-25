package com.silvio.log.transformer;

import com.silvio.log.model.ApacheAccessLog;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ApacheBufferTransformTest {



    final String sampleIncomplete = """
54.36.149.41 - - [22/Jan/2019:03:56:14 +0330] "GET /filter/27|13%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,27|%DA%A9%D9%85%D8%AA%D8%B1%20%D8%A7%D8%B2%205%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,p53 HTTP/1.1" 200 30577 "-" "Mozilla/5.0 (compatible; AhrefsBot/6.1; +http://ahrefs.com/robot/)" "-"
31.56.96.51 - - [22/Jan/2019:03:56:16 +0330] "GET /image/60844/productModel/200x200 HTTP/1.1" 200 5667 "https://www.zanbil.ir/m/filter/b113" "Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36" "-"
31.56.96.51 - - [22/Jan/2019:03:56:16 +0330] "GET /image/61474/productModel/200x200 HTTP/1.1" 200 5379 "https://www.zanbil.ir/m/filter/b113" "Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36" "-"
40.77.167.129 - - [22/Jan/2019:03:56:17 +0330] "GET /image/14925/productModel/100x100 HTTP/1.1" 200 1696 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
91.99.72.15 - - [22/Jan/2019:03:56:17 +0330] "GET /product/31893/62100/%D8%B3%D8%B4%D9%88%D8%A7%D8%B1-%D8%AE%D8%A7%D9%86%DA%AF%DB%8C-%D9%BE%D8%B1%D9%86%D8%B3%D9%84%DB%8C-%D9%85%D8%AF%D9%84-PR257AT HTTP/1.1" 200 41483 "-" "Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:16.0)Gecko/16.0 Firefox/16.0" "-"
40.77.167.129 - - [22/Jan/2019:03:56:17 +0330] "GET /image/23488/productModel/150x150 HTTP/1.1" 200 2654 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
40.77.167.129 - - [22/Jan/2019:03:56:18 +0330] "GET /image/45437/productModel/150x150 HTTP/1.1" 200 3688 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
40.77.167.129 - - [22/Jan/2019:03:56:18 +0330] "GET /image/576/article/100x100 HTTP/1.1" 200 14776 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
66.249.66.194 - - [22/Jan/2019:03:56:18 +0330] "GET /filter/b41,b665,c150%7C%D8%A8%D8%AE%D8%A7%D8%B1%D9%BE%D8%B2,p56 HTTP/1.1" 200 34277 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)" "-"
40.77.167.129 - - [22/Jan/2019:03:56:18 +0330] "GET /image/57710/productModel/100x100 HTTP/1.1" 200 1695 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
207.46.13.136 - - [22/Jan/2019:03:56:18 +0330] "GET /product/10214 HTTP/1.1" 200 39677 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
40.77.167.129 - - [22/Jan/2019:03:56:19 +0330] "GET /image/578/article/100x100 HTTP/1.1" 200 9831 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
178.253.33.51 - - [22/Jan/2019:03:56:19 +0330] "GET /m/product/32574/62991/%D9%85%D8%A7%D8%B4%DB%8C%D9%86-%D8%A7%D8%B5%D9%84%D8%A7%D8%AD-%D8%B5%D9%88%D8%B1%D8%AA-%D9%BE%D8%B1%D9%86%D8%B3%D9%84%DB%8C-%D9%85%D8%AF%D9%84-PR465AT HTTP/1.1" 200 20406 "https://www.zanbil.ir/m/filter/p5767%2Ct156?name=%D9%85%D8%A7%D8%B4%DB%8C%D9%86-%D8%A7%D8%B5%D9%84%D8%A7%D8%AD&productType=electric-shavers" "Mozilla/5.0 (Linux; Android 5.1; HTC Desire 728 dual sim) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.83 Mobile Safari/537.36" "-"
40.77.167.129 - - [22/Jan/2019:03:56:19 +0330] "GET /image/6229/productModel/100x100 HTTP/1.1" 200 1796 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
91.99.72.15 - - [22/Jan/2019:03:56:19 +0330] "GET /product/10075/13903/%D9%85%D8%A7%DB%8C%DA%A9%D8%B1%D9%88%D9%81%D8%B1-%D8%B1%D9%88%D9%85%DB%8C%D8%B2%DB%8C-%D8%B3%D8%A7%D9%85%D8%B3%D9%88%D9%86%DA%AF-%D9%85%D8%AF%D9%84-CE288 HTTP/1.1" 200 41725 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.92 Safari/537.36" "-"
40.77.167.129 - - [22/Jan/2019:03:56:19 +0330] "GET /image/6229/productModel/150x150 HTTP/1.1" 200 2739 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
207.46.13.136 - - [22/Jan/2019:03:56:19 +0330] "GET /product/14926 HTTP/1.1" 404 33617 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
40.77.167.129 - - [22/Jan/2019:03:56:19 +0330] "GET /image/6248/productModel/150x150 HTTP/1.1" 200 2788 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
40.77.167.129 - - [22/Jan/2019:03:56:20 +0330] "GET /image/64815/productModel/150x150 HTTP/1.1" 200 3481 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
66.249.66.194 - - [22/Jan/2019:03:56:20 +0330] "GET /m/filter/b2,p6 HTTP/1.1" 200 19451 "-" "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)" "-"
91.99.72.15 - - [22/Jan/2019:03:56:20 +0330] "GET /product/32798/63266/%DB%8C%D8%AE%DA%86%D8%A7%D9%84-%D9%81%D8%B1%DB%8C%D8%B2%D8%B1-%D8%B3%DB%8C%D9%86%D8%AC%D8%B1-%D9%85%D8%AF%D9%84-pearl-SR7 HTTP/1.1" 200 40250 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.92 Safari/537.36" "-"
178.253.33.51 - - [22/Jan/2019:03:56:20 +0330] "GET /settings/logo HTTP/1.1" 200 4120 "https://www.zanbil.ir/m/product/32574/62991/%D9%85%D8%A7%D8%B4%DB%8C%D9%86-%D8%A7%D8%B5%D9%84%D8%A7%D8%AD-%D8%B5%D9%88%D8%B1%D8%AA-%D9%BE%D8%B1%D9%86%D8%B3%D9%84%DB%8C-%D9%85%D8%AF%D9%84-PR465AT" "Mozilla/5.0 (Linux; Android 5.1; HTC Desire 728 dual sim) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.83 Mobile Safari/537.36" "-"
66.249.66.91 - - [22/Jan/2019:03:56:20 +0330] "GET /filter/b874%2Cb32%2Cb63%2Cb99%2Cb126%2Cb820%2Cb249%2Cb3%2Cb148%2Cb724%2Cb613%2Cb183%2Cb213%2Cb484%2Cb224%2Cb734%2Cb20%2Cb95%2Cb542%2Cb212%2Cb485%2Cb523%2Cb221%2Cb118%2Cb186%2Cb67?page=4 HTTP/1.1" 200 39660 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)" "-"
31.56.96.51 - - [22/Jan/2019:03:56:20 +0330] "GET /image/60819/productModel/200x200 HTTP/1.1" 200 7171 "https://www.zanbil.ir/m/filter/b113" "Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36" "-"
31.56.96.51 - - [22/Jan/2019:03:56:20 +0330] "GET /image/60847/productModel/200x200 HTTP/1.1" 200 5667 "https://www.zanbil.ir/m/filter/b113" "Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36" "-"
178.253.33.51 - - [22/Jan/2019:03:56:21 +0330] "GET /image/32574?name=pr465at.jpg&wh=max HTTP/1.1" 200 28825 "https://www.zanbil.ir/m/product/32574/62991/%D9%85%D8%A7%D8%B4%DB%8C%D9%86-%D8%A7%D8%B5%D9%84%D8%A7%D8%AD-%D8%B5%D9%88%D8%B1%D8%AA-%D9%BE%D8%B1%D9%86%D8%B3%D9%84%DB%8C-%D9%85%D8%AF%D9%84-PR465AT" "Mozilla/5.0 (Linux; Android 5.1; HTC Desire 728 dual sim) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.83 Mobile Safari/537.36" "-"
5.78.198.52 - - [22/Jan/2019:03:56:21 +0330] "GET /m/product/33978/64784/%DA%AF%D9%88%D8%B4%DB%8C-%D9%85%D9%88%D8%A8%D8%A7%DB%8C%D9%84-%D8%B3%D8%A7%D9%85%D8%B3%D9%88%D9%86%DA%AF-%D9%85%D8%AF%D9%84-Galaxy-A9-%282018%29-Dual-128GB-%28SM-A920%29 HTTP/1.1" 200 21931 "https://www.zanbil.ir/m/browse/cell-phone/%DA%AF%D9%88%D8%B4%DB%8C-%D9%85%D9%88%D8%A8%D8%A7%DB%8C%D9%84" "Mozilla/5.0 (Linux; Android 8.0.0; SAMSUNG SM-G950F Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/8.2 Chrome/63.0.3239.111 Mobile Safari/537.36" "-"
178.253.33.51 - - [22/Jan/2019:03:56:21 +0330] "GET /image/32574?name=pr465at3.jpg&wh=max HTTP/1.1" 200 24001 "https://www.zanbil.ir/m/product/32574/62991/%D9%85%D8%A7%D8%B4%DB%8C%D9%86-%D8%A7%D8%B5%D9%84%D8%A7%D8%AD-%D8%B5%D9%88%D8%B1%D8%AA-%D9%BE%D8%B1%D9%86%D8%B3%D9%84%DB%8C-%D9%85%D8%AF%D9%84-PR465AT" "Mozilla/5.0 (Linux; Android 5.1; HTC Desire 728 dual sim) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.83 Mobile Safari/537.36" "-"
207.46.13.136 - - [22/Jan/2019:03:56:21 +0330] "GET /product/30649?model=60398 HTTP/1.1" 200 41198 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"
91.99.72.15 - - [22/Jan/2019:03:56:21 +0330] "GET /product/7793/9663/%D9%85%D8%A7%DB%8C%DA%A9%D8%B
""";

    final String secondPart = """
HTTP/1.1" 200 41198 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)" "-"            
178.253.33.51 - - [22/Jan/2019:03:56:21 +0330] "GET /image/32574?name=pr465at.jpg&wh=max HTTP/1.1" 200 28825 "https://www.zanbil.ir/m/product/32574/62991/%D9%85%D8%A7%D8%B4%DB%8C%D9%86-%D8%A7%D8%B5%D9%84%D8%A7%D8%AD-%D8%B5%D9%88%D8%B1%D8%AA-%D9%BE%D8%B1%D9%86%D8%B3%D9%84%DB%8C-%D9%85%D8%AF%D9%84-PR465AT" "Mozilla/5.0 (Linux; Android 5.1; HTC Desire 728 dual sim) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.83 Mobile Safari/537.36" "-"
5.78.198.52 - - [22/Jan/2019:03:56:21 +0330] "GET /m/product/33978/64784/%DA%AF%D9%88%D8%B4%DB%8C-%D9%85%D9%88%D8%A8%D8%A7%DB%8C%D9%84-%D8%B3%D8%A7%D9%85%D8%B3%D9%88%D9%86%DA%AF-%D9%85%D8%AF%D9%84-Galaxy-A9-%282018%29-Dual-128GB-%28SM-A920%29 HTTP/1.1" 200 21931 "https://www.zanbil.ir/m/browse/cell-phone/%DA%AF%D9%88%D8%B4%DB%8C-%D9%85%D9%88%D8%A8%D8%A7%DB%8C%D9%84" "Mozilla/5.0 (Linux; Android 8.0.0; SAMSUNG SM-G950F Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/8.2 Chrome/63.0.3239.111 Mobile Safari/537.36" "-"
""";
    @Test
    void fromBuffer() {
        new ApacheBufferTransform().apply(Buffer.buffer(sampleIncomplete)).subscribe().with(
                System.out::println,
                System.out::println,
                () -> System.out.println("Done")
        );
    }

    @Test
    void complementingBuffer() {
        var transform = new ApacheBufferTransform();
        Buffer[] buffers = {Buffer.buffer(sampleIncomplete), Buffer.buffer(secondPart)};
        var multi = Multi.createFrom().items(buffers);
        multi.onItem().transformToMultiAndConcatenate(transform).subscribe().with(
                System.out::println,
                System.out::println,
                () -> System.out.println("Done")
        );
    }

}
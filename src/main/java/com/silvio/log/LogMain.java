package com.silvio.log;

import com.silvio.log.reader.ApacheFastFileReader;
import com.silvio.log.reader.HdfsFastFileReader;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.nio.file.Path;

@QuarkusMain
public class LogMain implements QuarkusApplication {

    public static void main(String... args) {
        io.quarkus.runtime.Quarkus.run(LogMain.class, args);
    }

    @Inject
    HdfsFastFileReader fastFileReader;

    @Override
    public int run(String... args) throws Exception {
        fastFileReader.processHdfsLog(Path.of(args[0]), Path.of(args[1]));

        return 0;
    }

}

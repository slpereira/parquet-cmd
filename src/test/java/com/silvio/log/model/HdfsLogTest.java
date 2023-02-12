package com.silvio.log.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HdfsLogTest {

    final String lines = """
081109 205157 752 INFO dfs.DataNode$PacketResponder: Received block blk_9212264480425680329 of size 67108864 from /10.251.123.1
081109 205315 29 INFO dfs.FSNamesystem: BLOCK* NameSystem.allocateBlock: /user/root/rand/_temporary/_task_200811092030_0001_m_000742_0/part-00742. blk_-7878121102358435702
081109 205409 28 INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.111.130:50010 is added to blk_4568434182693165548 size 67108864
081109 205412 832 INFO dfs.DataNode$PacketResponder: Received block blk_-5704899712662113150 of size 67108864 from /10.251.91.229
081109 205632 28 INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.74.79:50010 is added to blk_-4794867979917102672 size 67108864
081109 205739 29 INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.38.197:50010 is added to blk_8763662564934652249 size 67108864
081109 205742 1001 INFO dfs.DataNode$PacketResponder: Received block blk_-5861636720645142679 of size 67108864 from /10.251.70.211
081109 205746 29 INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.74.134:50010 is added to blk_7453815855294711849 size 67108864
081109 205749 997 INFO dfs.DataNode$DataXceiver: Receiving block blk_-28342503914935090 src: /10.251.123.132:57542 dest: /10.251.123.132:50010
081109 205754 952 INFO dfs.DataNode$PacketResponder: Received block blk_8291449241650212794 of size 67108864 from /10.251.89.155
081109 205858 31 INFO dfs.FSNamesystem: BLOCK* NameSystem.allocateBlock: /user/root/rand/_temporary/_task_200811092030_0001_m_000487_0/part-00487. blk_-5319073033164653435
081109 205931 13 INFO dfs.DataBlockScanner: Verification succeeded for blk_-4980916519894289629
081109 210022 1110 INFO dfs.DataNode$PacketResponder: Received block blk_-5974833545991408899 of size 67108864 from /10.251.31.180
            """;
    @Test
    void parse() {
        var line = lines.lines().findFirst().get();
        var log = HdfsLog.parse(line + "\n");
        Assertions.assertEquals(log.getDate(), "081109");
        Assertions.assertEquals(log.getTime(), "205157");
        Assertions.assertEquals(log.getProcessId(), "752");
        Assertions.assertEquals(log.getLogLevel(), "INFO");
        Assertions.assertEquals(log.getLogSource(), "dfs.DataNode$PacketResponder");
        Assertions.assertEquals(log.getLogMessage(), "Received block blk_9212264480425680329 of size 67108864 from /10.251.123.1");
    }
}
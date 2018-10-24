package util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

/**
 * 生成一个日志文件 模拟数据源
 */
public class DataGeneration {
    /**
     * @param args
     */
    public static void main(String[] args) {
        File logFile = new File("session_track.log");
        Random random = new Random();

        String[] hosts = { "www.cainiaowo.com" ,"www.qq.com","www.baidu.com"};

        String[] session_id = { "sdfhdsyeahshaebsy13766heuh7", "dsfjhsdofkeruj334648345", "dsfkldjsd37432487jkdhfdjf ",
                "sdsjfhsdjfh34234lmfl9872j;", "dskjfiouvdsnfu78234jkds98" };

        String[] time = { "2018-01-07 08:40:50", "2017-01-07 08:40:51", "2016-01-07 08:40:52", "2015-01-07 08:40:53",
                "2014-01-07 09:40:49", "2013-01-07 10:40:49", "2012-01-07 11:40:49", "2014-01-07 12:40:49" };


        StringBuffer sbBuffer = new StringBuffer() ;
        for (int i = 0; i < 50; i++) {
            sbBuffer.append(hosts[random.nextInt(3)]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]+"\n");
        }
        if(! logFile.exists())
        {
            try {
                logFile.createNewFile();
            } catch (IOException e) {
                System.out.println("Create logFile fail !");
            }
        }
        byte[] b = (sbBuffer.toString()).getBytes();

        FileOutputStream fs;
        try {
            fs = new FileOutputStream(logFile);
            fs.write(b);
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

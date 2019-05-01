package cn.edu.fudan;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 * Created by FengSi on 2017/07/16 at 11:00.
 */
public class Detect {
    public static void main(String[] args) {
        detect();
    }

    public static void detect() {
        JSch jSch = new JSch();
        try {
            Session session = jSch.getSession("wonders", "10.141.220.200", 22);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword("123456");
            session.connect();
            ChannelExec channelExec = (ChannelExec) session.openChannel("exec");
            channelExec.setCommand("\n " +
                    "spark-submit --class cn.edu.fudan.Test /opt/dataclean-1.0-SNAPSHOT.jar zip city\n");
            channelExec.setInputStream(null);
            channelExec.setErrStream(System.err);
            channelExec.connect();
            InputStream in = channelExec.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")));
            String buf;
            while ((buf = reader.readLine()) != null) {
                System.out.println(buf);
            }
            reader.close();
            channelExec.disconnect();
            session.disconnect();
        } catch (JSchException | IOException e) {
            e.printStackTrace();
        }

        System.out.println();
    }
}

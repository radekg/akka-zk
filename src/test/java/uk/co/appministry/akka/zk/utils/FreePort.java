package uk.co.appministry.akka.zk.utils;

import java.io.IOException;
import java.net.ServerSocket;

public class FreePort {

    public static int getFreePort() throws IOException {
        ServerSocket s = new ServerSocket(0);
        int port = s.getLocalPort();
        s.close();
        return port;
    }

}

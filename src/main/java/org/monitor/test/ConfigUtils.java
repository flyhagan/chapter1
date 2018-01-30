package org.monitor.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtils {


    private static Properties p = new Properties();
    static {
        String fileName="common.properties";
        InputStream in = ConfigUtils.class.getClassLoader().getResourceAsStream(fileName);
        try {
            if(in != null) {
                p.load(in);
                in.close();
            }else {
                throw new FileNotFoundException(fileName);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getValue(String key){
        return  p.getProperty(key);
    }
}

package com.sparkdemo;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Util {
    @NotNull
    public static Properties loadProperties () {
        Properties properties = new Properties();
        try (InputStream inputStream = App.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (inputStream == null) {
                System.out.println("Unable to find properties file");
                System.exit(1);
            }
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        validateWindowTimeUnit(properties.getProperty("window.time.unit"));
        return properties;
    }

    public static void validateWindowTimeUnit(String s) {
        try {
            WindowTimeUnit.valueOf(s);
        }
        catch (IllegalArgumentException e) {
            System.err.println("Unexpected value for window.time.unit");
            System.exit(1);
        }
    }
}

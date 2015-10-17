package com.dredgeplatform.dredge.lib;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DredgeUtils {
    public static Properties readDredgeProperties(String fileName) throws FileNotFoundException, IOException {
        final Properties props = new Properties();
        try (final InputStream iStream = new FileInputStream(fileName)) {
            props.load(iStream);
        }
        return props;
    }
}

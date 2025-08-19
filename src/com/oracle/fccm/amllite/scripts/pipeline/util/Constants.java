package com.oracle.fccm.amllite.scripts.pipeline.util;

import java.io.File;

public class Constants {
    public static String CURRENT_DIRECTORY = System.getProperty("user.dir");
    public static File PARENT_DIRECTORY = new File(CURRENT_DIRECTORY).getParentFile();
    public static String CONFIG_FILE_PATH = PARENT_DIRECTORY+File.separator+"bin"+File.separator+"config.properties";
    public static File OUTPUT_FOLDER = new File(Constants.PARENT_DIRECTORY, "out");
}

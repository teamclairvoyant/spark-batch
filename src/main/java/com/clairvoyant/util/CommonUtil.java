package com.clairvoyant.util;

import org.apache.spark.sql.*;

public class CommonUtil {
    private CommonUtil()
    {

    }

    /**
     * Returns Sparksession without Hive support
     * @return
     */
    public static SparkSession getSparkSessionWithoutHiveSupport(String mode) {

        return SparkSession
                .builder()
                .config("spark.sql.orc.impl","native")
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
               // .config("dfs.client.write.shortcircuit.skip.checksum", "true")
                .master(mode)
                .getOrCreate();
    }
    /**
     * Returns Sparksession with Hive support
     * @return
     */
    public static SparkSession getSparkSessionWithHiveSupport(String mode) {

        return SparkSession
                .builder()
                .config("spark.sql.orc.impl","native")
                .master(mode)
                .enableHiveSupport()
                .getOrCreate();
    }

    /**
     * Closes the sparksession passed as input
     * @param sparkSession
     */
    public static void closeSparkSession(SparkSession sparkSession)
    {
        sparkSession.close();
    }


}



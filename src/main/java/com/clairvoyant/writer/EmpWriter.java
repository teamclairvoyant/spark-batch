package com.clairvoyant.writer;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class EmpWriter {
   private EmpWriter()
   {

   }

    /**
     * Writes a given dataset as csv to outputfilepath
     * @param dataset
     * @param outputFilePath
     */
    public static void writeAsCSV(Dataset<Row> dataset, String outputFilePath)
    {
        //Saving transformed data to a csv file

        dataset.coalesce(1).write()
                .mode("overwrite")
                .option("header","true")
                .format("csv")
                .save(outputFilePath);
    }
}

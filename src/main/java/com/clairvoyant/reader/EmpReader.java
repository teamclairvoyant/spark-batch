package com.clairvoyant.reader;

import com.clairvoyant.model.Employee;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class EmpReader {
    private EmpReader()
    {

    }

    /**
     * Reads a CSV file
     * @param sparkSession
     * @param fileName
     * @return
     */
    public static Dataset<Employee> readFromCSV(SparkSession sparkSession, String fileName)
    {
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);

        //FilePath from resources folder
        String filePath=Thread.currentThread().getContextClassLoader().getResource(fileName).getPath();

        //Reading data from File using Sparksession and casting it as Employee Schema
        Dataset<Employee> employeeDF=sparkSession.read().option("header","true").csv(filePath).as(employeeEncoder);

        //Displaying data
        employeeDF.show(false);

        return employeeDF;

    }
}

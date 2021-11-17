package com.clairvoyant.main;

import com.clairvoyant.reader.EmpReader;
import com.clairvoyant.model.Employee;
import com.clairvoyant.transformations.EmpTransformations;
import com.clairvoyant.util.CommonUtil;
import com.clairvoyant.writer.EmpWriter;
import org.apache.spark.sql.*;

public class SparkBatchApp {

    public static void main(String[] args) {
        //Retrieve mode from args
        String mode = "local[*]";
        String fileName ="Employee_sample.csv";
        String outputFilePath="./output/";

        //Creating SparkSession
        SparkSession sparkSession= CommonUtil.getSparkSessionWithoutHiveSupport(mode);
        //Read data from CSV file
        Dataset<Employee> employeeDF= EmpReader.readFromCSV(sparkSession,fileName);
        //Transformation to get employee and his/her manager details
        Dataset<Row> transformedDF= EmpTransformations.EmpManagerDetails(employeeDF);
        //Write Data as CSV file
        EmpWriter.writeAsCSV(transformedDF,outputFilePath);
        //Closing Sparksession
        CommonUtil.closeSparkSession(sparkSession);
    }

}

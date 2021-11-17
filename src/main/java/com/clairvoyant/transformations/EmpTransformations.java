package com.clairvoyant.transformations;

import com.clairvoyant.model.Employee;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class EmpTransformations {
    private EmpTransformations()
    {

    }
    /**
     * Method to get Employee and Manager details
     * @param employeeDF
     * @return
     */
    public static Dataset<Row> EmpManagerDetails(Dataset<Employee> employeeDF)
    {
        //Transformation to get employee and his manager details
        Dataset<Row> transformedDF=employeeDF.as("emp").join(employeeDF.as("manager")
                ,col("emp.ManagerId").equalTo(col("manager.EmpId")))
                .selectExpr("emp.EmpId","emp.ManagerId","emp.EmpName","manager.EmpName as ManagerName");

        //Displaying transformed data
        transformedDF.show();
        return transformedDF;

    }
}

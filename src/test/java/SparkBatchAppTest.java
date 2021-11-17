import com.clairvoyant.main.SparkBatchApp;
import com.clairvoyant.model.Employee;
import com.clairvoyant.util.CommonUtil;
import org.apache.spark.sql.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.Serializable;

import static org.junit.Assert.assertEquals;

public class SparkBatchAppTest implements Serializable {
    //get spark session
    private SparkSession sparkSession = CommonUtil.getSparkSessionWithoutHiveSupport("local[*]");
    private Employee employee=new Employee();

    //source data
    @Before
    public void prepareData() {
       Dataset<Employee> employeeDataset=sparkSession.read().option("header","true").option("schema","true").csv(SparkBatchApp.class.getClassLoader()
               .getResource("Employee_sample.csv").getPath()).as(Encoders.bean(Employee.class));
        employee=employeeDataset.first();
    }


    @Test
    public void testEmployeeRecords() {
        assertEquals("Employee Name","John",employee.getEmpName());
      //  assertEquals("Employee Age","23",employee.getAge());
        // assertEquals("Employee Location","US",employee.getLocation());
    }

    @After
    public void sparkSessionClose()
    {
        sparkSession.close();
    }
}

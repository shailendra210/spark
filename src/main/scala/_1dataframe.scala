import java.util.Base64.Encoder

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
case class movies(uid:Int, movieId :Int, rating:Int, valueid:Int )
case class movieWithName(id:Int, name :String)

case class employee(eid:Int,name:String, deprt:String,salary:Int,dept:Int,deptId:Int)
case class dept(id:Int,name:String,loc:String)
case class newDataset(id:Int,name:String,eid:Int)

object _1dataframe {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("employees").master("local[*]").config("spark.sql.warehouse.dir", "/user/hive/warehouse").config("hive.metastore.uris","thrift://192.168.43.51:9083").enableHiveSupport().getOrCreate()
    val empSchema =Encoders.product[employee].schema
    val empdf = spark.read.schema(empSchema).csv("D:\\datasets\\suraj_dataset\\empdept\\employee.txt")
    val deptSchema = Encoders.product[dept].schema
    val deptdf = spark.read.schema(deptSchema).csv("D:\\datasets\\suraj_dataset\\empdept\\dept.txt")

    val schemaNew = Encoders.product[newDataset].schema
    val dataset: DataFrame =spark.read.schema(schemaNew).option("header","true").csv("D:\\datasets\\suraj_dataset\\new_dataset")

    import spark.implicits._
    //val new_joined = datanew.join(datanew,$"df1.id"===$"df2.eid","outer")
   // dataset.as("df1").join(dataset.as("df2"),$"df1.id" === $"df2.id").limit(2).show()

    spark.sql("show tables").show()

   // val df: Boolean = spark.catalog.tableExists("emp")
    dataset.select(dataset("id")).show()
  /*  if(df == true){
      print("exist")
    }
    else{
      print("not exists")
    }*/
    //datanew.show()

    //datanew.as("df1").join(datanew.as("df2"),$"df1.id" = $"df2.eid")

    //empdf.as("emp").join(deptdf.as("dept"),$"emp.id" === $"dept.deptId").show(false)
    //empdf.show()
    //deptdf.show()
  }
}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.{orderBy, partitionBy}
import org.apache.spark.sql.functions.{dense_rank, desc, row_number}
import com.typesafe.config.ConfigFactory

object Testing{
  def main(args : Array[String]): Unit = {

    val props = ConfigFactory.load()
    val envprops = props.getConfig(args(0))


    val spark = SparkSession.builder().
      appName("Testing Dataframes").
      master("local").
     // enableHiveSupport().
      getOrCreate()

    val inputEmp = envprops.getString("input.emp")
    val inputDept = envprops.getString("input.dept")

   spark.sparkContext.setLogLevel("ERROR")
    val logger = Logger.getLogger(this.getClass.getName)
    logger.info("dataframe operations started")


val df1 = spark.read.option("header","true").option("inferschema","true").csv(inputEmp)

    df1.printSchema()
    df1.show()
  //  df1.write
  //    .saveAsTable("sample.emp_details")

    logger.info("no errors in df1")

   /* val df2 = spark.read.option("header","true").option("inferschema","true").csv(inputDept)
    df2.printSchema()
    df2.show()

    // Inner JOin in data frame
    val Inner_join = df1.join(df2,df1.col("dept_no") === df2.col("dept_id"))

    // Outer joins
    // left outer join,right and full
    val left_join = df1.join(df2,df1.col("dept_no") === df2.col("dept_id"),"left_outer")
      left_join.show()
    val right_join = df1.join(df2,df1.col("dept_no") === df2.col("dept_id"),"right_outer")
    val full_outer = df1.join(df2,df1.col("dept_no") === df2.col("dept_id"),"full_outer")

    // semi joins
    // left semi and right semi
    val l_semi = df1.join(df2,df1.col("dept_no") === df2.col("dept_id"),"left_semi")
      l_semi.show()
    val l_anti = df1.join(df2,df1.col("dept_no") === df2.col("dept_id"),"left_anti")
      l_anti.show()

    //cross join
    val cr_join = df1.crossJoin(df2)
     cr_join.show()


    // row_number
    val wrn = df1.withColumn("row_number",row_number.over(partitionBy("salary").orderBy("salary")))
    wrn.show()

    //rank
    val wrank = df1.withColumn("dense_rank",dense_rank.over(orderBy(desc("salary"))))
    wrank.show()
    wrank.filter(wrank("dense_rank") === "2").show()


    */

    // val inidata = spark.read.option("multiline","true").json("file:///c://users//b0p03a0//desktop//colours.json")
    // inidata.show()

  }
}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class Find_highest {

  val spark = SparkSession.builder().
    appName("finding highest selling products").
    master("local").
    getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  val products_df = spark.read.option("inferschema","true").csv("file:///c://users//b0p03a0//desktop//Inputs//products.csv").
    toDF("product_id","product_category_id","product_name","product_description","product_price","product_imamge")

  val category_df = spark.read.option("inferschema","true").csv("file:///c://users//b0p03a0//desktop//Inputs//categoroes.csv").
    toDF("category_id","category_department_id","category_name")

  val orders_df = spark.read.option("inferschema","true").csv("file:///c://users//b0p03a0//desktop//Inputs//order_items.csv").
    toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")



  // getting the count
  def printing_count(): Unit = {
    println("*********************** printing the schema **************************")
  products_df.printSchema()
    category_df.printSchema()
    orders_df.printSchema()
    println("%%%%%%%%%% printing the count %%%%%%%%%%%%%%%%%")
    println("count of product :" +products_df.count())
    println("count of categories :" +category_df.count())
    println("total orders :" +orders_df.count())
  }
// agrigation to find the highest revenue products from acceserios

  def aggrigation: Unit = {
    println("########## starting aggregation ############")
    val res = products_df.select("product_id","product_category_id","product_name").
      join(category_df.select("category_id","category_name"),
        products_df.col("product_category_id") === category_df.col("category_id")).
      join(orders_df.select("order_item_product_id","order_item_subtotal"),
        products_df.col("product_id") === orders_df.col("order_item_product_id"))
      .filter(category_df.col("category_name") === "Accessories")
      .groupBy("category_name", "product_name")
      .agg(sum("order_item_subtotal").as("revenue"))
      .orderBy(desc("revenue"))
      .limit(5)

    res.show()



  }

}

package org.nupsea.flink.batch

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import java.util.Date

object ProductsAnalyser {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    /*
    Product,Vendor
    Mouse,Logitech
    Keyboard,Microsoft
     */
    val prodDS = env.readCsvFile[(String, String)]("src/main/resources/DATA/product_vendor.csv", ignoreFirstLine = true)
    prodDS.print()

    /*
    ID,Customer,Product,Date,Quantity,Rate,Tags
    1,Apple,Keyboard,2019/11/21,5,31.15,"Discount:Urgent"
    2,LinkedIn,Headset,2019/11/25,5,36.9,"Urgent:Pickup"
    */
    val ordersDS = env.readCsvFile[(Short, String, String, String, Short, Float, String)]("src/main/resources/DATA/sales_orders.csv", ignoreFirstLine = true)
    env.execute("products_analysis")
  }
}



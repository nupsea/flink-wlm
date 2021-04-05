package org.nupsea.flink.batch.products

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object ProductsAnalyser {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val config = ParameterTool.fromArgs(args)
    val data = config.get("data")
    print(s">>  Data directory: ${data}")

    /*
    Product,Vendor
    Mouse,Logitech
    Keyboard,Microsoft
     */
    val prodDS = env.readCsvFile[(String, String)](data + "/product_vendor.csv", ignoreFirstLine = true)
    //prodDS.print()

    /*
    ID,Customer,Product,Date,Quantity,Rate,Tags
    1,Apple,Keyboard,2019/11/21,5,31.15,"Discount:Urgent"
    2,LinkedIn,Headset,2019/11/25,5,36.9,"Urgent:Pickup"
    */
    val ordersDS = env.readCsvFile[(Short, String, String, String, Short, Float, String)](data + "/sales_orders.csv", ignoreFirstLine = true)
    //ordersDS.print()

    /**
     * Display product item count by vendor for each company.
     */
    val itemByVendor = ordersDS
      .join(prodDS)
      // 0 - based index keys
      .where(2)
      .equalTo(0) {
        (order, prod) => (order._2, order._3, prod._2, order._5) // 1 - based index
      }

    //    (Apple,5,Logitech)
    //    (LinkedIn,4,Logitech)

    itemByVendor.groupBy(0, 2)
      .sum(3)
      .print()

  }
}



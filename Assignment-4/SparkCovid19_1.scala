import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Covid19_1 {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Covid19_1")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val csv = sc.textFile(args(0))
    val rows = csv.map(line => line.split(",").map(_.trim))
    val header = rows.first
    val worldData = rows.filter(_(0) != header(0))
    val data = worldData.filter(_(1) != "World")
    val str = args(2);
    if (str.equals("true"))
      data.map(row => (row(1), row(2).toInt)).reduceByKey(_ + _).sortBy(_._1).saveAsTextFile(args(3))
    else
      data.map(row => (row(1), row(2).toInt)).reduceByKey(_ + _).sortBy(_._1).saveAsTextFile(args(3))

    sc.stop
  }
}
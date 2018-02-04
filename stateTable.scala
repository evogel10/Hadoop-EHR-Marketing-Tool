/* stateTable.scala*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object StateTable {
	def main(ars: Array[String]) {
		val conf = new SparkConf().setAppName("State Table")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR");
		
		var stateTableRawRDD = sc.textFile("hdfs://localhost:9000/final-project/state_table.csv")

		var stateTableNoHeaderRDD = stateTableRawRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

		val stateTableStateRegionRDD = stateTableNoHeaderRDD.map(line => (line.split(",")(1), (line.split(",")(13))))

		val stateTableCleanRDD = stateTableStateRegionRDD.map(rec => "\"" + rec._1 + "\"" + "," + rec._2)

		stateTableCleanRDD.saveAsTextFile("hdfs://localhost:9000/final-project-output-stateTable")

		sc.stop()

	}
}
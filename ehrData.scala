/* ehrData.scala*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object EhrData {
	def main(ars: Array[String]) {
		val conf = new SparkConf().setAppName("EHR Data")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR");

		var ehrDataRawRDD = sc.textFile("hdfs://localhost:9000/final-project/MU_REPORT_2016.csv")

		var ehrDataNoHeaderRDD = ehrDataRawRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

		val ehrDataStateProductRDD = ehrDataNoHeaderRDD.map(line => (line.split(",")(3), (line.split(",")(18))))

		val ehrDataGroupedRDD = ehrDataStateProductRDD.groupByKey().mapValues(_.toList)

		val ehrStatesWithMostProductsRDD = ehrDataGroupedRDD.map(t => (t._1, t._2.size))

		val ehrStatesWithMostProductsJoinRDD = ehrStatesWithMostProductsRDD.map(rec => (rec._1, (rec._1, rec._2)))

		val ehrDataProviderTypeRDD = ehrDataNoHeaderRDD.map(line => (line.split(",")(3), line.split(",")(2)))

		val ehrDataProviderTypeGroupedRDD = ehrDataProviderTypeRDD.groupByKey().mapValues(_.toList)

		def providerCount[T] ( ts:Iterable[T] ) = {
			ts.groupBy(identity).mapValues(_.size)
		}

		val ehrDataproviderPerStateRDD = ehrDataProviderTypeGroupedRDD.map(rec => (rec._1, providerCount(rec._2)))

		val ehrDataproviderPerStateExpandRDD = ehrDataproviderPerStateRDD.map(rec => (rec._1, rec._2.keys, rec._2.values))

		def largerProvider( a:Iterable[String], b:Iterable[Int] ) : String = {
			val providerSize = a.size
			val head = a.head
			if (providerSize == 1) {
				return a.head
			} else if (b.head < b.last) {
				return a.last
			} else {
				return a.head
			}
		}

		val ehrDataStateProviderRDD = ehrDataproviderPerStateExpandRDD.map(rec => (rec._1, largerProvider(rec._2, rec._3)))

		val ehrDataJoinedRDD = ehrStatesWithMostProductsJoinRDD.join(ehrDataStateProviderRDD)

		val ehrDataCleanRDD = ehrDataJoinedRDD.map(rec => rec._1 + "," + rec._2._1._2 + "," + rec._2._2)

		ehrDataCleanRDD.saveAsTextFile("hdfs://localhost:9000/final-project-output-ehrData")

		sc.stop()

	}
}
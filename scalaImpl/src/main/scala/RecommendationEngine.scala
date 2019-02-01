import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions._
import scala.math.log
import scala.collection.JavaConverters._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import scala.collection.mutable.WrappedArray


object RecommendationEngine {

	def main(args: Array[String]){
		//shannon entropy
		def xlogx(v: Long) : Double = if (v==0) 0 else v*log(v)

		//xlogx(sum) - sum(xlogx)
		def entropy(k: Seq[Long]) : Double =  {
		 	(xlogx(k.sum) - k.map(v => xlogx(v)).sum)
		}

		// lrr= -2* (H(k) - H(rowSums(k)) - H(colSums(k)))
		val lrr_fn = (num_users: Long,  num_users_R: Long, num_common_users: Long, num_total_users: Long) => {
			val k : Seq[Long]  = Seq(num_common_users,
				num_users_R-num_common_users,
				num_users - num_common_users,
				num_total_users - num_users-num_users_R+num_common_users)
			val rowSums : Seq[Long] = Seq(k(0)+k(1),k(2)+k(3))
			val colSums : Seq[Long] = Seq(k(0)+k(2),k(1)+k(3))

			val rowEntropy = entropy(rowSums)
			val colEntropy = entropy(colSums)
			val matrixEntropy = entropy(k)

			val lrr = (rowEntropy + colEntropy - matrixEntropy)*2.0
			if ( lrr < 0 ) 0.0 else lrr
		}
		

		val spark = SparkSession.builder().appName("RecommendationEngine").getOrCreate()
		spark.conf.set("spark.sql.crossJoin.enabled", true)
		import spark.implicits._

		//-----   Read in data into dataframe of columns = user, movie   -----------
		val dataDF = spark.read.option("sep","\t").
			csv("projects/recommendationEngine/test.data").
			toDF("user","movie","rating","dummy").
			withColumn("movie",  col("movie").cast("int")).
			drop("rating","dummy")
		dataDF.show(5)

		val num_total_users = dataDF.agg(countDistinct($"user") as "count" ).collect()(0).getLong(0)

		def get_similarity_fn(num_total_users : Long) = 
			(num_users: Long,  num_users_R: Long, num_common_users: Long) => {
			val lrr = lrr_fn(num_users, num_users_R, num_common_users, num_total_users)
			1.0 - 1.0/(1.0 + lrr)
		}




/*

		def testUdf = udf( (array: WrappedArray[Int]) => array.toList.flatMap(i => array.toList.map(j => (i,j) -> 1).toMap))

		val userItemsRDD : RDD[Row] = dataDF.groupBy("user").
			agg(collect_set("movie") as "movies").
			drop("user").
			withColumn("movies", testUdf(col("movies"))).
			rdd
			*/




		



		
		//---------   Convert to "set of users" for each movie    -----------

		val itemUsers = dataDF.groupBy("movie").
			agg(collect_set("user") as "users", count("user") as "num_users").
			orderBy(asc("movie"))
		itemUsers.show(5 )
		
		//val users_1 = itemUsers.filter($"movie" === 1)
			//.select("users").collect()(0).getList(0)
		//println(s"users_1 : ${users_1}")

		val df = itemUsers

		//---------   Cross join to get metrics for movie pairs  ----------------
		val common_set_size_fn =  (s1: Seq[String],s2: Seq[String]) => {(s1.toSet.intersect(s2.toSet)).size}
		val common_set_size_udf = udf(common_set_size_fn)


		// -----  Calculate the similarity using log-likelihood ratio  ----------------

		val similarity_udf = udf(get_similarity_fn(num_total_users))
		val join = df.
			join(df.toDF(df.columns.map(_+"_R"):_*), $"movie" < $"movie_R").
			withColumn("num_common",common_set_size_udf($"users",$"users_R")).
			drop("users","users_R").
			withColumn("similarity",similarity_udf($"num_users",$"num_users_R",$"num_common")).
			limit(25)

		join.show(25)

	}

	def getRecommendationsForUser(userId: String) {

		//---- Get preferences of user     ------------
		//val userPrefs = dataDF.filter(user == userId).groupBy("user").agg(collect_set("user") as "users").orderBy(asc("movie"))

		//---- matrix multiplication  ----------------
		//---  multiply each column i of similarity matrix with column i of userPrefs ----
		//---  same as multiply each row i of similarity matrix with col i of userPrefs ---
		// outvector = {}
		//  for ratedMovie in userPrefs :
		//     outvector += similarityMatrix[ratedMovie]*userPrefs[ratedMovie]

		// sort(outVector) and return top k cols
	}

}

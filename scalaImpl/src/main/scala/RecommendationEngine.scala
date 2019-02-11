import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions._
import scala.math.log


object RecommendationEngine {
	val THRESHOLD = 0.8
	val MAX_SIMILARITIES = 256
	val MIN_COUNT = 10
	val MIN_ITEM_COUNT_PER_USER = 1
	val MAX_RECOS=5

	var thresholdedSimilarities:org.apache.spark.rdd.RDD[(Int, (Int, Double, Int, Int, Int))]  = null

	var userRatings:org.apache.spark.rdd.RDD[(Int, Int, Int)] = null

	var coOccurenceMatrix:org.apache.spark.rdd.RDD[((Int, Int), Int)] = null

	var itemPairSimilarities:org.apache.spark.rdd.RDD[(Int, (Int, Double, Int, Int, Int))] = null

	def main(args: Array[String]){


		import java.io.File
		def deleteRecursively(file: File): Unit = {
			if (file.isDirectory)
				file.listFiles.foreach(deleteRecursively)
			if (file.exists && !file.delete)
				throw new Exception(s"Unable to delete ${file.getAbsolutePath}")   
			if (!file.exists)
			  println(s"Could not find file ${file}")
		}

		deleteRecursively(new File("/mapr/my.cluster.com/user/mapr/projects/recommendationEngine/output_scala"))


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

		def get_similarity_fn(num_total_users : Long) = 
			(num_users: Long,  num_users_R: Long, num_common_users: Long) => {
			val lrr = lrr_fn(num_users, num_users_R, num_common_users, num_total_users)
			1.0 - 1.0/(1.0 + lrr)
		}
	

		val spark = SparkSession.builder().appName("RecommendationEngine").getOrCreate()
		spark.conf.set("spark.sql.crossJoin.enabled", true)
		spark.conf.set("spark.hadoop.validateOutputSpecs", "false")
		import spark.implicits._

		//-----   Read in data into dataframe of columns = user, movie   -----------
		userRatings = spark.read.option("sep","\t").
			option("inferSchema","true").
			csv("projects/recommendationEngine/test.data").
			drop("_c3").
			rdd.
			map( x => (x(0).asInstanceOf[Int], x(1).asInstanceOf[Int], x(2).asInstanceOf[Int]))

		val userItem = userRatings.map(x => (x._1, x._2))

		val userItemCount = userItem.groupByKey().filter( {case (x,y) => y.toSeq.length > MIN_ITEM_COUNT_PER_USER})

		val itemUser = userItemCount.flatMapValues(x => x).
			map(x => (x._2, x._1))

		val userCountByItem = itemUser.map( { case (item,user) => (item, 1)}).reduceByKey(_ + _)
		
		val itemUserCounts = itemUser.join(userCountByItem) 
		//itemUserCounts is of the form (item, (user,total_users))

		val itemsByUser = itemUserCounts.map( {case (item, (user,usercount)) => (user, (item,usercount))})

		val userItemPairs = itemsByUser.join(itemsByUser). //pair up items from same user
			filter( x => x._2._1._1 != x._2._2._1). //ignore item paired up with itself
			map({ case (user, ((item1,usercount1),(item2,usercount2))) => ((item1.asInstanceOf[Int],item2.asInstanceOf[Int]), (usercount1, usercount2, 1))}). //assign a 1 to each pair inorder to get pair counts
			flatMap( x => Array(x)).
			reduceByKey( (a,b) => (a._1,a._2,a._3+b._3)).
			filter(x => x._2._3 >= MIN_COUNT ) //ignore item pairs that have very low count

		coOccurenceMatrix = userItemPairs.map( {case ((a,b),(c,d,e)) => ((a,b), c)})

		val num_total_users = itemsByUser.map(x => x._1).distinct().count()

		itemPairSimilarities = userItemPairs.map( {case ((it1,it2),(numUsers1,numUsers2,numCommonUsers)) =>
			(it1, (it2, get_similarity_fn(num_total_users)(numUsers1,numUsers2,numCommonUsers), numUsers1, numUsers2, numCommonUsers))})

		thresholdedSimilarities = itemPairSimilarities.filter(x => x._2._2 > THRESHOLD)

		val recommendations = thresholdedSimilarities.groupByKey().
			map( { case (x,y) => (x, y.toSeq.sortWith(_._2 > _._2).take(MAX_SIMILARITIES))})

		val sortedRecommendations = recommendations.flatMapValues(x => x).
			map(x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4, x._2._5)).
			sortBy(x => (x._1, x._2, -x._3))

		sortedRecommendations.
			map({ case (it1, it2, sim, nu1, nu2, cu) => s"$it1\t$it2\t$sim\t$nu1\t$nu2\t$cu"}).
			saveAsTextFile("projects/recommendationEngine/output_scala")

		
		/*********************************************************************/
		/**** Old method using set intersection and self join ****************/
		/*
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

			*/
	}

	def getRecosForKnownUser(userId: String) {
		//---- Get preferences of user     ------------
		//val userPrefs = dataDF.filter(user == userId).groupBy("user").agg(collect_set("user") as "users").orderBy(asc("movie"))
		val userPrefs = userRatings.
			filter({case (u,i,r) => u == userId.asInstanceOf[Int]}).
			flatMap({case (u,i,r) => Array((i, r))})

		val columnProducts = userPrefs.join(itemPairSimilarities).
			map({ case (it1, (r1, (it2, s2,d1,d2,d3))) => ( it2,  r1*s2) })

		val matrixProduct = columnProducts.
			reduceByKey(_ + _).
			sortBy( -_._2).
			take(MAX_RECOS)

		return matrixProduct.first()
	}


	def getRecosForNewUser(ratings : Array[(Int,Int)]) {
		//---- Get preferences of user     ------------
		val userPrefs = spark.sparkContext.sparallelize(ratings)

		val columnProducts = userPrefs.join(itemPairSimilarities).
			map({ case (it1, (r1, (it2, s2,d1,d2,d3))) => ( it2,  r1*s2) })

		val matrixProduct = columnProducts.
			reduceByKey(_ + _).
			sortBy( -_._2).
			take(MAX_RECOS)

		return matrixProduct.first()
	}
			


			

			

		//---- matrix multiplication  ----------------
		//---  multiply each column i of similarity matrix with column i of userPrefs ----
		//---  same as multiply each row i of similarity matrix with col i of userPrefs ---
		// outvector = {}
		//  for ratedMovie in userPrefs :
		//     outvector += similarityMatrix[ratedMovie]*userPrefs[ratedMovie]

		// sort(outVector) and return top k cols
	}

}

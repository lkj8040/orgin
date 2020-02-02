package com.liukuijian

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//标准推荐样例类
case class Recommendation(mid:Int, score:Double)

case class MongoConfig(uri: String, db:String)

//给用户的推荐
case class UserRecs(uid:Int,recs:Seq[Recommendation])

//电影相似度
case class MovieRecs(mid:Int,recs:Seq[Recommendation])

//做在线实时推荐的前提：业务系统来了一条评分数据，从kafka 的recommender主题消费这条数据，取出score，取出uid，取出mid
//从redis中找出该uid最近的K次评分数据mid,
//从电影相似度矩阵找出对应的K次评分数据，利用公式计算相似度，按相似度排序，取前几名推荐给用户
//先定义一个连接助手
object ConnHelper extends Serializable{
    lazy val jedis = new Jedis("hadoop101",6379)
    lazy val mongoClient = MongoClient(MongoClientURI("mongodb://hadoop101:27017/recommender"))
}

object StreamingRecommender {

    val MAX_USER_RATINGS_NUM = 20
    val MAX_SIM_MOVIES_NUM = 20
    val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_MOVIE_RECS_COLLECTION = "Movie_Recs"

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop101:27017/recommender",
            "mongo.db" -> "recommender",
            "kafka.topic" -> "recommender"
        )
        val conf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")

        val spark = SparkSession.builder().config(conf).getOrCreate()

        val sc = spark.sparkContext

        val ssc = new StreamingContext(sc, Seconds(2))

        import spark.implicits._

        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        //将离线推荐模块LFM训练得到的电影相似度矩阵读取进来
        val simRDD: RDD[MovieRecs] = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[MovieRecs]
                .rdd
        //这里将数据转为里层map外层map的原因是：方便后面的查询操作，这里查询的时间复杂度为O(1)
        val simMovieMatrix: collection.Map[Int, Map[Int, Double]] = simRDD.map {
            movie => (movie.mid, movie.recs.map(x => (x.mid, x.score)).toMap)
        }.collectAsMap()

        //由于电影相似度矩阵一般比较大，为了提高计算效率，将该矩阵作为广播变量
        val simMoviesMatrixBroadCast: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simMovieMatrix)

        //假设这时来了一条kafka日志数据
        val params = Map(
            "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "recommender",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (true: java.lang.Boolean)
        )

        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
                            KafkaUtils.createDirectStream[String, String](
                            ssc,
                            PreferConsistent, //固定写法
                            Subscribe[String, String](Array(config("kafka.topic")), params)
                            )

        val ratingStream: DStream[(Int, Int, Double, Long)] =
            kafkaDStream.map {
                //业务系统来了一条评分数据，从kafka 的recommender主题消费这条数据，取出score，取出uid，取出mid
                //uid |mid| score |timestamp
                msg => {
                    println("ratingStreaming come!>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                    val splits: Array[String] = msg.value().split("\\|")
                    (splits(0).trim.toInt, splits(1).trim.toInt, splits(2).trim.toDouble, splits(3).trim.toLong)
                }
            }
        ratingStream.foreachRDD{
            rdd => rdd.map{
                case (uid, mid, score, timestamp) =>
                    //从redis中获取当前用户最近的M次电影评分， Array[(Int,Double)] （mid | score）
                    val userRecentlyRatings: Array[(Int, Double)] =
                        getUserRecentlyRating(MAX_USER_RATINGS_NUM,uid, ConnHelper.jedis)

                    //从相似矩阵中获取电影mid最相似的K个电影， Array[Int]
                    val simMovies: Array[Int] =
                        getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMoviesMatrixBroadCast.value)

                    //计算待选电影的推荐优先级
                    val streamRecs: Array[(Int, Double)] =
                        computeMovieScores(simMoviesMatrixBroadCast.value, userRecentlyRatings, simMovies)

                    //将数据保存到MongoDB
                    saveRecsToMongoDB(uid, streamRecs)
            }.count()
        }

        ssc.start()

        ssc.awaitTermination()

    }
    def saveRecsToMongoDB(uid: Int, recs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig) ={

        val streamRecsCollection: MongoCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

        streamRecsCollection.findAndRemove(MongoDBObject("uid"->uid))

        streamRecsCollection.insert(MongoDBObject("uid"->uid, "recs" ->recs.map(x => MongoDBObject("mid"->x._1,"score"->x._2))))

    }
    def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] ={

        import scala.collection.JavaConversions._

        jedis.lrange("uid:" + uid.toString, 0, num).map{
            item =>
                val attr: Array[String] = item.split("\\:")
                (attr(0).trim.toInt, attr(1).trim.toDouble)
        }.toArray
    }

    def getTopSimMovies(num: Int, mid: Int, uid: Int,
                        simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig) ={
        //获取该mid对应的相似度向量
        val allMovies: Array[(Int, Double)] = simMovies(mid).toArray

        //获取用户已经看过的电影，这些电影需要被过滤掉
        val ratingExists: Array[Int] = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
                .find(MongoDBObject("uid" -> uid))
                .toArray
                .map {
                    item => item.get("mid").toString.toInt
                }
        //过滤掉所有看过的电影，并对相似电影进行排序，取前num个电影mid返回
        allMovies.filter(x => !ratingExists.contains(x._1))
                .sortBy(-_._2)
                .take(num)
                .map(_._1)
    }

    def computeMovieScores(simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]
                           , userRecentlyRatings: Array[(Int, Double)], topSimMovies: Array[Int]) = {
        //用于保存每个待选电影和最近评分的每一个电影的权重得分
        val score: ArrayBuffer[(Int, Double)] = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

        //用于保存每个电影的增强因子数
        val increMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int, Int]()
        //用于保存每个电影的减弱因子数
        val decreMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int, Int]()

        //对于topSimMovies中的每部电影[mid]，根据该用户最近的评分情况[mid, score]计算一个新的分数
        //topSimMovie: 和当前电影相似且用户没有看过的电影, 候选推荐电影
        //userRecentlyRating._1: 用户最近看过的电影
        for(topSimMovie <- topSimMovies; userRecentlyRating <- userRecentlyRatings){
            val simScore = getMoviesSimScore(simMovies, userRecentlyRating._1, topSimMovie)

            if(simScore > 0.7){
                //没看过的电影和用户看过的某个电影的相似度越高且用户给那个看过的电影的评分越高，该片就越应该推荐给用户
                //反之相似度越高，但是用户评分不高，也不会推荐给用户
                score += (topSimMovie -> (simScore * userRecentlyRating._2))
                if(userRecentlyRating._2 > 3){
                    increMap(topSimMovie) = increMap.getOrElse(topSimMovie, 0) + 1
                }else{
                    decreMap(topSimMovie) = decreMap.getOrElse(topSimMovie, 0) + 1
                }
            }
        }
        score.groupBy(_._1)
                .map{
                    //所有和候选推荐电影相似的电影都应该求加权分数
                    case (mid, sims) =>(mid, sims.map(_._2).sum/sims.length
                            + log(increMap.getOrElse(mid,1))-log(decreMap.getOrElse(mid,1)))
                }.toArray.sortBy(-_._2)
    }

    def getMoviesSimScore(simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]],
                          userRecentlyRatingMovie: Int, topSimMovie: Int)={

        simMovies.get(topSimMovie) match{
            case Some(sim) => sim.get(userRecentlyRatingMovie) match{
                case Some(score) => score
                case _ => 0.0
            }
            case _ => 0.0
        }
    }

    def log(n: Int) ={
        math.log(n) / math.log(10)
    }

}

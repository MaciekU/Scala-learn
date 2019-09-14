//cp -r /project/example /tmp
// ./sbt assembly
//flink run target/scala-2.11/flink-example-assembly-0.0.1.jar

package ml.lsdp.example

import java.util.Properties

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._
import org.apache.flink.api.java.utils._
import scala.collection.mutable.ListBuffer
import org.apache.flink.streaming.api.scala.DataStream
import java.util.StringTokenizer
import play.api.libs.json.Json
import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, StreamingEndpoint}


object Demo {
    def main(args: Array[String]) : Unit = {
        

        val log = LoggerFactory.getLogger(this.getClass)
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val p = new Properties();
        p.setProperty(TwitterSource.CONSUMER_KEY, "0000");
        p.setProperty(TwitterSource.CONSUMER_SECRET, "0000");
        p.setProperty(TwitterSource.TOKEN, "0000");
        p.setProperty(TwitterSource.TOKEN_SECRET, "0000");

        val End_init = new End_filter() // inicjalizacja endpoint
        val source = new TwitterSource(p)
        //source.setCustomEndpointInitializer(End_init) // ograniczenie strumienia

        val streamSource = env.addSource(source);
        
 
        streamSource.map(s => (0,1))
            .keyBy(0) 
            .timeWindow(Time.minutes(1), Time.seconds(30))
            .sum(1)
            .map(t => t._2)
            .writeAsText("/tmp/test.txt", WriteMode.OVERWRITE) //number of tweets

        streamSource.map(s => twitter_data(s))
            .keyBy(0) 
            .timeWindow(Time.minutes(1))
            .reduce((a,b) => add_ab(a,b))//((x, y) => x + y) występują też string
            .map(a => format_data_toString(a))
            .writeAsText("/tmp/twitter1.txt", WriteMode.OVERWRITE)

        env.execute("Twitter stream:")
    }

    def add_ab(a : (Int, Int, Set[String], List[(String, Int)]),
        b : (Int, Int, Set[String], List[(String, Int)]))
        : (Int, Int, Set[String], List[(String, Int)]) = {
        var vector_c =  (a._1 + b._1, a._2 + b._2, a._3 ++ b._3, add_hashtags(a._4, b._4))
        return vector_c
    }

    def top_hash(hashtags : List[(String, Int)]) : String = {
        if(hashtags.isEmpty) {
            return ""
        }
        var top_hashtags = hashtags.take(3).map(a => "\"" + a._1 + "\" " + a._2).reduce((a1, a2) => a1 + "  " + a2) //3 topowe
        return top_hashtags
    }

    def format_data_toString(a : (Int, Int, Set[String], List[(String, Int)]))
        : (String, String, String, String) = {
        var data_twitter = ("\n\nDlugosc tekstu: " + a._1,"\nLiczba slow: " + a._2, "\nUnikalne slowa: " + a._3.size,"\nPopularne hashtagi (top 3): " + top_hash(a._4))
        return data_twitter
    }

    def add_hashtags(a : List[(String, Int)], b : List[(String, Int)]) : List[(String, Int)] = {
        
        var unzi = a ++ b
        var sum_ab = unzi //https://stackoverflow.com/questions/39689313/scala-summing-values-of-a-list-of-tuples-in-style-string-int-based-on-1
        .groupBy(_._1).map { case (key, value) => key -> (value.map(_._2)).sum}
        .toList
        .sortWith(_._2 > _._2) //low to high
        return sum_ab
    }

    def twitter_data(jsonString: String) : (Int, Int, Set[String], List[(String, Int)]) = {
        //https://pedrorijo.com/blog/scala-json/
        var jsonObject = Json.parse(jsonString)
        var text = (jsonObject \ "text").as[String]
        var text_len = text.length
        var words = text.split("\\W+")
        var words_len = words.length
        var words_set = Set() ++ words //konkatenacja set ++

        var hashtags = (jsonObject \ "entities" \ "hashtags" \\ "text").map(_.as[String]).toList // hashtagi są w entities w text bez symbolu # https://www.playframework.com/documentation/2.6.x/ScalaJson
        var hashtags_map = hashtags.map(a => (a, 1))// do zliczania wystąpień

        return (text_len, words_len, words_set, hashtags_map)
       
    }

}



    

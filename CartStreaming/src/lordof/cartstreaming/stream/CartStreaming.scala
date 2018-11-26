
package lordof.cartstreaming.stream

import org.apache.spark._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerConfig
import scala.collection.mutable
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import lordof.cartstreaming.utilities.Utilities

object CartStreaming {
 
  def main(args: Array[String]) {
      
    
     // Configure Spark to connect to Kafka running on local machine
    val kafkaParam = new mutable.HashMap[String, String]()
    kafkaParam.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaParam.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParam.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParam.put(ConsumerConfig.GROUP_ID_CONFIG, "cartgroup")
    kafkaParam.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    kafkaParam.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")    
           
    val ssc = new StreamingContext("local[2]", "CartStreaming", Durations.seconds(10))
                
    val topicsSet = "kafka-cart".split(",").toSet                                  
           
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParam)
            
    val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)
   
    val lines = kafkaStream.map(consumerRecord => consumerRecord.value().asInstanceOf[String])
     
    val wordMap = lines.map(word => (word, 1))
    
    val wordCount = wordMap.reduceByKeyAndWindow((a:Int, b:Int) => (a + b), Durations.seconds(30),Durations.seconds(10))
           
    wordCount.foreachRDD(cart => {         
      if(cart.count() > 0){      
        val carts = new mutable.HashMap[String, Int]()  
     
        cart.saveAsTextFile("C:\\kafka-files")
        
        for(item <- cart.collect().toArray) {
          
          carts.put(item._1, item._2)                  
        
        }
        
        Utilities.post(carts)
      
      }      
    })
       
    ssc.start();
    ssc.awaitTermination();
  }
}
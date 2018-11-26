package lordof.cartstreaming.utilities

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.IOUtils
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable
import org.json4s.jackson.Serialization
import org.json4s.jackson.Json
import org.json4s.NoTypeHints
import scala.io._

object Utilities {
  
  def post(wordCount:mutable.Map[String,Int]){
    
    val client = HttpClients.createDefault()
    val uri = new URI(s"https://billing.mundipagg.com/Ext/wordcloud")
    val post = new HttpPost(uri)
    post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
      
    implicit val formats = Serialization.formats(NoTypeHints)
    
    val json = Serialization.write(wordCount)
    
    val teste = "{\"words\":"+ json +"}"
    
    print(teste)
    
    post.setEntity(new StringEntity(teste))    
            
    val result =  client.execute(post)    
  
    writeFile(wordCount)
    
  }
  
  def writeFile(wordCount:mutable.Map[String,Int]){
    
    val pathFile = "C:\\kafka-files\\cart-streaming.txt"
    
    if(!Files.exists(Paths.get(pathFile))){
      
      val file = new PrintWriter(new File(pathFile))
      
      for(( x , y ) <- wordCount) file.write(x.toString()+","+y.toString() +'\n')
        
      file.close()
      
    }else{
      
     val file = new FileWriter(pathFile,true)
     
     for(( x , y ) <- wordCount) file.write(x.toString()+","+y.toString() +'\n')
      
     file.close()
    }       
  }
}
package cc.nlplab
import com.fasterxml.jackson.databind.ObjectMapper 
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


class UnigramMap(toIdxMap:Map[String, Int], fromIdxMap:Map[Int, String]) {
  def apply(unigram:String) = toIdxMap(unigram)
  def apply(count:Int) = fromIdxMap(count)
  def contains(unigram:String) = toIdxMap contains unigram
  def contains(count:Int) = fromIdxMap contains count
}

object UnigramMap {
  def apply(jsonPath: String) = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val _to:Map[String, Int] =  mapper.readValue[Map[String,Int]](new java.io.File (jsonPath))
    val _from = _to map {_.swap}
    new UnigramMap(_to, _from)
  }
}

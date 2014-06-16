package controllers

import play.api._
import play.api.mvc._
import cc.nlplab._
import play.api.libs.json._

object Application extends Controller {

  def index = Action {
    Ok(views.html.index("hi"))
  }

  def query(q: String) = Action {
    val lgl =new Linggle("hbase-site.xml", "web1t-linggle", "web1t_unigrams_300000up.json")

    var result =lgl.query(q) 
    var s = (for(r <- result) yield r.count).sum
    val df = new java.text.DecimalFormat()
    var rows = for(r <- result) yield Json.toJson(Map(
      "count" -> Json.toJson(r.count),
      "phrase" -> Json.toJson((for((word, i) <- r.ngram.zipWithIndex) yield (if(r.positions.contains(i)) "<strong>"+word+"</strong>" else word)).mkString(" ")),
      "count_str" -> Json.toJson(df.format(r.count)),
      "percent" -> Json.toJson(if(100*r.count/s == 0) " < 1 %" else 100*r.count/s+" %")
    ))
    var json = Json.toJson(Seq(Json.toJson(Seq(Json.toJson("old"),Json.toJson(rows)))))
    Ok(json)
  }
}

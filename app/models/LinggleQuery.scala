package cc.nlplab



import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get,Scan,ResultScanner,Result}
import org.apache.hadoop.conf.Configuration
import java.io.FileInputStream

import org.apache.hadoop.hbase.util.{Bytes, Writables}




case class LinggleQuery(terms: Vector[String] , length: Int , positions: Vector[Int], filters: Vector[Tuple2[Int, String]])
{
  override def toString = "LQ(ts: \"%s\", l: %d, ps: %s, fs: [%s])" format
  (terms.mkString(" "), length, positions.mkString(""), filters.mkString(", "))
}


object LinggleQuery {

  trait Atom

  trait HereAtom extends Atom

  case class Or(terms: List[NonWildCard]) extends HereAtom {
    override def toString = terms.mkString("Or(", ", ", ")")
  }
  case object WildCard extends HereAtom
  trait NonWildCard extends HereAtom

  case class POS(pos: String) extends NonWildCard
  case class Term(term: String) extends NonWildCard

  case class Maybe(expr: HereAtom) extends Atom
  case object AnyWildCard extends Atom


  import scala.util.parsing.combinator._

  object QueryParser extends JavaTokenParsers {
    val wildCard  = "_".r ^^^ { WildCard}
    val anyWildCard = "*" ^^^ { AnyWildCard}

    val term = raw"""[a-zA-Z0-9'.]+""".r ^^ {Term(_)}
    val partOfSpeech = ("adj." | "n." | "v." | "prep." | "det." |  "adv.") ^^ { POS(_)}

    val nonWildCard = partOfSpeech | term
    val or = (nonWildCard <~ "|") ~ rep1sep(nonWildCard, raw"|")  ^^ { case t~ts => Or(t :: ts) }
    val hereAtom = wildCard | or | nonWildCard
    val maybe = "?" ~> hereAtom ^^ { Maybe(_) }

    val atom: Parser[Atom] = anyWildCard | maybe | hereAtom
    val expr: Parser[List[Atom]] = rep(atom) 
    def parse(userQuery : String) = parseAll(expr, userQuery)
  }

  def handleHereAtom(queries: List[LinggleQuery], hereAtom: HereAtom): List[LinggleQuery] =
    hereAtom match {
      case Or(nonWCs) =>
        for {
          nonWC <- nonWCs
          newLQ <- handleHereAtom(queries, nonWC)
        } yield newLQ

      case _ => 
        for {
          LinggleQuery(ts, l, ps, fs) <- queries
          newL = l + 1
          if newL <= 5
        } yield hereAtom match {
          case Term(newT) => LinggleQuery(ts :+ newT, newL, ps :+ l, fs)
          case WildCard => LinggleQuery(ts, newL, ps, fs)
          case POS(pos) => LinggleQuery(ts, newL, ps, fs :+ (l, pos))
        }
    }
  

  def handleAtom(queries: List[LinggleQuery], atom: Atom): List[LinggleQuery] =
    atom match {
      case ha:HereAtom => handleHereAtom(queries, ha)

      case Maybe(hereAtom) => for {
        newLQs <- List(queries, handleHereAtom(queries, hereAtom))
        newLQ <- newLQs
      } yield newLQ

      case AnyWildCard => for {
        LinggleQuery(ts, l, ps, fs) <- queries
        newL <- l to 5
      } yield LinggleQuery(ts, newL, ps, fs)
    }

  def parse(userQuery: String) = 
    QueryParser.parse(userQuery) map { atoms =>
      (atoms.foldLeft
        (List[LinggleQuery](LinggleQuery(Vector(), 0, Vector(), Vector())))
        (handleAtom(_,_)) 
        filter {case LinggleQuery(ts, l, ps, fs) => ts.size > 0})
    }

  def queryDemo(query: String) = {
    import scala.io.AnsiColor._
    println(QueryParser.parse(query).get)
      println(
        s"""query:$query
         |%s
         |""".format( parse(query).get.mkString("  ", "\n  ", "")).stripMargin)
    }

  def main(args: Array[String]) {
    queryDemo("a b c")
  }
}





class Linggle(hBaseConfFileName: String, table: String, unigramMapJson: String) {
  implicit object CountOrdering extends scala.math.Ordering[Row] {
  def compare(a: Row, b: Row) = - (a.count compare b.count)
}
case class Row(ngram: Vector[String], count: Int, positions: Vector[Int] ) 

  val unigramMap = UnigramMap(unigramMapJson)
  val hTable = hTableConnect(hBaseConfFileName, table)

  def hTableConnect(hBaseConfFileName: String, table: String):HTable = {
    val conf = new Configuration
    conf.addResource(new FileInputStream(hBaseConfFileName))
    val config = HBaseConfiguration.create(conf)
    new HTable(config, table)
  }

  // def merge(ss: Vector[Stream[Row]]): Stream[Row] =
  //   ss map {case (s #:: ssTail) }

  //   }

  //   (ls, rs) match {
  //   case (Stream.Empty, _) => rs
  //   case (_, Stream.Empty) => ls
  //   case (l #:: ls1, r #:: rs1) =>
  //     if (l > r) l #:: merge(ls1, rs)
  //     else r #:: merge(ls, rs1)
  // }

  def toHex(buf: Array[Byte]): String =
    buf.map("\\%02X" format _).mkString

  def scan(linggleQuery: LinggleQuery): Stream[Row] = {

    val LinggleQuery(terms, length, positions, filters) = linggleQuery
    val column = s"${length}-${positions.mkString}"
    val columnBytes = column.getBytes
    val startRow = (terms
      map (t => Bytes.toBytes(unigramMap(t)))) reduce {_++_}
    val stopRow: Array[Byte] = startRow.init :+ (startRow.last + 1).toByte
    println(linggleQuery)
    println(toHex(startRow), toHex(stopRow), column)
    
    val scan = new Scan(startRow, stopRow)
    scan.addColumn("sel".getBytes, columnBytes)
    // scan.addFamily
    val scanner = hTable.getScanner(scan)

    def resultToRow(result: Result): Row ={
      val value = new String(result.getValue("sel".getBytes, columnBytes))
      val Array(_ngram, _count) = value.split("\t")
      val count = _count.toInt
      val ngram = _ngram.split(" ").toVector
      Row(ngram, count, positions)
    }
      


    // def scanToStream(scanner: ResultScanner): Stream[Row] =
      // (scanner.next(100) map resultToRow).toStream #::: scanToStream(scanner)

    // scanToStream(scanner) take 100
    (scanner.next(100) map resultToRow ).toStream
  }

  def query(q: String): Stream[Row] = {
    val lqs: List[LinggleQuery] = LinggleQuery.parse(q).get
    ((lqs map {scan(_) take 100} ) reduce (_#:::_)).sorted
  }
}



object Tester {
  def linggle = {
    println(LinggleQuery.parse("kill the * ").get.head)

    val lgl =new Linggle("hbase-site.xml", "web1t-linggle", "web1t_unigrams_300000up.json")

    (lgl.query("kill the *") take 100).toList
  }
}



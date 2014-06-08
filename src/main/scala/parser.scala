package nlplab


case class LinggleQuery(terms: Vector[String] , length: Int , positions: Vector[Int], filters: Vector[Tuple2[Int, String]]) {
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
    def wildCard  = "_".r ^^^ { WildCard}
    def anyWildCard = "*" ^^^ { AnyWildCard}

    def term = raw"""[a-zA-Z0-9'.]+""".r ^^ {Term(_)}
    def partOfSpeech = ("adj." | "n." | "v." | "prep." | "det." |  "adv.") ^^ { POS(_)}

    def nonWildCard = partOfSpeech | term 
    def or = (nonWildCard <~ "|") ~ rep1sep(nonWildCard, raw"|")  ^^ { case t~ts => Or(t :: ts) }
    def hereAtom = wildCard | or | nonWildCard
    def maybe = "?" ~> hereAtom ^^ { Maybe(_) }

    def atom: Parser[Atom] = anyWildCard | maybe | hereAtom
    def expr: Parser[List[Atom]] = rep(atom) 
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
    QueryParser.parse(userQuery) map {
      _.foldLeft(List[LinggleQuery](LinggleQuery(Vector(), 0, Vector(), Vector()))) (handleAtom(_,_)) filter {case LinggleQuery(ts, l, ps, fs) => ts.size > 0}
    } 

// def escape(raw: String): String = {
  // import compat._ 
  // import c.universe._
  // Literal(Constant(raw)).toString
  // }

  def queryDemo(query: String) = {
    import scala.io.AnsiColor._
      println(
        s"""query:$query
         |%s
         |""".format( parse(query).get.mkString("  ", "\n  ", "")).stripMargin)
    }

  def main(args: Array[String]) {

    queryDemo("a b c")
  }


}


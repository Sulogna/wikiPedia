package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.log4j.{Logger, Level}

import org.apache.spark.rdd.RDD
import scala.util.Properties.isWin

case class WikipediaArticle(title: String, text: String) {
  /**
   * @return Whether the text of this article mentions `lang` or not
   * @param lang Language to look for (e.g. "Scala")
   */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking extends WikipediaRankingInterface :
  // Reduce Spark logging verbosity
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  //Create SparkConf
  val conf: SparkConf = new SparkConf()
    .setAppName("MySparkApplication")
    .setMaster("local[*]")
  //Create SparkContext
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.parallelize`, `WikipediaData.lines` and `WikipediaData.parse`
  //Load data using WikipediaData Object
  val lines: List[String] = WikipediaData.lines
  //Create RDD from the list of lines
  val linesRdd: RDD[String] = sc.parallelize(lines)
  //Parse lines into WikipediaArticle object
  val wikiRdd: RDD[WikipediaArticle] = linesRdd.map(WikipediaData.parse)


  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */

  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.aggregate(0)(
      (count, article) => count + (if (article.mentionsLanguage(lang)) 1 else 0),
      (count1, count2) => count1 + count2
    )
  }
  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    // Compute the occurrences of each language
    val occurrences = langs.map(lang => (lang, occurrencesOfLang(lang, rdd)))

    // Sort the list of (language, occurrence) tuples in descending order of occurrences
    val sortedOccurrences = occurrences.sortBy(-_._2)

    sortedOccurrences
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    // Create pairs of (language, article) for each language that the article mentions
    val langArticlePairs = rdd.flatMap(article =>
      langs.filter(lang => article.mentionsLanguage(lang)).map(lang => (lang, article))
    )

    // Group the pairs by language
    val index = langArticlePairs.groupByKey()

    index
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    // Map each(language, articles) pair to(language, number of articles)
    val langArticleCount = index.mapValues(articles => articles.size)

    // Collect and sort the results
    langArticleCount.collect().toList.sortBy(-_._2)
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {

    // Map each article to pairs of (language, 1) for each language it mentions
    val langPairs = rdd.flatMap(article =>
      langs.filter(lang => article.mentionsLanguage(lang)).map(lang => (lang, 1))
    )

    // Reduce by key to count the occurrences of each language
    val langCounts = langPairs.reduceByKey(_ + _)

    // Collect and sort the results
    langCounts.collect().toList.sortBy(-_._2)
  }

  def main(args: Array[String]): Unit =

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T =
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result

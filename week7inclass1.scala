import org.apache.spark._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object mov {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("C:/Temp/u.item").getLines()
    for (line <- lines) {var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }


/** Our main function where the action happens */
def main(args: Array[String]): Unit = {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf().setMaster("local[*]").setAppName("PopularMovies").set("spark.driver.host", "localhost");

  // Create a SparkContext using every core of the local machine, named PopularMovies
  // alternative: val sc = new SparkContext("local[*]", "PopularMovies")
  val sc = new SparkContext(conf)

  // Create a broadcast variable of our ID -> movie name map
  var nameDict = sc.broadcast(loadMovieNames)

  // Read in each rating line
  val lines = sc.textFile("C:/Temp/u.data")
  val movies = lines.map(x => (x.split("\t+")(1).toInt, 1))
  val movieCounts = movies.reduceByKey( (x, y) => x + y )
  val flipped = movieCounts.map( x => (x._2, x._1) )

  val sortedMovies = flipped.sortByKey()

  // Fold in the movie names from the broadcast variable
  val sortedMoviesWithNames = sortedMovies.map(x => (nameDict.value(x._2), x._1))

  val results = sortedMoviesWithNames.collect()

  results.foreach(println)
}
}
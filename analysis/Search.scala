import scala.util.matching.Regex

//import de.l3s.archivespark._
//import de.l3s.archivespark.implicits._
//import de.l3s.archivespark.enrich.functions._
//import de.l3s.archivespark.specific.warc.implicits._
//import de.l3s.archivespark.specific.warc.specs._
//import de.l3s.archivespark.specific.warc.enrichfunctions._

//val cdxPath = "/Users/ed/Projects/spn/spn/*/*.cdx.gz"
//val warcPath = "/Users/ed/Projects/spn/spn/"
//val rdd = ArchiveSpark.load(WarcCdxHdfsSpec(cdxPath, warcPath))


val urlMatch(pattern: String, url: String) : Boolean => {
    //val match = pattern.r findFirstIn url
    val m = "foo";
    if (m == None) {
        return true
    } else {
        return false
    }
}

println(urlMatch("geocities", "https://geocities.com"))




import scala.util.matching.Regex

import de.l3s.archivespark._
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.enrich.functions._
import de.l3s.archivespark.specific.warc.implicits._
import de.l3s.archivespark.specific.warc.specs._
import de.l3s.archivespark.specific.warc.enrichfunctions._

def urlMatch(pattern: String, url: String) : Boolean = {
    val m = pattern.r findFirstIn url
    if (m != None) {
        return true
    } else {
        return false
    }
}

val cdxPath = "/Users/edsu/Data/spn/*/*.cdx.gz"
val warcPath = "/Users/edsu/Data/spn"
val rdd = ArchiveSpark.load(WarcCdxHdfsSpec(cdxPath, warcPath))

var filtered = rdd.filter(r => urlMatch("geocities", r.originalUrl))

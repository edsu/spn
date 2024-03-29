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

val cdxPath = "/pylon5/ec5fp4p/edsu/spn/*/*.cdx.gz"
val warcPath = "/pylon5/ec5fp4p/edsu/spn"
val rdd = ArchiveSpark.load(WarcCdxHdfsSpec(cdxPath, warcPath))

var filtered = rdd.filter(r => urlMatch("geocities", r.originalUrl))
filtered.saveAsJson("geocities-json")
//filtered.saveWarc('geocities-warc')

sys.exit

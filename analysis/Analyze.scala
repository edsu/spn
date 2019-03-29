// Perform AUT analysis for a specified day

import io.archivesunleashed._
import io.archivesunleashed.matchbox._

// change these as needed
val date = "20131025"

val storage = "/pylon5/ec5fp4p/edsu"
// val storage = "/Users/ed/Projects/spn"

val warcsDir = storage + "/spn"
val warcs = warcsDir + "/liveweb-" + date + "*/*.warc.gz"
val outputDir = storage + "/spn-output/" + date

println(warcs)

// get valid pages
val pages = RecordLoader
  .loadArchives(warcs, sc)
  .keepValidPages()

// extract domain counts
val domainCounts = pages
  .map(r => ExtractDomain(r.getUrl))
  .countItems()
  .toDF()
  .coalesce(1)
  .write
  .format("com.databricks.spark.csv")
  .save(outputDir + "/domains/")

/*

link extraction...

val links = pages
    .map(r => (
      r.getCrawlDate, 
      ExtractLinks(r.getUrl, r.getContentString)
    ))
    .flatMap(
      r => r._2.map(
        f => (
          r._1,
          ExtractDomain(f._1).replaceAll("^\\\\s*www\\\\.", ""),
          ExtractDomain(f._2).replaceAll("^\\\\s*www\\\\.", "")
        )
      )
    )
    .filter(r => r._2 != "" && r._3 != "")
    .countItems()
    .filter(r => r._2 > 5)

WriteGraphML(links, outputDir + "/gephi/")

*/

sys.exit()
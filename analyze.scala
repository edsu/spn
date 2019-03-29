# Perform AUT analysis for a specified day

import io.archivesunleashed._
import io.archivesunleashed.matchbox._

// change this as needed
val date = "20131025"

val warcs = "/pylon5/ec5fp4p/edsu/spn/liveweb-" + date + "*/*.warc.gz"
val output_dir = "/pylon5/ec5fp4p/edsu/spn-output/" + date

// domains

val r = RecordLoader.loadArchives(warc_files, sc)
  .keepValidPages()
  .map(r => ExtractDomain(r.getUrl))
  .countItems()
  .saveAsTextFile(output_dir + "/domains/")

// more analysis!



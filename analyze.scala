# loosely based on

import io.archivesunleashed._
import io.archivesunleashed.matchbox._

val date = "20131025"

val warcs = "/pylon5/ec5fp4p/edsu/spn/liveweb-" + date + "*/*.warc.gz"
val output_dir = "/pylon5/ec5fp4p/edsu/spn-output/" + date

# domains

val r = RecordLoader.loadArchives(warc_files, sc)
  .keepValidPages()
  .map(r => ExtractDomain(r.getUrl))
  .countItems()
  .saveAsTextFile(output_dir + "/domains/")

# tbd



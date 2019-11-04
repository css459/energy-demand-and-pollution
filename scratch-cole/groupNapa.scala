// Selects Napa Valley by yearly sample averages
import bdad.etl.airdata.AirDataset
val airdata = new AirDataset(AirDataset.makeYearList(2010,2019), "gasses/*")
val napa = df.filter(f => f.toString.toLowerCase.contains("napa"))
napa.select("dateGMT", "value").groupBy(year(col("dateGMT"))).mean().show

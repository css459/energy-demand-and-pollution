import bdad.etl.airdata.AirDataset

val airdata = new AirDataset(2019, "gasses/*")
val pivot = airdata.pivotedDF(dropNull = true, dropUnit = true)
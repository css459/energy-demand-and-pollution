# BDAD Final Project

## Workflow

1. Load your environment:
    `source modload.sh`
    <br/>
2. Build the project:
    `sbt package`
    <br/>
3. Submit the job using `submit.sh`:
    * Make sure to edit main class in submit file
    * `./submit.sh`
    
4. Optional: `./spark-shell.sh` to have access to shell with this project
 classes available.
 
 ## Scenario Analysis
 
 Analysis of data sets defined by the `bdad.etl.Scenarios`
 package are available in `doc/`. This includes a more
 in-depth look at the underlying signal(s) within the
 scenario.
 
 Currently this is defined for the Criteria Gasses
 (2014-2019) data set.

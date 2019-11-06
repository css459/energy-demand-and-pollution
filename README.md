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

# reconciliation-service

Perform Reconciliation between data sources based on pre-defined rules

Currently operates only on excel and csv files.
Uses **Apache POI** to pass rows to **sparkSession** to build a dataframe.
Spark session referes to Apache Spark.


Based on rules defined in zestRecon.yml reconciliation is done between two excel files.
You can modify the rules
### Getting Started
- To build the project load all the maven dependencies in pom.xml.
- Project must be build in java 11.
- Edit file `paths` path\to\excel\files in `src/main/java/com/service/reconciliation_service/ReconService.java`.
- Create your own config file similar to zestRecon.yml based on your use case.
- Run the `ReconService.java` file. Results will be saved in `ReconLog.txt` file

# reconciliation-service
Perform Reconciliation between data sources based on pre-defined rules

Currently operates only on excel files.
Uses Apache POI to pass rows to sparkSession to build a dataframe.


Based on rules defined in zestRecon.yml reconciliation is done between two excel files.
You can modify the rules

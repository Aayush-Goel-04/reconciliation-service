    package com.pchf.reconciliationservice.controllers;

    import com.opencsv.CSVReader;
    import com.opencsv.exceptions.CsvValidationException;
    import org.jetbrains.annotations.NotNull;
    import org.springframework.web.multipart.MultipartFile;
    import org.yaml.snakeyaml.Yaml;

    import java.io.FileInputStream;
    import java.io.IOException;
    import java.io.InputStream;
    import java.io.InputStreamReader;
    import java.time.LocalDate;
    import java.time.format.DateTimeFormatter;
    import java.util.*;

    import lombok.extern.log4j.Log4j2;
    import org.bson.Document;
    import org.springframework.http.HttpStatus;
    import org.springframework.http.ResponseEntity;
    import org.springframework.web.bind.annotation.PostMapping;
    import org.springframework.web.bind.annotation.RequestParam;
    import org.springframework.web.bind.annotation.RestController;

    import com.mongodb.ConnectionString;
    import com.mongodb.client.MongoClients;
    import com.mongodb.client.MongoCollection;
    import com.mongodb.client.MongoDatabase;

    @Log4j2
    @RestController
    public class ReconciliationServiceController {

        private final String mongodbConnectionString = "mongodb+srv://prf_intengg_mongo_dev_common_user:dev001mongo001prfreadpaswd@piramal-mongo-dev-cluster-pl-0.fh5op.mongodb.net/%3Clead_db%3E?retryWrites=true&w=majority";
        private final String databaseName = "reconciliation_db";
        private final String collectionName = "file";
        private final String validationFilePath = "/Users/hanan.basheer/Documents/Code/Piramal/EF/reconciliationservice/src/main/java/com/pchf/reconciliationservice/config/validation.yml";

        @PostMapping("/v1/uploadCSV")
        public ResponseEntity<String> uploadCSVData(@RequestParam("file") MultipartFile file) {
            try {
                // Check if the file name starts with a part    icular name and ends with ".csv"
                String fileName = file.getOriginalFilename();
                if (fileName == null || !fileName.toLowerCase().endsWith(".csv")) {
                    throw new IllegalArgumentException("Invalid file name");
                }

                // Read and process the CSV file
                List<Document> entries = processCSV(file, fileName);

                // Save the entries in the MongoDB database
                if (!entries.isEmpty()) {
                    appendEntriesToMongoDB(entries);
                }

                // Return success response
                return ResponseEntity.ok("CSV data Ok");
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().body("Invalid file: " + e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to upload CSV data");
            }
        }

        private @NotNull List<Document> processCSV(MultipartFile file, String fileName) throws Exception {
            List<Document> entries = new ArrayList<>();

            // Read the YAML file
            Yaml yaml = new Yaml();
            Map<String, Object> validationFile;
            try (InputStream inputStream = new FileInputStream(validationFilePath)) {
                validationFile = yaml.load(inputStream);
            }

            // Check if the file name is present in the list of file names
            if (validationFile.containsKey("validation") && validationFile.get("validation") instanceof Map) {
                Map<String, Object> validation = (Map<String, Object>) validationFile.get("validation");

                Map<String, List<String>> columnNamesMap = getColumnNamesFromValidation(validation);
                List<String> expectedColumnNames = columnNamesMap.getOrDefault(fileName, Collections.emptyList());
                List<String> actualColumnNames = getColumnNamesFromCSV(file);

                // Check if the actual column names contain all the expected column names
                if (actualColumnNames.containsAll(expectedColumnNames)) {
                    // Perform the necessary operations based on the rules
                    // Write an entry in the MongoDB database
                    Document entry = new Document();
                    entry.append("file_name", file.getOriginalFilename());

                    // Enter today's date
                    LocalDate today = LocalDate.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                    String formattedDate = today.format(formatter);
                    entry.append("created_date", formattedDate);

                    entry.append("file_doc_id", "YOUR_FILE_DOC_ID");
                    entry.append("file_availibility", true);
                    entries.add(entry);
                    log.info("File is stored");
                } else {
                    log.warn("Column names do not match the validation rules");
                }
            }

            return entries;
        }

        private Map<String, List<String>> getColumnNamesFromValidation(@NotNull Map<String, Object> validation) {
            Map<String, List<String>> columnNamesMap = new HashMap<>();

            Map<String, List<String>> validationMap = (Map<String, List<String>>) validation.get("columnNames");
            if (validationMap != null) {
                for (Map.Entry<String, List<String>> entry : validationMap.entrySet()) {
                    String fileName = entry.getKey();
                    List<String> columnNames = entry.getValue();
                    columnNamesMap.put(fileName, columnNames);
                }
            }

            return columnNamesMap;
        }

        private @NotNull List<String> getColumnNamesFromCSV(MultipartFile file) throws IOException, CsvValidationException {
            List<String> columnNames = new ArrayList<>();

            try (CSVReader reader = new CSVReader(new InputStreamReader(file.getInputStream()))) {
                String[] headers = reader.readNext();
                if (headers != null) {
                    columnNames.addAll(Arrays.asList(headers));
                }
            }

            return columnNames;
        }

        private void appendEntriesToMongoDB(@NotNull List<Document> entries) {
            try (com.mongodb.client.MongoClient mongoClient = MongoClients.create(new ConnectionString(mongodbConnectionString))) {
                MongoDatabase database = mongoClient.getDatabase(databaseName);
                MongoCollection<Document> collection = database.getCollection(collectionName);
                // Get the current count of entries in the file collection
                long currentCount = collection.countDocuments();

                // Set the file_id as the current count + 1
                long fileId = currentCount + 1;
                for (Document entry : entries) {
                    // Set the file_id in each entry
                    entry.append("file_id", fileId);
                    collection.insertOne(entry);
                }
            }
        }
    }

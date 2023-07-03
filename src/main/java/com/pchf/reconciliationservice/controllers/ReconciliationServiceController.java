package com.pchf.reconciliationservice.controllers;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.bind.annotation.RestController;
import org.yaml.snakeyaml.Yaml;

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

    @PostMapping("/v1/uploadCSV")
    public ResponseEntity<String> uploadCSVData(@RequestParam("file") MultipartFile file) {
        try {
            // Check if the file name starts with a particular name and ends with ".csv"
            String fileName = file.getOriginalFilename();
            if (fileName == null || !fileName.startsWith("bank") || !fileName.toLowerCase().endsWith(".csv")) {
                throw new IllegalArgumentException("Invalid file name");
            }

            // Read and process the CSV file
            List<Document> entries = processCSV(file);

            // Save the entries in the MongoDB database
            if (!entries.isEmpty()) {
                appendEntriesToMongoDB(entries);
            }

            // Return success response
            return ResponseEntity.ok("CSV data uploaded successfully");
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body("Invalid file: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to upload CSV data");
        }
    }

    private List<Document> processCSV(MultipartFile file) throws Exception {
        List<Document> entries = new ArrayList<>();

        // Specify the path to the validation.yml file
        String validationFilePath = "/Users/hanan.basheer/Documents/Code/Piramal/EF/reconciliationservice/src/main/java/com/pchf/reconciliationservice/config/validation.yml";

        // Read the YAML file
        Yaml yaml = new Yaml();
        Map<String, Object> validationFile;
        try (InputStream inputStream = new FileInputStream(validationFilePath)) {
            validationFile = yaml.load(inputStream);
        }
        log.info("File before validation");
        // Check if the file name is present in the list of file names
        if (validationFile.containsKey("validation") && validationFile.get("validation") instanceof Map) {
            Map<String, Object> validation = (Map<String, Object>) validationFile.get("validation");
            log.info("Within validation");
            if (validation.containsKey("fileNames") && validation.get("fileNames") instanceof List) {
                log.info("within filenames");
                List<String> fileNames = (List<String>) validation.get("fileNames");

                // Check for partial match of the file name
                boolean isPartialMatch = false;
                for (String fileName : fileNames) {
                    if (file.getOriginalFilename().toLowerCase().contains(fileName.toLowerCase())) {
                        isPartialMatch = true;
                        break;
                    }
                }

                if (isPartialMatch) {
                    // Perform the necessary operations based on the rules
                    // Write an entry in the MongoDB database
                    Document entry = new Document();
                    entry.append("file_id", "YOUR_FILE_ID");
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
                }
            }
        }

        return entries;
    }

    private void appendEntriesToMongoDB(List<Document> entries) {
        try (com.mongodb.client.MongoClient mongoClient = MongoClients.create(new ConnectionString(mongodbConnectionString))) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            for (Document entry : entries) {
                collection.insertOne(entry);
            }
        }
    }
}

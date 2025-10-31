package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	log.Println("🧪 Testing Progress Calculation for New Processes...")

	baseURL := "http://localhost:8080"

	// Test file_import job
	fileImportJob := map[string]interface{}{
		"fileName":    "test_import.xlsx",
		"processType": "file_import",
	}

	// Test report_gen job
	reportGenJob := map[string]interface{}{
		"fileName":    "monthly_report.pdf",
		"processType": "report_gen",
	}

	// Create file_import job
	log.Println("📋 Creating file_import job...")
	fileImportID := createJob(baseURL, fileImportJob)

	// Create report_gen job
	log.Println("📋 Creating report_gen job...")
	reportGenID := createJob(baseURL, reportGenJob)

	// Monitor progress for 15 seconds
	log.Println("👀 Monitoring progress for 15 seconds...")
	for i := 0; i < 15; i++ {
		time.Sleep(1 * time.Second)

		// Check file_import progress
		fileImportJob := getJob(baseURL, fileImportID)
		if fileImportJob != nil {
			log.Printf("📊 File Import: %s [%s] %d%% - %s",
				fileImportJob["fileName"],
				fileImportJob["status"],
				int(fileImportJob["progress"].(float64)),
				fileImportJob["currentStepName"])
		}

		// Check report_gen progress
		reportJob := getJob(baseURL, reportGenID)
		if reportJob != nil {
			log.Printf("📊 Report Gen: %s [%s] %d%% - %s",
				reportJob["fileName"],
				reportJob["status"],
				int(reportJob["progress"].(float64)),
				reportJob["currentStepName"])
		}

		log.Println("---")
	}

	log.Println("🎉 Progress monitoring completed!")
}

func createJob(baseURL string, jobData map[string]interface{}) string {
	jsonData, _ := json.Marshal(jobData)
	resp, err := http.Post(baseURL+"/jobs", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("❌ Error creating job: %v", err)
		return ""
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if job, ok := result["job"].(map[string]interface{}); ok {
		return job["id"].(string)
	}
	return ""
}

func getJob(baseURL string, jobID string) map[string]interface{} {
	if jobID == "" {
		return nil
	}

	resp, err := http.Get(fmt.Sprintf("%s/jobs/%s", baseURL, jobID))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if job, ok := result["job"].(map[string]interface{}); ok {
		return job
	}
	return nil
}

package process

import (
	"time"
)

// Pre-defined process configurations
var ProcessConfigurations = map[string]*JobProcessConfig{
	"data_analysis": NewDataAnalysisProcess(),
	"file_import":   NewFileImportProcess(),
	"report_gen":    NewReportGenerationProcess(),
}

// NewDataAnalysisProcess creates the default data analysis job configuration
func NewDataAnalysisProcess() *JobProcessConfig {
	constants := DefaultProcessConstants()

	return &JobProcessConfig{
		ProcessName: "Data Analysis",
		Steps: []JobStepConfig{
			{
				Name:        "DOWNLOAD_SOURCE",
				Description: "กำลังดาวน์โหลดไฟล์ต้นฉบับ",
				SubSteps: []JobSubStepConfig{
					{Name: "DOWNLOAD_SOURCE_CONNECTING", Description: "กำลังเชื่อมต่อกับเซิร์ฟเวอร์", Duration: constants.StepProcessingTime / 2},
					{Name: "DOWNLOAD_SOURCE_DOWNLOADING", Description: "กำลังดาวน์โหลดไฟล์", Duration: constants.StepProcessingTime},
					{Name: "DOWNLOAD_SOURCE_VALIDATING", Description: "กำลังตรวจสอบไฟล์ที่ดาวน์โหลด", Duration: constants.StepProcessingTime / 2},
					{Name: "DOWNLOAD_SOURCE_COMPLETED", Description: "ดาวน์โหลดไฟล์เสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "DECOMPRESS_FILE",
				Description: "กำลังแตกไฟล์ข้อมูล",
				SubSteps: []JobSubStepConfig{
					{Name: "DECOMPRESS_FILE_READING", Description: "กำลังอ่านไฟล์บีบอัด", Duration: constants.StepProcessingTime / 2},
					{Name: "DECOMPRESS_FILE_EXTRACTING", Description: "กำลังแตกไฟล์", Duration: constants.StepProcessingTime},
					{Name: "DECOMPRESS_FILE_VERIFYING", Description: "กำลังตรวจสอบไฟล์ที่แตก", Duration: constants.StepProcessingTime / 2},
					{Name: "DECOMPRESS_FILE_COMPLETED", Description: "แตกไฟล์เสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "CLEANING_DATA",
				Description: "กำลังทำความสะอาดข้อมูล",
				SubSteps: []JobSubStepConfig{
					{Name: "CLEANING_DATA_SCANNING", Description: "กำลังสแกนหาข้อมูลที่ผิดปกติ", Duration: constants.StepProcessingTime / 2},
					{Name: "CLEANING_DATA_FILTERING", Description: "กำลังกรองข้อมูลที่ไม่ถูกต้อง", Duration: constants.StepProcessingTime},
					{Name: "CLEANING_DATA_NORMALIZING", Description: "กำลังปรับรูปแบบข้อมูล", Duration: constants.StepProcessingTime / 2},
					{Name: "CLEANING_DATA_COMPLETED", Description: "ทำความสะอาดข้อมูลเสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "ANALYSIS_MODEL_A",
				Description: "กำลังวิเคราะห์ด้วยโมเดล A",
				SubSteps: []JobSubStepConfig{
					{Name: "ANALYSIS_MODEL_A_LOADING", Description: "กำลังโหลดโมเดลวิเคราะห์ A", Duration: constants.StepProcessingTime / 2},
					{Name: "ANALYSIS_MODEL_A_PROCESSING", Description: "กำลังประมวลผลด้วยโมเดล A", Duration: constants.StepProcessingTime},
					{Name: "ANALYSIS_MODEL_A_CALCULATING", Description: "กำลังคำนวณผลลัพธ์โมเดล A", Duration: constants.StepProcessingTime / 2},
					{Name: "ANALYSIS_MODEL_A_COMPLETED", Description: "วิเคราะห์ด้วยโมเดล A เสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "ANALYSIS_MODEL_B",
				Description: "กำลังวิเคราะห์ด้วยโมเดล B",
				SubSteps: []JobSubStepConfig{
					{Name: "ANALYSIS_MODEL_B_LOADING", Description: "กำลังโหลดโมเดลวิเคราะห์ B", Duration: constants.StepProcessingTime / 2},
					{Name: "ANALYSIS_MODEL_B_PROCESSING", Description: "กำลังประมวลผลด้วยโมเดล B", Duration: constants.StepProcessingTime},
					{Name: "ANALYSIS_MODEL_B_CALCULATING", Description: "กำลังคำนวณผลลัพธ์โมเดล B", Duration: constants.StepProcessingTime / 2},
					{Name: "ANALYSIS_MODEL_B_COMPLETED", Description: "วิเคราะห์ด้วยโมเดล B เสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "GENERATING_REPORT",
				Description: "กำลังสร้างรายงาน",
				SubSteps: []JobSubStepConfig{
					{Name: "GENERATING_REPORT_COLLECTING", Description: "กำลังรวบรวมผลการวิเคราะห์", Duration: constants.StepProcessingTime / 2},
					{Name: "GENERATING_REPORT_FORMATTING", Description: "กำลังจัดรูปแบบรายงาน", Duration: constants.StepProcessingTime},
					{Name: "GENERATING_REPORT_FINALIZING", Description: "กำลังจัดเรียงรายงานขั้นสุดท้าย", Duration: constants.StepProcessingTime / 2},
					{Name: "GENERATING_REPORT_COMPLETED", Description: "สร้างรายงานเสร็จสิ้น", Duration: 0},
				},
			},
		},
	}
}

// NewFileImportProcess creates a simple file import job configuration
func NewFileImportProcess() *JobProcessConfig {
	return &JobProcessConfig{
		ProcessName: "File Import",
		Steps: []JobStepConfig{
			{
				Name:        "UPLOAD_FILE",
				Description: "กำลังอัพโหลดไฟล์",
				SubSteps: []JobSubStepConfig{
					{Name: "UPLOAD_FILE_VALIDATING", Description: "กำลังตรวจสอบไฟล์", Duration: time.Second},
					{Name: "UPLOAD_FILE_UPLOADING", Description: "กำลังอัพโหลด", Duration: 3 * time.Second},
					{Name: "UPLOAD_FILE_COMPLETED", Description: "อัพโหลดเสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "PROCESS_DATA",
				Description: "กำลังประมวลผลข้อมูล",
				SubSteps: []JobSubStepConfig{
					{Name: "PROCESS_DATA_PARSING", Description: "กำลังแปลงข้อมูล", Duration: 2 * time.Second},
					{Name: "PROCESS_DATA_IMPORTING", Description: "กำลังนำเข้าข้อมูล", Duration: 3 * time.Second},
					{Name: "PROCESS_DATA_COMPLETED", Description: "ประมวลผลเสร็จสิ้น", Duration: 0},
				},
			},
		},
	}
}

// NewReportGenerationProcess creates a report generation job configuration
func NewReportGenerationProcess() *JobProcessConfig {
	return &JobProcessConfig{
		ProcessName: "Report Generation",
		Steps: []JobStepConfig{
			{
				Name:        "COLLECT_DATA",
				Description: "กำลังรวบรวมข้อมูล",
				SubSteps: []JobSubStepConfig{
					{Name: "COLLECT_DATA_QUERYING", Description: "กำลังค้นหาข้อมูล", Duration: 2 * time.Second},
					{Name: "COLLECT_DATA_AGGREGATING", Description: "กำลังรวมข้อมูล", Duration: 3 * time.Second},
					{Name: "COLLECT_DATA_COMPLETED", Description: "รวบรวมข้อมูลเสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "GENERATE_CHARTS",
				Description: "กำลังสร้างกราฟ",
				SubSteps: []JobSubStepConfig{
					{Name: "GENERATE_CHARTS_CREATING", Description: "กำลังสร้างกราฟ", Duration: 2 * time.Second},
					{Name: "GENERATE_CHARTS_STYLING", Description: "กำลังตกแต่งกราฟ", Duration: time.Second},
					{Name: "GENERATE_CHARTS_COMPLETED", Description: "สร้างกราฟเสร็จสิ้น", Duration: 0},
				},
			},
			{
				Name:        "EXPORT_REPORT",
				Description: "กำลังส่งออกรายงาน",
				SubSteps: []JobSubStepConfig{
					{Name: "EXPORT_REPORT_FORMATTING", Description: "กำลังจัดรูปแบบ", Duration: time.Second},
					{Name: "EXPORT_REPORT_SAVING", Description: "กำลังบันทึกไฟล์", Duration: 2 * time.Second},
					{Name: "EXPORT_REPORT_COMPLETED", Description: "ส่งออกรายงานเสร็จสิ้น", Duration: 0},
				},
			},
		},
	}
}

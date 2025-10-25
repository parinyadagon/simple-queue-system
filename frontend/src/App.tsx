import { useState, useEffect, useCallback } from "react";

// 1. ตรงกับ struct `domain.Job` ของ Go
interface Job {
  id: string;
  file_name: string;
  status: "PENDING" | "RUNNING" | "PAUSED" | "FAILED" | "CANCELED" | "COMPLETED";
  progress: number;
  created_at: string;
}

// 2. Hook สำหรับ WebSocket
function useJobSocket(onJobUpdate: (job: Job) => void) {
  useEffect(() => {
    // (ใช้ wss:// ถ้าเป็น production)
    const ws = new WebSocket("ws://localhost:8080/ws/status");

    ws.onopen = () => console.log("WebSocket Connected");
    ws.onclose = () => console.log("WebSocket Disconnected");

    ws.onmessage = (event) => {
      try {
        const job = JSON.parse(event.data) as Job;
        onJobUpdate(job); // เรียก Callback เมื่อมี message
      } catch (error) {
        console.error("Failed to parse WebSocket message:", error);
      }
    };

    return () => {
      ws.close(); // ปิดการเชื่อมต่อเมื่อ Component unmount
    };
  }, [onJobUpdate]);
}

export default function App() {
  const [jobs, setJobs] = useState<Record<string, Job>>({}); // ใช้ Record/Object เพื่อ lookup O(1)

  // 3. Callback ที่จะถูกเรียกโดย WebSocket
  const handleJobUpdate = useCallback((updatedJob: Job) => {
    setJobs((currentJobs) => ({
      ...currentJobs,
      [updatedJob.id]: updatedJob,
    }));
  }, []);

  // 4. เชื่อมต่อ WebSocket
  useJobSocket(handleJobUpdate);

  // 5. ดึงข้อมูล Job ทั้งหมดครั้งแรก
  useEffect(() => {
    fetch("http://localhost:8080/jobs")
      .then((res) => res.json())
      .then((initialJobs: Job[]) => {
        const jobsMap = initialJobs.reduce((acc, job) => {
          acc[job.id] = job;
          return acc;
        }, {} as Record<string, Job>);
        setJobs(jobsMap);
      });
  }, []);

  // 6. ฟังก์ชันควบคุม
  const createJob = () => {
    fetch("http://localhost:8080/jobs", { method: "POST" });
  };

  const controlJob = (id: string, command: "PAUSE" | "RESTART" | "CANCEL") => {
    fetch(`http://localhost:8080/jobs/${id}/control`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command }),
    });
  };

  const jobList = Object.values(jobs).sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());

  return (
    <div className="bg-gray-900 text-white min-h-screen p-8">
      <div className="max-w-4xl mx-auto">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-3xl font-bold">Job Processing Dashboard</h1>
          <button onClick={createJob} className="bg-blue-600 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded">
            Create New Job
          </button>
        </div>

        <div className="bg-gray-800 rounded-lg shadow-lg overflow-hidden">
          <ul className="divide-y divide-gray-700">
            {jobList.map((job) => (
              <JobItem key={job.id} job={job} onControl={controlJob} />
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
}

// --- Component ลูก (JobItem) ---

interface JobItemProps {
  job: Job;
  onControl: (id: string, command: "PAUSE" | "RESTART" | "CANCEL") => void;
}

function JobItem({ job, onControl }: JobItemProps) {
  const getStatusColor = (status: Job["status"]) => {
    switch (status) {
      case "RUNNING":
        return "text-blue-400";
      case "COMPLETED":
        return "text-green-400";
      case "PAUSED":
        return "text-yellow-400";
      case "CANCELED":
        return "text-gray-500";
      case "FAILED":
        return "text-red-400";
      default:
        return "text-gray-400";
    }
  };

  return (
    <li className="p-4 flex flex-col sm:flex-row justify-between items-start sm:items-center">
      <div className="flex-1 mb-4 sm:mb-0">
        <div className="flex items-center">
          <span className={`font-bold ${getStatusColor(job.status)}`}>{job.status}</span>
          <span className="ml-3 text-gray-400 text-sm">{job.id}</span>
        </div>
        <div className="text-lg text-gray-200">{job.file_name}</div>

        {/* Progress Bar */}
        {(job.status === "RUNNING" || job.status === "COMPLETED") && (
          <div className="w-full bg-gray-700 rounded-full h-2.5 mt-2">
            <div className="bg-blue-500 h-2.5 rounded-full" style={{ width: `${job.progress}%`, transition: "width 0.3s" }}></div>
          </div>
        )}
      </div>

      {/* Control Buttons */}
      <div className="flex space-x-2">
        {job.status === "RUNNING" && (
          <button onClick={() => onControl(job.id, "PAUSE")} className="bg-yellow-600 hover:bg-yellow-700 text-white text-sm py-1 px-3 rounded">
            Pause
          </button>
        )}
        {job.status === "PAUSED" && (
          <button onClick={() => onControl(job.id, "RESTART")} className="bg-green-600 hover:bg-green-700 text-white text-sm py-1 px-3 rounded">
            Restart
          </button>
        )}
        {(job.status === "RUNNING" || job.status === "PAUSED" || job.status === "PENDING") && (
          <button onClick={() => onControl(job.id, "CANCEL")} className="bg-red-600 hover:bg-red-700 text-white text-sm py-1 px-3 rounded">
            Cancel
          </button>
        )}
      </div>
    </li>
  );
}

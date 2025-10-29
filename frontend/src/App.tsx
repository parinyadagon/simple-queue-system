import { useState, useEffect, useCallback } from "react";

// 1. ตรงกับ struct `domain.Job` ของ Go
interface Job {
  id: string;
  file_name: string;
  status: "PENDING" | "RUNNING" | "PAUSED" | "FAILED" | "CANCELED" | "COMPLETED";
  progress: number;
  created_at: string;
  current_step_name?: string;
}

// 2. Interface สำหรับ Job Statistics
interface JobStats {
  total: number;
  pending: number;
  running: number;
  paused: number;
  completed: number;
  failed: number;
  canceled: number;
}

// 3. Hook สำหรับ WebSocket
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

  // 4. คำนวณ Statistics จาก jobs
  const jobStats: JobStats = (jobs ? Object.values(jobs) : []).reduce(
    (stats, job) => {
      stats.total++;
      switch (job.status) {
        case "PENDING":
          stats.pending++;
          break;
        case "RUNNING":
          stats.running++;
          break;
        case "PAUSED":
          stats.paused++;
          break;
        case "COMPLETED":
          stats.completed++;
          break;
        case "FAILED":
          stats.failed++;
          break;
        case "CANCELED":
          stats.canceled++;
          break;
      }
      return stats;
    },
    { total: 0, pending: 0, running: 0, paused: 0, completed: 0, failed: 0, canceled: 0 }
  );

  // 5. Callback ที่จะถูกเรียกโดย WebSocket
  const handleJobUpdate = useCallback((updatedJob: Job) => {
    setJobs((currentJobs) => ({
      ...currentJobs,
      [updatedJob.id]: updatedJob,
    }));
  }, []);

  // 6. เชื่อมต่อ WebSocket
  useJobSocket(handleJobUpdate);

  // 7. ดึงข้อมูล Job ทั้งหมดครั้งแรก
  useEffect(() => {
    fetch("http://localhost:8080/jobs")
      .then((res) => res.json())
      .then((initialJobs: Job[] | null) => {
        if (initialJobs && Array.isArray(initialJobs)) {
          const jobsMap = initialJobs.reduce((acc, job) => {
            acc[job.id] = job;
            return acc;
          }, {} as Record<string, Job>);
          setJobs(jobsMap);
        } else {
          setJobs({});
        }
      })
      .catch((error) => {
        console.error("Failed to fetch jobs:", error);
        setJobs({});
      });
  }, []);

  // 8. ฟังก์ชันควบคุม
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

  const jobList = (jobs ? Object.values(jobs) : []).sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());

  return (
    <div className="bg-gray-900 text-white min-h-screen p-8">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-3xl font-bold">Job Processing Dashboard</h1>
          <button onClick={createJob} className="bg-blue-600 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded">
            Create New Job
          </button>
        </div>

        {/* Job Statistics Cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-4 mb-6">
          <StatCard label="Total" count={jobStats.total} color="bg-gray-700" />
          <StatCard label="Pending" count={jobStats.pending} color="bg-gray-600" />
          <StatCard label="Running" count={jobStats.running} color="bg-blue-600" />
          <StatCard label="Paused" count={jobStats.paused} color="bg-yellow-600" />
          <StatCard label="Completed" count={jobStats.completed} color="bg-green-600" />
          <StatCard label="Failed" count={jobStats.failed} color="bg-red-600" />
          <StatCard label="Canceled" count={jobStats.canceled} color="bg-gray-500" />
        </div>

        {/* Job List */}
        <div className="bg-gray-800 rounded-lg shadow-lg overflow-hidden">
          {jobList.length === 0 ? (
            <div className="p-8 text-center text-gray-400">
              <p className="text-lg">No jobs found</p>
              <p className="text-sm mt-2">Click "Create New Job" to get started</p>
            </div>
          ) : (
            <ul className="divide-y divide-gray-700">
              {jobList.map((job) => (
                <JobItem key={job.id} job={job} onControl={controlJob} />
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}

// --- Statistics Card Component ---
interface StatCardProps {
  label: string;
  count: number;
  color: string;
}

function StatCard({ label, count, color }: StatCardProps) {
  return (
    <div className={`${color} rounded-lg p-4 text-center`}>
      <div className="text-2xl font-bold text-white">{count}</div>
      <div className="text-sm text-gray-200 uppercase tracking-wide">{label}</div>
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

        {/* Current Step Name */}
        {job.current_step_name && <div className="text-sm text-blue-300 mt-1">{job.current_step_name}</div>}

        {/* Progress Bar */}
        {(job.status === "RUNNING" || job.status === "COMPLETED") && (
          <div className="w-full bg-gray-700 rounded-full h-2.5 mt-2">
            <div className="bg-blue-500 h-2.5 rounded-full" style={{ width: `${job.progress}%`, transition: "width 0.3s" }}></div>
            <div className="text-xs text-gray-400 mt-1 text-right">{job.progress}%</div>
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

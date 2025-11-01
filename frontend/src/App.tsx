import { useState, useEffect, useCallback } from "react";

// 1. ตรงกับ struct `domain.Job` ของ Go
interface Job {
  id: string;
  process_type: string; // NEW: Process type
  process_version: string; // NEW: Process version
  file_name: string;
  status: "PENDING" | "RUNNING" | "PAUSED" | "FAILED" | "CANCELED" | "COMPLETED";
  progress: number;
  created_at: string;
  current_step_name?: string;
  current_main_step?: string; // NEW: Main step name
  current_sub_step?: string; // NEW: Sub step name
}

// 2. Interface สำหรับ Process
interface Process {
  id: string;
  name: string;
  description: string;
  steps: number;
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
  const [processes, setProcesses] = useState<Process[]>([]);
  const [selectedProcess, setSelectedProcess] = useState<string>("all");
  const [loading, setLoading] = useState(false);

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

  // 7. ดึงข้อมูล Processes
  useEffect(() => {
    fetch("http://localhost:8080/processes")
      .then((res) => res.json())
      .then((processData: Process[]) => {
        setProcesses(processData || []);
      })
      .catch((error) => {
        console.error("Failed to fetch processes:", error);
        setProcesses([]);
      });
  }, []);

  // 8. ดึงข้อมูล Jobs (filtered by process)
  const fetchJobs = useCallback(() => {
    const url = selectedProcess === "all" ? "http://localhost:8080/jobs" : `http://localhost:8080/jobs?process_type=${selectedProcess}`;

    fetch(url)
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
  }, [selectedProcess]);

  // 9. เรียก fetchJobs เมื่อ selectedProcess เปลี่ยน
  useEffect(() => {
    fetchJobs();
  }, [fetchJobs]);

  // 10. ฟังก์ชันควบคุม
  const createJob = (processType: string = "data_analysis") => {
    setLoading(true);
    fetch("http://localhost:8080/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        file_name: `sample_${processType}_${Date.now()}.csv`,
        process_type: processType,
      }),
    }).finally(() => setLoading(false));
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
    <div className="bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 text-white min-h-screen">
      {/* Animated Background Pattern */}
      <div className="absolute inset-0 overflow-hidden">
        <div className="absolute -top-40 -right-40 w-80 h-80 bg-purple-500 rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-pulse"></div>
        <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-blue-500 rounded-full mix-blend-multiply filter blur-xl opacity-20 animate-pulse animation-delay-2000"></div>
        <div className="absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 w-80 h-80 bg-pink-500 rounded-full mix-blend-multiply filter blur-xl opacity-10 animate-pulse animation-delay-4000"></div>
      </div>

      <div className="relative z-10 p-6 sm:p-8">
        <div className="max-w-7xl mx-auto">
          {/* Modern Header */}
          <div className="flex flex-col lg:flex-row justify-between items-start lg:items-center mb-10 space-y-6 lg:space-y-0">
            <div className="space-y-2">
              <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold bg-gradient-to-r from-blue-400 via-purple-400 to-pink-400 bg-clip-text text-transparent leading-tight">
                Job Processing Dashboard
              </h1>
              <p className="text-gray-400 text-lg sm:text-xl">Real-time job monitoring and control center</p>
              <div className="flex items-center space-x-2 text-sm text-gray-500">
                <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse"></div>
                <span>Live updates enabled</span>
              </div>
            </div>
            <div className="flex flex-col sm:flex-row gap-4">
              {/* Process Selector */}
              <div className="relative">
                <select
                  value={selectedProcess}
                  onChange={(e) => setSelectedProcess(e.target.value)}
                  className="bg-slate-800 border border-slate-700 text-white rounded-xl px-4 py-2 pr-8 appearance-none focus:outline-none focus:ring-2 focus:ring-purple-500 transition-all duration-200">
                  <option value="all">All Processes</option>
                  {processes.map((process) => (
                    <option key={process.id} value={process.id}>
                      {process.name}
                    </option>
                  ))}
                </select>
                <div className="absolute inset-y-0 right-0 flex items-center px-2 pointer-events-none">
                  <svg className="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </div>
              </div>

              {/* Create Job Dropdown */}
              <div className="relative group">
                <button
                  className="group relative bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white font-semibold py-4 px-8 rounded-2xl shadow-2xl transform hover:scale-105 hover:-translate-y-1 transition-all duration-300 flex items-center space-x-3"
                  disabled={loading}>
                  {loading ? (
                    <div className="w-6 h-6 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                  ) : (
                    <svg
                      className="w-6 h-6 group-hover:rotate-90 transition-transform duration-300"
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
                    </svg>
                  )}
                  <span className="text-lg">{loading ? "Creating..." : "Create New Job"}</span>
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>

                {/* Dropdown Menu */}
                <div className="absolute right-0 mt-2 w-64 bg-slate-800 border border-slate-700 rounded-xl shadow-2xl opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all duration-200 z-50">
                  {processes.map((process) => (
                    <button
                      key={process.id}
                      onClick={() => createJob(process.id)}
                      className="w-full text-left px-4 py-3 hover:bg-slate-700 transition-colors first:rounded-t-xl last:rounded-b-xl"
                      disabled={loading}>
                      <div className="font-semibold text-white">{process.name}</div>
                      <div className="text-sm text-gray-400">{process.description}</div>
                      <div className="text-xs text-purple-300 mt-1">{process.steps} steps</div>
                    </button>
                  ))}
                </div>
              </div>
            </div>
          </div>

          {/* Enhanced Job Statistics Cards */}
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-7 gap-4 sm:gap-6 mb-10">
            <StatCard label="Total" count={jobStats.total} color="from-slate-600 to-slate-700" icon="📊" />
            <StatCard label="Pending" count={jobStats.pending} color="from-gray-600 to-gray-700" icon="⏳" />
            <StatCard label="Running" count={jobStats.running} color="from-blue-600 to-blue-700" icon="🚀" />
            <StatCard label="Paused" count={jobStats.paused} color="from-yellow-600 to-yellow-700" icon="⏸️" />
            <StatCard label="Completed" count={jobStats.completed} color="from-green-600 to-green-700" icon="✅" />
            <StatCard label="Failed" count={jobStats.failed} color="from-red-600 to-red-700" icon="❌" />
            <StatCard label="Canceled" count={jobStats.canceled} color="from-gray-500 to-gray-600" icon="🚫" />
          </div>

          {/* Enhanced Job List */}
          <div className="bg-white/5 backdrop-blur-lg rounded-3xl shadow-2xl border border-white/10 overflow-hidden">
            {jobList.length === 0 ? (
              <div className="p-12 text-center">
                <div className="w-24 h-24 mx-auto mb-6 bg-gradient-to-br from-purple-500 to-blue-500 rounded-full flex items-center justify-center">
                  <svg className="w-12 h-12 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M9 5H7a2 2 0 00-2 2v10a2 2 0 002 2h8a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"
                    />
                  </svg>
                </div>
                <h3 className="text-2xl font-bold text-gray-300 mb-2">No jobs found</h3>
                <p className="text-gray-500 text-lg mb-6">Click "Create New Job" to get started with your first job</p>
                <div className="inline-block px-4 py-2 bg-white/5 rounded-full text-sm text-gray-400">Jobs will appear here in real-time</div>
              </div>
            ) : (
              <div className="divide-y divide-white/10">
                {jobList.map((job, index) => (
                  <JobItem key={job.id} job={job} onControl={controlJob} index={index} />
                ))}
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="mt-8 text-center text-gray-500 text-sm">
            <p>© 2025 Job Processing Dashboard - Built with React + Go</p>
          </div>
        </div>
      </div>
    </div>
  );
}

// --- Enhanced Statistics Card Component ---
interface StatCardProps {
  label: string;
  count: number;
  color: string;
  icon: string;
}

function StatCard({ label, count, color, icon }: StatCardProps) {
  return (
    <div
      className={`bg-gradient-to-br ${color} rounded-2xl p-4 sm:p-6 text-center transform hover:scale-105 transition-all duration-300 hover:shadow-2xl border border-white/10`}>
      <div className="text-2xl sm:text-3xl mb-2">{icon}</div>
      <div className="text-3xl sm:text-4xl font-bold text-white mb-1 tabular-nums">{count}</div>
      <div className="text-xs sm:text-sm text-gray-200 uppercase tracking-wide font-medium">{label}</div>
    </div>
  );
}

// --- Enhanced Job Item Component ---
interface JobItemProps {
  job: Job;
  onControl: (id: string, command: "PAUSE" | "RESTART" | "CANCEL") => void;
  index: number;
}

function JobItem({ job, onControl, index }: JobItemProps) {
  const getStatusColor = (status: Job["status"]) => {
    switch (status) {
      case "RUNNING":
        return { bg: "bg-blue-500/20", text: "text-blue-400", border: "border-blue-500/30" };
      case "COMPLETED":
        return { bg: "bg-green-500/20", text: "text-green-400", border: "border-green-500/30" };
      case "PAUSED":
        return { bg: "bg-yellow-500/20", text: "text-yellow-400", border: "border-yellow-500/30" };
      case "CANCELED":
        return { bg: "bg-gray-500/20", text: "text-gray-400", border: "border-gray-500/30" };
      case "FAILED":
        return { bg: "bg-red-500/20", text: "text-red-400", border: "border-red-500/30" };
      default:
        return { bg: "bg-gray-500/20", text: "text-gray-400", border: "border-gray-500/30" };
    }
  };

  const getStatusIcon = (status: Job["status"]) => {
    switch (status) {
      case "RUNNING":
        return "🚀";
      case "COMPLETED":
        return "✅";
      case "PAUSED":
        return "⏸️";
      case "CANCELED":
        return "🚫";
      case "FAILED":
        return "❌";
      default:
        return "⏳";
    }
  };

  const statusColors = getStatusColor(job.status);

  return (
    <div className="p-6 hover:bg-white/5 transition-all duration-300" style={{ animationDelay: `${index * 50}ms` }}>
      <div className="flex flex-col lg:flex-row lg:items-center justify-between space-y-4 lg:space-y-0">
        <div className="flex-1 space-y-3">
          {/* Status, Process Type, and ID */}
          <div className="flex items-center space-x-3 flex-wrap gap-2">
            <div className={`flex items-center space-x-2 px-3 py-1 rounded-full ${statusColors.bg} ${statusColors.border} border`}>
              <span className="text-lg">{getStatusIcon(job.status)}</span>
              <span className={`font-semibold text-sm uppercase tracking-wide ${statusColors.text}`}>{job.status}</span>
            </div>

            {/* Process Type Badge */}
            <div className="flex items-center space-x-2 px-3 py-1 rounded-full bg-purple-500/20 border border-purple-500/30">
              <span className="text-sm">🔧</span>
              <span className="font-semibold text-sm text-purple-300 capitalize">{job.process_type?.replace("_", " ") || "Unknown"}</span>
            </div>

            <div className="text-gray-500 text-sm font-mono bg-white/5 px-2 py-1 rounded">ID: {job.id.substring(0, 8)}...</div>
          </div>

          {/* File Name */}
          <div className="text-xl font-semibold text-white">📄 {job.file_name}</div>

          {/* Enhanced Step Display - Main Step and Sub Step */}
          {(job.current_main_step || job.current_sub_step || job.current_step_name) && (
            <div className="space-y-2">
              {/* Main Step */}
              {job.current_main_step && (
                <div className="flex items-center space-x-2 text-purple-300">
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M9 5H7a2 2 0 00-2 2v10a2 2 0 002 2h8a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"
                    />
                  </svg>
                  <span className="text-sm font-semibold">Main: {job.current_main_step}</span>
                </div>
              )}

              {/* Sub Step */}
              {job.current_sub_step && (
                <div className="flex items-center space-x-2 text-blue-300 ml-6">
                  <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                  </svg>
                  <span className="text-sm font-medium">Sub: {job.current_sub_step}</span>
                </div>
              )}

              {/* Fallback to current_step_name if new fields are not available */}
              {!job.current_main_step && !job.current_sub_step && job.current_step_name && (
                <div className="flex items-center space-x-2 text-blue-300">
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                  </svg>
                  <span className="text-sm font-medium">{job.current_step_name}</span>
                </div>
              )}
            </div>
          )}

          {/* Enhanced Progress Bar */}
          {(job.status === "RUNNING" || job.status === "COMPLETED" || job.status === "PAUSED") && (
            <div className="space-y-2">
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-400">Progress</span>
                <span className="text-sm font-semibold text-white tabular-nums">{job.progress}%</span>
              </div>
              <div className="relative w-full bg-white/10 rounded-full h-3 overflow-hidden">
                <div
                  className="absolute top-0 left-0 h-full bg-gradient-to-r from-blue-500 to-purple-500 rounded-full transition-all duration-500 ease-out"
                  style={{ width: `${job.progress}%` }}>
                  <div className="absolute inset-0 bg-white/20 animate-pulse"></div>
                </div>
              </div>
            </div>
          )}

          {/* Timestamp */}
          <div className="text-xs text-gray-500">Created: {new Date(job.created_at).toLocaleString()}</div>
        </div>

        {/* Enhanced Control Buttons */}
        <div className="flex flex-wrap gap-2 lg:flex-col lg:w-32">
          {job.status === "RUNNING" && (
            <button
              onClick={() => onControl(job.id, "PAUSE")}
              className="flex items-center justify-center space-x-2 bg-yellow-600 hover:bg-yellow-700 text-white text-sm font-semibold py-2 px-4 rounded-xl transition-all duration-200 hover:scale-105 hover:shadow-lg">
              <span>⏸️</span>
              <span>Pause</span>
            </button>
          )}
          {job.status === "PAUSED" && (
            <button
              onClick={() => onControl(job.id, "RESTART")}
              className="flex items-center justify-center space-x-2 bg-green-600 hover:bg-green-700 text-white text-sm font-semibold py-2 px-4 rounded-xl transition-all duration-200 hover:scale-105 hover:shadow-lg">
              <span>▶️</span>
              <span>Resume</span>
            </button>
          )}
          {(job.status === "RUNNING" || job.status === "PAUSED" || job.status === "PENDING") && (
            <button
              onClick={() => onControl(job.id, "CANCEL")}
              className="flex items-center justify-center space-x-2 bg-red-600 hover:bg-red-700 text-white text-sm font-semibold py-2 px-4 rounded-xl transition-all duration-200 hover:scale-105 hover:shadow-lg">
              <span>🚫</span>
              <span>Cancel</span>
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

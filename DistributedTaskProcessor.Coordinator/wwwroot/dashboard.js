// Configuration
const API_BASE_URL = "/api";
const REFRESH_INTERVAL = 2000; // 2 seconds
const MAX_EVENTS = 50;
const CHART_MAX_POINTS = 30;

// State
let dashboardState = {
  events: [],
  throughputHistory: [],
  lastTimestamp: null,
  chartInstance: null,
  autoRefreshEnabled: true,
  lastWorkerState: {},
  lastTaskStats: {
    inProgress: 0,
    completed: 0,
    failed: 0,
  },
};

// Multi-tenant state
let multiTenantState = {
  selectedTenantId: null,
  tenants: [],
  tenantMetrics: {},
  sourceDataList: [],
  taskPartitions: [],
};

// Initialize Dashboard
document.addEventListener("DOMContentLoaded", function () {
  console.log("Dashboard initializing...");
  initializeChart();
  loadTenants();
  startAutoRefresh();
  updateTime();
  setInterval(updateTime, 1000);
});

// Load tenants on initialization
async function loadTenants() {
  try {
    // TODO: Replace with actual endpoint once implemented
    multiTenantState.tenants = [];
    renderTenantSelector();
  } catch (error) {
    console.error("Error loading tenants:", error);
  }
}

// Update time display
function updateTime() {
  const now = new Date();
  document.getElementById("currentTime").textContent = now.toLocaleTimeString();
}

// Initialize Chart
function initializeChart() {
  const ctx = document.getElementById("throughputChart").getContext("2d");
  dashboardState.chartInstance = new Chart(ctx, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Tasks/sec",
          data: [],
          borderColor: "#3b82f6",
          backgroundColor: "rgba(59, 130, 246, 0.1)",
          tension: 0.4,
          fill: true,
          yAxisID: "y",
        },
        {
          label: "Rows/sec (÷1000)",
          data: [],
          borderColor: "#10b981",
          backgroundColor: "rgba(16, 185, 129, 0.1)",
          tension: 0.4,
          fill: true,
          yAxisID: "y",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      interaction: {
        mode: "index",
        intersect: false,
      },
      plugins: {
        legend: {
          labels: {
            color: "#cbd5e1",
            usePointStyle: true,
            padding: 15,
          },
        },
      },
      scales: {
        y: {
          type: "linear",
          display: true,
          position: "left",
          grid: {
            color: "rgba(51, 65, 85, 0.2)",
            drawBorder: false,
          },
          ticks: {
            color: "#cbd5e1",
          },
        },
        x: {
          grid: {
            color: "rgba(51, 65, 85, 0.2)",
            drawBorder: false,
          },
          ticks: {
            color: "#cbd5e1",
          },
        },
      },
    },
  });
}

// Auto-refresh function
function startAutoRefresh() {
  refreshDashboard();
  setInterval(() => {
    if (dashboardState.autoRefreshEnabled) {
      refreshDashboard();
    }
  }, REFRESH_INTERVAL);
}

// Main dashboard refresh
async function refreshDashboard() {
  try {
    const [dashboard, distribution, workers] = await Promise.all([
      axios.get(`${API_BASE_URL}/monitoring/dashboard`),
      axios.get(`${API_BASE_URL}/monitoring/task-distribution`),
      axios.get(`${API_BASE_URL}/monitoring/workers`),
    ]);

    updateSystemHealth(dashboard.data);
    updateTaskStatistics(dashboard.data);
    updatePerformanceMetrics(dashboard.data);
    updateAlerts(dashboard.data);
    updateWorkersList(workers.data);
    updateDistribution(distribution.data);
    updateThroughputChart(dashboard.data);
    addLiveEvents(dashboard.data);

    // Update system status indicator
    const health = dashboard.data.systemHealth.status;
    updateStatusIndicator(health);

    // Update detailed views if they're visible
    const tasksTab = document.getElementById("tasksTab");
    const workerTasksTab = document.getElementById("workerTasksTab");

    if (tasksTab && tasksTab.classList.contains("active")) {
      refreshDetailedTasks();
    }
    if (workerTasksTab && workerTasksTab.classList.contains("active")) {
      refreshWorkerTasks();
    }
  } catch (error) {
    console.error("Error refreshing dashboard:", error);
    updateStatusIndicator("Critical");
    addEvent("error", "❌ Dashboard API Error", "danger");
  }
}

// Update system health
function updateSystemHealth(data) {
  const health = data.systemHealth;
  document.getElementById("activeWorkers").textContent = health.activeWorkers;
  document.getElementById("totalWorkers").textContent = health.totalWorkers;
  document.getElementById("taskThroughput").textContent = health.taskThroughput;
  document.getElementById("dataThroughput").textContent = health.dataThroughput;
  document.getElementById(
    "systemHealthText"
  ).textContent = `System: ${health.status}`;
}

// Update task statistics
function updateTaskStatistics(data) {
  const stats = data.taskStatistics;
  document.getElementById("pendingCount").textContent = stats.pending;
  document.getElementById("inProgressCount").textContent = stats.inProgress;
  document.getElementById("completedCount").textContent = stats.completed;
  document.getElementById("failedCount").textContent = stats.failed;
  document.getElementById("deadLetterCount").textContent = stats.deadLetter;

  const successRate = parseFloat(stats.successRate) || 0;
  const successPercentage = Math.min(100, Math.max(0, successRate));
  document.getElementById("successRateBar").style.width =
    successPercentage + "%";
  document.getElementById(
    "successRateText"
  ).textContent = `Success Rate: ${stats.successRate}`;
}

// Update performance metrics
function updatePerformanceMetrics(data) {
  const perf = data.performance;
  document.getElementById("avgTaskDuration").textContent =
    perf.averageTaskDuration;
  document.getElementById("errorRate").textContent = perf.errorRate;
  document.getElementById("totalRowsProcessed").textContent = formatNumber(
    parseInt(perf.totalRowsProcessed)
  );

  const stats = data.taskStatistics;
  document.getElementById("successRate").textContent = stats.successRate;
}

// Update alerts
function updateAlerts(data) {
  const alertsList = document.getElementById("alertsList");
  const alertCountBadge = document.getElementById("alertCountBadge");
  const alerts = data.alerts || [];

  alertCountBadge.textContent = alerts.length;

  if (alerts.length === 0) {
    alertsList.innerHTML =
      '<div class="no-data">✓ No alerts - System healthy</div>';
    return;
  }

  alertsList.innerHTML = alerts
    .map(
      (alert) => `
        <div class="alert-item ${alert.severity.toLowerCase()}">
            <div class="alert-severity">${alert.severity}</div>
            <div class="alert-message">${alert.message}</div>
        </div>
    `
    )
    .join("");
}

// Update workers list
function updateWorkersList(data) {
  const workersList = document.getElementById("workersList");
  const workerCountBadge = document.getElementById("workerCountBadge");
  const workers = data.workers || [];

  workerCountBadge.textContent = workers.length;

  if (workers.length === 0) {
    workersList.innerHTML = '<div class="no-data">No active workers</div>';
    return;
  }

  // Detect worker changes
  workers.forEach((worker) => {
    const wasActive = dashboardState.lastWorkerState[worker.workerId];
    if (wasActive === undefined && worker.status === "Active") {
      addEvent("worker-added", `👷 Worker added: ${worker.workerId}`, "info");
    } else if (wasActive && worker.status !== "Active") {
      addEvent(
        "worker-removed",
        `⛔ Worker removed: ${worker.workerId}`,
        "warning"
      );
    }
    dashboardState.lastWorkerState[worker.workerId] =
      worker.status === "Active";
  });

  workersList.innerHTML = workers
    .map(
      (worker) => `
        <div class="worker-item ${
          worker.status === "Active" ? "" : "inactive"
        }" onclick="showWorkerDetails('${worker.workerId}')">
            <div class="worker-info">
                <div class="worker-id">${worker.workerId}
                    <span class="worker-badge ${
                      worker.status === "Active" ? "" : "inactive"
                    }">
                        ${worker.status}
                    </span>
                </div>
                <div class="worker-status">
                    Uptime: ${worker.uptimeMinutes.toFixed(1)}m | 
                    Active Tasks: ${worker.activeTasks}
                </div>
            </div>
            <div class="worker-stats">
                <div class="worker-stat">
                    <span>📊 ${worker.tasksCompleted}</span>
                </div>
                <div class="worker-stat">
                    <span>⚡ ${parseFloat(worker.successRate).toFixed(
                      1
                    )}%</span>
                </div>
            </div>
        </div>
    `
    )
    .join("");
}

// Update workload distribution
function updateDistribution(data) {
  const balance = data.workloadBalance;
  document.getElementById("maxTasksPerWorker").textContent =
    balance.maxTasksPerWorker;
  document.getElementById("minTasksPerWorker").textContent =
    balance.minTasksPerWorker;
  document.getElementById("avgTasksPerWorker").textContent =
    balance.averageTasksPerWorker.toFixed(1);

  const balanceStatus = document.getElementById("balanceStatus");
  balanceStatus.textContent = balance.isBalanced
    ? "✓ Balanced"
    : "⚠ Imbalanced";
  balanceStatus.style.color = balance.isBalanced ? "#22c55e" : "#ea580c";
}

// Update throughput chart
function updateThroughputChart(data) {
  const now = new Date().toLocaleTimeString();
  const taskThroughput = parseFloat(data.systemHealth.taskThroughput) || 0;
  const rowThroughput =
    (parseInt(data.performance.totalRowsProcessed) || 0) / 1000;

  const labels = dashboardState.chartInstance.data.labels;
  const taskData = dashboardState.chartInstance.data.datasets[0].data;
  const rowData = dashboardState.chartInstance.data.datasets[1].data;

  labels.push(now);
  taskData.push(taskThroughput);
  rowData.push(rowThroughput);

  // Keep only last N points
  if (labels.length > CHART_MAX_POINTS) {
    labels.shift();
    taskData.shift();
    rowData.shift();
  }

  dashboardState.chartInstance.update("none");
}

// Add live event
function addLiveEvents(data) {
  const stats = data.taskStatistics;
  const lastStats = dashboardState.lastTaskStats;

  // Only add events if values changed
  if (stats.inProgress !== lastStats.inProgress && stats.inProgress > 0) {
    addEvent(
      "task-assigned",
      `📋 ${stats.inProgress} tasks being processed`,
      "info"
    );
  }

  if (stats.completed !== lastStats.completed && stats.completed > 0) {
    addEvent("task-completed", `✅ ${stats.completed} tasks completed`, "info");
  }

  if (stats.failed !== lastStats.failed && stats.failed > 0) {
    addEvent("task-failed", `❌ ${stats.failed} tasks failed`, "error");
  }

  // Update last stats for next comparison
  dashboardState.lastTaskStats = {
    inProgress: stats.inProgress,
    completed: stats.completed,
    failed: stats.failed,
  };
}

// Add event to list
function addEvent(type, message, severity = "info") {
  const now = new Date().toLocaleTimeString();
  const event = { type, message, time: now, severity };

  dashboardState.events.unshift(event);
  if (dashboardState.events.length > MAX_EVENTS) {
    dashboardState.events.pop();
  }

  updateEventsDisplay();
}

// Update events display
function updateEventsDisplay() {
  const eventsList = document.getElementById("eventsList");

  if (dashboardState.events.length === 0) {
    eventsList.innerHTML = '<div class="no-data">Waiting for events...</div>';
    return;
  }

  eventsList.innerHTML = dashboardState.events
    .map(
      (event) => `
        <div class="event-item ${event.type}">
            <div class="event-time">${event.time}</div>
            <div class="event-message">${event.message}</div>
        </div>
    `
    )
    .join("");
}

// Update status indicator
function updateStatusIndicator(status) {
  const indicator = document.querySelector(".status-indicator");
  indicator.className = "status-indicator";

  if (status && status.toLowerCase() === "healthy") {
    indicator.classList.add("healthy");
  } else if (status && status.toLowerCase() === "warning") {
    indicator.classList.add("warning");
  } else {
    indicator.classList.add("critical");
  }
}

// Show worker details
async function showWorkerDetails(workerId) {
  const modal = document.getElementById("workerModal");
  const modalBody = document.getElementById("workerModalBody");
  document.getElementById(
    "workerModalTitle"
  ).textContent = `Worker: ${workerId}`;

  try {
    const response = await axios.get(
      `${API_BASE_URL}/monitoring/workers/${workerId}`
    );
    const worker = response.data;

    modalBody.innerHTML = `
            <div class="worker-details">
                <div class="detail-section">
                    <h3>Status</h3>
                    <div class="detail-grid">
                        <div class="detail-item">
                            <span class="label">Status</span>
                            <span class="value">${worker.status}</span>
                        </div>
                        <div class="detail-item">
                            <span class="label">Success Rate</span>
                            <span class="value">${worker.successRate}</span>
                        </div>
                    </div>
                </div>

                <div class="detail-section">
                    <h3>Metrics</h3>
                    <div class="detail-grid">
                        <div class="detail-item">
                            <span class="label">Tasks Started</span>
                            <span class="value">${worker.tasksStarted}</span>
                        </div>
                        <div class="detail-item">
                            <span class="label">Tasks Completed</span>
                            <span class="value">${worker.tasksCompleted}</span>
                        </div>
                        <div class="detail-item">
                            <span class="label">Tasks Failed</span>
                            <span class="value">${worker.tasksFailed}</span>
                        </div>
                        <div class="detail-item">
                            <span class="label">Active Tasks</span>
                            <span class="value">${worker.activeTasks}</span>
                        </div>
                        <div class="detail-item">
                            <span class="label">Rows Processed</span>
                            <span class="value">${formatNumber(
                              worker.totalRowsProcessed
                            )}</span>
                        </div>
                    </div>
                </div>

                <div class="detail-section">
                    <h3>Processing Times</h3>
                    <div class="detail-grid">
                        <div class="detail-item">
                            <span class="label">Average</span>
                            <span class="value">${
                              worker.processingTimeStats.average
                            }</span>
                        </div>
                        <div class="detail-item">
                            <span class="label">P50</span>
                            <span class="value">${
                              worker.processingTimeStats.p50
                            }</span>
                        </div>
                        <div class="detail-item">
                            <span class="label">P95</span>
                            <span class="value">${
                              worker.processingTimeStats.p95
                            }</span>
                        </div>
                        <div class="detail-item">
                            <span class="label">P99</span>
                            <span class="value">${
                              worker.processingTimeStats.p99
                            }</span>
                        </div>
                    </div>
                </div>

                <div class="detail-section">
                    <h3>Uptime</h3>
                    <div class="detail-grid">
                        <div class="detail-item">
                            <span class="label">Last Heartbeat</span>
                            <span class="value">${new Date(
                              worker.lastHeartbeat
                            ).toLocaleString()}</span>
                        </div>
                        <div class="detail-item">
                            <span class="label">Uptime</span>
                            <span class="value">${
                              worker.uptimeMinutes
                            } min</span>
                        </div>
                    </div>
                </div>
            </div>
        `;

    modal.classList.add("show");
  } catch (error) {
    console.error("Error fetching worker details:", error);
    modalBody.innerHTML =
      '<div class="error">Failed to load worker details</div>';
  }
}

// Close worker modal
function closeWorkerModal() {
  document.getElementById("workerModal").classList.remove("show");
}

// Clear live events
function clearLiveEvents() {
  dashboardState.events = [];
  updateEventsDisplay();
}

// Utility: Format number
function formatNumber(num) {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + "M";
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + "K";
  }
  return num.toString();
}

// Switch between detailed view tabs
function switchTab(tabName) {
  // Hide all tab contents
  document.querySelectorAll(".tab-content").forEach((tab) => {
    tab.classList.remove("active");
  });

  // Remove active class from all buttons
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.classList.remove("active");
  });

  // Show selected tab content
  const tabElement = document.getElementById(tabName + "Tab");
  if (tabElement) {
    tabElement.classList.add("active");
  }

  // Add active class to clicked button
  const buttons = document.querySelectorAll(".tab-btn");
  buttons.forEach((btn) => {
    if (
      btn.textContent
        .toLowerCase()
        .includes(tabName === "tasks" ? "tasks" : "worker")
    ) {
      btn.classList.add("active");
    }
  });

  // Refresh the selected tab's data
  if (tabName === "tasks") {
    refreshDetailedTasks();
  } else if (tabName === "worker-tasks") {
    refreshWorkerTasks();
  }
}

// Fetch and display detailed tasks
async function refreshDetailedTasks() {
  try {
    const response = await axios.get(
      `${API_BASE_URL}/monitoring/tasks/detailed`
    );
    const tasks = response.data.tasks || [];
    const tableBody = document.getElementById("tasksTableBody");

    if (tasks.length === 0) {
      tableBody.innerHTML =
        '<tr><td colspan="8" class="no-data">No tasks found</td></tr>';
      return;
    }

    tableBody.innerHTML = tasks
      .map((task) => {
        const statusClass = getStatusClass(task.status);
        const createdDate = new Date(task.createdAt).toLocaleString();
        const rowsProcessed = task.endRow - task.startRow;

        return `
          <tr class="${statusClass}">
            <td><span class="task-id">${task.taskId}</span></td>
            <td>${task.symbol || "—"}</td>
            <td>${task.fund || "—"}</td>
            <td>${task.startRow} - ${task.endRow}</td>
            <td><strong>${formatNumber(task.rowsProcessed || 0)}</strong></td>
            <td>${task.worker || "—"}</td>
            <td><span class="status-badge ${statusClass}">${
          task.status
        }</span></td>
            <td>${createdDate}</td>
          </tr>
        `;
      })
      .join("");
  } catch (error) {
    console.error("Error fetching detailed tasks:", error);
    const tableBody = document.getElementById("tasksTableBody");
    tableBody.innerHTML =
      '<tr><td colspan="8" class="no-data">Error loading tasks</td></tr>';
  }
}

// Fetch and display worker tasks
async function refreshWorkerTasks() {
  try {
    const response = await axios.get(`${API_BASE_URL}/monitoring/worker-tasks`);
    const workerTasks = response.data.workerTasks || [];
    const container = document.getElementById("workerTasksContainer");

    if (workerTasks.length === 0) {
      container.innerHTML =
        '<div class="no-data">No workers with tasks found</div>';
      return;
    }

    container.innerHTML = workerTasks
      .map((worker) => {
        const activeTasksCount = worker.activeTasks || 0;
        const workerStatus = worker.status === "Active" ? "active" : "inactive";

        return `
          <div class="worker-task-block">
            <div class="worker-task-header">
              <div class="worker-name">${worker.workerId}</div>
              <div class="worker-stats-inline">
                <span class="worker-stat-badge ${workerStatus}">
                  ${worker.status}
                </span>
                <span class="worker-stat-item">
                  ✅ ${worker.tasksCompleted || 0} Completed
                </span>
                <span class="worker-stat-item">
                  ❌ ${worker.tasksFailed || 0} Failed
                </span>
                <span class="worker-stat-item">
                  📊 ${formatNumber(worker.totalRowsProcessed || 0)} Rows
                </span>
                <span class="worker-stat-item">
                  ⚡ ${activeTasksCount} Active
                </span>
              </div>
            </div>
            <div class="task-items">
              ${(worker.tasks || [])
                .map((task) => {
                  const statusClass = getStatusClass(task.status);
                  return `
                    <div class="task-item ${statusClass}">
                      <div class="task-item-id">${task.taskId}</div>
                      <div class="task-item-stat">
                        Rows: ${formatNumber(task.rowsProcessed || 0)}
                      </div>
                      <div class="task-item-status">
                        <span class="status-badge ${statusClass}">
                          ${task.status}
                        </span>
                      </div>
                    </div>
                  `;
                })
                .join("")}
            </div>
          </div>
        `;
      })
      .join("");
  } catch (error) {
    console.error("Error fetching worker tasks:", error);
    const container = document.getElementById("workerTasksContainer");
    container.innerHTML =
      '<div class="no-data">Error loading worker tasks</div>';
  }
}

// Helper function to get CSS class for status
function getStatusClass(status) {
  if (!status) return "pending";
  const lowerStatus = status.toLowerCase();
  if (lowerStatus.includes("pending")) return "pending";
  if (lowerStatus.includes("progress")) return "in-progress";
  if (lowerStatus.includes("completed")) return "completed";
  if (lowerStatus.includes("failed")) return "failed";
  return "pending";
}

// Close modal when clicking outside
document.addEventListener("click", function (event) {
  const modal = document.getElementById("workerModal");
  if (event.target === modal) {
    closeWorkerModal();
  }
});

// Render tenant selector dropdown
function renderTenantSelector() {
  const selector = document.getElementById("tenantSelector");
  if (!selector) return;

  selector.innerHTML = multiTenantState.tenants
    .map(
      (tenant) => `
      <option value="${tenant.tenantId}">${tenant.tenantName}</option>
    `
    )
    .join("");

  // Add change event listener
  selector.addEventListener("change", function (event) {
    const tenantId = event.target.value;
    if (tenantId) {
      multiTenantState.selectedTenantId = tenantId;
      loadTenantMetrics(tenantId);
    }
  });
}

// Fetch tenant-specific metrics
async function loadTenantMetrics(tenantId) {
  try {
    // TODO: Replace with actual endpoint once implemented
    const response = {
      tenantId: tenantId,
      pendingRows: 0,
      processedRows: 0,
      tasksInProgress: 0,
      completionPercentage: 0,
    };
    multiTenantState.tenantMetrics[tenantId] = response;
    renderTenantMetrics(response);
  } catch (error) {
    console.error("Error loading tenant metrics:", error);
  }
}

// Render tenant-specific metrics on dashboard
function renderTenantMetrics(metrics) {
  const pendingElement = document.getElementById("tenantPendingRows");
  const processedElement = document.getElementById("tenantProcessedRows");
  const inProgressElement = document.getElementById("tenantTasksInProgress");
  const completionElement = document.getElementById(
    "tenantCompletionPercentage"
  );

  if (pendingElement)
    pendingElement.textContent = formatNumber(metrics.pendingRows);
  if (processedElement)
    processedElement.textContent = formatNumber(metrics.processedRows);
  if (inProgressElement)
    inProgressElement.textContent = metrics.tasksInProgress;
  if (completionElement)
    completionElement.textContent =
      metrics.completionPercentage.toFixed(1) + "%";
}

// Format number with thousands separator
function formatNumber(num) {
  return num.toLocaleString();
}

console.log("Dashboard script loaded successfully");

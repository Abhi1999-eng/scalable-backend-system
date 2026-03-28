"use client";

import { FormEvent, useEffect, useMemo, useState, useTransition } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";

type DatasetStatus = {
  datasetId: string;
  batchId: string;
  originalFilename: string;
  storedFilename: string;
  storagePath: string;
  contentType: string;
  fileSizeBytes: number;
  status: string;
  datasetType: string;
  rowCount: number;
  columnCount: number;
  errorMessage?: string | null;
  createdAt: string;
  startedAt?: string;
  finishedAt?: string;
};

type AuthUser = {
  id: number;
  name: string;
  email: string;
};

type AuthResponse = {
  token: string;
  user: AuthUser;
};

type DatasetListResponse = {
  batchId: string;
  datasets: DatasetStatus[];
};

type DatasetJob = {
  jobId: string;
  datasetId: string;
  batchId: string;
  status: string;
  progressPercentage: number;
  totalRows: number;
  processedRows: number;
  retryCount: number;
  processingTimeMs?: number | null;
  errorMessage?: string | null;
};

type DatasetDashboardItem = {
  dataset: DatasetStatus;
  job?: DatasetJob | null;
};

type DatasetBatchProgressResponse = {
  batchId: string;
  datasetCount: number;
  queuedCount: number;
  processingCount: number;
  completedCount: number;
  failedCount: number;
  progressPercentage: number;
  processedRows: number;
  totalRows: number;
  items: DatasetDashboardItem[];
};

type DatasetColumnProfile = {
  id: number;
  datasetId: string;
  columnName: string;
  columnOrderIndex: number;
  inferredType: string;
  nonNullCount: number;
  nullCount: number;
  distinctCount: number;
  sampleValue?: string | null;
  minValue?: string | null;
  maxValue?: string | null;
};

type DatasetProfileResponse = {
  dataset: DatasetStatus;
  columns: DatasetColumnProfile[];
};

type DatasetRow = Record<string, string | number | boolean | null>;

type DatasetChartPoint = {
  label: string;
  x: number;
  y: number;
};

type DatasetBoxPlotStats = {
  min: number;
  q1: number;
  median: number;
  q3: number;
  max: number;
};

type DatasetChartResponse = {
  chartType: string;
  datasetId: string;
  xColumn: string;
  yColumn?: string | null;
  aggregation: string;
  labels?: string[];
  values?: number[];
  points?: DatasetChartPoint[];
  boxPlot?: DatasetBoxPlotStats | null;
};

type DatasetQueryResponse = {
  datasetId: string;
  aggregation: string;
  totalRows: number;
  limit: number;
  offset: number;
  rows: DatasetRow[];
  aggregates: Array<Record<string, string | number | boolean | null>>;
};

type ChartSeriesItem = {
  name: string;
  value: number;
};

type SavedChart = {
  chartId: string;
  datasetId: string;
  name: string;
  chartType: string;
  xColumn: string;
  yColumn?: string | null;
  aggregation: string;
  limitValue: number;
};

const chartOptions = [
  { value: "bar", label: "Bar Graph" },
  { value: "line", label: "Line Graph" },
  { value: "pie", label: "Pie Chart" },
  { value: "histogram", label: "Histogram" },
  { value: "scatter", label: "Scatter Plot" },
  { value: "box", label: "Box Plot" },
  { value: "pictograph", label: "Pictograph" },
  { value: "area", label: "Area Graph" }
];

const chartHelp: Record<string, string> = {
  bar: "Best for comparing totals across categories.",
  line: "Best for trend lines across time or ordered values.",
  pie: "Best for small category sets. Large sets are grouped into Others.",
  histogram: "Best for showing distribution of a numeric column.",
  scatter: "Best for comparing two numeric columns.",
  box: "Best for seeing spread, quartiles, and outliers for one numeric column.",
  pictograph: "Best for lightweight category comparisons with icon counts.",
  area: "Best for showing volume change over a sequence."
};

const pieColors = ["#f7c66a", "#f4f4f1", "#98a2ad", "#5dd39e", "#7f88d1", "#ff6b81", "#6ad4f7", "#d6b8ff"];

function formatBytes(value: number) {
  if (!value) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB"];
  let amount = value;
  let index = 0;
  while (amount >= 1024 && index < units.length - 1) {
    amount /= 1024;
    index += 1;
  }
  return `${amount.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function truncateLabel(value: string, length = 22) {
  if (value.length <= length) {
    return value;
  }
  return `${value.slice(0, length - 1)}…`;
}

async function readJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const text = await response.text();
    try {
      const payload = JSON.parse(text) as { message?: string };
      throw new Error(payload.message || text || `Request failed with ${response.status}`);
    } catch {
      throw new Error(text || `Request failed with ${response.status}`);
    }
  }
  return response.json() as Promise<T>;
}

async function readOptionalJson<T>(response: Response): Promise<T | null> {
  if (response.status === 404) {
    return null;
  }
  return readJson<T>(response);
}

function formatDatasetLabel(dataset: DatasetStatus) {
  return `${dataset.originalFilename} · ${dataset.status}`;
}

function formatColumnBadge(column: DatasetColumnProfile) {
  return `${column.columnName} (${column.inferredType.toLowerCase()})`;
}

function preparePieSeries(items: ChartSeriesItem[]) {
  const sorted = [...items].sort((left, right) => right.value - left.value);
  if (sorted.length <= 6) {
    return sorted;
  }

  const head = sorted.slice(0, 5);
  const remainder = sorted.slice(5).reduce((sum, item) => sum + item.value, 0);
  return [...head, { name: "Others", value: remainder }];
}

export default function HomePage() {
  const [authMode, setAuthMode] = useState<"login" | "signup">("login");
  const [authUser, setAuthUser] = useState<AuthUser | null>(null);
  const [authToken, setAuthToken] = useState("");
  const [authName, setAuthName] = useState("");
  const [authEmail, setAuthEmail] = useState("");
  const [authPassword, setAuthPassword] = useState("");
  const [selectedUploads, setSelectedUploads] = useState<FileList | null>(null);
  const [batchId, setBatchId] = useState("");
  const [batch, setBatch] = useState<DatasetListResponse | null>(null);
  const [batchProgress, setBatchProgress] = useState<DatasetBatchProgressResponse | null>(null);
  const [dashboardItems, setDashboardItems] = useState<DatasetDashboardItem[]>([]);
  const [jobMap, setJobMap] = useState<Record<string, DatasetJob>>({});
  const [selectedDatasetId, setSelectedDatasetId] = useState("");
  const [profile, setProfile] = useState<DatasetProfileResponse | null>(null);
  const [rows, setRows] = useState<DatasetRow[]>([]);
  const [chartType, setChartType] = useState("bar");
  const [chart, setChart] = useState<DatasetChartResponse | null>(null);
  const [savedCharts, setSavedCharts] = useState<SavedChart[]>([]);
  const [chartSaveName, setChartSaveName] = useState("");
  const [selectedCategoryColumn, setSelectedCategoryColumn] = useState("");
  const [selectedValueColumn, setSelectedValueColumn] = useState("");
  const [aggregation, setAggregation] = useState("count");
  const [lastError, setLastError] = useState("");
  const [retryingDatasetId, setRetryingDatasetId] = useState("");
  const [isPending, startTransition] = useTransition();

  const datasets = batchProgress?.items.map((item) => item.dataset) ?? batch?.datasets ?? dashboardItems.map((item) => item.dataset);
  const selectedDataset = datasets.find((dataset) => dataset.datasetId === selectedDatasetId) ?? null;
  const numericColumns = useMemo(
    () => profile?.columns.filter((column) => column.inferredType === "NUMBER") ?? [],
    [profile]
  );
  const allColumns = profile?.columns ?? [];
  const rowColumns = rows[0] ? Object.keys(rows[0]) : [];
  const readyCount = batchProgress?.completedCount ?? datasets.filter((dataset) => dataset.status === "READY").length;
  const processingCount = batchProgress?.processingCount ?? datasets.filter((dataset) => {
    const job = jobMap[dataset.datasetId];
    return job?.status === "PROCESSING" || job?.status === "QUEUED";
  }).length;
  const failedCount = batchProgress?.failedCount ?? datasets.filter((dataset) => {
    const job = jobMap[dataset.datasetId];
    return job?.status === "FAILED";
  }).length;
  const batchProgressPercentage = batchProgress?.progressPercentage ?? (datasets.length === 0
    ? 0
    : Math.round(datasets.reduce((sum, dataset) => sum + (jobMap[dataset.datasetId]?.progressPercentage ?? (dataset.status === "READY" ? 100 : 0)), 0) / datasets.length));

  const chartSeries = useMemo(
    () => chart?.labels?.map((label, index) => ({
      name: label,
      value: chart.values?.[index] ?? 0
    })) ?? [],
    [chart]
  );
  const pieSeries = useMemo(() => preparePieSeries(chartSeries), [chartSeries]);
  const scatterSeries = useMemo(
    () => chart?.points?.map((point) => ({ x: point.x, y: point.y, label: point.label })) ?? [],
    [chart]
  );
  const pictographTotal = chartSeries.reduce((sum, item) => sum + item.value, 0);

  const needsCategoryColumn = !["histogram", "box"].includes(chartType);
  const needsValueColumn = chartType === "scatter"
    || chartType === "box"
    || chartType === "histogram"
    || aggregation === "sum";
  const aggregationDisabled = ["histogram", "scatter", "box"].includes(chartType);

  async function apiFetch(path: string, init?: RequestInit, authenticated = true) {
    const headers = new Headers(init?.headers);
    if (authenticated && authToken) {
      headers.set("Authorization", `Bearer ${authToken}`);
    }
    return fetch(path, {
      ...init,
      headers
    });
  }

  useEffect(() => {
    const storedToken = window.localStorage.getItem("auth_token");
    if (storedToken) {
      setAuthToken(storedToken);
    }
  }, []);

  useEffect(() => {
    if (!authToken) {
      setAuthUser(null);
      setDashboardItems([]);
      setBatch(null);
      setBatchProgress(null);
      setSelectedDatasetId("");
      return;
    }
    void loadMe();
  }, [authToken]);

  useEffect(() => {
    if (!batchId && authToken) {
      void loadDashboard();
    }
  }, [batchId, authToken]);

  useEffect(() => {
    if (!batchId) {
      return;
    }

    let active = true;
    const poll = async () => {
      try {
        const nextBatchProgress = await fetchBatchProgress(batchId);
        if (!active) {
          return;
        }
        setBatchProgress(nextBatchProgress);
        setBatch({
          batchId: nextBatchProgress.batchId,
          datasets: nextBatchProgress.items.map((item) => item.dataset)
        });
        setDashboardItems([]);
        setJobMap(
          nextBatchProgress.items.reduce<Record<string, DatasetJob>>((accumulator, item) => {
            if (item.job) {
              accumulator[item.dataset.datasetId] = item.job;
            }
            return accumulator;
          }, {})
        );

        if (!selectedDatasetId && nextBatchProgress.items[0]) {
          setSelectedDatasetId(nextBatchProgress.items[0].dataset.datasetId);
        }

        if (nextBatchProgress.processingCount > 0 || nextBatchProgress.queuedCount > 0) {
          window.setTimeout(poll, 2000);
        }
      } catch (error) {
        if (active) {
          setLastError(error instanceof Error ? error.message : "Unable to refresh batch.");
        }
      }
    };

    void poll();
    return () => {
      active = false;
    };
  }, [batchId, selectedDatasetId]);

  useEffect(() => {
    if (!selectedDatasetId) {
      return;
    }
    void loadDataset(selectedDatasetId);
  }, [selectedDatasetId]);

  useEffect(() => {
    if (!allColumns.length) {
      return;
    }

    const firstText = allColumns.find((column) => column.inferredType !== "NUMBER")?.columnName
      ?? allColumns[0]?.columnName
      ?? "";
    const firstNumeric = numericColumns[0]?.columnName ?? "";
    const secondNumeric = numericColumns[1]?.columnName ?? firstNumeric;

    if (!selectedCategoryColumn || !allColumns.some((column) => column.columnName === selectedCategoryColumn)) {
      setSelectedCategoryColumn(firstText || firstNumeric);
    }

    if (!selectedValueColumn || !numericColumns.some((column) => column.columnName === selectedValueColumn)) {
      setSelectedValueColumn(secondNumeric || firstNumeric);
    }
  }, [allColumns, numericColumns, selectedCategoryColumn, selectedValueColumn]);

  useEffect(() => {
    if (!aggregationDisabled) {
      return;
    }
    setAggregation("count");
  }, [aggregationDisabled]);

  async function fetchBatch(id: string) {
    const response = await apiFetch(`/api/datasets/batches/${id}`);
    return readJson<DatasetListResponse>(response);
  }

  async function fetchBatchProgress(id: string) {
    const response = await apiFetch(`/api/datasets/batches/${id}/progress`);
    return readJson<DatasetBatchProgressResponse>(response);
  }

  async function loadMe() {
    try {
      const response = await apiFetch("/api/auth/me");
      const user = await readJson<AuthUser>(response);
      setAuthUser(user);
    } catch {
      window.localStorage.removeItem("auth_token");
      setAuthToken("");
      setAuthUser(null);
    }
  }

  async function loadDashboard() {
    try {
      const response = await apiFetch("/api/datasets?page=0&size=20");
      const items = await readJson<DatasetDashboardItem[]>(response);
      setDashboardItems(items);
      setBatchProgress(null);
      setBatch(null);
      setJobMap(
        items.reduce<Record<string, DatasetJob>>((accumulator, item) => {
          if (item.job) {
            accumulator[item.dataset.datasetId] = item.job;
          }
          return accumulator;
        }, {})
      );
      if (!selectedDatasetId && items[0]) {
        setSelectedDatasetId(items[0].dataset.datasetId);
      }
    } catch (error) {
      setLastError(error instanceof Error ? error.message : "Unable to load dataset dashboard.");
    }
  }

  async function loadDataset(id: string) {
    setLastError("");
    try {
      const [nextProfile, nextQuery, nextJob, nextSavedCharts] = await Promise.all([
        readJson<DatasetProfileResponse>(await apiFetch(`/api/datasets/${id}/profile`)),
        readJson<DatasetQueryResponse>(await apiFetch(`/api/datasets/${id}/query`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            limit: 25,
            offset: 0
          })
        })),
        readOptionalJson<DatasetJob>(await apiFetch(`/api/datasets/${id}/job`)),
        readJson<SavedChart[]>(await apiFetch(`/api/datasets/${id}/saved-charts`))
      ]);

      setProfile(nextProfile);
      setRows(nextQuery.rows);
      if (nextJob) {
        setJobMap((current) => ({ ...current, [id]: nextJob }));
      }
      setSavedCharts(nextSavedCharts);
      setChart(null);
    } catch (error) {
      setLastError(error instanceof Error ? error.message : "Unable to load selected dataset.");
    }
  }

  async function handleAuthSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLastError("");
    startTransition(async () => {
      try {
        const path = authMode === "signup" ? "/api/auth/signup" : "/api/auth/login";
        const payload =
          authMode === "signup"
            ? { name: authName, email: authEmail, password: authPassword }
            : { email: authEmail, password: authPassword };

        const response = await apiFetch(path, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify(payload)
        }, false);
        const auth = await readJson<AuthResponse>(response);
        window.localStorage.setItem("auth_token", auth.token);
        setAuthToken(auth.token);
        setAuthUser(auth.user);
        setAuthPassword("");
        setBatchId("");
      } catch (error) {
        setLastError(error instanceof Error ? error.message : "Authentication failed.");
      }
    });
  }

  function logout() {
    window.localStorage.removeItem("auth_token");
    setAuthToken("");
    setAuthUser(null);
    setBatch(null);
    setBatchProgress(null);
    setDashboardItems([]);
    setProfile(null);
    setRows([]);
    setSavedCharts([]);
    setChart(null);
  }

  async function handleUpload(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedUploads || selectedUploads.length === 0) {
      setLastError("Choose one or more CSV or ZIP files first.");
      return;
    }

    setLastError("");
    startTransition(async () => {
      try {
        const formData = new FormData();
        Array.from(selectedUploads).forEach((file) => {
          const lower = file.name.toLowerCase();
          if (lower.endsWith(".zip")) {
            formData.append("archives", file);
          } else if (lower.endsWith(".csv")) {
            formData.append("files", file);
          }
        });

        const response = await apiFetch("/api/datasets/upload", {
          method: "POST",
          body: formData
        });

        const payload = await readJson<DatasetListResponse>(response);
        setBatch(payload);
        setBatchProgress(null);
        setDashboardItems([]);
        setBatchId(payload.batchId);
        setSelectedDatasetId(payload.datasets[0]?.datasetId ?? "");
        setProfile(null);
        setRows([]);
        setChart(null);
      } catch (error) {
        setLastError(error instanceof Error ? error.message : "Upload failed.");
      }
    });
  }

  async function loadBatch() {
    if (!batchId.trim()) {
      setLastError("Enter a batch ID first.");
      return;
    }

    setLastError("");
    startTransition(async () => {
      try {
        const payload = await fetchBatch(batchId.trim());
        setBatch(payload);
        setBatchProgress(null);
        setDashboardItems([]);
        if (payload.datasets[0]) {
          setSelectedDatasetId(payload.datasets[0].datasetId);
        }
      } catch (error) {
        setLastError(error instanceof Error ? error.message : "Unable to load batch.");
      }
    });
  }

  async function generateChart() {
    if (!selectedDatasetId) {
      setLastError("Select a dataset first.");
      return;
    }

    if (needsCategoryColumn && !selectedCategoryColumn) {
      setLastError("Choose an X or category column.");
      return;
    }

    if (needsValueColumn && !selectedValueColumn) {
      setLastError("Choose a numeric value column.");
      return;
    }

    const payload = {
      chartType,
      xColumn:
        chartType === "histogram" || chartType === "box"
          ? selectedValueColumn
          : selectedCategoryColumn,
      yColumn:
        chartType === "scatter"
          ? selectedValueColumn
          : aggregation === "sum"
            ? selectedValueColumn
            : null,
      aggregation: aggregationDisabled ? "distribution" : aggregation,
      limit: chartType === "pie" ? 6 : 10
    };

    setLastError("");
    startTransition(async () => {
      try {
        const response = await apiFetch(`/api/datasets/${selectedDatasetId}/charts`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify(payload)
        });

        const nextChart = await readJson<DatasetChartResponse>(response);
        setChart(nextChart);
      } catch (error) {
        setLastError(error instanceof Error ? error.message : "Unable to generate chart.");
      }
    });
  }

  async function saveCurrentChart() {
    if (!selectedDatasetId || !chart) {
      setLastError("Build a chart first, then save it.");
      return;
    }

    const name = chartSaveName.trim() || `${chartType} chart`;
    try {
      const response = await apiFetch(`/api/datasets/${selectedDatasetId}/saved-charts`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          name,
          chartType,
          xColumn: chartType === "histogram" || chartType === "box" ? selectedValueColumn : selectedCategoryColumn,
          yColumn: chartType === "scatter" || aggregation === "sum" ? selectedValueColumn : null,
          aggregation: aggregationDisabled ? "distribution" : aggregation,
          limit: chartType === "pie" ? 6 : 10
        })
      });
      const saved = await readJson<SavedChart>(response);
      setSavedCharts((current) => [saved, ...current.filter((item) => item.chartId !== saved.chartId)]);
      setChartSaveName("");
    } catch (error) {
      setLastError(error instanceof Error ? error.message : "Unable to save chart.");
    }
  }

  async function applySavedChart(savedChart: SavedChart) {
    setChartType(savedChart.chartType);
    setSelectedCategoryColumn(savedChart.xColumn);
    setSelectedValueColumn(savedChart.yColumn ?? "");
    setAggregation(savedChart.aggregation);

    try {
      const response = await apiFetch(`/api/datasets/${savedChart.datasetId}/charts`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          chartType: savedChart.chartType,
          xColumn: savedChart.xColumn,
          yColumn: savedChart.yColumn,
          aggregation: savedChart.aggregation,
          limit: savedChart.limitValue
        })
      });
      setChart(await readJson<DatasetChartResponse>(response));
    } catch (error) {
      setLastError(error instanceof Error ? error.message : "Unable to apply saved chart.");
    }
  }

  async function deleteSavedChart(chartId: string) {
    try {
      await readJson<void>(await apiFetch(`/api/saved-charts/${chartId}`, {
        method: "DELETE"
      }));
    } catch {
      const response = await apiFetch(`/api/saved-charts/${chartId}`, { method: "DELETE" });
      if (!response.ok) {
        const text = await response.text();
        setLastError(text || "Unable to delete saved chart.");
        return;
      }
    }
    setSavedCharts((current) => current.filter((chartItem) => chartItem.chartId !== chartId));
  }

  async function retryDatasetJob(datasetId: string) {
    setLastError("");
    setRetryingDatasetId(datasetId);
    try {
      const response = await fetch(`/api/datasets/${datasetId}/jobs/retry`, {
        method: "POST"
      });
      const job = await readJson<DatasetJob>(response);
      setJobMap((current) => ({ ...current, [datasetId]: job }));
      if (batchId) {
        const nextBatchProgress = await fetchBatchProgress(batchId);
        setBatchProgress(nextBatchProgress);
        setBatch({
          batchId: nextBatchProgress.batchId,
          datasets: nextBatchProgress.items.map((item) => item.dataset)
        });
      } else {
        await loadDashboard();
      }
    } catch (error) {
      setLastError(error instanceof Error ? error.message : "Unable to retry dataset ingestion.");
    } finally {
      setRetryingDatasetId("");
    }
  }

  return (
    <main className="page-shell">
      <section className="hero">
        <div className="eyebrow">CSV Workspace</div>
        <div className="hero-grid hero-grid-single">
          <div className="hero-copy">
            <h1>Upload a batch, pick one CSV, and turn it into a chart in a few clicks.</h1>
            <p>
              The flow stays simple: upload CSVs or ZIPs, choose a dataset from the left, preview the table in the
              center, and build one graph on the right with only the controls that matter for that chart.
            </p>
            <div className="kpi-ribbon">
              <span className="kpi-pill">Multi-CSV upload</span>
              <span className="kpi-pill">ZIP extraction</span>
              <span className="kpi-pill">Compact dataset preview</span>
              <span className="kpi-pill">8 chart types</span>
            </div>
          </div>
        </div>
      </section>

      <section className="panel" style={{ marginTop: 18 }}>
        <div className="panel-header">
          <div>
            <h2>{authUser ? "Workspace Access" : authMode === "signup" ? "Create Account" : "Sign In"}</h2>
            <p>{authUser ? "Your datasets and saved charts are tied to this account." : "Use JWT auth to access your own datasets and chart library."}</p>
          </div>
        </div>

        {authUser ? (
          <div className="actions" style={{ justifyContent: "space-between" }}>
            <div className="dataset-inline-meta">
              <span className="dataset-inline-pill">{authUser.name}</span>
              <span className="dataset-inline-pill">{authUser.email}</span>
            </div>
            <button className="button secondary" type="button" onClick={logout}>Log Out</button>
          </div>
        ) : (
          <form className="form-grid" onSubmit={handleAuthSubmit}>
            <div className="form-row">
              {authMode === "signup" ? (
                <div className="field">
                  <label htmlFor="auth-name">Name</label>
                  <input id="auth-name" value={authName} onChange={(event) => setAuthName(event.target.value)} />
                </div>
              ) : null}
              <div className="field">
                <label htmlFor="auth-email">Email</label>
                <input id="auth-email" type="email" value={authEmail} onChange={(event) => setAuthEmail(event.target.value)} />
              </div>
              <div className="field">
                <label htmlFor="auth-password">Password</label>
                <input id="auth-password" type="password" value={authPassword} onChange={(event) => setAuthPassword(event.target.value)} />
              </div>
            </div>
            <div className="actions">
              <button className="button" type="submit" disabled={isPending}>
                {authMode === "signup" ? "Create Account" : "Sign In"}
              </button>
              <button
                className="button secondary"
                type="button"
                onClick={() => setAuthMode((current) => current === "signup" ? "login" : "signup")}
              >
                {authMode === "signup" ? "Use Login Instead" : "Create New Account"}
              </button>
            </div>
          </form>
        )}
      </section>

      {!authUser ? (
        <section className="panel" style={{ marginTop: 18 }}>
          <div className="list-row">
            <strong>Authentication required</strong>
            <span>Sign in above to upload datasets, view your dashboard, and save chart configurations.</span>
          </div>
        </section>
      ) : null}

      <section className="stats-grid">
        <article className="stat-tile">
          <p>Batch ID</p>
          <strong>{batchId || "Pending"}</strong>
        </article>
        <article className="stat-tile">
          <p>{batchId ? "CSVs in batch" : "Datasets shown"}</p>
          <strong>{datasets.length}</strong>
        </article>
        <article className="stat-tile">
          <p>Ready datasets</p>
          <strong>{readyCount}</strong>
        </article>
        <article className="stat-tile">
          <p>Selected rows</p>
          <strong>{selectedDataset?.rowCount ?? 0}</strong>
        </article>
      </section>

      <section className="stack-grid" style={{ marginTop: 22, opacity: authUser ? 1 : 0.45, pointerEvents: authUser ? "auto" : "none" }}>
        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Upload Batch</h2>
              <p>Upload multiple CSV files, ZIPs of CSVs, or reload any previous batch using its batch ID.</p>
            </div>
          </div>

          {lastError ? (
            <div className="status-banner">
              <strong>Last error</strong>
              <p>{lastError}</p>
            </div>
          ) : null}

          <form className="form-grid" onSubmit={handleUpload}>
            <div className="form-row">
              <div className="field">
                <label htmlFor="batch-files">CSV or ZIP files</label>
                <input
                  id="batch-files"
                  type="file"
                  multiple
                  accept=".csv,.zip,text/csv,application/zip"
                  onChange={(event) => setSelectedUploads(event.target.files)}
                />
              </div>

              <div className="field">
                <label htmlFor="batch-id">Existing batch ID</label>
                <input
                  id="batch-id"
                  value={batchId}
                  onChange={(event) => setBatchId(event.target.value)}
                  placeholder="Paste a batch ID to reload its CSV list"
                />
              </div>
            </div>

            <div className="actions">
              <button className="button" type="submit" disabled={isPending}>
                {isPending ? "Working..." : "Upload Batch"}
              </button>
              <button className="button secondary" type="button" onClick={loadBatch} disabled={isPending}>
                Load Batch
              </button>
            </div>
          </form>

          {datasets.length > 0 ? (
            <div className="progress-overview">
              <div className="split-note">
                <strong>Batch progress</strong>
                <span>{batchProgressPercentage}% complete</span>
              </div>
              <div className="meter">
                <div style={{ width: `${batchProgressPercentage}%` }} />
              </div>
              <div className="progress-stats">
                <span>{readyCount} ready</span>
                <span>{processingCount} processing</span>
                <span>{failedCount} failed</span>
                {batchProgress ? (
                  <span>{batchProgress.processedRows}/{batchProgress.totalRows || 0} rows processed</span>
                ) : null}
              </div>
            </div>
          ) : null}

          {failedCount > 0 ? (
            <div className="status-banner danger">
              <strong>Failed datasets need attention</strong>
              <p>
                {failedCount} dataset{failedCount === 1 ? "" : "s"} failed during ingestion. Open a CSV row to inspect
                the error and retry it.
              </p>
            </div>
          ) : null}
        </section>

        <section className="stack-grid">
          <section className="panel">
            <div className="panel-header">
              <div>
                <h2>CSV List</h2>
                <p>{batchId ? "Each row is one CSV in the batch." : "Recent datasets are shown when no batch is loaded."} Click a row to open it below.</p>
              </div>
            </div>

            <div className="dataset-accordion">
              {datasets.length === 0 ? (
                <div className="list-row">
                  <strong>No datasets loaded</strong>
                  <span>Upload a batch or load an existing batch ID.</span>
                </div>
              ) : (
                datasets.map((dataset) => {
                  const isActive = dataset.datasetId === selectedDatasetId;
                  const itemJob = batchProgress?.items.find((item) => item.dataset.datasetId === dataset.datasetId)?.job
                    ?? dashboardItems.find((item) => item.dataset.datasetId === dataset.datasetId)?.job
                    ?? null;
                  const liveJob = jobMap[dataset.datasetId] ?? itemJob ?? null;
                  return (
                    <div key={dataset.datasetId} className={`dataset-card ${isActive ? "active" : ""}`}>
                      <button
                        type="button"
                        className={`dataset-row ${isActive ? "active" : ""}`}
                        onClick={() => setSelectedDatasetId(isActive ? "" : dataset.datasetId)}
                      >
                        <div className="dataset-row-main">
                          <strong>{dataset.originalFilename}</strong>
                          <span className="dataset-row-arrow">{isActive ? "−" : "+"}</span>
                        </div>
                        <div className="dataset-row-meta">
                          <span>{dataset.status}</span>
                          <span>{dataset.rowCount} rows</span>
                          <span>{dataset.columnCount} columns</span>
                          <span title={dataset.storagePath}>
                            {liveJob && liveJob.status !== "COMPLETED"
                              ? `${liveJob.progressPercentage}% job progress`
                              : truncateLabel(dataset.storagePath, 40)}
                          </span>
                        </div>
                        {liveJob ? (
                          <div className="dataset-row-progress">
                            <div className="meter">
                              <div style={{ width: `${liveJob.progressPercentage}%` }} />
                            </div>
                            <div className="progress-stats">
                              <span>{liveJob.status}</span>
                              <span>{liveJob.processedRows}/{liveJob.totalRows || dataset.rowCount || 0} rows</span>
                              <span>{liveJob.retryCount} retries</span>
                            </div>
                          </div>
                        ) : null}
                        {liveJob?.errorMessage ? (
                          <div className="dataset-failure-note">
                            <strong>Failure</strong>
                            <span>{truncateLabel(liveJob.errorMessage, 100)}</span>
                          </div>
                        ) : null}
                      </button>

                      {isActive ? (
                        <div className="dataset-dropdown">
                          <section className="dataset-workspace">
                            <div className="stack-grid">
                              <section className="panel panel-embedded">
                                <div className="panel-header compact-header">
                                  <div>
                                    <h2>Dataset Table</h2>
                                    <p>{formatDatasetLabel(dataset)}</p>
                                  </div>
                                  <span className={`status-chip ${dataset.status === "READY" ? "success" : ""}`}>
                                    <span className={`status-dot ${dataset.status === "READY" ? "success" : ""}`} />
                                    {dataset.status}
                                  </span>
                                </div>

                                <div className="dataset-inline-meta">
                                  <span className="dataset-inline-pill">{dataset.rowCount} rows</span>
                                  <span className="dataset-inline-pill">{dataset.columnCount} columns</span>
                                  <span className="dataset-inline-pill">{formatBytes(dataset.fileSizeBytes)}</span>
                                  {liveJob ? (
                                    <span className="dataset-inline-pill">
                                      Job {liveJob.status} · {liveJob.progressPercentage}%
                                    </span>
                                  ) : null}
                                </div>

                                {liveJob ? (
                                  <div className="job-card">
                                    <div className="split-note">
                                      <strong>Ingestion progress</strong>
                                      <span>{liveJob.progressPercentage}%</span>
                                    </div>
                                    <div className="meter">
                                      <div style={{ width: `${liveJob.progressPercentage}%` }} />
                                    </div>
                                    <div className="progress-stats">
                                      <span>{liveJob.processedRows}/{liveJob.totalRows || dataset.rowCount || 0} rows</span>
                                      <span>{liveJob.processingTimeMs ? `${Math.round(liveJob.processingTimeMs / 1000)}s` : "timing pending"}</span>
                                      <span>{liveJob.retryCount} retries</span>
                                    </div>
                                    {liveJob.errorMessage ? (
                                      <div className="job-error">{liveJob.errorMessage}</div>
                                    ) : null}
                                    {liveJob.status === "FAILED" ? (
                                      <div className="job-actions">
                                        <button
                                          className="button ghost"
                                          type="button"
                                          onClick={() => retryDatasetJob(dataset.datasetId)}
                                          disabled={retryingDatasetId === dataset.datasetId}
                                        >
                                          {retryingDatasetId === dataset.datasetId ? "Retrying..." : "Retry ingestion"}
                                        </button>
                                      </div>
                                    ) : null}
                                  </div>
                                ) : null}

                                <div className="column-chip-row">
                                  {allColumns.slice(0, 8).map((column) => (
                                    <span className="column-chip" key={column.id}>
                                      {formatColumnBadge(column)}
                                    </span>
                                  ))}
                                  {allColumns.length > 8 ? <span className="column-chip">+{allColumns.length - 8} more</span> : null}
                                </div>

                                {rows.length === 0 ? (
                                  <div className="list-row">
                                    <strong>No table preview yet</strong>
                                    <span>The table appears here when the dataset is ready.</span>
                                  </div>
                                ) : (
                                  <div className="table-wrap compact-table-wrap">
                                    <table className="data-table" style={{ minWidth: `${Math.max(900, rowColumns.length * 150)}px` }}>
                                      <thead>
                                        <tr>
                                          {rowColumns.map((column) => (
                                            <th key={column}>{column}</th>
                                          ))}
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {rows.map((row, rowIndex) => (
                                          <tr key={rowIndex}>
                                            {rowColumns.map((column) => (
                                              <td key={`${rowIndex}-${column}`} title={String(row[column] ?? "")}>
                                                {String(row[column] ?? "")}
                                              </td>
                                            ))}
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  </div>
                                )}
                              </section>
                            </div>

                            <div className="stack-grid">
                              <section className="panel panel-embedded">
                                <div className="panel-header">
                                  <div>
                                    <h2>Chart Builder</h2>
                                    <p>{chartHelp[chartType]}</p>
                                  </div>
                                </div>

                                <div className="form-grid">
                                  <div className="form-row">
                                    <div className="field">
                                      <label htmlFor="chart-type">Graph type</label>
                                      <select id="chart-type" value={chartType} onChange={(event) => setChartType(event.target.value)}>
                                        {chartOptions.map((option) => (
                                          <option key={option.value} value={option.value}>
                                            {option.label}
                                          </option>
                                        ))}
                                      </select>
                                    </div>

                                    <div className="field">
                                      <label htmlFor="aggregation">Calculation</label>
                                      <select
                                        id="aggregation"
                                        value={aggregation}
                                        onChange={(event) => setAggregation(event.target.value)}
                                        disabled={aggregationDisabled}
                                      >
                                        <option value="count">Count</option>
                                        <option value="sum">Sum</option>
                                      </select>
                                    </div>
                                  </div>

                                  {needsCategoryColumn ? (
                                    <div className="field">
                                      <label htmlFor="category-column">
                                        {chartType === "line" || chartType === "area" ? "X-axis column" : "Category / X column"}
                                      </label>
                                      <select
                                        id="category-column"
                                        value={selectedCategoryColumn}
                                        onChange={(event) => setSelectedCategoryColumn(event.target.value)}
                                      >
                                        <option value="">Select a column</option>
                                        {allColumns.map((column) => (
                                          <option key={column.id} value={column.columnName}>
                                            {column.columnName}
                                          </option>
                                        ))}
                                      </select>
                                    </div>
                                  ) : null}

                                  {needsValueColumn ? (
                                    <div className="field">
                                      <label htmlFor="value-column">
                                        {chartType === "box"
                                          ? "Numeric column"
                                          : chartType === "scatter"
                                            ? "Y-axis numeric column"
                                            : chartType === "histogram"
                                              ? "Distribution column"
                                              : "Numeric value column"}
                                      </label>
                                      <select
                                        id="value-column"
                                        value={selectedValueColumn}
                                        onChange={(event) => setSelectedValueColumn(event.target.value)}
                                      >
                                        <option value="">Select a numeric column</option>
                                        {numericColumns.map((column) => (
                                          <option key={column.id} value={column.columnName}>
                                            {column.columnName}
                                          </option>
                                        ))}
                                      </select>
                                    </div>
                                  ) : null}

                                  <div className="actions">
                                    <button
                                      className="button"
                                      type="button"
                                      onClick={generateChart}
                                      disabled={isPending || !selectedDatasetId || dataset.status !== "READY"}
                                    >
                                      {isPending ? "Building..." : "Build Chart"}
                                    </button>
                                  </div>
                                </div>

                                <div className="chart-shell">
                                  {dataset.status !== "READY" ? (
                                    <div className="list-row">
                                      <strong>Charting waits for a ready dataset</strong>
                                      <span>Finish or retry ingestion first, then the chart builder will work normally.</span>
                                    </div>
                                  ) : chart ? (
                                    renderChart(chartType, chart, chartSeries, pieSeries, scatterSeries, pictographTotal)
                                  ) : (
                                    <div className="list-row">
                                      <strong>No chart yet</strong>
                                      <span>Pick a graph type, choose columns, and build the chart for the selected CSV.</span>
                                    </div>
                                  )}
                                </div>

                                {dataset.status === "READY" ? (
                                  <div className="stack-grid" style={{ marginTop: 14 }}>
                                    <div className="field">
                                      <label htmlFor={`save-chart-${dataset.datasetId}`}>Save this chart</label>
                                      <input
                                        id={`save-chart-${dataset.datasetId}`}
                                        value={chartSaveName}
                                        onChange={(event) => setChartSaveName(event.target.value)}
                                        placeholder="Quarterly revenue chart"
                                      />
                                    </div>
                                    <div className="actions">
                                      <button className="button secondary" type="button" onClick={saveCurrentChart} disabled={!chart}>
                                        Save Chart
                                      </button>
                                    </div>
                                    <div className="list-grid">
                                      {savedCharts.length === 0 ? (
                                        <div className="list-row">
                                          <strong>No saved charts yet</strong>
                                          <span>Save a chart config here to reuse it later.</span>
                                        </div>
                                      ) : (
                                        savedCharts.map((savedChart) => (
                                          <div className="list-row" key={savedChart.chartId}>
                                            <strong>{savedChart.name}</strong>
                                            <span>{savedChart.chartType} · {savedChart.aggregation}</span>
                                            <div className="actions">
                                              <button className="button ghost" type="button" onClick={() => applySavedChart(savedChart)}>
                                                Apply
                                              </button>
                                              <button className="button ghost" type="button" onClick={() => deleteSavedChart(savedChart.chartId)}>
                                                Delete
                                              </button>
                                            </div>
                                          </div>
                                        ))
                                      )}
                                    </div>
                                  </div>
                                ) : null}
                              </section>
                            </div>
                          </section>
                        </div>
                      ) : null}
                    </div>
                  );
                })
              )}
            </div>
          </section>
        </section>
      </section>
    </main>
  );
}

function renderChart(
  chartType: string,
  chart: DatasetChartResponse,
  chartSeries: ChartSeriesItem[],
  pieSeries: ChartSeriesItem[],
  scatterSeries: Array<{ x: number; y: number; label: string }>,
  pictographTotal: number
) {
  if (chartType === "pie") {
    return (
      <div className="pie-layout">
        <ResponsiveContainer width="100%" height={280}>
          <PieChart>
            <Pie data={pieSeries} dataKey="value" nameKey="name" outerRadius={100} innerRadius={56}>
              {pieSeries.map((entry, index) => (
                <Cell key={entry.name} fill={pieColors[index % pieColors.length]} />
              ))}
            </Pie>
            <Tooltip />
          </PieChart>
        </ResponsiveContainer>

        <div className="chart-legend-grid">
          {pieSeries.map((entry, index) => (
            <div className="chart-legend-row" key={entry.name}>
              <span className="chart-swatch" style={{ background: pieColors[index % pieColors.length] }} />
              <span title={entry.name}>{truncateLabel(entry.name, 24)}</span>
              <strong>{entry.value.toFixed(0)}</strong>
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (chartType === "line") {
    return (
      <ResponsiveContainer width="100%" height={320}>
        <LineChart data={chartSeries}>
          <CartesianGrid stroke="rgba(255,255,255,0.08)" />
          <XAxis dataKey="name" stroke="#98a2ad" tickFormatter={(value) => truncateLabel(String(value), 14)} />
          <YAxis stroke="#98a2ad" />
          <Tooltip />
          <Line type="monotone" dataKey="value" stroke="#f7c66a" strokeWidth={3} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    );
  }

  if (chartType === "area") {
    return (
      <ResponsiveContainer width="100%" height={320}>
        <AreaChart data={chartSeries}>
          <CartesianGrid stroke="rgba(255,255,255,0.08)" />
          <XAxis dataKey="name" stroke="#98a2ad" tickFormatter={(value) => truncateLabel(String(value), 14)} />
          <YAxis stroke="#98a2ad" />
          <Tooltip />
          <Area type="monotone" dataKey="value" stroke="#f7c66a" fill="rgba(247, 198, 106, 0.28)" />
        </AreaChart>
      </ResponsiveContainer>
    );
  }

  if (chartType === "scatter") {
    return (
      <ResponsiveContainer width="100%" height={320}>
        <ScatterChart>
          <CartesianGrid stroke="rgba(255,255,255,0.08)" />
          <XAxis type="number" dataKey="x" name={chart.xColumn} stroke="#98a2ad" />
          <YAxis type="number" dataKey="y" name={chart.yColumn ?? "y"} stroke="#98a2ad" />
          <Tooltip cursor={{ strokeDasharray: "3 3" }} />
          <Scatter data={scatterSeries} fill="#f7c66a" />
        </ScatterChart>
      </ResponsiveContainer>
    );
  }

  if (chartType === "box" && chart.boxPlot) {
    const stats = chart.boxPlot;
    const range = Math.max((stats.max ?? 1) - (stats.min ?? 0), 1);
    const left = (((stats.q1 ?? 0) - (stats.min ?? 0)) / range) * 100;
    const width = (((stats.q3 ?? 0) - (stats.q1 ?? 0)) / range) * 100;
    const median = (((stats.median ?? 0) - (stats.min ?? 0)) / range) * 100;

    return (
      <div className="boxplot-shell">
        <div className="boxplot-scale">
          <span>{stats.min?.toFixed(2)}</span>
          <span>{stats.max?.toFixed(2)}</span>
        </div>
        <div className="boxplot-track">
          <div className="boxplot-whisker" />
          <div className="boxplot-box" style={{ left: `${left}%`, width: `${Math.max(width, 4)}%` }}>
            <div className="boxplot-median" style={{ left: `${Math.max(median - left, 2)}%` }} />
          </div>
        </div>
        <div className="boxplot-stats">
          <span>Q1: {stats.q1?.toFixed(2)}</span>
          <span>Median: {stats.median?.toFixed(2)}</span>
          <span>Q3: {stats.q3?.toFixed(2)}</span>
        </div>
      </div>
    );
  }

  if (chartType === "pictograph") {
    return (
      <div className="pictograph-grid">
        {chartSeries.map((item) => {
          const icons = pictographTotal === 0 ? 0 : Math.max(1, Math.round((item.value / pictographTotal) * 24));
          return (
            <div className="pictograph-row" key={item.name}>
              <div className="split-note">
                <strong>{truncateLabel(item.name, 28)}</strong>
                <span>{item.value.toFixed(0)}</span>
              </div>
              <div className="pictograph-icons">
                {Array.from({ length: icons }).map((_, index) => (
                  <span className="pictograph-icon" key={`${item.name}-${index}`} />
                ))}
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={320}>
      <BarChart data={chartSeries}>
        <CartesianGrid stroke="rgba(255,255,255,0.08)" />
        <XAxis dataKey="name" stroke="#98a2ad" tickFormatter={(value) => truncateLabel(String(value), 14)} />
        <YAxis stroke="#98a2ad" />
        <Tooltip />
        <Bar dataKey="value" fill="#f7c66a" radius={[8, 8, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

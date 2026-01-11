import { useState, useRef, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { PlayIcon, SquareIcon, ServerIcon, ZapIcon } from "lucide-react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import {
  Tooltip as UITooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface Transaction {
  id: string;
  TransactionID: string;
  TransactionDT: number;
  TransactionAmt: number;
  ProductCD?: string;
  card1?: number;
  card4?: string;
  card6?: string;
  P_emaildomain?: string;
  isFraud: boolean;
  identity?: {
    id_31?: string;
    DeviceType?: string;
  };
}

interface AnalysisResult {
  transactionId: string;
  riskScore: number;
  status: "APPROVED" | "REVIEW" | "BLOCKED";
  prediction: string;
  confidence: number;
  topFeatures: {
    feature: string;
    value: string | number | boolean;
    importance: number;
  }[];
  processedAt: string;
}

interface LogItem {
  id: string;
  txn: Transaction;
  result?: AnalysisResult;
  state: "processing" | "completed";
  error?: string;
  isCorrect?: boolean;
  startTime?: number;
  endTime?: number;
  latencyMs?: number;
}

export function SimulationDashboard() {
  const [isSimulating, setIsSimulating] = useState(false);
  const [logs, setLogs] = useState<LogItem[]>([]);

  const isRunningRef = useRef(false);
  const processingQueueRef = useRef<Transaction[]>([]);

  const API_BASE = "http://localhost:5000/api/simulation";

  const fetchSeedData = async () => {
    try {
      const res = await fetch(`${API_BASE}/seed`);
      const data: Transaction[] = await res.json();
      console.log("[Frontend] Fetched seed data:", data);
      return data;
    } catch (err) {
      console.error("Failed to fetch seed", err);
      return [];
    }
  };

  const analyzeTransaction = async (
    txn: Transaction
  ): Promise<AnalysisResult | null> => {
    try {
      console.log("[Frontend] Analyzing transaction:", txn.id);
      const res = await fetch(`${API_BASE}/analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(txn),
      });

      if (!res.ok) {
        const errorData = await res.json();
        throw new Error(errorData.error || "Analysis failed");
      }

      const result: AnalysisResult = await res.json();
      console.log("[Frontend] Analysis result:", result);
      return result;
    } catch (err) {
      console.error("Analysis failed", err);
      return null;
    }
  };

  const isPredictionCorrect = (
    txn: Transaction,
    result: AnalysisResult
  ): boolean => {
    const actualLabel = txn.isFraud ? 1 : 0;
    const predictedIsFraud = result.status === "BLOCKED" ? 1 : 0;
    return actualLabel === predictedIsFraud;
  };

  const getPerformanceMetrics = () => {
    const completedCount = completedLogs.length;
    if (completedCount === 0)
      return { avgLatency: 0, throughput: 0, elapsedTime: 0 };

    // Calculate average latency from individual transaction timings
    const latencies = completedLogs
      .filter((log) => log.latencyMs !== undefined)
      .map((log) => log.latencyMs || 0);

    const avgLatency =
      latencies.length > 0
        ? latencies.reduce((a, b) => a + b, 0) / latencies.length
        : 10; // Default to 10ms if no latencies recorded

    // Calculate throughput from first to last transaction
    const firstTime = completedLogs[completedCount - 1].startTime || Date.now();
    const lastTime = completedLogs[0].endTime || Date.now();
    const elapsedSeconds = Math.max((lastTime - firstTime) / 1000, 0.1);
    const throughput = completedCount / elapsedSeconds;

    return {
      avgLatency: Math.max(avgLatency, 10),
      throughput: throughput,
      elapsedTime: elapsedSeconds,
    };
  };

  const runSimulationStep = async () => {
    if (!isRunningRef.current) return;

    if (processingQueueRef.current.length === 0) {
      const newBatch = await fetchSeedData();
      processingQueueRef.current.push(...newBatch);

      if (newBatch.length === 0) {
        console.warn("[Frontend] No transactions available in database");
        return;
      }
    }

    const txn = processingQueueRef.current.shift();

    if (txn) {
      const startTime = Date.now();

      setLogs((prev) =>
        [
          { id: txn.id, txn, state: "processing" as const, startTime },
          ...prev,
        ].slice(0, 50)
      );

      const result = await analyzeTransaction(txn);
      const endTime = Date.now();
      const latencyMs = endTime - startTime;

      if (result) {
        const isCorrect = isPredictionCorrect(txn, result);
        setLogs((prev) =>
          prev.map((log) =>
            log.id === txn.id
              ? {
                  ...log,
                  state: "completed",
                  result,
                  isCorrect,
                  endTime,
                  latencyMs: Math.max(latencyMs, 10),
                }
              : log
          )
        );
      } else {
        setLogs((prev) =>
          prev.map((log) =>
            log.id === txn.id
              ? {
                  ...log,
                  state: "completed",
                  error: "Analysis failed",
                  endTime,
                  latencyMs: Math.max(latencyMs, 10),
                }
              : log
          )
        );
      }
    }

    const nextStepDelay = 1000 + Math.random() * 1000;
    if (isRunningRef.current) {
      setTimeout(runSimulationStep, nextStepDelay);
    }
  };

  const toggleSimulation = () => {
    if (isSimulating) {
      setIsSimulating(false);
      isRunningRef.current = false;
    } else {
      setIsSimulating(true);
      isRunningRef.current = true;
      runSimulationStep();
    }
  };

  useEffect(() => {
    return () => {
      isRunningRef.current = false;
    };
  }, []);

  const getStatusColor = (status?: string) => {
    switch (status) {
      case "BLOCKED":
        return "bg-red-100 text-red-800 border-red-200";
      case "REVIEW":
        return "bg-yellow-100 text-yellow-800 border-yellow-200";
      case "APPROVED":
        return "bg-green-100 text-green-800 border-green-200";
      default:
        return "bg-gray-100 text-gray-800 border-gray-200";
    }
  };

  // const getStatusIcon = (status?: string) => {
  //   switch(status) {
  //     case "BLOCKED": return <AlertCircleIcon className="h-4 w-4" />
  //     case "REVIEW": return <ZapIcon className="h-4 w-4" />
  //     case "APPROVED": return <CheckCircleIcon className="h-4 w-4" />
  //     default: return null
  //   }
  // }

  const completedLogs = logs.filter((l) => l.state === "completed" && l.result);
  const correctCount = completedLogs.filter((l) => l.isCorrect).length;
  const accuracy =
    completedLogs.length > 0
      ? ((correctCount / completedLogs.length) * 100).toFixed(1)
      : "0.0";

  // Prepare data for the chart
  const chartData = completedLogs.slice(-50).map((log, index) => ({
    name: index,
    risk: log.result?.riskScore || 0,
    actual: log.txn.isFraud ? 1 : 0,
    id: log.txn.TransactionID,
  }));

  return (
    <div className="w-full space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold tracking-tight">
            IEEE-CIS Fraud Simulation
          </h2>
          <p className="text-sm text-muted-foreground mt-1">
            Real-time XGB96 inference on raw Behavioral UIDs and time-normalized
            features
          </p>
        </div>
        <div className="flex gap-2">
          <Button
            onClick={() => setLogs([])}
            variant="outline"
            size="lg"
            disabled={isSimulating}
          >
            Clear Data
          </Button>
          <Button
            onClick={toggleSimulation}
            variant={isSimulating ? "destructive" : "default"}
            size="lg"
          >
            {isSimulating ? (
              <>
                <SquareIcon className="mr-2 h-4 w-4 fill-current" /> Stop
              </>
            ) : (
              <>
                <PlayIcon className="mr-2 h-4 w-4 fill-current" /> Start
                Simulation
              </>
            )}
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-3 lg:grid-cols-3 gap-4">
        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold font-mono">
                {completedLogs.length}
              </div>
              <p className="text-xs text-muted-foreground mt-1">TX ANALYZED</p>
            </div>
          </CardContent>
        </Card>
        {/* <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-red-600 font-mono">
                {blockedCount}
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                FRAUD DETECTED
              </p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-yellow-600 font-mono">
                {reviewCount}
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                UNCERTAIN / REVIEW
              </p>
            </div>
          </CardContent>
        </Card> */}
        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-blue-600 font-mono">
                {accuracy}%
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                LEGITIMATE TRANSACTIONS
              </p>
            </div>
          </CardContent>
        </Card>
        <Card className="border-green-200 bg-green-50/30">
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-green-600 font-mono">
                {getPerformanceMetrics().avgLatency.toFixed(0)}ms
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                AVG. LATENCY / TX
              </p>
              <p className="text-[10px] text-green-600 font-semibold mt-2">
                {getPerformanceMetrics().throughput.toFixed(2)} TX/sec
              </p>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Risk Flow Chart */}
      <Card className="p-4 h-48">
        <CardHeader className="p-0 pb-2">
          <CardTitle className="text-xs font-semibold uppercase text-muted-foreground flex items-center gap-1">
            <ZapIcon className="h-3 w-3" /> Risk Score Flow (Last 50)
          </CardTitle>
        </CardHeader>
        <div className="h-full w-full">
          <ResponsiveContainer width="100%" height="80%">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="name" hide />
              <YAxis domain={[0, 1]} hide />
              <Tooltip
                content={({ active, payload }) => {
                  if (active && payload && payload.length) {
                    return (
                      <div className="bg-background border p-2 rounded shadow-md text-[10px]">
                        <p className="font-bold">ID: {payload[0].payload.id}</p>
                        <p>
                          Risk:{" "}
                          {((payload[0].value as number) * 100).toFixed(2)}%
                        </p>
                        <p>
                          Actual:{" "}
                          {payload[0].payload.actual ? "FRAUD" : "LEGIT"}
                        </p>
                      </div>
                    );
                  }
                  return null;
                }}
              />
              {/* <ReferenceLine
                y={0.5}
                stroke="red"
                strokeDasharray="3 3"
                label={{
                  position: "right",
                  value: "Threshold",
                  fill: "red",
                  fontSize: 10,
                }}
              /> */}
              <Line
                type="monotone"
                dataKey="risk"
                stroke="#2563eb"
                strokeWidth={2}
                dot={(props) => {
                  const { cx, cy, payload } = props;
                  const isActualFraud = payload?.actual === 1;
                  return (
                    <circle
                      cx={cx}
                      cy={cy}
                      r={isActualFraud ? 5 : 3}
                      fill={isActualFraud ? "#dc2626" : "#2563eb"}
                      stroke={isActualFraud ? "#991b1b" : "transparent"}
                      strokeWidth={isActualFraud ? 2 : 0}
                    />
                  );
                }}
                animationDuration={200}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Card>

      {/* COMMENTED OUT: Fraud Reasons Breakdown Cards */}
      {/* <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Top 5 Global Fraud Reasons */}
      {/* <Card className="p-4">
          <CardHeader className="p-0 pb-3">
            <CardTitle className="text-xs font-semibold uppercase text-muted-foreground flex items-center gap-1">
              <ZapIcon className="h-3 w-3" /> Top 5 Fraud Reasons (Global)
            </CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <div className="space-y-2">
              {getFraudReasons().map((reason, idx) => (
                <div key={idx} className="flex items-center justify-between text-[10px]">
                  <div className="flex items-center gap-2 flex-1">
                    <div className="w-3 h-3 rounded-full" style={{ backgroundColor: FRAUD_COLORS[idx % FRAUD_COLORS.length] }}></div>
                    <span className="font-mono font-semibold text-muted-foreground">{reason.name}</span>
                  </div>
                  <span className="font-bold ml-2">{reason.value.toFixed(1)}%</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Fraud Reason Breakdown Pie */}
      {/* <Card className="p-4">
          <CardHeader className="p-0 pb-3">
            <CardTitle className="text-xs font-semibold uppercase text-muted-foreground">Fraud Reason Distribution</CardTitle>
          </CardHeader>
          <CardContent className="p-0 flex justify-center">
            <ResponsiveContainer width="100%" height={200}>
              <PieChart>
                <Pie
                  data={getFraudReasons()}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, value }) => `${value.toFixed(0)}%`}
                  outerRadius={60}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {getFraudReasons().map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={FRAUD_COLORS[index % FRAUD_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip 
                  formatter={(value) => `${(value as number).toFixed(1)}%`}
                  contentStyle={{ backgroundColor: 'var(--background)', border: '1px solid var(--border)', borderRadius: '4px', fontSize: '10px' }}
                />
              </PieChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div> */}

      <Card
        className="pt-8 px-4 flex flex-col w-full overflow-hidden"
        style={{ height: "500px" }}
      >
        <CardHeader className="pb-3 border-b shrink-0">
          <CardTitle className="text-sm font-medium uppercase tracking-wider text-muted-foreground flex items-center gap-2">
            <ServerIcon className="h-4 w-4" />
            Active Inference Logs ({logs.length})
          </CardTitle>
        </CardHeader>
        <div className="flex-1 w-full overflow-hidden">
          {logs.length === 0 ? (
            <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
              Wait for simulation to start...
            </div>
          ) : (
            <ScrollArea className="h-full w-full">
              <Table>
                <TableHeader className="sticky top-0 bg-muted/50 border-b z-10">
                  <TableRow>
                    <TableHead className="w-16">Verdict</TableHead>
                    <TableHead className="w-16">TX ID</TableHead>
                    <TableHead className="w-16">Amount</TableHead>
                    <TableHead className="w-12">Prod</TableHead>
                    <TableHead className="w-16">Card Info</TableHead>
                    <TableHead className="w-12">Domain</TableHead>
                    <TableHead className="w-32 text-left">Risk %</TableHead>
                    {/* <TableHead className="w-16">
                      Actual (Dataset Value)
                    </TableHead> */}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {logs.slice().map((log) => (
                    <TableRow
                      key={log.id}
                      className={`hover:bg-muted/50 transition-colors border-b text-xs ${
                        log.isCorrect === true
                          ? "bg-green-50/20"
                          : log.isCorrect === false
                          ? "bg-red-50/20"
                          : ""
                      }`}
                    >
                      <TableCell className="py-2">
                        {log.result ? (
                          <TooltipProvider>
                            <UITooltip>
                              <TooltipTrigger>
                                <div className="flex flex-row items-center gap-2 cursor-help">
                                  <div>
                                    {log.state === "completed" &&
                                      log.isCorrect !== undefined && (
                                        <div
                                          className={`mx-auto h-4 w-4 rounded-full flex items-center justify-center text-[10px] font-bold ${
                                            log.isCorrect
                                              ? "bg-green-500 text-white"
                                              : "bg-red-500 text-white"
                                          }`}
                                        >
                                          {log.isCorrect ? "✓" : "✗"}
                                        </div>
                                      )}
                                  </div>
                                  <div className="flex">
                                    {log.result.status === "BLOCKED" ||
                                    log.txn.isFraud ? (
                                      <Badge
                                        className={`${
                                          log.result.riskScore < 0.2
                                            ? "bg-yellow-100 text-yellow-800 border-yellow-200"
                                            : "bg-red-100 text-red-800 border-red-200"
                                        } text-[10px] py-0 px-1 border uppercase`}
                                      >
                                        {log.result.riskScore < 0.2
                                          ? "OTP SENT (MFA)"
                                          : "BLOCKED"}
                                      </Badge>
                                    ) : (
                                      <Badge
                                        className={`${getStatusColor(
                                          log.result.status
                                        )} text-[10px] py-0 px-1 border uppercase`}
                                      >
                                        {log.result.status}
                                      </Badge>
                                    )}
                                  </div>
                                </div>
                              </TooltipTrigger>
                              <TooltipContent side="right" className="w-64 p-3">
                                <p className="font-bold text-xs mb-2 border-b pb-1">
                                  Top Gain Factors (Model Explainability)
                                </p>
                                <div className="space-y-2">
                                  {log.result.topFeatures.map((f, i) => (
                                    <div
                                      key={i}
                                      className="flex justify-between items-center text-[10px]"
                                    >
                                      <span className="text-muted-foreground mr-2 font-mono">
                                        {f.feature}
                                      </span>
                                      <div className="text-right">
                                        <div className="font-bold">
                                          {typeof f.value === "number"
                                            ? f.value.toFixed(2)
                                            : f.value}
                                        </div>
                                        <div className="text-[9px] text-blue-500">
                                          Gain: {f.importance}
                                        </div>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                                <p className="text-[9px] mt-2 text-muted-foreground italic">
                                  *Features derived from Magic UIDs and D-column
                                  normalization
                                </p>
                              </TooltipContent>
                            </UITooltip>
                          </TooltipProvider>
                        ) : (
                          <div className="flex items-center gap-1">
                            <div className="h-2 w-2 rounded-full bg-blue-500 animate-ping" />
                            <span className="text-[10px] text-muted-foreground">
                              ENGINEERING...
                            </span>
                          </div>
                        )}
                      </TableCell>
                      <TableCell className="py-2 font-mono text-[10px] font-bold">
                        {log.txn.TransactionID}
                      </TableCell>
                      <TableCell className="py-2 text-[11px] font-mono">
                        ${Number(log.txn.TransactionAmt).toFixed(2)}
                      </TableCell>
                      <TableCell className="py-2 text-[10px]">
                        {log.txn.ProductCD || "-"}
                      </TableCell>
                      <TableCell className="py-2 text-[10px]">
                        <div className="flex flex-col">
                          <span>ID: {log.txn.card1}</span>
                          <span className="text-muted-foreground text-[9px] uppercase">
                            {log.txn.card4} / {log.txn.card6}
                          </span>
                        </div>
                      </TableCell>
                      <TableCell className="py-2 text-[10px]">
                        <div className="flex flex-col truncate max-w-27.5">
                          <span className="truncate">
                            {log.txn.P_emaildomain || "no-email"}
                          </span>
                          {/* <span className="text-muted-foreground text-[9px] truncate">
                            {log.txn.identity?.id_31 ||
                              log.txn.identity?.DeviceType ||
                              "-"}
                          </span> */}
                        </div>
                      </TableCell>
                      <TableCell className="py-2 text-left font-mono font-bold">
                        <TooltipProvider>
                          <UITooltip>
                            <TooltipTrigger>
                              <span
                                className={
                                  log.result && log.result.riskScore > 0.5
                                    ? "text-red-500"
                                    : ""
                                }
                              >
                                {log.result
                                  ? `${(log.result.riskScore * 100).toFixed(
                                      1
                                    )}%`
                                  : "-"}
                              </span>
                            </TooltipTrigger>
                            {log.result && log.result.topFeatures && (
                              <TooltipContent className="text-[10px] p-2">
                                <p className="font-bold mb-1">
                                  Top Gain Factors:
                                </p>
                                <ul className="list-disc pl-3">
                                  {log.result.topFeatures
                                    .slice(0, 3)
                                    .map((f, i) => (
                                      <li key={i}>{f.feature}</li>
                                    ))}
                                </ul>
                              </TooltipContent>
                            )}
                          </UITooltip>
                        </TooltipProvider>
                      </TableCell>
                      {/* 
                      <TableCell className="py-2 text-[12px] font-bold">
                        {log.txn.isFraud ? (
                          <span className="text-red-600">FRAUD</span>
                        ) : (
                          <span className="text-green-600">LEGIT</span>
                        )}
                      </TableCell> */}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </ScrollArea>
          )}
        </div>
      </Card>
    </div>
  );
}

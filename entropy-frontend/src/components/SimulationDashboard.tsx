import { useState, useRef, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { PlayIcon, SquareIcon, ServerIcon, AlertCircleIcon, CheckCircleIcon, ZapIcon } from "lucide-react"

interface Transaction {
  id: string
  amountNgn: number
  senderAccount: string
  receiverAccount: string
  transactionType: string
  timestamp: string
  isFraud?: number | boolean
}

interface AnalysisResult {
  transactionId: string
  riskScore: number
  status: "APPROVED" | "REVIEW" | "BLOCKED"
  prediction: string
  confidence: number
  topFeatures: string[]
  processedAt: string
}

interface LogItem {
  id: string
  txn: Transaction
  result?: AnalysisResult
  state: "processing" | "completed"
  error?: string
  isCorrect?: boolean
}

export function SimulationDashboard() {
  const [isSimulating, setIsSimulating] = useState(false)
  const [logs, setLogs] = useState<LogItem[]>([])
  
  const isRunningRef = useRef(false)
  const processingQueueRef = useRef<Transaction[]>([])

  const API_BASE = "http://localhost:5000/api/simulation"

  const fetchSeedData = async () => {
    try {
      const res = await fetch(`${API_BASE}/seed`)
      const data: Transaction[] = await res.json()
      console.log("[Frontend] Fetched seed data:", data)
      return data
    } catch (err) {
      console.error("Failed to fetch seed", err)
      return []
    }
  }

  const analyzeTransaction = async (txn: Transaction): Promise<AnalysisResult | null> => {
    try {
      console.log("[Frontend] Analyzing transaction:", txn.id)
      const res = await fetch(`${API_BASE}/analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(txn),
      })
      
      if (!res.ok) {
        const errorData = await res.json()
        throw new Error(errorData.error || "Analysis failed")
      }
      
      const result: AnalysisResult = await res.json()
      console.log("[Frontend] Analysis result:", result)
      return result
    } catch (err) {
      console.error("Analysis failed", err)
      return null
    }
  }

  const isPredictionCorrect = (txn: Transaction, result: AnalysisResult): boolean => {
    const actualLabel = txn.isFraud ? 1 : 0
    const predictedIsFraud = result.status === "BLOCKED" ? 1 : 0
    return actualLabel === predictedIsFraud
  }

  const runSimulationStep = async () => {
    if (!isRunningRef.current) return

    if (processingQueueRef.current.length === 0) {
      const newBatch = await fetchSeedData()
      processingQueueRef.current.push(...newBatch)
      
      if (newBatch.length === 0) {
        console.warn("[Frontend] No transactions available in database")
        return
      }
    }

    const txn = processingQueueRef.current.shift()
    
    if (txn) {
      setLogs(prev => [
        { id: txn.id, txn, state: "processing" as const }, 
        ...prev
      ].slice(0, 50))

      const result = await analyzeTransaction(txn)
      
      if (result) {
        const isCorrect = isPredictionCorrect(txn, result)
        setLogs(prev => prev.map(log => 
          log.id === txn.id 
            ? { ...log, state: "completed", result, isCorrect } 
            : log
        ))
      } else {
        setLogs(prev => prev.map(log => 
          log.id === txn.id 
            ? { ...log, state: "completed", error: "Analysis failed" } 
            : log
        ))
      }
    }

    const nextStepDelay = 1000 + Math.random() * 1000
    if (isRunningRef.current) {
      setTimeout(runSimulationStep, nextStepDelay)
    }
  }

  const toggleSimulation = () => {
    if (isSimulating) {
      setIsSimulating(false)
      isRunningRef.current = false
    } else {
      setIsSimulating(true)
      isRunningRef.current = true
      runSimulationStep()
    }
  }

  useEffect(() => {
    return () => {
      isRunningRef.current = false
    }
  }, [])

  const getStatusColor = (status?: string) => {
    switch(status) {
      case "BLOCKED": return "bg-red-100 text-red-800 border-red-200"
      case "REVIEW": return "bg-yellow-100 text-yellow-800 border-yellow-200"
      case "APPROVED": return "bg-green-100 text-green-800 border-green-200"
      default: return "bg-gray-100 text-gray-800 border-gray-200"
    }
  }

  const getStatusIcon = (status?: string) => {
    switch(status) {
      case "BLOCKED": return <AlertCircleIcon className="h-4 w-4" />
      case "REVIEW": return <ZapIcon className="h-4 w-4" />
      case "APPROVED": return <CheckCircleIcon className="h-4 w-4" />
      default: return null
    }
  }

  const completedLogs = logs.filter(l => l.state === 'completed' && l.result)
  const blockedCount = completedLogs.filter(l => l.result?.status === "BLOCKED").length
  const reviewCount = completedLogs.filter(l => l.result?.status === "REVIEW").length
  const correctCount = completedLogs.filter(l => l.isCorrect).length
  const accuracy = completedLogs.length > 0 ? ((correctCount / completedLogs.length) * 100).toFixed(1) : "0.0"

  return (
    <div className="w-full space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold tracking-tight">Live Simulation</h2>
          <p className="text-sm text-muted-foreground mt-1">Real-time transaction fraud detection with ML predictions</p>
        </div>
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
            </>
          )}
        </Button>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold">{completedLogs.length}</div>
              <p className="text-xs text-muted-foreground mt-1">Processed</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-red-600">{blockedCount}</div>
              <p className="text-xs text-muted-foreground mt-1">Blocked/Fraud</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-yellow-600">{reviewCount}</div>
              <p className="text-xs text-muted-foreground mt-1">Needs Review</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-blue-600">{accuracy}%</div>
              <p className="text-xs text-muted-foreground mt-1">Accuracy</p>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card className="flex flex-col w-full overflow-hidden" style={{ height: "600px" }}>
        <CardHeader className="bg-muted/30 pb-3 border-b shrink-0">
          <CardTitle className="text-sm font-medium uppercase tracking-wider text-muted-foreground flex items-center gap-2">
            <ServerIcon className="h-4 w-4" />
            Processed Transactions ({logs.length})
          </CardTitle>
        </CardHeader>
        <div className="flex-1 w-full overflow-hidden">
          {logs.length === 0 ? (
            <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
              No transactions yet. Click Start to begin.
            </div>
          ) : (
            <ScrollArea className="h-full w-full">
              <Table>
                <TableHeader className="sticky top-0 bg-muted/50 border-b">
                  <TableRow>
                    <TableHead className="w-20">Status</TableHead>
                    <TableHead className="w-24">TX ID</TableHead>
                    <TableHead className="w-24">Amount (₦)</TableHead>
                    <TableHead className="w-20">Type</TableHead>
                    <TableHead className="w-16">Actual</TableHead>
                    <TableHead className="w-16">Risk %</TableHead>
                    <TableHead className="w-12">Match</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {logs.map((log) => (
                    <TableRow 
                      key={log.id}
                      className={`hover:bg-muted/50 transition-colors border-b text-xs ${
                        log.isCorrect === true ? 'bg-green-50/30' : 
                        log.isCorrect === false ? 'bg-red-50/30' : ''
                      }`}
                    >
                      <TableCell className="py-2">
                        {log.result ? (
                          <div className="flex items-center gap-1">
                            {getStatusIcon(log.result.status)}
                            <Badge className={`${getStatusColor(log.result.status)} text-xs py-0 px-1`}>
                              {log.result.status}
                            </Badge>
                          </div>
                        ) : (
                          <Badge variant="outline" className="text-xs py-0 px-1 animate-pulse">
                            ...
                          </Badge>
                        )}
                      </TableCell>
                      <TableCell className="py-2 font-mono text-xs">
                        ...{log.id.substring(log.id.length - 6)}
                      </TableCell>
                      <TableCell className="py-2 text-xs">
                        {(log.txn.amountNgn || 0).toLocaleString()}
                      </TableCell>
                      <TableCell className="py-2 text-xs">
                        {log.txn.transactionType}
                      </TableCell>
                      <TableCell className="py-2 text-xs font-semibold">
                        {log.txn.isFraud ? (
                          <span className="text-red-600">Fraud</span>
                        ) : (
                          <span className="text-green-600">Legit</span>
                        )}
                      </TableCell>
                      <TableCell className="py-2 text-xs font-semibold">
                        {log.result ? `${(log.result.riskScore * 100).toFixed(1)}%` : '-'}
                      </TableCell>
                      <TableCell className="py-2 text-xs text-center">
                        {log.state === "completed" && log.isCorrect !== undefined && (
                          <Badge className={log.isCorrect ? 'bg-green-100 text-green-800 text-xs py-0 px-1' : 'bg-red-100 text-red-800 text-xs py-0 px-1'}>
                            {log.isCorrect ? '✓' : '✗'}
                          </Badge>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </ScrollArea>
          )}
        </div>
      </Card>
    </div>
  )
}

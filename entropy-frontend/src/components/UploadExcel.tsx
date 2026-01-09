"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { UploadIcon, CheckCircleIcon, AlertTriangleIcon } from "lucide-react"

const API_URL = "http://localhost:5000/api/upload-excel"

interface UploadResult {
  success: boolean
  message: string
  datasetRunId: string
  recordsProcessed: number
  recordsFailed: number
  errors?: string[]
}

export function UploadExcel() {
  const [file, setFile] = useState<File | null>(null)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<UploadResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0]
    if (selectedFile) {
      setFile(selectedFile)
      setError(null)
      setResult(null)
    }
  }

  const handleUpload = async () => {
    if (!file) return

    setLoading(true)
    setError(null)

    try {
      const formData = new FormData()
      formData.append("file", file)

      const response = await fetch(API_URL, {
        method: "POST",
        body: formData,
      })

      const data = await response.json()

      if (!response.ok) {
        throw new Error(data.error || "Upload failed")
      }

      setResult(data)
      setFile(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : "An error occurred")
    } finally {
      setLoading(false)
    }
  }

  const handleClear = () => {
    setFile(null)
    setResult(null)
    setError(null)
  }

  return (
    <div className="w-full max-w-2xl mx-auto p-4">
      <Card>
        <CardHeader>
          <CardTitle>📊 Excel Upload</CardTitle>
          <CardDescription>Upload your transaction data for fraud analysis</CardDescription>
        </CardHeader>

        <CardContent className="space-y-4">
          {/* File Input */}
          <div className="space-y-2">
            <label className="text-sm font-medium">Select Excel File</label>
            <Input
              type="file"
              accept=".xlsx,.xls"
              onChange={handleFileChange}
              disabled={loading}
              className="cursor-pointer"
            />
            {file && (
              <p className="text-sm text-muted-foreground">
                📄 {file.name} ({(file.size / 1024).toFixed(2)} KB)
              </p>
            )}
          </div>

          {/* Buttons */}
          <div className="flex gap-2">
            <Button
              onClick={handleUpload}
              disabled={!file || loading}
              className="flex-1"
            >
              {loading ? (
                <>
                  <span className="animate-spin mr-2">⏳</span> Processing...
                </>
              ) : (
                <>
                  <UploadIcon className="w-4 h-4 mr-2" /> Upload & Process
                </>
              )}
            </Button>
            <Button
              onClick={handleClear}
              variant="outline"
              disabled={loading}
            >
              Clear
            </Button>
          </div>

          {/* Error Alert */}
          {error && (
            <Alert variant="destructive">
              <AlertTriangleIcon className="h-4 w-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          {/* Success Alert */}
          {result?.success && (
            <Alert className="border-green-200 bg-green-50">
              <CheckCircleIcon className="h-4 w-4 text-green-600" />
              <AlertDescription className="text-green-800">
                ✅ {result.message}
              </AlertDescription>
            </Alert>
          )}

          {/* Results */}
          {result?.success && (
            <div className="space-y-2 p-4 bg-muted rounded-lg text-sm">
              <p>
                <strong>Dataset ID:</strong> {result.datasetRunId}
              </p>
              <p>
                <strong>Records Processed:</strong> {result.recordsProcessed}
              </p>
              {result.recordsFailed > 0 && (
                <p className="text-yellow-700">
                  <strong>Records Failed:</strong> {result.recordsFailed}
                </p>
              )}
              {result.errors && result.errors?.length > 0 && (
                <div className="mt-2">
                  <strong>Errors:</strong>
                  <ul className="list-disc list-inside text-xs mt-1">
                    {result.errors.slice(0, 3).map((err: string, idx: number) => (
                      <li key={idx}>{err}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
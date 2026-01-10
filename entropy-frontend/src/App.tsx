import { useState } from "react"
import { UploadExcel } from "@/components/UploadExcel"
import { SimulationDashboard } from "@/components/SimulationDashboard"
import {
  Sidebar,
  SidebarContent,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarTrigger,
} from "@/components/ui/sidebar"
import { Separator } from "@/components/ui/separator"
import { DatabaseIcon, TrendingUpIcon } from "lucide-react"

function AppSidebar({ activeTab, setActiveTab }: { activeTab: string; setActiveTab: (tab: string) => void }) {
  return (
    <Sidebar>
      <SidebarHeader className="p-4 border-b">
        <div className="flex flex-col space-y-1">
          <h1 className="text-lg font-bold">Entropy</h1>
          <p className="text-xs text-muted-foreground">Fraud Detection</p>
        </div>
      </SidebarHeader>
      <SidebarContent className="p-4">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              onClick={() => setActiveTab("upload")}
              isActive={activeTab === "upload"}
              className="mb-2"
            >
              <DatabaseIcon className="h-4 w-4" />
              <span>Data Ingest (Excel)</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
          <SidebarMenuItem>
            <SidebarMenuButton
              onClick={() => setActiveTab("simulation")}
              isActive={activeTab === "simulation"}
            >
              <TrendingUpIcon className="h-4 w-4" />
              <span>Live Simulation</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarContent>
    </Sidebar>
  )
}

export function App() {
  const [activeTab, setActiveTab] = useState("upload")

  return (
    <SidebarProvider>
      <AppSidebar activeTab={activeTab} setActiveTab={setActiveTab} />
      <SidebarInset>
        <div className="flex-1 flex flex-col">
          {/* Header */}
          <header className="border-b sticky top-0 z-10 bg-background">
            <div className="flex items-center gap-4 px-6 py-4">
              <SidebarTrigger className="-ml-1" />
              <Separator orientation="vertical" className="h-6" />
              <div>
                <h1 className="text-2xl font-bold tracking-tight">Entropy Fraud Detection</h1>
                <p className="text-sm text-muted-foreground">Financial Transaction Analysis System</p>
              </div>
            </div>
          </header>

          {/* Content */}
          <main className="flex-1 overflow-auto p-8">
            {activeTab === "upload" && <UploadExcel />}
            {activeTab === "simulation" && <SimulationDashboard />}
          </main>
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}

export default App
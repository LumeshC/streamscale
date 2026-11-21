import { Toolbar } from "./components/Toolbar";
import { DashboardProvider } from "./hooks/useDashboard";
import { ActiveUsers } from "./sections/ActiveUsers";
import { ConsumerLag } from "./sections/ConsumerLag";
import { EventTypes, Regions } from "./sections/Distribution";
import { Latency } from "./sections/Latency";
import { Payments } from "./sections/Payments";
import { RecentErrors } from "./sections/RecentErrors";
import { Revenue } from "./sections/Revenue";
import { SystemOverview } from "./sections/SystemOverview";
import { Throughput } from "./sections/Throughput";

export function Dashboard() {
  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <img src="/favicon.svg" width={28} height={28} alt="" />
          <div>
            <h1>StreamScale</h1>
            <p>Real-time event analytics</p>
          </div>
        </div>
        <Toolbar />
      </header>
      <main className="grid">
        <div className="span-12"><SystemOverview /></div>
        <div className="span-8"><Throughput /></div>
        <div className="span-4"><ConsumerLag /></div>
        <div className="span-6"><Latency /></div>
        <div className="span-6"><ActiveUsers /></div>
        <div className="span-6"><Revenue /></div>
        <div className="span-6"><Payments /></div>
        <div className="span-6"><Regions /></div>
        <div className="span-6"><EventTypes /></div>
        <div className="span-12"><RecentErrors /></div>
      </main>
    </div>
  );
}

export default function App() {
  return (
    <DashboardProvider>
      <Dashboard />
    </DashboardProvider>
  );
}

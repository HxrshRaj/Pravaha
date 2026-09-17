"use client";

import { clearSession, getEmail, getRole, getToken } from "@/lib/api";
import { clsx } from "@/lib/format";
import {
  Activity,
  AlertTriangle,
  BarChart3,
  Boxes,
  Database,
  FileWarning,
  GaugeCircle,
  GitBranch,
  Home,
  Layers,
  ListTree,
  Radio,
  RefreshCw,
  ScrollText,
  Settings,
  ShieldCheck,
  Sparkles,
  Users,
  Waves,
} from "lucide-react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";

const NAV: { href: string; label: string; icon: any }[] = [
  { href: "/", label: "Overview", icon: Home },
  { href: "/live", label: "Live Stream", icon: Radio },
  { href: "/events", label: "Events", icon: ListTree },
  { href: "/pipelines", label: "Pipelines", icon: GitBranch },
  { href: "/schemas", label: "Schemas", icon: Layers },
  { href: "/producers", label: "Producers", icon: Users },
  { href: "/analytics", label: "Analytics", icon: Activity },
  { href: "/batch", label: "Batch Analytics", icon: BarChart3 },
  { href: "/anomalies", label: "Anomalies", icon: AlertTriangle },
  { href: "/alerts", label: "Alerts", icon: GaugeCircle },
  { href: "/consumers", label: "Consumer Groups", icon: Boxes },
  { href: "/consumers/lag", label: "Consumer Lag", icon: Waves },
  { href: "/data-quality", label: "Data Quality", icon: ShieldCheck },
  { href: "/dlq", label: "DLQ", icon: FileWarning },
  { href: "/replay", label: "Replay", icon: RefreshCw },
  { href: "/ai", label: "AI Intelligence", icon: Sparkles },
  { href: "/system", label: "System Health", icon: Database },
  { href: "/audit", label: "Audit", icon: ScrollText },
  { href: "/settings", label: "Settings", icon: Settings },
];

export function Shell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const [authed, setAuthed] = useState<boolean | null>(null);
  const [email, setEmail] = useState<string | null>(null);
  const [role, setRole] = useState<string | null>(null);

  useEffect(() => {
    const tok = getToken();
    if (!tok && pathname !== "/login") {
      router.replace("/login");
      return;
    }
    setAuthed(!!tok);
    setEmail(getEmail());
    setRole(getRole());
  }, [pathname, router]);

  if (pathname === "/login") return <>{children}</>;
  if (authed === null) return null;

  return (
    <div className="flex min-h-screen">
      <aside className="hidden w-56 shrink-0 flex-col border-r border-border bg-surface md:flex">
        <div className="flex items-center gap-2 border-b border-border px-4 py-3">
          <span className="text-lg font-bold tracking-tight">Pravāha</span>
          <span className="text-[10px] text-muted">streaming</span>
        </div>
        <nav className="flex-1 overflow-y-auto px-2 py-2">
          {NAV.map((n) => {
            const active =
              n.href === "/" ? pathname === "/" : pathname.startsWith(n.href);
            const Icon = n.icon;
            return (
              <Link
                key={n.href}
                href={n.href}
                className={clsx(
                  "flex items-center gap-2 rounded-md px-2.5 py-1.5 text-sm",
                  active
                    ? "bg-accent/15 font-medium text-accent"
                    : "text-muted hover:bg-surface2 hover:text-fg",
                )}
              >
                <Icon size={15} />
                {n.label}
              </Link>
            );
          })}
        </nav>
        <div className="border-t border-border px-3 py-2 text-xs text-muted">
          <div className="truncate">{email ?? "—"}</div>
          <div className="flex items-center justify-between">
            <span>{role ?? ""}</span>
            <button
              className="text-accent hover:underline"
              onClick={() => {
                clearSession();
                router.replace("/login");
              }}
            >
              sign out
            </button>
          </div>
        </div>
      </aside>
      <main className="flex-1 overflow-x-hidden">
        <div className="mx-auto max-w-[1400px] px-4 py-5 md:px-6">{children}</div>
      </main>
    </div>
  );
}

export function PageHeader({
  title,
  desc,
  right,
}: {
  title: string;
  desc?: React.ReactNode;
  right?: React.ReactNode;
}) {
  return (
    <div className="mb-4 flex flex-wrap items-end justify-between gap-2">
      <div>
        <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
        {desc && <p className="mt-0.5 text-sm text-muted">{desc}</p>}
      </div>
      {right}
    </div>
  );
}

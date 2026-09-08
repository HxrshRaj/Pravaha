"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { api, ApiError, sseUrl } from "./api";

export function useApi<T>(
  path: string | null,
  opts?: { intervalMs?: number; deps?: unknown[] },
) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [loading, setLoading] = useState<boolean>(!!path);
  const savedPath = useRef(path);
  savedPath.current = path;

  const refetch = useCallback(async () => {
    if (!savedPath.current) return;
    try {
      const d = await api<T>(savedPath.current);
      setData(d);
      setError(null);
    } catch (e) {
      if (e instanceof ApiError) setError(e);
      else setError(new ApiError(0, "NETWORK", String(e)));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    setLoading(!!path);
    if (!path) return;
    refetch();
    if (opts?.intervalMs) {
      const id = setInterval(refetch, opts.intervalMs);
      return () => clearInterval(id);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, opts?.intervalMs, refetch, ...(opts?.deps ?? [])]);

  return { data, error, loading, refetch };
}

export function useSSE<T>(
  path: string | null,
  event: string,
  opts?: { max?: number },
) {
  const max = opts?.max ?? 200;
  const [items, setItems] = useState<T[]>([]);
  const [connected, setConnected] = useState(false);
  const pausedRef = useRef(false);
  const [paused, setPaused] = useState(false);

  const setPausedBoth = useCallback((v: boolean) => {
    pausedRef.current = v;
    setPaused(v);
  }, []);

  useEffect(() => {
    if (!path) return;
    const es = new EventSource(sseUrl(path));
    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);
    es.addEventListener(event, (ev) => {
      if (pausedRef.current) return;
      try {
        const parsed = JSON.parse((ev as MessageEvent).data) as T;
        setItems((prev) => {
          const next = [parsed, ...prev];
          return next.length > max ? next.slice(0, max) : next;
        });
      } catch {
        /* ignore malformed frame */
      }
    });
    return () => es.close();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, event, max]);

  const clear = useCallback(() => setItems([]), []);
  return { items, connected, paused, setPaused: setPausedBoth, clear };
}

export function useSSEValue<T>(path: string | null, event: string) {
  const [value, setValue] = useState<T | null>(null);
  const [connected, setConnected] = useState(false);
  useEffect(() => {
    if (!path) return;
    const es = new EventSource(sseUrl(path));
    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);
    es.addEventListener(event, (ev) => {
      try {
        setValue(JSON.parse((ev as MessageEvent).data) as T);
      } catch {
        /* ignore */
      }
    });
    return () => es.close();
  }, [path, event]);
  return { value, connected };
}

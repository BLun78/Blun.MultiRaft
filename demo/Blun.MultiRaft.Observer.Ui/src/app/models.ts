/** Mirrors the observer's Contracts.cs. Group ids arrive as strings; the cluster group does not fit in a
 *  JavaScript number. */

export interface NodeView {
  id: number;
  resourceName: string;
  statusPort: number;
  online: boolean;
  aspireState: string | null;
  clusterState: string | null;
  clusterMode: string | null;
  isClusterLeader: boolean;
  clusterLeader: string | null;
  voters: number[];
  learners: number[];
  recentEvents: string[];
  latencyMs: number | null;
  error: string | null;
  walBytes: number;
  walEntries: number;
}

export interface WalStatus {
  sizeBytes: number;
  segments: number;
  firstIndex: number;
  lastIndex: number;
  entries: number;
  bytesPerEntry: number;
}

export interface SendStatus {
  running: boolean;
  sent: number;
  failed: number;
  total: number;
  size: number;
  ratePerSecond: number;
  error: string | null;
}

export interface GroupCell {
  node: number;
  online: boolean;
  role: string;
  term: number;
  leader: string | null;
  commitIndex: number;
  wal: WalStatus | null;
}

export interface GroupView {
  group: string;
  label: string;
  leader: string | null;
  disagreement: boolean;
  cells: GroupCell[];
  send: SendStatus | null;
}

export interface ClusterSnapshot {
  timestampMs: number;
  online: number;
  total: number;
  required: number;
  hasQuorum: boolean;
  clusterLeader: string | null;
  leaderDisagreement: boolean;
  nodes: NodeView[];
  groups: GroupView[];
}

/** One frame of the merged log stream. `reset` means: drop what you hold for this node, a replay follows. */
export interface LogEvent {
  node: number;
  lineNumber: number;
  content: string;
  isError: boolean;
  reset: boolean;
}

/** A log line with the node it came from, so the merged view can colour it. */
export interface NodeLogLine {
  node: number;
  lineNumber: number;
  content: string;
  isError: boolean;
  sequence: number;

  /** The wall-clock part the app host puts in front of every line, split off so it can be dimmed. */
  time: string;
}

export type NodeCommand = 'start' | 'stop' | 'restart';

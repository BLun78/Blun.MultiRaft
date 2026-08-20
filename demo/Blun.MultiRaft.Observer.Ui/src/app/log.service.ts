import { computed, Injectable, signal } from '@angular/core';
import { LogEvent, NodeLogLine } from './models';

/** How many lines are kept per node, and how many the merged view shows. Enough to see an election. */
const PER_NODE = 400;
const MERGED = 800;

/** The colour codes the node wrote for a terminal. */
const ANSI = new RegExp('\\u001b\\[[0-9;]*[A-Za-z]', 'g');

/** The ISO timestamp the app host puts in front of every captured line. */
const TIMESTAMPED = /^(\d{4}-\d{2}-\d{2}T(\d{2}:\d{2}:\d{2})\.\d+Z)\s?([\s\S]*)$/;

@Injectable({ providedIn: 'root' })
export class LogService {
  private readonly _lines = signal<ReadonlyMap<number, readonly NodeLogLine[]>>(new Map());
  private sequence = 0;

  readonly merged = computed(() => {
    const all: NodeLogLine[] = [];

    for (const lines of this._lines().values()) {
      all.push(...lines);
    }

    all.sort((left, right) => left.sequence - right.sequence);
    return all.slice(-MERGED);
  });

  constructor() {
    // One connection for all five nodes. Five would be five of the six a browser allows per origin over
    // HTTP/1.1, and with the cluster stream that is all of them -- every button would then queue behind a
    // connection that never ends. The observer merges them and tags each line with the node it came from.
    const source = new EventSource('/api/logs/stream');
    source.onmessage = (event) => this.accept(JSON.parse(event.data) as LogEvent);
  }

  lines(node: number): readonly NodeLogLine[] {
    return this._lines().get(node) ?? [];
  }

  private accept(event: LogEvent): void {
    // A reset says the observer reconnected to that node and is about to replay its backlog. Dropping what
    // is held is what keeps a reconnect from duplicating the history.
    if (event.reset) {
      this._lines.update((current) => {
        const next = new Map(current);
        next.set(event.node, []);
        return next;
      });

      return;
    }

    this._lines.update((current) => {
      const next = new Map(current);
      const existing = next.get(event.node) ?? [];

      next.set(event.node, [...existing, this.parse(event)].slice(-PER_NODE));
      return next;
    });
  }

  /**
   * Lines arrive as the app host captured them: an ISO timestamp, then the process's own output including
   * the colour codes it wrote for a terminal. The timestamp is split off so it can be dimmed, and the
   * escape sequences are dropped -- rendered as text they are noise, and this is not a terminal.
   */
  private parse(event: LogEvent): NodeLogLine {
    const withoutAnsi = event.content.replace(ANSI, '');
    const match = TIMESTAMPED.exec(withoutAnsi);

    return {
      node: event.node,
      lineNumber: event.lineNumber,
      isError: event.isError,
      sequence: this.sequence++,
      time: match ? match[2] : '',
      content: match ? match[3] : withoutAnsi,
    };
  }
}

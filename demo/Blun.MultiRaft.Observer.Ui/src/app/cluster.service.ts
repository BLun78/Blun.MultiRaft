import { computed, Injectable, signal } from '@angular/core';
import { ClusterSnapshot, NodeCommand } from './models';

@Injectable({ providedIn: 'root' })
export class ClusterService {
  private readonly _snapshot = signal<ClusterSnapshot | null>(null);
  private readonly _streaming = signal(false);
  private readonly _busy = signal<ReadonlySet<string>>(new Set());
  private readonly _lastResult = signal<string | null>(null);

  readonly snapshot = this._snapshot.asReadonly();
  readonly streaming = this._streaming.asReadonly();
  readonly lastResult = this._lastResult.asReadonly();

  readonly nodes = computed(() => this._snapshot()?.nodes ?? []);
  readonly groups = computed(() => this._snapshot()?.groups ?? []);

  constructor() {
    // EventSource reconnects on its own, which is what should happen: the observer going away is a thing
    // to display, not a thing to give up on.
    const source = new EventSource('/api/cluster/stream');

    source.onmessage = (event) => {
      this._snapshot.set(JSON.parse(event.data) as ClusterSnapshot);
      this._streaming.set(true);
    };

    source.onerror = () => this._streaming.set(false);
  }

  /** True while a start/stop/restart is in flight for this resource. */
  busy(resource: string): boolean {
    return this._busy().has(resource);
  }

  /**
   * Runs a command and waits for it. The button stays disabled until the answer arrives rather than
   * flipping optimistically -- a node that refuses to stop should look like a node that did not stop.
   */
  async command(resource: string, command: NodeCommand): Promise<void> {
    if (this._busy().has(resource)) {
      return;
    }

    this._busy.update((busy) => new Set(busy).add(resource));

    try {
      const response = await fetch(`/api/resources/${resource}/${command}`, { method: 'POST' });
      const body = await response.text();
      this._lastResult.set(`${command} ${resource}: ${response.status} ${body}`);
    } catch (error) {
      this._lastResult.set(`${command} ${resource} failed: ${error}`);
    } finally {
      this._busy.update((busy) => {
        const next = new Set(busy);
        next.delete(resource);
        return next;
      });
    }
  }

  /**
   * Starts a run of generated traffic into a group's log. The observer routes it to that group's leader,
   * which is where an append has to go — not necessarily the cluster leader.
   */
  async sendMessages(group: string, count: number, intervalMs = 10, size = 256): Promise<void> {
    try {
      const response = await fetch(
        `/api/groups/${group}/messages?count=${count}&intervalMs=${intervalMs}&size=${size}`,
        { method: 'POST' },
      );

      const body = await response.text();
      this._lastResult.set(`send ${count} → group ${group}: ${response.ok ? body : `HTTP ${response.status} ${body}`}`);
    } catch (error) {
      this._lastResult.set(`send ${count} → group ${group} failed: ${error}`);
    }
  }

  async stopSending(group: string): Promise<void> {
    try {
      const response = await fetch(`/api/groups/${group}/messages`, { method: 'DELETE' });
      this._lastResult.set(`stop sending ${group}: ${await response.text()}`);
    } catch (error) {
      this._lastResult.set(`stop sending ${group} failed: ${error}`);
    }
  }

  /**
   * Captures one node's own applied state as a snapshot and compacts its log up to it. Local to that node —
   * unlike an append, this needs no quorum — so it can fail on its own if nothing has been applied yet.
   */
  async takeSnapshot(group: string, node: number): Promise<void> {
    try {
      const response = await fetch(`/api/groups/${group}/snapshot/${node}`, { method: 'POST' });
      const body = await response.text();
      this._lastResult.set(`snapshot ${group} @ node ${node}: ${response.ok ? body : `HTTP ${response.status} ${body}`}`);
    } catch (error) {
      this._lastResult.set(`snapshot ${group} @ node ${node} failed: ${error}`);
    }
  }

  /**
   * Takes a node out of the cluster group's configuration, or puts it back as a learner. A removal does not
   * stick while the node is still configured on the others: the coordinator adopts it again within a
   * reconcile pass and promotes it back. Watching that happen is the point of the button.
   */
  async changeMembership(node: number, remove: boolean): Promise<void> {
    try {
      const response = await fetch(`/api/cluster/nodes/${node}`, { method: remove ? 'DELETE' : 'POST' });
      this._lastResult.set(`${remove ? 'remove' : 'add'} ${node}: ${await response.text()}`);
    } catch (error) {
      this._lastResult.set(`${remove ? 'remove' : 'add'} ${node} failed: ${error}`);
    }
  }

  /**
   * Asks for a group's leadership to move. `node` may be null, which lets the group's own leader pick the
   * best-placed candidate. The answer is shown verbatim: Lagging and NotResponding are the interesting ones.
   */
  async transferLeadership(group: string, node: number | null): Promise<string> {
    const query = node === null ? '' : `?node=${node}`;

    try {
      const response = await fetch(`/api/groups/${group}/leader${query}`, { method: 'POST' });
      const body = await response.text();

      // The status is part of the answer, not decoration: a 404 with an empty body reads exactly like a
      // request that was accepted and did nothing, which is the worst thing this button could say.
      const result = response.ok ? body : `HTTP ${response.status} ${body}`.trim();

      this._lastResult.set(`leader ${group}: ${result}`);
      return result;
    } catch (error) {
      const result = `failed: ${error}`;
      this._lastResult.set(`leader ${group} ${result}`);
      return result;
    }
  }
}

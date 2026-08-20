import { ChangeDetectionStrategy, Component, computed, inject, input, signal } from '@angular/core';
import { ClusterService } from './cluster.service';
import { formatBytes, formatCount } from './format';
import { NodeCommand, NodeView } from './models';

@Component({
  selector: 'app-node-card',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <article
      [attr.data-testid]="'node-' + node().id"
      class="flex flex-col gap-3 rounded-lg border bg-white p-4 dark:bg-neutral-900"
      [class]="node().isClusterLeader
        ? 'border-emerald-500/70 ring-1 ring-emerald-500/30'
        : 'border-neutral-200 dark:border-neutral-800'">

      <header class="flex items-start justify-between gap-2">
        <div class="flex items-baseline gap-2">
          <span class="text-xs uppercase tracking-widest text-neutral-500">node</span>
          <span class="font-mono text-2xl leading-none">{{ node().id }}</span>
        </div>

        <div class="flex flex-col items-end gap-1">
          <span class="inline-flex items-center gap-1.5 text-xs font-medium"
                [class]="node().online ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'">
            <span class="h-2 w-2 rounded-full"
                  [class]="node().online ? 'bg-emerald-500' : 'bg-rose-500'"></span>
            {{ node().online ? 'online' : 'offline' }}
          </span>

          @if (node().isClusterLeader) {
            <span class="rounded bg-emerald-500/15 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-emerald-700 dark:text-emerald-300">
              cluster leader
            </span>
          }
        </div>
      </header>

      <!-- Two truths, kept apart on purpose: Aspire knows whether the process exists, the poll knows
           whether it answers. A node can be Running and silent. -->
      <dl class="grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
        <dt class="text-neutral-500">process</dt>
        <dd class="text-right font-mono">{{ node().aspireState ?? '—' }}</dd>

        <dt class="text-neutral-500">answers in</dt>
        <dd class="text-right font-mono">{{ latency() }}</dd>

        <dt class="text-neutral-500">plane</dt>
        <dd class="text-right font-mono">{{ node().clusterState ?? '—' }}</dd>

        <dt class="text-neutral-500">mode</dt>
        <dd class="text-right font-mono">{{ node().clusterMode ?? '—' }}</dd>

        <dt class="text-neutral-500">sees leader</dt>
        <dd class="text-right font-mono">{{ node().clusterLeader ?? '—' }}</dd>

        <dt class="text-neutral-500">status port</dt>
        <dd class="text-right font-mono">{{ node().statusPort }}</dd>

        <!-- Every group's log this node carries, added up, as the filesystem reports it. -->
        <dt class="text-neutral-500">wal on disk</dt>
        <dd class="text-right font-mono" [attr.data-testid]="'node-' + node().id + '-wal'">
          {{ walBytes() }}
        </dd>

        <dt class="text-neutral-500">wal entries</dt>
        <dd class="text-right font-mono">{{ walEntries() }}</dd>
      </dl>

      <div class="text-xs">
        <span class="text-neutral-500">voters</span>
        <span class="ml-2 font-mono">{{ voters() }}</span>
        @if (node().learners.length) {
          <span class="ml-3 text-neutral-500">learners</span>
          <span class="ml-2 font-mono">{{ node().learners.join(', ') }}</span>
        }
      </div>

      @if (node().error) {
        <p class="rounded bg-rose-500/10 px-2 py-1 font-mono text-[11px] text-rose-700 dark:text-rose-300">
          {{ node().error }}
        </p>
      }

      @if (node().recentEvents.length) {
        <ul class="space-y-0.5 font-mono text-[11px] text-neutral-500">
          @for (event of lastEvents(); track event) {
            <li class="truncate">{{ event }}</li>
          }
        </ul>
      }

      <!-- Membership, not process control: this takes the node out of the cluster group's configuration
           while it keeps running. The coordinator adopts it back within a reconcile pass, which is the
           single-server change rule doing its work in plain sight. -->
      <div class="flex gap-2 text-xs">
        <button type="button"
                [attr.data-testid]="'node-' + node().id + '-out'"
                (click)="cluster.changeMembership(node().id, true)"
                [disabled]="!node().online"
                class="flex-1 rounded border border-neutral-300 px-2 py-1 hover:bg-neutral-100
                       disabled:cursor-not-allowed disabled:opacity-40
                       dark:border-neutral-700 dark:hover:bg-neutral-800">
          out of cluster
        </button>
        <button type="button"
                [attr.data-testid]="'node-' + node().id + '-in'"
                (click)="cluster.changeMembership(node().id, false)"
                [disabled]="!node().online"
                class="flex-1 rounded border border-neutral-300 px-2 py-1 hover:bg-neutral-100
                       disabled:cursor-not-allowed disabled:opacity-40
                       dark:border-neutral-700 dark:hover:bg-neutral-800">
          back in
        </button>
      </div>

      <footer class="mt-auto flex gap-2 pt-1">
        @for (command of commands; track command) {
          <button type="button"
                  [attr.data-testid]="'node-' + node().id + '-' + command"
                  (click)="run(command)"
                  [disabled]="busy() || !applies(command)"
                  class="flex-1 rounded border px-2 py-1 text-xs font-medium transition
                         disabled:cursor-not-allowed disabled:opacity-40"
                  [class]="pending() === command
                    ? 'border-amber-500 bg-amber-500/15 text-amber-700 dark:text-amber-300'
                    : 'border-neutral-300 hover:bg-neutral-100 dark:border-neutral-700 dark:hover:bg-neutral-800'">
            {{ pending() === command ? 'confirm' : command }}
          </button>
        }
      </footer>
    </article>
  `,
})
export class NodeCard {
  readonly node = input.required<NodeView>();

  protected readonly cluster = inject(ClusterService);

  /** Clicked once, waiting for a second click. Not a modal: a dialog would block the log streams behind it. */
  readonly pending = signal<NodeCommand | null>(null);

  readonly commands: readonly NodeCommand[] = ['start', 'stop', 'restart'];

  readonly busy = computed(() => this.cluster.busy(this.node().resourceName));

  readonly voters = computed(() => {
    const voters = this.node().voters;
    return voters.length ? voters.join(', ') : '—';
  });

  readonly lastEvents = computed(() => this.node().recentEvents.slice(-3));

  readonly walBytes = computed(() => formatBytes(this.node().walBytes));

  readonly walEntries = computed(() => formatCount(this.node().walEntries));

  /** Over loopback the round trip is routinely under a millisecond, and "0 ms" reads like a missing value. */
  readonly latency = computed(() => {
    const ms = this.node().latencyMs;

    if (ms === null) return '—';
    return ms === 0 ? '<1 ms' : `${ms} ms`;
  });

  /**
   * Whether a command means anything in the node's current state. Starting something already running is not
   * an error, it simply sits there for half a minute and then reports success -- which reads as a hung
   * button. The state Aspire reports is the one that decides, not whether the node answers.
   */
  applies(command: NodeCommand): boolean {
    const running = this.node().aspireState === 'Running';
    return command === 'start' ? !running : running;
  }

  run(command: NodeCommand): void {
    if (this.pending() !== command) {
      this.pending.set(command);
      setTimeout(() => this.pending.update((current) => (current === command ? null : current)), 4000);
      return;
    }

    this.pending.set(null);
    void this.cluster.command(this.node().resourceName, command);
  }
}

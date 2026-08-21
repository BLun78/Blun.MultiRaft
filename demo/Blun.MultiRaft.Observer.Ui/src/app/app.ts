import { ChangeDetectionStrategy, Component, computed, effect, inject, signal } from '@angular/core';
import { ClusterService } from './cluster.service';
import { GroupMatrix } from './group-matrix';
import { LogView } from './log-view';
import { NodeCard } from './node-card';
import { WalPanel } from './wal-panel';

@Component({
  selector: 'app-root',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NodeCard, GroupMatrix, LogView, WalPanel],
  template: `
    <div class="mx-auto flex max-w-7xl flex-col gap-6 p-4 sm:p-6">

      <header class="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 class="text-lg font-semibold tracking-tight">Blun.MultiRaft observer</h1>
          <p class="text-xs text-neutral-500">
            five nodes · three queue groups · one administrative plane
          </p>
        </div>

        <div class="flex flex-wrap items-center gap-4">
          <!-- A tally of opinions, not a fact. Each node reports the leader it believes in, and during an
               election they legitimately differ; hiding that would be the one dishonest thing here. -->
          <div class="text-right">
            <div class="text-[10px] uppercase tracking-widest text-neutral-500">cluster leader</div>
            <div class="flex items-baseline gap-2">
              <span data-testid="cluster-leader" class="font-mono text-3xl leading-none" [class]="leaderClass()">
                {{ cluster.snapshot()?.clusterLeader ?? '—' }}
              </span>
              @if (cluster.snapshot()?.leaderDisagreement) {
                <span class="rounded bg-amber-500/15 px-1.5 py-0.5 text-[10px] font-semibold uppercase text-amber-700 dark:text-amber-300">
                  disputed
                </span>
              }
              <!-- A leader is named, but the node carrying that name is not answering the observer.
                   Measured, so that nobody deletes this as dead code: it does NOT appear when a node is
                   stopped or killed, because the remaining nodes clear their leader within a heartbeat or
                   two, faster than the half-second poll can see. What it is for is the other case -- the
                   observer cannot reach a node the cluster still considers its leader, which is a statement
                   about the observer's view of the network, not about the cluster. -->
              @if (leaderStale()) {
                <span class="rounded bg-rose-500/15 px-1.5 py-0.5 text-[10px] font-semibold uppercase text-rose-700 dark:text-rose-300">
                  not answering
                </span>
              }
            </div>
          </div>

          <div data-testid="quorum" class="rounded-lg border px-3 py-2 text-right"
               [class]="quorumClass()">
            <div class="text-[10px] uppercase tracking-widest opacity-70">quorum</div>
            <div class="font-mono text-sm">
              {{ cluster.snapshot()?.online ?? 0 }} / {{ cluster.snapshot()?.total ?? 0 }} answering
            </div>
            <div class="text-[11px] font-medium">
              {{ hasQuorum() ? 'holds (' + (cluster.snapshot()?.required ?? 0) + ' needed)' : 'lost — writes stop' }}
            </div>
          </div>

          <button type="button"
                  data-testid="theme-toggle"
                  (click)="dark.set(!dark())"
                  class="rounded border border-neutral-300 px-2 py-1 text-xs dark:border-neutral-700">
            {{ dark() ? 'light' : 'dark' }}
          </button>
        </div>
      </header>

      @if (!cluster.streaming()) {
        <p class="rounded border border-amber-500/50 bg-amber-500/10 px-3 py-2 text-xs text-amber-800 dark:text-amber-300">
          not connected to the observer — retrying
        </p>
      }

      <section class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5">
        @for (node of cluster.nodes(); track node.id) {
          <app-node-card [node]="node" />
        }
      </section>

      <section class="flex flex-col gap-2">
        <h2 class="text-xs uppercase tracking-widest text-neutral-500">groups</h2>
        <app-group-matrix [groups]="cluster.groups()" [nodes]="cluster.nodes()" />
      </section>

      <section class="flex flex-col gap-2">
        <h2 class="text-xs uppercase tracking-widest text-neutral-500">write-ahead log</h2>
        <app-wal-panel [groups]="cluster.groups()" [nodes]="cluster.nodes()" />
      </section>

      <section class="flex flex-col gap-2">
        <h2 class="text-xs uppercase tracking-widest text-neutral-500">logs</h2>
        <app-log-view [nodes]="cluster.nodes()" />
      </section>

      @if (cluster.lastResult()) {
        <footer data-testid="last-result" class="rounded border border-neutral-200 bg-white p-2 font-mono text-[11px] break-all dark:border-neutral-800 dark:bg-neutral-900">
          {{ cluster.lastResult() }}
        </footer>
      }
    </div>
  `,
})
export class App {
  protected readonly cluster = inject(ClusterService);

  protected readonly dark = signal(
    window.matchMedia?.('(prefers-color-scheme: dark)').matches ?? false,
  );

  protected readonly hasQuorum = computed(() => this.cluster.snapshot()?.hasQuorum ?? false);

  /** A leader is named but the node carrying that name is not answering. */
  protected readonly leaderStale = computed(() => {
    const leader = this.cluster.snapshot()?.clusterLeader ?? null;
    return leader !== null && !this.cluster.nodes().some((n) => String(n.id) === leader && n.online);
  });

  protected readonly leaderClass = computed(() => {
    if (this.cluster.snapshot()?.clusterLeader == null) {
      return 'text-neutral-400';
    }

    return this.leaderStale()
      ? 'text-rose-600 dark:text-rose-400'
      : 'text-emerald-600 dark:text-emerald-400';
  });

  protected readonly quorumClass = computed(() =>
    this.hasQuorum()
      ? 'border-neutral-200 text-neutral-700 dark:border-neutral-800 dark:text-neutral-300'
      : 'border-rose-500/60 bg-rose-500/10 text-rose-700 dark:text-rose-300',
  );

  constructor() {
    effect(() => document.documentElement.classList.toggle('dark', this.dark()));

    // Nothing else to wire: the cluster snapshot and the merged log stream both connect themselves, and
    // neither needs to be told the topology.
  }
}

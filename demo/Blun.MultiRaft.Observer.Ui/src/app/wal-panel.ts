import { ChangeDetectionStrategy, Component, inject, input, signal } from '@angular/core';
import { ClusterService } from './cluster.service';
import { formatBytes, formatCount } from './format';
import { GroupCell, GroupView, NodeView } from './models';

/** The three run sizes on offer. Ten milliseconds apart, so the largest is a hundred seconds of writing. */
const BATCHES = [100, 1000, 10000] as const;

@Component({
  selector: 'app-wal-panel',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="overflow-x-auto rounded-lg border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900">
      <p class="border-b border-neutral-200 px-3 py-2 text-xs text-neutral-500 dark:border-neutral-800">
        Bytes as the filesystem holds them, per node, segments included — not a count of what was appended.
        Sending writes real commands through the group's leader, so every follower's log grows with it. The
        pacing is <em>at most</em> one every 10&nbsp;ms: each append waits for its commit, so the rate shown
        is what a replicated round trip actually costs, not what was asked for. A snapshot line appears
        under a node's log only once that node has actually stored one for the group — none of this demo's
        groups compact automatically, so it stays empty unless a snapshot was installed from a peer.
      </p>

      <table class="w-full min-w-[52rem] border-collapse text-sm">
        <thead>
          <tr class="border-b border-neutral-200 text-xs uppercase tracking-wide text-neutral-500 dark:border-neutral-800">
            <th class="px-3 py-2 text-left font-medium">group</th>
            @for (node of nodes(); track node.id) {
              <th class="px-3 py-2 text-right font-medium">node {{ node.id }}</th>
            }
            <th class="px-3 py-2 text-right font-medium">total</th>
            <th class="px-3 py-2 text-right font-medium">send</th>
          </tr>
        </thead>

        <tbody>
          @for (group of groups(); track group.group) {
            <tr class="border-b border-neutral-100 last:border-0 dark:border-neutral-800/60">
              <td class="px-3 py-2 align-top">
                <div class="font-medium">{{ group.label }}</div>
                <div class="text-xs text-neutral-500">
                  @if (leaderWal(group); as wal) {
                    index {{ wal.firstIndex }}–{{ wal.lastIndex }} · ≈{{ wal.bytesPerEntry }} B/entry
                  } @else {
                    no log yet
                  }
                </div>
                @if (leaderSnapshot(group); as snapshot) {
                  <div class="text-xs text-sky-600 dark:text-sky-400">
                    snapshot @ {{ snapshot.lastIncludedIndex }} (term {{ snapshot.lastIncludedTerm }}) ·
                    {{ bytes(snapshot.sizeBytes) }}
                  </div>
                }
              </td>

              @for (cell of group.cells; track cell.node) {
                <td class="px-3 py-2 text-right align-top">
                  @if (cell.wal; as wal) {
                    <div [attr.data-testid]="'wal-' + group.group + '-node-' + cell.node"
                         class="font-mono" [class]="cell.online ? '' : 'text-neutral-400 dark:text-neutral-600'">
                      {{ bytes(wal.sizeBytes) }}
                    </div>
                    <div class="font-mono text-[11px] text-neutral-500">
                      {{ count(wal.entries) }} · {{ wal.segments }} seg
                    </div>
                  } @else {
                    <span class="text-neutral-400">—</span>
                  }

                  @if (cell.snapshot; as snapshot) {
                    <div [attr.data-testid]="'snapshot-' + group.group + '-node-' + cell.node"
                         class="mt-0.5 font-mono text-[11px] text-sky-600 dark:text-sky-400">
                      snap @ {{ snapshot.lastIncludedIndex }}
                      <span class="text-neutral-400 dark:text-neutral-600">({{ bytes(snapshot.sizeBytes) }})</span>
                    </div>
                  }

                  @if (cell.online) {
                    <button type="button"
                            [attr.data-testid]="'snapshot-' + group.group + '-node-' + cell.node + '-take'"
                            [disabled]="snapshotting().has(group.group + ':' + cell.node)"
                            (click)="takeSnapshot(group, cell.node)"
                            class="mt-0.5 rounded border border-neutral-300 px-1.5 py-0.5 text-[10px] font-medium
                                   hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40
                                   dark:border-neutral-700 dark:hover:bg-neutral-800">
                      snapshot
                    </button>
                  }
                </td>
              }

              <td class="px-3 py-2 text-right align-top font-mono">{{ bytes(total(group)) }}</td>

              <td class="px-3 py-2 text-right align-top">
                <div class="inline-flex gap-1">
                  @for (batch of batches; track batch) {
                    <button type="button"
                            [attr.data-testid]="'wal-' + group.group + '-send-' + batch"
                            [disabled]="!group.leader || pending().has(group.group) || running(group)"
                            (click)="send(group, batch)"
                            class="rounded border border-neutral-300 px-2 py-1 text-xs font-medium
                                   hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40
                                   dark:border-neutral-700 dark:hover:bg-neutral-800">
                      {{ label(batch) }}
                    </button>
                  }
                </div>

                @if (group.send; as send) {
                  <div [attr.data-testid]="'wal-' + group.group + '-progress'"
                       class="mt-1 font-mono text-[11px]"
                       [class]="send.running ? 'text-emerald-600 dark:text-emerald-400' : 'text-neutral-500'">
                    {{ count(send.sent) }} / {{ count(send.total) }}
                    · {{ send.ratePerSecond }}/s
                    @if (send.failed) {
                      · {{ send.failed }} failed
                    }
                    @if (send.running) {
                      <button type="button"
                              [attr.data-testid]="'wal-' + group.group + '-stop'"
                              (click)="stop(group)"
                              class="ml-1 underline">stop</button>
                    }
                  </div>

                  @if (send.error) {
                    <div class="mt-0.5 max-w-[16rem] break-all text-right font-mono text-[11px] text-rose-600 dark:text-rose-400">
                      {{ send.error }}
                    </div>
                  }
                }
              </td>
            </tr>
          }
        </tbody>
      </table>
    </div>
  `,
})
export class WalPanel {
  readonly groups = input.required<readonly GroupView[]>();
  readonly nodes = input.required<readonly NodeView[]>();

  private readonly cluster = inject(ClusterService);

  readonly batches = BATCHES;
  readonly pending = signal<ReadonlySet<string>>(new Set());
  readonly snapshotting = signal<ReadonlySet<string>>(new Set());

  bytes = formatBytes;
  count = formatCount;

  label(batch: number): string {
    return batch >= 1000 ? `${batch / 1000}k` : String(batch);
  }

  /** The log of whichever node leads the group — the one the sending goes through. */
  leaderWal(group: GroupView) {
    return group.cells.find((cell: GroupCell) => cell.role === 'Leader')?.wal ?? null;
  }

  /** The leader's stored snapshot, if it has taken one. */
  leaderSnapshot(group: GroupView) {
    return group.cells.find((cell: GroupCell) => cell.role === 'Leader')?.snapshot ?? null;
  }

  /** Every replica's copy added up: what this group costs the cluster, not one machine. */
  total(group: GroupView): number {
    return group.cells.reduce((sum: number, cell: GroupCell) => sum + (cell.wal?.sizeBytes ?? 0), 0);
  }

  running(group: GroupView): boolean {
    return group.send?.running === true;
  }

  async send(group: GroupView, count: number): Promise<void> {
    this.pending.update((current) => new Set(current).add(group.group));

    try {
      await this.cluster.sendMessages(group.group, count);
    } finally {
      this.pending.update((current) => {
        const next = new Set(current);
        next.delete(group.group);
        return next;
      });
    }
  }

  stop(group: GroupView): Promise<void> {
    return this.cluster.stopSending(group.group);
  }

  async takeSnapshot(group: GroupView, node: number): Promise<void> {
    const key = `${group.group}:${node}`;
    this.snapshotting.update((current) => new Set(current).add(key));

    try {
      await this.cluster.takeSnapshot(group.group, node);
    } finally {
      this.snapshotting.update((current) => {
        const next = new Set(current);
        next.delete(key);
        return next;
      });
    }
  }
}

import { ChangeDetectionStrategy, Component, effect, inject, input, signal } from '@angular/core';
import { ClusterService } from './cluster.service';
import { GroupCell, GroupView, NodeView } from './models';

/** How long a row stays marked after its leadership moved. */
const FLASH_MS = 2500;

@Component({
  selector: 'app-group-matrix',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="overflow-x-auto rounded-lg border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900">
      <p class="border-b border-neutral-200 px-3 py-2 text-xs text-neutral-500 dark:border-neutral-800">
        Click a cell to move that group's leadership to that node — or use the picker on the right and let
        the group's own leader choose the best-placed one.
      </p>

      <table class="w-full min-w-[44rem] border-collapse text-sm">
        <thead>
          <tr class="border-b border-neutral-200 text-xs uppercase tracking-wide text-neutral-500 dark:border-neutral-800">
            <th class="px-3 py-2 text-left font-medium">group</th>
            @for (node of nodes(); track node.id) {
              <th class="px-3 py-2 text-center font-medium">node {{ node.id }}</th>
            }
            <th class="px-3 py-2 text-right font-medium">or pick a target</th>
          </tr>
        </thead>

        <tbody>
          @for (group of groups(); track group.group) {
            <!-- A leadership change is the thing this table exists to show, and between two polls it is a
                 badge quietly moving one column over. The row is marked for a moment so it cannot be missed. -->
            <tr [attr.data-testid]="'group-' + group.group"
                class="border-b border-neutral-100 transition-colors duration-500 last:border-0 dark:border-neutral-800/60"
                [class]="flashing().has(group.group) ? 'bg-emerald-500/10' : ''">
              <td class="px-3 py-2">
                <div class="font-medium">{{ group.label }}</div>
                <div class="text-xs text-neutral-500">
                  leader
                  <span [attr.data-testid]="'group-' + group.group + '-leader'" class="font-mono" [class]="stale(group) ? 'text-rose-600 dark:text-rose-400' : ''">
                    {{ group.leader ?? '—' }}
                  </span>
                  @if (group.disagreement) {
                    <span class="ml-1 text-amber-600 dark:text-amber-400">· disputed</span>
                  }
                  @if (stale(group)) {
                    <span class="ml-1 text-rose-600 dark:text-rose-400">· not answering</span>
                  }
                  @if (flashing().has(group.group)) {
                    <span class="ml-1 font-medium text-emerald-600 dark:text-emerald-400">· just moved</span>
                  }
                </div>
              </td>

              @for (cell of group.cells; track cell.node) {
                <td class="px-1 py-1 text-center">
                  <!-- The cell is the pair the request is about, so the cell is what you click. Only a
                       candidate for it is clickable: not the node already leading, not one that is silent. -->
                  <button type="button"
                          [attr.data-testid]="'group-' + group.group + '-cell-' + cell.node"
                          [disabled]="!eligible(cell) || pending().has(group.group)"
                          (click)="moveTo(group, cell.node)"
                          [title]="eligible(cell)
                            ? 'move ' + group.label + ' leadership to node ' + cell.node
                            : ''"
                          class="w-full rounded px-2 py-1 transition
                                 enabled:hover:bg-neutral-100 enabled:hover:ring-1 enabled:hover:ring-neutral-300
                                 disabled:cursor-default
                                 dark:enabled:hover:bg-neutral-800 dark:enabled:hover:ring-neutral-700">
                    <span class="flex flex-col items-center gap-0.5">
                      <span class="rounded px-2 py-0.5 text-xs font-semibold" [class]="roleClass(cell)">
                        {{ cell.online ? cell.role : 'offline' }}
                      </span>
                      @if (cell.online) {
                        <span class="font-mono text-[11px] text-neutral-500">
                          t{{ cell.term }} · c{{ cell.commitIndex }}
                        </span>
                      }
                    </span>
                  </button>
                </td>
              }

              <td class="px-3 py-2 text-right align-top">
                <div class="inline-flex items-center gap-1">
                  <select #target
                          [attr.data-testid]="'group-' + group.group + '-select'"
                          class="rounded border border-neutral-300 bg-white px-1.5 py-1 text-xs text-neutral-900
                                 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100">
                    <option value="">best placed</option>
                    @for (node of nodes(); track node.id) {
                      <option [value]="node.id">node {{ node.id }}</option>
                    }
                  </select>
                  <button type="button"
                          [attr.data-testid]="'group-' + group.group + '-request'"
                          (click)="transfer(group, target.value)"
                          [disabled]="pending().has(group.group)"
                          class="rounded border border-neutral-300 px-2 py-1 text-xs font-medium
                                 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40
                                 dark:border-neutral-700 dark:hover:bg-neutral-800">
                    {{ pending().has(group.group) ? '…' : 'request' }}
                  </button>
                </div>

                <!-- The server's own words, next to the button that asked. Lagging and NotResponding are the
                     answers worth reading, and a page-footer log is the wrong place to read them. -->
                @if (results()[group.group]; as result) {
                  <div [attr.data-testid]="'group-' + group.group + '-result'" class="mt-1 max-w-[18rem] break-all text-right font-mono text-[11px] text-neutral-500">
                    {{ result }}
                  </div>
                }
              </td>
            </tr>
          }
        </tbody>
      </table>
    </div>
  `,
})
export class GroupMatrix {
  readonly groups = input.required<readonly GroupView[]>();
  readonly nodes = input.required<readonly NodeView[]>();

  private readonly cluster = inject(ClusterService);
  private readonly previous = new Map<string, string | null>();

  readonly flashing = signal<ReadonlySet<string>>(new Set());
  readonly pending = signal<ReadonlySet<string>>(new Set());
  readonly results = signal<Record<string, string>>({});

  constructor() {
    effect(() => {
      for (const group of this.groups()) {
        const before = this.previous.get(group.group);

        // Only a move from one live leader to another. Coming back from "nobody" after an election is not
        // a handover, and marking it would make the row blink through every recovery.
        if (this.previous.has(group.group) && before !== null && group.leader !== null && before !== group.leader) {
          this.flash(group.group);
        }

        this.previous.set(group.group, group.leader);
      }
    });
  }

  /** The group names a leader, but that node is not answering — an election is due. */
  stale(group: GroupView): boolean {
    return (
      group.leader !== null &&
      !this.nodes().some((node) => String(node.id) === group.leader && node.online)
    );
  }

  roleClass(cell: GroupCell): string {
    if (!cell.online) {
      return 'bg-rose-500/10 text-rose-700 dark:text-rose-300';
    }

    switch (cell.role) {
      case 'Leader':
        return 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-300';
      case 'Candidate':
        return 'bg-amber-500/15 text-amber-700 dark:text-amber-300';
      case 'Follower':
        return 'bg-neutral-500/10 text-neutral-600 dark:text-neutral-300';
      case 'Learner':
        return 'bg-sky-500/10 text-sky-700 dark:text-sky-300';
      default:
        return 'bg-neutral-500/10 text-neutral-500';
    }
  }

  /** A node worth asking for: answering, and not the one already leading this group. */
  eligible(cell: GroupCell): boolean {
    return cell.online && cell.role !== 'Leader';
  }

  /** Clicking a cell means "put this group's leadership on this node". */
  moveTo(group: GroupView, node: number): Promise<void> {
    return this.transfer(group, String(node));
  }

  async transfer(group: GroupView, node: string): Promise<void> {
    if (this.pending().has(group.group)) {
      return;
    }

    this.pending.update((current) => new Set(current).add(group.group));

    try {
      const result = await this.cluster.transferLeadership(group.group, node === '' ? null : Number(node));
      this.results.update((current) => ({ ...current, [group.group]: result }));
    } finally {
      this.pending.update((current) => {
        const next = new Set(current);
        next.delete(group.group);
        return next;
      });
    }
  }

  private flash(group: string): void {
    this.flashing.update((current) => new Set(current).add(group));

    setTimeout(
      () =>
        this.flashing.update((current) => {
          const next = new Set(current);
          next.delete(group);
          return next;
        }),
      FLASH_MS,
    );
  }
}

import {
  afterRenderEffect,
  ChangeDetectionStrategy,
  Component,
  computed,
  ElementRef,
  inject,
  input,
  signal,
  viewChild,
} from '@angular/core';
import { LogService } from './log.service';
import { NodeLogLine, NodeView } from './models';

/** One colour per node, used in the merged view so a line's origin is readable without looking left. */
const NODE_COLOURS = [
  'text-sky-600 dark:text-sky-400',
  'text-violet-600 dark:text-violet-400',
  'text-amber-600 dark:text-amber-400',
  'text-teal-600 dark:text-teal-400',
  'text-pink-600 dark:text-pink-400',
];

@Component({
  selector: 'app-log-view',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="rounded-lg border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900">
      <div class="flex flex-wrap items-center gap-2 border-b border-neutral-200 p-2 dark:border-neutral-800">
        <button type="button"
                data-testid="log-tab-all"
                (click)="selected.set('all')"
                class="rounded px-2 py-1 text-xs font-medium"
                [class]="selected() === 'all'
                  ? 'bg-neutral-900 text-white dark:bg-neutral-100 dark:text-neutral-900'
                  : 'hover:bg-neutral-100 dark:hover:bg-neutral-800'">
          all nodes
        </button>

        @for (node of nodes(); track node.id) {
          <button type="button"
                  [attr.data-testid]="'log-tab-' + node.id"
                  (click)="selected.set(node.id)"
                  class="rounded px-2 py-1 text-xs font-medium"
                  [class]="selected() === node.id
                    ? 'bg-neutral-900 text-white dark:bg-neutral-100 dark:text-neutral-900'
                    : 'hover:bg-neutral-100 dark:hover:bg-neutral-800'">
            node {{ node.id }}
          </button>
        }

        <input type="search"
               data-testid="log-filter"
               placeholder="filter"
               [value]="filter()"
               (input)="filter.set($any($event.target).value)"
               class="ml-auto w-40 rounded border border-neutral-300 bg-transparent px-2 py-1 text-xs
                      dark:border-neutral-700" />

        <label class="flex items-center gap-1.5 text-xs text-neutral-500">
          <input type="checkbox" data-testid="log-stderr" [checked]="errorsOnly()" (change)="errorsOnly.set($any($event.target).checked)" />
          stderr only
        </label>

        <span data-testid="log-count" class="text-xs text-neutral-500">{{ visible().length }} lines</span>
      </div>

      <div #scroller
           data-testid="log-scroller"
           (scroll)="onScroll()"
           class="logs h-80 overflow-y-auto p-2 font-mono text-[11px] leading-relaxed">
        @for (line of visible(); track line.node + ':' + line.sequence) {
          <div class="flex gap-2">
            <span class="shrink-0 tabular-nums text-neutral-400 dark:text-neutral-600">{{ line.time }}</span>
            @if (selected() === 'all') {
              <span class="shrink-0 tabular-nums" [class]="colour(line.node)">{{ line.node }}</span>
            }
            <span class="whitespace-pre-wrap break-all"
                  [class]="line.isError ? 'text-rose-600 dark:text-rose-400' : ''">{{ line.content }}</span>
          </div>
        } @empty {
          <p class="text-neutral-500">nothing yet</p>
        }
      </div>

      @if (!stick()) {
        <div class="border-t border-neutral-200 px-2 py-1 text-xs text-neutral-500 dark:border-neutral-800">
          scrolled up — following paused
          <button type="button" data-testid="log-follow" (click)="resume()" class="ml-2 underline">follow again</button>
        </div>
      }
    </div>
  `,
})
export class LogView {
  readonly nodes = input.required<readonly NodeView[]>();

  private readonly logs = inject(LogService);
  private readonly scroller = viewChild<ElementRef<HTMLDivElement>>('scroller');

  readonly selected = signal<number | 'all'>('all');
  readonly filter = signal('');
  readonly errorsOnly = signal(false);

  /** Following the tail. Turned off the moment the reader scrolls away from the bottom. */
  readonly stick = signal(true);

  readonly visible = computed(() => {
    const selected = this.selected();
    const needle = this.filter().toLowerCase();
    const errorsOnly = this.errorsOnly();

    const lines: readonly NodeLogLine[] =
      selected === 'all' ? this.logs.merged() : this.logs.lines(selected);

    return lines.filter(
      (line) =>
        (!errorsOnly || line.isError) &&
        (needle === '' || line.content.toLowerCase().includes(needle)),
    );
  });

  constructor() {
    // After render, not during: the lines have to exist before scrolling to the last one means anything.
    afterRenderEffect(() => {
      this.visible();

      if (!this.stick()) {
        return;
      }

      const element = this.scroller()?.nativeElement;

      if (element) {
        element.scrollTop = element.scrollHeight;
      }
    });
  }

  colour(node: number): string {
    return NODE_COLOURS[(node - 1) % NODE_COLOURS.length];
  }

  onScroll(): void {
    const element = this.scroller()?.nativeElement;

    if (element) {
      this.stick.set(element.scrollHeight - element.scrollTop - element.clientHeight < 40);
    }
  }

  resume(): void {
    this.stick.set(true);
  }
}

import { catchError, EMPTY, exhaustMap, from, mergeMap, type Observable, timer } from 'rxjs';

/**
 * Runs `poll` at once and then every `intervalMs`, one at a time, emitting
 * each result it returns; a poll that fails is skipped, and the next runs on
 * time.
 *
 * @param poll What to run; resolves with the results to emit.
 * @param intervalMs How long between polls, in ms.
 * @returns Each result, as polls return them.
 */
export function poll$<T>(poll: () => Promise<T[]>, intervalMs: number): Observable<T> {
    return timer(0, intervalMs).pipe(
        exhaustMap(() => from(poll()).pipe(catchError(() => EMPTY))),
        mergeMap((results) => results)
    );
}

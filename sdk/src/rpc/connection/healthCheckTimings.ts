export interface HealthCheckTimings {
    /** Delay between background health checks while ready, in ms (default: 5000). */
    healthCheckIntervalMs?: number;
    /** Time a health check may take before the endpoint counts as unreachable, in ms (default: 3000). */
    healthCheckTimeoutMs?: number;
    /**
     * Time any request through an http connection may take before it fails
     * as one that never reached the node, in ms (default: 10000). It reports
     * the failure, so the connection checks itself at once.
     */
    requestTimeoutMs?: number;
    /** Exponential backoff between health checks while unreachable. */
    retryBackoff?: {
        /** Wait before the first retry, in ms (default: 1000). */
        initialDelayMs?: number;
        /** Longest wait between retries, in ms (default: 30000). */
        maxDelayMs?: number;
    };
}

/** `HealthCheckTimings` with every default filled in. */
export interface ResolvedHealthCheckTimings {
    healthCheckIntervalMs: number;
    healthCheckTimeoutMs: number;
    requestTimeoutMs: number;
    initialDelayMs: number;
    maxDelayMs: number;
}

/**
 * Fills in the defaults of `HealthCheckTimings`.
 *
 * @param timings The timings a caller set, if any.
 * @returns Every timing, with defaults for those not set.
 */
export function resolveHealthCheckTimings({
    healthCheckIntervalMs = 5000,
    healthCheckTimeoutMs = 3000,
    requestTimeoutMs = 10_000,
    retryBackoff: { initialDelayMs = 1000, maxDelayMs = 30_000 } = {},
}: HealthCheckTimings = {}): ResolvedHealthCheckTimings {
    return {
        healthCheckIntervalMs,
        healthCheckTimeoutMs,
        requestTimeoutMs,
        initialDelayMs,
        maxDelayMs,
    };
}

/**
 * The wait before checking an unreachable endpoint again: `initialDelayMs`
 * after the first failed check, doubling with each one after it, at most
 * `maxDelayMs`.
 *
 * @param failedChecks How many health checks have failed in a row (at least 1).
 * @param timings The resolved timings.
 * @returns The wait in ms.
 */
export function retryDelayMs(
    failedChecks: number,
    timings: ResolvedHealthCheckTimings
): number {
    return Math.min(
        timings.initialDelayMs * 2 ** Math.max(0, failedChecks - 1),
        timings.maxDelayMs
    );
}

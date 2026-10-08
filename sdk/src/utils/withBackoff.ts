/** How many times a failed read is retried before its error is thrown. */
const MAX_RETRIES = 5;
/** The wait before the first retry; it doubles with each retry after it. */
const BASE_BACKOFF_MS = 500;

/**
 * Runs `read`, retrying it with exponential backoff while it fails.
 *
 * @param read The read to run.
 * @param shouldRetry Whether an error is worth retrying; every error is
 *   retried when omitted. An error it rejects is thrown at once.
 * @returns The read's result.
 * @throws The last error once `MAX_RETRIES` retries have failed, or the
 *   first error `shouldRetry` rejects.
 */
export async function withBackoff<T>(
    read: () => Promise<T>,
    shouldRetry: (error: unknown) => boolean = () => true
): Promise<T> {
    for (let attempt = 0; ; attempt++) {
        try {
            return await read();
        } catch (error) {
            if (attempt >= MAX_RETRIES || !shouldRetry(error)) throw error;
            await new Promise((resolve) =>
                setTimeout(resolve, BASE_BACKOFF_MS * 2 ** attempt)
            );
        }
    }
}

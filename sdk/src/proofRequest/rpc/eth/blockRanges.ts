/**
 * Max blocks per `ProofRequested` log query, kept under typical provider
 * block-range and result-count limits.
 */
export const MAX_BLOCK_RANGE_PER_QUERY = 2000;

/**
 * Splits the blocks from `start` to `end` (both included) into ranges of at
 * most `size` blocks, for log queries that providers limit by block range.
 *
 * @param start The first block: the lowest for `asc`, the highest for `desc`.
 * @param end The last block: the highest for `asc`, the lowest for `desc`.
 * @param size The most blocks in one range.
 * @param order `asc` walks up from `start`, `desc` walks down from it.
 * @returns Each range as `[fromBlock, toBlock]`, in walking order.
 */
export function* blockRanges(
    start: number,
    end: number,
    size: number,
    order: 'asc' | 'desc'
): Generator<[number, number]> {
    if (order === 'asc') {
        for (let from = start; from <= end; from += size) {
            yield [from, Math.min(from + size - 1, end)];
        }
    } else {
        for (let to = start; to >= end; to -= size) {
            yield [Math.max(to - size + 1, end), to];
        }
    }
}

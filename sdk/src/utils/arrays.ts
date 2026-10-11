/**
 * Splits `items` into chunks of at most `size`.
 *
 * @param items The items.
 * @param size The most items in one chunk.
 * @returns The chunks, in order.
 */
export function chunk<T>(items: T[], size: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < items.length; i += size) chunks.push(items.slice(i, i + size));
    return chunks;
}

/**
 * The values from `from` up to `to` (excluded), `step` apart.
 *
 * @param from The first value.
 * @param to The bound, excluded.
 * @param step The distance between values (default: 1).
 * @returns The values.
 */
export function range(from: bigint, to: bigint, step = 1n): bigint[] {
    const values: bigint[] = [];
    for (let value = from; value < to; value += step) values.push(value);
    return values;
}

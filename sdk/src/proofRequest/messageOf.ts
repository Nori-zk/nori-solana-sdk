/**
 * Turns an error into the message kept in node data.
 *
 * @param error What a read or request threw.
 * @returns The error's message.
 */
export function messageOf(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}

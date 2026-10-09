import {
    BrowserProvider,
    JsonRpcProvider,
    type Eip1193Provider,
    type Provider,
} from 'ethers';

export type EthereumProvider = Provider;

export type CreateEthereumProviderOptions = {
    provider?: EthereumProvider;
    rpcUrl?: string;
    injectedProvider?: Eip1193Provider;
};

/**
 * Checks an Ethereum RPC URL is an absolute HTTP(S) URL.
 *
 * @param rpcUrl The URL to check.
 * @returns The URL, normalised.
 * @throws When the URL is not absolute or not HTTP(S).
 */
export function parseRpcUrl(rpcUrl: string): string {
    let url: URL;

    try {
        url = new URL(rpcUrl);
    } catch {
        throw new Error('ETH_RPC_URL must be an absolute HTTP(S) URL.');
    }

    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
        throw new Error('ETH_RPC_URL must use HTTP or HTTPS.');
    }

    return url.toString();
}

export function createEthereumProvider({
    provider,
    rpcUrl,
    injectedProvider,
}: CreateEthereumProviderOptions = {}): EthereumProvider {
    if (provider) return provider;

    if (rpcUrl !== undefined) {
        return new JsonRpcProvider(parseRpcUrl(rpcUrl));
    }

    if (injectedProvider) {
        return new BrowserProvider(injectedProvider);
    }

    throw new Error(
        'No Ethereum provider configured. Set ETH_RPC_URL or provide an EIP-1193 provider.'
    );
}

let ethereumProvider: EthereumProvider | undefined;

/**
 * The `ETH_RPC_URL` environment variable, read through `globalThis` so the
 * same module works in Node and in browsers.
 *
 * @returns The configured RPC URL, or `undefined` when none is set.
 */
export function getRpcUrl(): string | undefined {
    return (
        globalThis as {
            process?: { env?: { ETH_RPC_URL?: string } };
        }
    ).process?.env?.ETH_RPC_URL;
}

/**
 * The wallet's injected EIP-1193 provider (`window.ethereum`), read through
 * `globalThis` so the same module works in Node and in browsers.
 *
 * @returns The injected provider, or `undefined` when there is none.
 */
export function getInjectedProvider(): Eip1193Provider | undefined {
    return (globalThis as { ethereum?: Eip1193Provider }).ethereum;
}

export function getEthereumProvider(): EthereumProvider {
    ethereumProvider ??= createEthereumProvider({
        rpcUrl: getRpcUrl(),
        injectedProvider: getInjectedProvider(),
    });

    return ethereumProvider;
}

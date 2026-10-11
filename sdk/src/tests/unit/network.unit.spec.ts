import { jest } from '@jest/globals';
import { createNetworkMachine } from '../../rpc/connection/network.impl.js';
import { nodesUntil, statesDuring, waitForNode } from '../testUtils.js';

describe('network machine', () => {
    let internetUp = true;
    const fetchSpy = jest.spyOn(globalThis, 'fetch');

    beforeEach(() => {
        internetUp = true;
        fetchSpy.mockImplementation(async () => {
            if (!internetUp) throw new TypeError('fetch failed');
            return new Response(null, { status: 204 });
        });
    });
    afterAll(() => fetchSpy.mockRestore());

    test('goes online when a probe answers, offline when probes stop answering, and back', async () => {
        const network = createNetworkMachine({
            probeIntervalMs: 50,
            probeTimeoutMs: 100,
        });
        expect(await nodesUntil(network, 'online')).toEqual(['online']);

        internetUp = false;
        // Passing background probes may keep it `online` until one fails.
        expect((await nodesUntil(network, 'offline')).filter((node) => node !== 'online')).toEqual(['offline']);
        const offline = await waitForNode(network, 'offline');
        expect(offline.data).toEqual({ since: expect.any(Number) });

        internetUp = true;
        expect(await nodesUntil(network, 'online')).toEqual(['online']);
        network.close();
    });

    test('starts offline when the first probe gets no answer', async () => {
        internetUp = false;
        const network = createNetworkMachine({
            probeIntervalMs: 50,
            probeTimeoutMs: 100,
        });
        await waitForNode(network, 'offline');
        network.close();
    });

    test('a background probe that passes stays online without leaving it', async () => {
        const network = createNetworkMachine({
            probeIntervalMs: 30,
            probeTimeoutMs: 100,
        });
        await waitForNode(network, 'online');
        const visited = (await statesDuring(network, 150)).map(({ node }) => node);
        network.close();
        expect(visited.filter((node) => node !== 'online')).toEqual([]);
        expect(visited.length).toBeGreaterThan(2);
    });

    test('closes from any node', async () => {
        const network = createNetworkMachine({
            probeIntervalMs: 50,
            probeTimeoutMs: 100,
        });
        await waitForNode(network, 'online');
        network.close();
        await waitForNode(network, 'closed');
    });
});

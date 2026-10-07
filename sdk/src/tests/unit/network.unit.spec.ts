import { jest } from '@jest/globals';
import { createNetworkMachine } from '../../proofRequest/rpc/ystate/network.impl.js';
import { reach, recordNodes, sleep } from '../testUtils.js';

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
        const { network, close } = createNetworkMachine({
            probeIntervalMs: 50,
            probeTimeoutMs: 100,
        });
        const visited = recordNodes(network);
        await reach(network, 'online');

        internetUp = false;
        const offline = await reach(network, 'offline');
        expect(offline.data).toEqual({ since: expect.any(Number) });

        internetUp = true;
        await reach(network, 'online');
        close();
        expect(visited).toEqual(
            expect.arrayContaining(['checking', 'online', 'offline'])
        );
        expect(visited.indexOf('offline')).toBeGreaterThan(
            visited.indexOf('online')
        );
        expect(visited.lastIndexOf('online')).toBeGreaterThan(
            visited.indexOf('offline')
        );
    });

    test('starts offline when the first probe gets no answer', async () => {
        internetUp = false;
        const { network, close } = createNetworkMachine({
            probeIntervalMs: 50,
            probeTimeoutMs: 100,
        });
        await reach(network, 'offline');
        close();
    });

    test('a background probe that passes stays online without leaving it', async () => {
        const { network, close } = createNetworkMachine({
            probeIntervalMs: 30,
            probeTimeoutMs: 100,
        });
        await reach(network, 'online');
        const visited = recordNodes(network);
        await sleep(150);
        close();
        expect(
            visited.filter((node) => node !== 'online' && node !== 'closed')
        ).toEqual([]);
        expect(
            visited.filter((node) => node === 'online').length
        ).toBeGreaterThan(2);
    });

    test('closes from any node', async () => {
        const { network, close } = createNetworkMachine({
            probeIntervalMs: 50,
            probeTimeoutMs: 100,
        });
        await reach(network, 'online');
        close();
        await reach(network, 'closed');
    });
});

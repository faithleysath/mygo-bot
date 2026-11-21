((io, emit, close, decode) => {
    const stringify = JSON.stringify;
    const parse = JSON.parse;
    const call = (op, args, data) => io(op, stringify(args), data);
    const chunkSize = 64 * 1024;

    function reader(id) {
        let done = false;
        let reading = false;
        function finish() {
            if (!done) close(id);
            done = true;
            return { done: true };
        }
        return {
            [Symbol.asyncIterator]() { return this; },
            async return() { return finish(); },
            async next() {
                if (done) return { done: true };
                if (reading) throw new TypeError('stream read already in progress');
                reading = true;
                try {
                    const value = await call('stream.next', { id });
                    return value === null || done ? finish() : { done: false, value };
                } catch (error) {
                    if (done) return { done: true };
                    finish();
                    throw error;
                } finally {
                    reading = false;
                }
            },
        };
    }

    // Both file writes and HTTP uploads pull one chunk at a time. A sink may
    // stop accepting input early (for example, a server returning HTTP 413).
    async function pump(source, id, state) {
        let total = 0;
        while (!state.stopped) {
            const step = await source.next();
            if (state.stopped) break;
            if (step.done) {
                state.finished = true;
                await call('stream.finish', { id });
                break;
            }
            const bytes = step.value;
            if (!(bytes instanceof Uint8Array)) throw new TypeError('expected Uint8Array stream chunks');
            for (let at = 0; at < bytes.length && !state.stopped; at += chunkSize) {
                const part = bytes.subarray(at, at + chunkSize);
                if (await call('stream.write', { id }, part) === false) return total;
                total += part.length;
            }
        }
        return total;
    }

    async function consume(body, open, request) {
        const source = body[Symbol.asyncIterator]();
        const state = { stopped: false, finished: false };
        let id;
        try {
            id = await open();
            if (!request) return await pump(source, id, state);
            const response = request(id);
            try {
                return await Promise.race([response, pump(source, id, state).then(() => response)]);
            } catch (error) {
                // Header delivery can race a failing producer. Discard a response
                // handle that was opened in Rust but never reached the caller.
                response.then(reply => {
                    if (typeof reply.body === 'number') close(reply.body);
                }, () => {});
                throw error;
            }
        } finally {
            state.stopped = true;
            if (id !== undefined) close(id);
            if (!state.finished) {
                // A custom generator can be waiting on arbitrary JS. An early
                // response must not wait for that pending next() to finish.
                Promise.resolve(source.return?.()).catch(() => {});
            }
        }
    }

    Object.assign(globalThis, {
        fs: {
            async read(path, { encoding, stream = false } = {}) {
                if (stream && encoding !== undefined) throw new TypeError('stream reads produce bytes');
                const value = await call('fs.read', { path, encoding, stream });
                if (stream) return reader(value);
                return encoding === 'utf8' ? decode(value, false) : value;
            },
            async write(path, data, { append } = {}) {
                if (data?.[Symbol.asyncIterator]) {
                    return consume(data, () => call('fs.write', { path, append, stream: true }));
                }
                return call('fs.write', { path, append }, data);
            },
            async *readDir(path) { yield* reader(await call('fs.readDir', { path })); },
            async stat(path, { followSymlinks } = {}) { return call('fs.stat', { path, followSymlinks }); },
            async mkdir(path, { recursive } = {}) { await call('fs.mkdir', { path, recursive }); },
            async rename(from, to) { await call('fs.rename', { from, to }); },
            async remove(path, { recursive } = {}) { await call('fs.remove', { path, recursive }); },
        },
        http: {
            async request({ url, method, headers, body, timeout_ms, stream = false }) {
                if (typeof stream !== 'boolean') throw new TypeError('stream must be a boolean');
                const args = { url, method, headers, timeout_ms, stream };
                const response = body?.[Symbol.asyncIterator]
                    ? await consume(body, () => call('upload.open', {}), upload => call('http.request', { ...args, upload }))
                    : await call('http.request', args, body);
                if (stream) {
                    response.body = reader(response.body);
                } else {
                    response.text = () => decode(response.body, false);
                    response.json = () => parse(response.text());
                }
                return response;
            },
        },
        proc: {
            async exec(program, args = [], { cwd, env, stdin, timeout_ms } = {}) {
                return call('proc.exec', { program, args, cwd, env, timeout_ms }, stdin);
            },
        },
        async sleep(ms) { await call('sleep', { ms }); },
        async emitEvent(value) { return emit(value); },
    });
})

((io, emit, close, decode, log, sessionId, taskId) => {
    const stringify = JSON.stringify;
    const parse = JSON.parse;
    const objectTag = Object.prototype.toString;
    const isView = ArrayBuffer.isView;
    const call = (op, args, data) => io(op, stringify(args), data);
    const chunkSize = 64 * 1024;

    function formatValue(value) {
        if (typeof value === 'string') return value;
        if (value === null || typeof value !== 'object') return String(value);
        try {
            if (value instanceof Error) {
                return value.stack || `${value.name || 'Error'}: ${value.message || ''}`;
            }
            const binaryLabel = v => `${v[Symbol.toStringTag] || 'DataView'}(${v.byteLength})`;
            if (isView(value)) return binaryLabel(value);
            const json = stringify(value, (_, v) => isView(v) ? binaryLabel(v) : v);
            if (json !== undefined) return json;
            return objectTag.call(value);
        } catch {
            try { return objectTag.call(value); } catch { return '[object]'; }
        }
    }

    function writeLog(prefix, args) {
        log(prefix + args.map(formatValue).join(' '));
    }

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

    // Rust packs the decoded media in one binary reply. Only content slots carry
    // ranges into it; structuredContent and resource links are ordinary JSON.
    async function mcpResult(op, args) {
        const result = await call(op, args);
        if (!Object.hasOwn(result, 'body')) return result;
        const bytes = result.body;
        delete result.body;
        const blocks = result.content ?? (result.contents ?? result.messages).map(entry => entry.content);
        for (const outer of blocks) {
            const block = outer.type === 'resource' ? outer.resource.content : outer;
            if (block.type === 'image' || block.type === 'audio' || block.type === 'video' || block.type === 'blob') {
                const [start, end] = block.data;
                block.data = bytes.subarray(start, end);
            }
        }
        return result;
    }

    async function sendRequest(op, args, body, stream) {
        const response = body?.[Symbol.asyncIterator]
            ? await consume(body, () => call('upload.open', {}), upload => call(op, { ...args, upload }))
            : await call(op, args, body);
        if (stream) {
            response.body = reader(response.body);
        } else {
            response.text = () => decode(response.body, false);
            response.json = () => parse(response.text());
        }
        return response;
    }

    function createNode(target) {
        if (typeof target !== 'string' || !target.trim() || target.includes('\0')) {
            throw new TypeError('node target must be a nonempty string');
        }
        const node = target;
        return Object.freeze({
            fs: Object.freeze({
                async read(path, { encoding, stream = false } = {}) {
                    if (stream && encoding !== undefined) throw new TypeError('stream reads produce bytes');
                    const value = await call('fs.read', { node, path, encoding, stream });
                    if (stream) return reader(value);
                    return encoding === 'utf8' ? decode(value, false) : value;
                },
                async write(path, data, { append } = {}) {
                    if (data?.[Symbol.asyncIterator]) {
                        return consume(data, () => call('fs.write', { node, path, append, stream: true }));
                    }
                    return call('fs.write', { node, path, append }, data);
                },
                async *readDir(path) { yield* reader(await call('fs.readDir', { node, path })); },
                async stat(path, { followSymlinks } = {}) { return call('fs.stat', { node, path, followSymlinks }); },
                async mkdir(path, { recursive } = {}) { await call('fs.mkdir', { node, path, recursive }); },
                async rename(from, to) { await call('fs.rename', { node, from, to }); },
                async remove(path, { recursive } = {}) { await call('fs.remove', { node, path, recursive }); },
            }),
            proc: Object.freeze({
                async exec(program, args = [], { cwd, env, stdin, timeout_ms } = {}) {
                    return call('proc.exec', { node, program, args, cwd, env, timeout_ms }, stdin);
                },
            }),
            http: Object.freeze({
                async request({ url, method, headers, body, timeout_ms, stream = false }) {
                    if (typeof stream !== 'boolean') throw new TypeError('stream must be a boolean');
                    return sendRequest('http.request', { node, url, method, headers, timeout_ms, stream }, body, stream);
                },
            }),
            mcp: Object.freeze({
                async servers() { return call('mcp.servers', { node }); },
                async tools(server, { query, cursor } = {}) { return call('mcp.tools', { node, server, query, cursor }); },
                async call(server, tool, args = {}) { return mcpResult('mcp.call', { node, server, tool, args }); },
                async resources(server, { cursor } = {}) { return call('mcp.resources', { node, server, cursor }); },
                async resourceTemplates(server, { cursor } = {}) { return call('mcp.resourceTemplates', { node, server, cursor }); },
                async readResource(server, uri) { return mcpResult('mcp.readResource', { node, server, uri }); },
                async prompts(server, { cursor } = {}) { return call('mcp.prompts', { node, server, cursor }); },
                async getPrompt(server, name, args) { return mcpResult('mcp.getPrompt', { node, server, name, args }); },
            }),
            mygo: Object.freeze({
                async request({ path, method, headers, body, timeout_ms, stream = false }) {
                    if (typeof stream !== 'boolean') throw new TypeError('stream must be a boolean');
                    let reqHeaders = headers;
                    let reqBody = body;
                    if (body !== undefined && body !== null && !body[Symbol.asyncIterator]
                        && typeof body !== 'string' && !(body instanceof Uint8Array)) {
                        if (isView(body) || body instanceof ArrayBuffer) {
                            throw new TypeError('expected string, Uint8Array, async iterable, or JSON value');
                        }
                        const json = stringify(body);
                        if (json === undefined) {
                            throw new TypeError('request body cannot be serialized as JSON');
                        }
                        reqBody = json;
                        if (headers === undefined || headers === null || (typeof headers === 'object' && !Array.isArray(headers))) {
                            const hasType = headers && Object.keys(headers).some(k => k.toLowerCase() === 'content-type');
                            if (!hasType) reqHeaders = { ...headers, 'content-type': 'application/json' };
                        }
                    }
                    return sendRequest('mygo.request', { node, path, method, headers: reqHeaders, timeout_ms, stream }, reqBody, stream);
                },
            }),
        });
    }

    Object.assign(globalThis, {
        cluster: Object.freeze({
            async nodes() { return call('cluster.nodes', {}); },
            node(target) { return createNode(target); },
        }),
        mygo: Object.freeze({
            sessionId,
            taskId: taskId ?? null,
        }),
        task: {
            async list() { return call('task.list', {}); },
            async status(id) { return call('task.status', { id }); },
        },
        async sleep(ms) { await call('sleep', { ms }); },
        async emitEvent(value) { return emit(value); },
    });
    Object.defineProperty(globalThis, 'console', {
        value: {
            log(...args) { writeLog('', args); },
            info(...args) { writeLog('', args); },
            debug(...args) { writeLog('', args); },
            warn(...args) { writeLog('warn: ', args); },
            error(...args) { writeLog('error: ', args); },
            assert(condition, ...args) {
                if (!condition) writeLog(args.length ? 'Assertion failed: ' : 'Assertion failed', args);
            },
            clear() { log(null); },
        },
        writable: true,
        configurable: true,
    });
})

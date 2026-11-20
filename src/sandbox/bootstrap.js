((io, emit, append, decode) => {
    const stringify = JSON.stringify;
    const parse = JSON.parse;
    const call = (op, args, data = undefined) => io(op, stringify(args), data);

    Object.assign(globalThis, {
        fs: {
            async read(path, { encoding } = {}) {
                const value = await call("fs.read", { path, encoding });
                return encoding === "utf8" && typeof value !== "string" ? decode(value) : value;
            },
            async write(path, data, { append } = {}) {
                return call("fs.write", { path, append }, data);
            },
        },
        http: {
            async request({ url, method, headers, body, timeout_ms }) {
                const response = await call("http.request", { url, method, headers, timeout_ms }, body);
                response.text = () => decode(response.body);
                response.json = () => parse(response.text());
                return response;
            },
        },
        proc: {
            async exec(program, args = [], { cwd, env, stdin, timeout_ms } = {}) {
                return call("proc.exec", { program, args, cwd, env, timeout_ms }, stdin);
            },
        },
        async bash(script, { cwd, env, stdin, timeout_ms } = {}) {
            return call("bash", { script, cwd, env, timeout_ms }, stdin);
        },
        async sleep(ms) { await call("sleep", { ms }); },
        async emitEvent(value) { return emit(value); },
        result: append,
    });
})

((decode) => {
    const empty = new Uint8Array(0);
    const chunkSize = 64 * 1024;
    const labels = new Set(["utf-8", "utf8", "unicode-1-1-utf-8", "unicode11utf8", "unicode20utf8", "x-unicode20utf8"]);

    function dictionary(value) {
        if (value != null && typeof value !== "object" && typeof value !== "function") {
            throw new TypeError("expected an options object");
        }
        return value;
    }

    // Use the built-in getters. A view can shadow buffer, byteLength and byteOffset.
    const getter = (proto, key) => Object.getOwnPropertyDescriptor(proto, key).get;
    const viewGetters = (proto) => ({
        buffer: getter(proto, "buffer"), length: getter(proto, "byteLength"), offset: getter(proto, "byteOffset"),
    });
    const typedProto = Object.getPrototypeOf(Uint8Array.prototype);
    const typedTag = getter(typedProto, Symbol.toStringTag);
    const typed = viewGetters(typedProto);
    const dataView = viewGetters(DataView.prototype);
    const bufferLength = getter(ArrayBuffer.prototype, "byteLength");
    const sharedLength = typeof SharedArrayBuffer === "function" && getter(SharedArrayBuffer.prototype, "byteLength");

    function bytesOf(input) {
        if (input === undefined) return empty;
        let length;
        if (ArrayBuffer.isView(input)) {
            const view = typedTag.call(input) === undefined ? dataView : typed;
            try {
                length = view.length.call(input);
            } catch {
                // A detached or out-of-bounds DataView throws when reading its length.
                return empty;
            }
            return length ? new Uint8Array(view.buffer.call(input), view.offset.call(input), length) : empty;
        }
        try {
            length = bufferLength.call(input);
        } catch (error) {
            if (!sharedLength) throw error;
            length = sharedLength.call(input);
        }
        return length ? new Uint8Array(input, 0, length) : empty;
    }

    // Only a legal, unfinished UTF-8 sequence may be carried into the next chunk.
    // A bad lead or second byte must reach the native decoder immediately.
    function completePrefix(bytes) {
        const end = bytes.length;
        let i = end - 1;
        while (i >= 0 && end - i <= 3 && (bytes[i] & 0xc0) === 0x80) i--;
        if (i < 0 || end - i > 3) return end;
        const lead = bytes[i];
        if (lead < 0xc2 || lead > 0xf4) return end;
        const width = lead < 0xe0 ? 2 : lead < 0xf0 ? 3 : 4;
        if (end - i >= width) return end;
        const second = bytes[i + 1];
        if ((lead === 0xe0 && second < 0xa0) || (lead === 0xed && second > 0x9f) ||
            (lead === 0xf0 && second < 0x90) || (lead === 0xf4 && second > 0x8f)) return end;
        return i;
    }

    class TextDecoder {
        #fatal;
        #ignoreBOM;
        #bomSeen = false;
        #pending = empty;

        constructor(label = "utf-8", options = undefined) {
            const name = `${label}`.replace(/^[\t\n\f\r ]+|[\t\n\f\r ]+$/g, "").toLowerCase();
            dictionary(options);
            this.#fatal = Boolean(options?.fatal);
            this.#ignoreBOM = Boolean(options?.ignoreBOM);
            if (!labels.has(name)) throw new RangeError("unsupported text encoding");
        }

        get encoding() { return "utf-8"; }
        get fatal() { return this.#fatal; }
        get ignoreBOM() { return this.#ignoreBOM; }
        get [Symbol.toStringTag]() { return "TextDecoder"; }

        decode(input = undefined, options = undefined) {
            const stream = Boolean(dictionary(options)?.stream);
            const bytes = bytesOf(input);
            let pending = this.#pending;
            this.#pending = empty;
            const parts = [];
            let offset = 0;
            try {
                do {
                    const end = Math.min(offset + chunkSize - pending.length, bytes.length);
                    let chunk = bytes.subarray(offset, end);
                    if (pending.length) {
                        const joined = new Uint8Array(pending.length + chunk.length);
                        joined.set(pending);
                        joined.set(chunk, pending.length);
                        chunk = joined;
                    }
                    offset = end;
                    const cut = stream || end < bytes.length ? completePrefix(chunk) : chunk.length;
                    pending = cut < chunk.length ? chunk.slice(cut) : empty;
                    if (cut) parts.push(decode(chunk.subarray(0, cut), this.#fatal));
                } while (offset < bytes.length);
                this.#pending = pending;
                const text = parts.length === 1 ? parts[0] : parts.join("");
                if (this.#bomSeen || !text.length) return text;
                this.#bomSeen = true;
                return !this.#ignoreBOM && text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
            } finally {
                if (!stream) this.#bomSeen = false;
            }
        }
    }

    Object.defineProperty(globalThis, "TextDecoder", { value: TextDecoder, writable: true, configurable: true });
})

// A reader for dialog-answer frames (rust/dialog-answer), for the page
// side of the wire: it learns where each token sits and reads a cell only
// when asked, decoding text from the frame's bytes in place.

const MAGIC = 0x31464144; // "DAF1", little-endian
const ABSENT = 0xffffffff;
const UNBOUND = 0xfffffffe;
const decoder = new TextDecoder("utf-8", { fatal: true });

export class Dictionary {
  constructor() {
    // Per token: which frame, where, how long, its tag.
    this.frames = [];
    this.tokenFrame = new Uint32Array(1024);
    this.tokenAt = new Uint32Array(1024);
    this.tokenLen = new Uint32Array(1024);
    this.tokenTag = new Uint8Array(1024);
    this.length = 0;
    // Decoded text, by token id: a token read twice is decoded once.
    this.texts = [];
  }

  grow(need) {
    if (need <= this.tokenAt.length) return;
    let size = this.tokenAt.length;
    while (size < need) size *= 2;
    for (const [name, Kind] of [["tokenFrame", Uint32Array], ["tokenAt", Uint32Array], ["tokenLen", Uint32Array], ["tokenTag", Uint8Array]]) {
      const next = new Kind(size);
      next.set(this[name]);
      this[name] = next;
    }
  }

  accept(bytes) {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    if (view.getUint32(0, true) !== MAGIC) throw new Error("not an answer frame");
    const kind = view.getUint8(4);
    const base = view.getUint32(5, true);
    if (base !== this.length) throw new Error(`frame starts at ${base}, expected ${this.length}`);
    const count = view.getUint32(9, true);
    const frame = this.frames.length;
    this.frames.push(bytes);
    this.grow(base + count);
    let at = 13;
    for (let i = 0; i < count; i++) {
      const id = base + i;
      this.tokenTag[id] = view.getUint8(at);
      const len = view.getUint32(at + 1, true);
      this.tokenFrame[id] = frame;
      this.tokenAt[id] = at + 5;
      this.tokenLen[id] = len;
      at += 5 + len;
    }
    this.length = base + count;
    const columnCount = view.getUint16(at, true);
    at += 2;
    const columns = [];
    for (let i = 0; i < columnCount; i++) {
      columns.push(this.read(view.getUint32(at, true)));
      at += 4;
    }
    const sections = [];
    for (let s = 0; s < (kind === 1 ? 2 : 1); s++) {
      const rows = view.getUint32(at, true);
      at += 4;
      sections.push({ rows, lanes: new Uint32Array(bytes.buffer.slice(bytes.byteOffset + at, bytes.byteOffset + at + rows * columnCount * 4)) });
      at += rows * columnCount * 4;
    }
    return new Answer(this, columns, sections);
  }

  read(id) {
    if (id === ABSENT || id === UNBOUND) return undefined;
    const cached = this.texts[id];
    if (cached !== undefined) return cached;
    const bytes = this.frames[this.tokenFrame[id]];
    const at = this.tokenAt[id];
    const len = this.tokenLen[id];
    const tag = this.tokenTag[id];
    let value;
    if (tag <= 3) {
      value = decoder.decode(bytes.subarray(at, at + len));
    } else if (tag === 6 || tag === 7) {
      // A u128 / i128: the low 53 bits are a JS number, as JSON gives.
      const view = new DataView(bytes.buffer, bytes.byteOffset + at, len);
      value = view.getUint32(0, true) + view.getUint32(4, true) * 2 ** 32;
    } else if (tag === 8) {
      value = new DataView(bytes.buffer, bytes.byteOffset + at, 8).getFloat64(0, true);
    } else if (tag === 9) {
      value = bytes[at] !== 0;
    } else {
      value = bytes.subarray(at, at + len);
    }
    this.texts[id] = value;
    return value;
  }
}

export class Answer {
  constructor(dictionary, columns, sections) {
    this.dictionary = dictionary;
    this.columns = columns;
    this.sections = sections;
  }

  column(name) {
    return this.columns.indexOf(name);
  }

  rows(section = 0) {
    return this.sections[section].rows;
  }

  // The value row `row` of `section` holds for `column`.
  get(row, column, section = 0) {
    const { rows, lanes } = this.sections[section];
    return this.dictionary.read(lanes[column * rows + row]);
  }
}

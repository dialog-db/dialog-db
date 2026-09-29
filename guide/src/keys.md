# Keys

Dialog keeps its facts in a sorted tree, and a sorted tree can only find things by the order of their keys. A fact gets looked up in three different ways, though. An app shows one grocery item, and needs every fact about `item:1`. It shows the whole list, and needs every `grocery/name`. It asks which items are tagged `"breakfast"`, and needs to start from a value.

No single sort order serves all three. So Dialog writes every fact three times, each time with its parts in a different order:

<figure class="dg">
<svg class="dg" viewBox="0 0 670 98" width="670" height="98" xmlns="http://www.w3.org/2000/svg" role="img">
<text class="label small muted" x="8" y="26.0">EAV  tag 00</text>
<rect class="entity" x="100" y="8" width="130" height="22"/>
<text class="entity" x="165.0" y="23.0" text-anchor="middle">item:1</text>
<rect class="attribute" x="234" y="8" width="130" height="22"/>
<text class="attribute" x="299.0" y="23.0" text-anchor="middle">grocery/name</text>
<rect class="value" x="368" y="8" width="130" height="22"/>
<text class="value" x="433.0" y="23.0" text-anchor="middle">&quot;Oat milk&quot;</text>
<text class="small muted" x="510" y="23.0">find by entity</text>
<text class="label small muted" x="8" y="54.0">AEV  tag 01</text>
<rect class="attribute" x="100" y="36" width="130" height="22"/>
<text class="attribute" x="165.0" y="51.0" text-anchor="middle">grocery/name</text>
<rect class="entity" x="234" y="36" width="130" height="22"/>
<text class="entity" x="299.0" y="51.0" text-anchor="middle">item:1</text>
<rect class="value" x="368" y="36" width="130" height="22"/>
<text class="value" x="433.0" y="51.0" text-anchor="middle">&quot;Oat milk&quot;</text>
<text class="small muted" x="510" y="51.0">find by attribute</text>
<text class="label small muted" x="8" y="82.0">VAE  tag 02</text>
<rect class="value" x="100" y="64" width="130" height="22"/>
<text class="value" x="165.0" y="79.0" text-anchor="middle">&quot;Oat milk&quot;</text>
<rect class="attribute" x="234" y="64" width="130" height="22"/>
<text class="attribute" x="299.0" y="79.0" text-anchor="middle">grocery/name</text>
<rect class="entity" x="368" y="64" width="130" height="22"/>
<text class="entity" x="433.0" y="79.0" text-anchor="middle">item:1</text>
<text class="small muted" x="510" y="79.0">find by value</text>
</svg>
</figure>

Each copy is called an index. The first byte of the key, the tag, says which index it belongs to. All the indexes live side by side in one tree, and the tag keeps them apart:

| Tag | Index | Parts in order | Used to find |
|---|---|---|---|
| `00` | EAV | entity, attribute, value | everything about a thing |
| `01` | AEV | attribute, entity, value | everything with an attribute |
| `02` | VAE | value, attribute, entity | things that have a value |
| `03` | History | version, entity, attribute, value | the log of changes |
| `04` | Blob | hash | stored blobs |
| `05` | Coverage | version, entity, attribute, value hash | what a change removed |

The first three hold the facts themselves. The last three are Dialog's own bookkeeping, and later chapters come back to them.

## A key, byte by byte

Here is the EAV key for *the grocery name of item 1 is "Oat milk"*:

<figure class="dg">
<svg class="dg" viewBox="0 0 462 172" width="462" height="172" xmlns="http://www.w3.org/2000/svg" role="img">
<rect class="shade" x="8" y="6" width="26" height="38"/>
<text x="21.0" y="23" text-anchor="middle">00</text>
<rect class="entity" x="36" y="6" width="26" height="38"/>
<text x="49.0" y="23" text-anchor="middle">i</text>
<text class="small muted" x="49.0" y="38" text-anchor="middle">69</text>
<rect class="entity" x="64" y="6" width="26" height="38"/>
<text x="77.0" y="23" text-anchor="middle">t</text>
<text class="small muted" x="77.0" y="38" text-anchor="middle">74</text>
<rect class="entity" x="92" y="6" width="26" height="38"/>
<text x="105.0" y="23" text-anchor="middle">e</text>
<text class="small muted" x="105.0" y="38" text-anchor="middle">65</text>
<rect class="entity" x="120" y="6" width="26" height="38"/>
<text x="133.0" y="23" text-anchor="middle">m</text>
<text class="small muted" x="133.0" y="38" text-anchor="middle">6d</text>
<rect class="entity" x="148" y="6" width="26" height="38"/>
<text x="161.0" y="23" text-anchor="middle">:</text>
<text class="small muted" x="161.0" y="38" text-anchor="middle">3a</text>
<rect class="entity" x="176" y="6" width="26" height="38"/>
<text x="189.0" y="23" text-anchor="middle">1</text>
<text class="small muted" x="189.0" y="38" text-anchor="middle">31</text>
<rect class="entity" x="204" y="6" width="26" height="38"/>
<text x="217.0" y="23" text-anchor="middle">00</text>
<rect class="attribute" x="232" y="6" width="26" height="38"/>
<text x="245.0" y="23" text-anchor="middle">g</text>
<text class="small muted" x="245.0" y="38" text-anchor="middle">67</text>
<rect class="attribute" x="260" y="6" width="26" height="38"/>
<text x="273.0" y="23" text-anchor="middle">r</text>
<text class="small muted" x="273.0" y="38" text-anchor="middle">72</text>
<rect class="attribute" x="288" y="6" width="26" height="38"/>
<text x="301.0" y="23" text-anchor="middle">o</text>
<text class="small muted" x="301.0" y="38" text-anchor="middle">6f</text>
<rect class="attribute" x="316" y="6" width="26" height="38"/>
<text x="329.0" y="23" text-anchor="middle">c</text>
<text class="small muted" x="329.0" y="38" text-anchor="middle">63</text>
<rect class="attribute" x="344" y="6" width="26" height="38"/>
<text x="357.0" y="23" text-anchor="middle">e</text>
<text class="small muted" x="357.0" y="38" text-anchor="middle">65</text>
<rect class="attribute" x="372" y="6" width="26" height="38"/>
<text x="385.0" y="23" text-anchor="middle">r</text>
<text class="small muted" x="385.0" y="38" text-anchor="middle">72</text>
<rect class="attribute" x="400" y="6" width="26" height="38"/>
<text x="413.0" y="23" text-anchor="middle">y</text>
<text class="small muted" x="413.0" y="38" text-anchor="middle">79</text>
<rect class="attribute" x="428" y="6" width="26" height="38"/>
<text x="441.0" y="23" text-anchor="middle">/</text>
<text class="small muted" x="441.0" y="38" text-anchor="middle">2f</text>
<rect class="attribute" x="8" y="89" width="26" height="38"/>
<text x="21.0" y="106" text-anchor="middle">n</text>
<text class="small muted" x="21.0" y="121" text-anchor="middle">6e</text>
<rect class="attribute" x="36" y="89" width="26" height="38"/>
<text x="49.0" y="106" text-anchor="middle">a</text>
<text class="small muted" x="49.0" y="121" text-anchor="middle">61</text>
<rect class="attribute" x="64" y="89" width="26" height="38"/>
<text x="77.0" y="106" text-anchor="middle">m</text>
<text class="small muted" x="77.0" y="121" text-anchor="middle">6d</text>
<rect class="attribute" x="92" y="89" width="26" height="38"/>
<text x="105.0" y="106" text-anchor="middle">e</text>
<text class="small muted" x="105.0" y="121" text-anchor="middle">65</text>
<rect class="attribute" x="120" y="89" width="26" height="38"/>
<text x="133.0" y="106" text-anchor="middle">00</text>
<rect class="value vtype" x="148" y="89" width="26" height="38"/>
<text x="161.0" y="106" text-anchor="middle">03</text>
<rect class="value" x="176" y="89" width="26" height="38"/>
<text x="189.0" y="106" text-anchor="middle">O</text>
<text class="small muted" x="189.0" y="121" text-anchor="middle">4f</text>
<rect class="value" x="204" y="89" width="26" height="38"/>
<text x="217.0" y="106" text-anchor="middle">a</text>
<text class="small muted" x="217.0" y="121" text-anchor="middle">61</text>
<rect class="value" x="232" y="89" width="26" height="38"/>
<text x="245.0" y="106" text-anchor="middle">t</text>
<text class="small muted" x="245.0" y="121" text-anchor="middle">74</text>
<rect class="value" x="260" y="89" width="26" height="38"/>
<text x="273.0" y="106" text-anchor="middle">20</text>
<rect class="value" x="288" y="89" width="26" height="38"/>
<text x="301.0" y="106" text-anchor="middle">m</text>
<text class="small muted" x="301.0" y="121" text-anchor="middle">6d</text>
<rect class="value" x="316" y="89" width="26" height="38"/>
<text x="329.0" y="106" text-anchor="middle">i</text>
<text class="small muted" x="329.0" y="121" text-anchor="middle">69</text>
<rect class="value" x="344" y="89" width="26" height="38"/>
<text x="357.0" y="106" text-anchor="middle">l</text>
<text class="small muted" x="357.0" y="121" text-anchor="middle">6c</text>
<rect class="value" x="372" y="89" width="26" height="38"/>
<text x="385.0" y="106" text-anchor="middle">k</text>
<text class="small muted" x="385.0" y="121" text-anchor="middle">6b</text>
<rect class="value" x="400" y="89" width="26" height="38"/>
<text x="413.0" y="106" text-anchor="middle">00</text>
<path class="wire" d="M9,49 v4 H33 v-4"/>
<text class="label small" x="21.0" y="67" text-anchor="middle">tag</text>
<path class="wire" d="M37,49 v4 H229 v-4"/>
<text class="label small" x="133.0" y="67" text-anchor="middle">entity</text>
<path class="wire" d="M233,49 v4 H453 v-4"/>
<text class="label small" x="343.0" y="67" text-anchor="middle">attribute</text>
<path class="wire" d="M9,132 v4 H145 v-4"/>
<path class="wire" d="M149,132 v4 H173 v-4"/>
<text class="label small" x="161.0" y="150" text-anchor="middle">type</text>
<path class="wire" d="M177,132 v4 H425 v-4"/>
<text class="label small" x="301.0" y="150" text-anchor="middle">value</text>
</svg>
</figure>

The key starts with the tag, `00`. Then each part follows, written so that comparing the raw bytes gives the same answer as comparing the parts themselves:

- **Strings** (the entity URI, the attribute, and string values) are written as their UTF-8 bytes followed by a `00` terminator. Any `00` inside the string is written as `00 FF`, so the terminator is never ambiguous.
- **The type byte** of the value comes right before the value. It is the value's type number from the [Facts](./facts.md) chapter; `03` is String.
- **Numbers** are fixed width and big-endian, so bigger numbers sort later. Unsigned integers take 16 bytes. Signed integers take 16 bytes with the sign bit flipped, so negative numbers sort before positive ones. Floats take 8 bytes: a non-negative float has its sign bit flipped, a negative one has every bit flipped, so they sort numerically.
- **Booleans** are one byte, `00` for false and `01` for true.

The terminator is what makes prefixes sort first. `item:1` ends with `00`, and `item:10` continues with `30` (the character `0`) at the same position. Since `00` is smaller, every key for `item:1` comes before every key for `item:10`, just as the strings would. Nothing about item 1 ever lands in the middle of item 10.

<div class="aside">

**Why no field starts with `FF`.** A terminator followed by `FF` would read back as an escaped zero. So no part of a key that follows a terminated string may begin with the byte `FF`, and the range bounds Dialog builds for scans use `FE` as their filler byte instead. The spill marker below starts with `01` for the same reason.

</div>

The VAE key holds the same bytes in a different order. The tag is `02`, and the value comes first, led by its type:

<figure class="dg">
<svg class="dg" viewBox="0 0 462 187" width="462" height="187" xmlns="http://www.w3.org/2000/svg" role="img">
<rect class="shade" x="8" y="6" width="26" height="38"/>
<text x="21.0" y="23" text-anchor="middle">02</text>
<rect class="value vtype" x="36" y="6" width="26" height="38"/>
<text x="49.0" y="23" text-anchor="middle">03</text>
<rect class="value" x="64" y="6" width="26" height="38"/>
<text x="77.0" y="23" text-anchor="middle">O</text>
<text class="small muted" x="77.0" y="38" text-anchor="middle">4f</text>
<rect class="value" x="92" y="6" width="26" height="38"/>
<text x="105.0" y="23" text-anchor="middle">a</text>
<text class="small muted" x="105.0" y="38" text-anchor="middle">61</text>
<rect class="value" x="120" y="6" width="26" height="38"/>
<text x="133.0" y="23" text-anchor="middle">t</text>
<text class="small muted" x="133.0" y="38" text-anchor="middle">74</text>
<rect class="value" x="148" y="6" width="26" height="38"/>
<text x="161.0" y="23" text-anchor="middle">20</text>
<rect class="value" x="176" y="6" width="26" height="38"/>
<text x="189.0" y="23" text-anchor="middle">m</text>
<text class="small muted" x="189.0" y="38" text-anchor="middle">6d</text>
<rect class="value" x="204" y="6" width="26" height="38"/>
<text x="217.0" y="23" text-anchor="middle">i</text>
<text class="small muted" x="217.0" y="38" text-anchor="middle">69</text>
<rect class="value" x="232" y="6" width="26" height="38"/>
<text x="245.0" y="23" text-anchor="middle">l</text>
<text class="small muted" x="245.0" y="38" text-anchor="middle">6c</text>
<rect class="value" x="260" y="6" width="26" height="38"/>
<text x="273.0" y="23" text-anchor="middle">k</text>
<text class="small muted" x="273.0" y="38" text-anchor="middle">6b</text>
<rect class="value" x="288" y="6" width="26" height="38"/>
<text x="301.0" y="23" text-anchor="middle">00</text>
<rect class="attribute" x="316" y="6" width="26" height="38"/>
<text x="329.0" y="23" text-anchor="middle">g</text>
<text class="small muted" x="329.0" y="38" text-anchor="middle">67</text>
<rect class="attribute" x="344" y="6" width="26" height="38"/>
<text x="357.0" y="23" text-anchor="middle">r</text>
<text class="small muted" x="357.0" y="38" text-anchor="middle">72</text>
<rect class="attribute" x="372" y="6" width="26" height="38"/>
<text x="385.0" y="23" text-anchor="middle">o</text>
<text class="small muted" x="385.0" y="38" text-anchor="middle">6f</text>
<rect class="attribute" x="400" y="6" width="26" height="38"/>
<text x="413.0" y="23" text-anchor="middle">c</text>
<text class="small muted" x="413.0" y="38" text-anchor="middle">63</text>
<rect class="attribute" x="428" y="6" width="26" height="38"/>
<text x="441.0" y="23" text-anchor="middle">e</text>
<text class="small muted" x="441.0" y="38" text-anchor="middle">65</text>
<rect class="attribute" x="8" y="104" width="26" height="38"/>
<text x="21.0" y="121" text-anchor="middle">r</text>
<text class="small muted" x="21.0" y="136" text-anchor="middle">72</text>
<rect class="attribute" x="36" y="104" width="26" height="38"/>
<text x="49.0" y="121" text-anchor="middle">y</text>
<text class="small muted" x="49.0" y="136" text-anchor="middle">79</text>
<rect class="attribute" x="64" y="104" width="26" height="38"/>
<text x="77.0" y="121" text-anchor="middle">/</text>
<text class="small muted" x="77.0" y="136" text-anchor="middle">2f</text>
<rect class="attribute" x="92" y="104" width="26" height="38"/>
<text x="105.0" y="121" text-anchor="middle">n</text>
<text class="small muted" x="105.0" y="136" text-anchor="middle">6e</text>
<rect class="attribute" x="120" y="104" width="26" height="38"/>
<text x="133.0" y="121" text-anchor="middle">a</text>
<text class="small muted" x="133.0" y="136" text-anchor="middle">61</text>
<rect class="attribute" x="148" y="104" width="26" height="38"/>
<text x="161.0" y="121" text-anchor="middle">m</text>
<text class="small muted" x="161.0" y="136" text-anchor="middle">6d</text>
<rect class="attribute" x="176" y="104" width="26" height="38"/>
<text x="189.0" y="121" text-anchor="middle">e</text>
<text class="small muted" x="189.0" y="136" text-anchor="middle">65</text>
<rect class="attribute" x="204" y="104" width="26" height="38"/>
<text x="217.0" y="121" text-anchor="middle">00</text>
<rect class="entity" x="232" y="104" width="26" height="38"/>
<text x="245.0" y="121" text-anchor="middle">i</text>
<text class="small muted" x="245.0" y="136" text-anchor="middle">69</text>
<rect class="entity" x="260" y="104" width="26" height="38"/>
<text x="273.0" y="121" text-anchor="middle">t</text>
<text class="small muted" x="273.0" y="136" text-anchor="middle">74</text>
<rect class="entity" x="288" y="104" width="26" height="38"/>
<text x="301.0" y="121" text-anchor="middle">e</text>
<text class="small muted" x="301.0" y="136" text-anchor="middle">65</text>
<rect class="entity" x="316" y="104" width="26" height="38"/>
<text x="329.0" y="121" text-anchor="middle">m</text>
<text class="small muted" x="329.0" y="136" text-anchor="middle">6d</text>
<rect class="entity" x="344" y="104" width="26" height="38"/>
<text x="357.0" y="121" text-anchor="middle">:</text>
<text class="small muted" x="357.0" y="136" text-anchor="middle">3a</text>
<rect class="entity" x="372" y="104" width="26" height="38"/>
<text x="385.0" y="121" text-anchor="middle">1</text>
<text class="small muted" x="385.0" y="136" text-anchor="middle">31</text>
<rect class="entity" x="400" y="104" width="26" height="38"/>
<text x="413.0" y="121" text-anchor="middle">00</text>
<path class="wire" d="M9,49 v4 H33 v-4"/>
<text class="label small" x="21.0" y="67" text-anchor="middle">tag</text>
<path class="wire" d="M37,49 v4 H61 v-4"/>
<line x1="49.0" y1="53" x2="49.0" y2="71" class="dashed"/>
<text class="label small" x="49.0" y="82" text-anchor="middle">type</text>
<path class="wire" d="M65,49 v4 H313 v-4"/>
<text class="label small" x="189.0" y="67" text-anchor="middle">value</text>
<path class="wire" d="M317,49 v4 H453 v-4"/>
<text class="label small" x="385.0" y="67" text-anchor="middle">attribute</text>
<path class="wire" d="M9,147 v4 H229 v-4"/>
<path class="wire" d="M233,147 v4 H425 v-4"/>
<text class="label small" x="329.0" y="165" text-anchor="middle">entity</text>
</svg>
</figure>

## Sorted, the list tells its own story

Put Alice's keys in byte order and each index turns into a set of neat runs. The type bytes are left out here to keep the rows short, and part of the AEV index is elided:

<figure class="dg">
<svg class="dg" viewBox="0 0 598 378" width="598" height="378" xmlns="http://www.w3.org/2000/svg" role="img">
<rect class="shade" x="8" y="8" width="36" height="20"/>
<text x="26.0" y="22.0" text-anchor="middle">00</text>
<rect class="entity" x="48" y="8" width="130" height="20"/>
<text class="entity" x="113.0" y="22.0" text-anchor="middle">item:1</text>
<rect class="attribute" x="182" y="8" width="130" height="20"/>
<text class="attribute" x="247.0" y="22.0" text-anchor="middle">grocery/done</text>
<rect class="value" x="316" y="8" width="110" height="20"/>
<text class="value" x="371.0" y="22.0" text-anchor="middle">false</text>
<text class="small muted" x="438" y="22.0">everything about item:1</text>
<rect class="shade" x="8" y="34" width="36" height="20"/>
<text x="26.0" y="48.0" text-anchor="middle">00</text>
<rect class="entity" x="48" y="34" width="130" height="20"/>
<text class="entity" x="113.0" y="48.0" text-anchor="middle">item:1</text>
<rect class="attribute" x="182" y="34" width="130" height="20"/>
<text class="attribute" x="247.0" y="48.0" text-anchor="middle">grocery/name</text>
<rect class="value" x="316" y="34" width="110" height="20"/>
<text class="value" x="371.0" y="48.0" text-anchor="middle">&quot;Oat milk&quot;</text>
<rect class="shade" x="8" y="60" width="36" height="20"/>
<text x="26.0" y="74.0" text-anchor="middle">00</text>
<rect class="entity" x="48" y="60" width="130" height="20"/>
<text class="entity" x="113.0" y="74.0" text-anchor="middle">item:2</text>
<rect class="attribute" x="182" y="60" width="130" height="20"/>
<text class="attribute" x="247.0" y="74.0" text-anchor="middle">grocery/done</text>
<rect class="value" x="316" y="60" width="110" height="20"/>
<text class="value" x="371.0" y="74.0" text-anchor="middle">false</text>
<text class="small muted" x="438" y="74.0">everything about item:2</text>
<rect class="shade" x="8" y="86" width="36" height="20"/>
<text x="26.0" y="100.0" text-anchor="middle">00</text>
<rect class="entity" x="48" y="86" width="130" height="20"/>
<text class="entity" x="113.0" y="100.0" text-anchor="middle">item:2</text>
<rect class="attribute" x="182" y="86" width="130" height="20"/>
<text class="attribute" x="247.0" y="100.0" text-anchor="middle">grocery/name</text>
<rect class="value" x="316" y="86" width="110" height="20"/>
<text class="value" x="371.0" y="100.0" text-anchor="middle">&quot;Eggs&quot;</text>
<rect class="shade" x="8" y="112" width="36" height="20"/>
<text x="26.0" y="126.0" text-anchor="middle">00</text>
<rect class="entity" x="48" y="112" width="130" height="20"/>
<text class="entity" x="113.0" y="126.0" text-anchor="middle">item:2</text>
<rect class="attribute" x="182" y="112" width="130" height="20"/>
<text class="attribute" x="247.0" y="126.0" text-anchor="middle">grocery/tag</text>
<rect class="value" x="316" y="112" width="110" height="20"/>
<text class="value" x="371.0" y="126.0" text-anchor="middle">&quot;breakfast&quot;</text>
<rect class="shade" x="8" y="138" width="36" height="20"/>
<text x="26.0" y="152.0" text-anchor="middle">01</text>
<rect class="attribute" x="48" y="138" width="130" height="20"/>
<text class="attribute" x="113.0" y="152.0" text-anchor="middle">grocery/done</text>
<rect class="entity" x="182" y="138" width="130" height="20"/>
<text class="entity" x="247.0" y="152.0" text-anchor="middle">item:1</text>
<rect class="value" x="316" y="138" width="110" height="20"/>
<text class="value" x="371.0" y="152.0" text-anchor="middle">false</text>
<text class="small muted" x="438" y="152.0">every item&#x27;s done flag</text>
<rect class="shade" x="8" y="164" width="36" height="20"/>
<text x="26.0" y="178.0" text-anchor="middle">01</text>
<rect class="attribute" x="48" y="164" width="130" height="20"/>
<text class="attribute" x="113.0" y="178.0" text-anchor="middle">grocery/done</text>
<rect class="entity" x="182" y="164" width="130" height="20"/>
<text class="entity" x="247.0" y="178.0" text-anchor="middle">item:2</text>
<rect class="value" x="316" y="164" width="110" height="20"/>
<text class="value" x="371.0" y="178.0" text-anchor="middle">false</text>
<rect class="shade" x="8" y="190" width="36" height="20"/>
<text x="26.0" y="204.0" text-anchor="middle">01</text>
<rect class="attribute" x="48" y="190" width="130" height="20"/>
<text class="attribute" x="113.0" y="204.0" text-anchor="middle">grocery/name</text>
<rect class="entity" x="182" y="190" width="130" height="20"/>
<text class="entity" x="247.0" y="204.0" text-anchor="middle">item:1</text>
<rect class="value" x="316" y="190" width="110" height="20"/>
<text class="value" x="371.0" y="204.0" text-anchor="middle">&quot;Oat milk&quot;</text>
<rect class="shade" x="182" y="216" width="130" height="20"/>
<text x="247.0" y="230.0" text-anchor="middle">…</text>
<rect class="shade" x="8" y="242" width="36" height="20"/>
<text x="26.0" y="256.0" text-anchor="middle">02</text>
<rect class="value" x="48" y="242" width="130" height="20"/>
<text class="value" x="113.0" y="256.0" text-anchor="middle">false</text>
<rect class="attribute" x="182" y="242" width="130" height="20"/>
<text class="attribute" x="247.0" y="256.0" text-anchor="middle">grocery/done</text>
<rect class="entity" x="316" y="242" width="110" height="20"/>
<text class="entity" x="371.0" y="256.0" text-anchor="middle">item:1</text>
<text class="small muted" x="438" y="256.0">every item not yet done</text>
<rect class="shade" x="8" y="268" width="36" height="20"/>
<text x="26.0" y="282.0" text-anchor="middle">02</text>
<rect class="value" x="48" y="268" width="130" height="20"/>
<text class="value" x="113.0" y="282.0" text-anchor="middle">false</text>
<rect class="attribute" x="182" y="268" width="130" height="20"/>
<text class="attribute" x="247.0" y="282.0" text-anchor="middle">grocery/done</text>
<rect class="entity" x="316" y="268" width="110" height="20"/>
<text class="entity" x="371.0" y="282.0" text-anchor="middle">item:2</text>
<rect class="shade" x="8" y="294" width="36" height="20"/>
<text x="26.0" y="308.0" text-anchor="middle">02</text>
<rect class="value" x="48" y="294" width="130" height="20"/>
<text class="value" x="113.0" y="308.0" text-anchor="middle">&quot;Eggs&quot;</text>
<rect class="attribute" x="182" y="294" width="130" height="20"/>
<text class="attribute" x="247.0" y="308.0" text-anchor="middle">grocery/name</text>
<rect class="entity" x="316" y="294" width="110" height="20"/>
<text class="entity" x="371.0" y="308.0" text-anchor="middle">item:2</text>
<rect class="shade" x="8" y="320" width="36" height="20"/>
<text x="26.0" y="334.0" text-anchor="middle">02</text>
<rect class="value" x="48" y="320" width="130" height="20"/>
<text class="value" x="113.0" y="334.0" text-anchor="middle">&quot;Oat milk&quot;</text>
<rect class="attribute" x="182" y="320" width="130" height="20"/>
<text class="attribute" x="247.0" y="334.0" text-anchor="middle">grocery/name</text>
<rect class="entity" x="316" y="320" width="110" height="20"/>
<text class="entity" x="371.0" y="334.0" text-anchor="middle">item:1</text>
<rect class="shade" x="8" y="346" width="36" height="20"/>
<text x="26.0" y="360.0" text-anchor="middle">02</text>
<rect class="value" x="48" y="346" width="130" height="20"/>
<text class="value" x="113.0" y="360.0" text-anchor="middle">&quot;breakfast&quot;</text>
<rect class="attribute" x="182" y="346" width="130" height="20"/>
<text class="attribute" x="247.0" y="360.0" text-anchor="middle">grocery/tag</text>
<rect class="entity" x="316" y="346" width="110" height="20"/>
<text class="entity" x="371.0" y="360.0" text-anchor="middle">item:2</text>
</svg>
</figure>

Every fact about `item:2` is one contiguous run in EAV. Every `grocery/done` is one run in AEV. In VAE, `false` comes before every string, because Boolean is type `02` and String is type `03`. Among the strings, `"Eggs"` and `"Oat milk"` come before `"breakfast"`, because uppercase letters have smaller byte values than lowercase ones.

A query is mostly a matter of picking the index whose runs match the question, then reading one run.

## The value lives in the key

The value is part of every key, not stored beside it. That means a lookup by value in VAE finds the entity without reading anything else. It also means the tree never holds two entries for one fact in the same index: the same entity, attribute and value always give the same key.

Each key still has a small payload next to it. The payload holds no part of the fact, only bookkeeping: the version or versions that wrote the fact, and the fact's optional `cause` (see [Commits and History](./history.md)).

## Big values spill

A key holding a whole photo would make a terrible key. So a value whose encoded form is longer than 4,096 bytes spills. The value's place in the key keeps only the encoding of its first 64 bytes, and the key ends with a `01` byte and the 32-byte BLAKE3 hash of the whole value. In EAV and AEV that tail comes right after the value; in VAE it comes after the entity, the key's last part. The value itself is stored on its own, as a block named by that hash:

<figure class="dg">
<svg class="dg" viewBox="0 0 680 190" width="680" height="190" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="A spilled value: the key keeps a 64-byte prefix and the whole value's hash; the value is its own block">
<defs><marker id="spill-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="8" markerHeight="8" orient="auto"><path d="M0,0 L8,4 L0,8 z"/></marker></defs>
<text class="title" x="10" y="18">EAV key of a spilled value</text>
<rect class="shade" x="10" y="34" width="26" height="30"/>
<text x="23" y="54" text-anchor="middle">00</text>
<rect class="entity" x="38" y="34" width="90" height="30"/>
<text class="entity" x="83" y="54" text-anchor="middle">item:3</text>
<rect class="attribute" x="130" y="34" width="110" height="30"/>
<text class="attribute" x="185" y="54" text-anchor="middle">grocery/note</text>
<rect class="shade" x="242" y="34" width="26" height="30"/>
<text x="255" y="54" text-anchor="middle">03</text>
<rect class="value" x="270" y="34" width="180" height="30"/>
<text class="value" x="360" y="54" text-anchor="middle">first 64 bytes, 00</text>
<rect class="shade" x="452" y="34" width="26" height="30"/>
<text x="465" y="54" text-anchor="middle">01</text>
<rect class="hash" x="480" y="34" width="180" height="30"/>
<text class="hash" x="570" y="54" text-anchor="middle">blake3(value)</text>
<path class="wire" d="M270,70 v4 H450 v-4"/>
<text class="label small" x="360" y="88" text-anchor="middle">sorts by its first 64 bytes</text>
<path class="wire" d="M452,70 v4 H478 v-4"/>
<text class="label small" x="465" y="88" text-anchor="middle">spilled</text>
<path class="wire" d="M480,70 v4 H660 v-4"/>
<text class="label small" x="570" y="88" text-anchor="middle">32 bytes</text>
<path class="wire" d="M570,96 V140 H470" marker-end="url(#spill-arrow)"/>
<rect class="value" x="250" y="120" width="216" height="40"/>
<text class="value" x="358" y="138" text-anchor="middle">the whole value</text>
<text class="small muted" x="358" y="153" text-anchor="middle">its own block, found by its hash</text>
</svg>
</figure>

The prefix keeps a spilled value sorting by its first 64 bytes, so a range scan over strings still finds it in about the right neighborhood. The hash keeps two different long values from sharing a key. A reader tells the two shapes apart by what is left after the key's last part: nothing for an inline value, exactly 33 bytes for a spilled one.

The two numbers, 4,096 and 64, are not fixed forever. They belong to the tree's format, which every node carries, written out only where it differs from these defaults, as the [Nodes](./nodes.md) chapter shows. A reader that guessed them wrong would build different keys and miss facts, so they are among the few settings a reader must understand.

<div class="aside">

**Implementations.** Keys are built by `build_key` in [`dialog-artifacts/src/key/varkey.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/key/varkey.rs). The string and number encodings are in [`artifacts/ordkey.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/artifacts/ordkey.rs) and [`artifacts/ordvalue.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/artifacts/ordvalue.rs). Spilling is decided by `EncodedValue::new` in [`key.rs`](https://github.com/dialog-db/dialog-db/blob/main/rust/dialog-artifacts/src/key.rs).

</div>

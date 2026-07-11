import init, { Artifacts, generateEntity, encode, Entity, InstructionType, ValueDataType, Artifact, ArtifactApi } from "./dialog-artifacts";
import { assert, expect } from "@open-wc/testing";

await init();

interface HackerProfile {
    name: string,
    handle: string
}

describe('artifacts', () => {
    const populateWithHackers = async (artifacts: Artifacts): Promise<Map<string, HackerProfile>> => {
        const hackers = [
            {
                name: "Emmanuel Goldstein",
                handle: "Cereal Killer"
            },
            {
                name: "Paul Cook",
                handle: "Lord Nikon"
            },
            {
                name: "Dade Murphy",
                handle: "Zero Cool"
            },
            {
                name: "Kate Libby",
                handle: "Acid Burn"
            },
            {
                name: "Eugene Belford",
                handle: "The Plague"
            }
        ];
        const entityMap = new Map();

        for (const hacker of hackers) {
            // NOTE: We purposefully break this up into multiple transactions
            // to test that they compound in their effects
            let entity = generateEntity();

            entityMap.set(encode(entity), hacker);

            await artifacts.commit([
                {
                    type: InstructionType.Assert,
                    artifact: {
                        the: "profile/name",
                        of: entity,
                        is: {
                            type: ValueDataType.String,
                            value: hacker.name
                        }
                    }
                },
                {
                    type: InstructionType.Assert,
                    artifact: {
                        the: "profile/handle",
                        of: entity,
                        is: {
                            type: ValueDataType.String,
                            value: hacker.handle
                        }
                    }
                }
            ]);
        }

        return entityMap;
    }

    it('can restore from a revision', async () => {
        let artifacts = await Artifacts.anonymous();
        let entityMap = await populateWithHackers(artifacts);
        let identifier = await artifacts.identifier();

        let restored_artifacts = await Artifacts.open(identifier);

        let query = restored_artifacts.select({
            the: "profile/handle"
        });

        let count = 0;

        for await (const artifact of query) {
            let expectedHandle = entityMap.get(encode(artifact.of))?.handle;
            expect(expectedHandle).to.be.ok;
            expect(artifact.is.value).to.be.eq(expectedHandle!)
            count++;
        }

        expect(count).to.be.eq(5);
    });

    it('throws for invalid entities', async () => {
        let artifacts = await Artifacts.anonymous();

        await populateWithHackers(artifacts);

        let query;

        try {
            query = artifacts.select({
                of: new Uint8Array()
            });
        } catch (error) { expect(error).to.be.ok } finally {
            expect(query).to.be.undefined;
        }
    });

    it('can store an artifacts and select them again', async () => {
        let artifacts = await Artifacts.anonymous();
        let entityMap = await populateWithHackers(artifacts);

        let query = artifacts.select({
            the: "profile/handle"
        });

        let count = 0;

        for await (const artifact of query) {
            let expectedHandle = entityMap.get(encode(artifact.of))?.handle;
            expect(expectedHandle).to.be.ok;
            expect(artifact.is.value).to.be.eq(expectedHandle!)
            count++;
        }

        expect(count).to.be.eq(5);
    });

    it('can update an artifact and record a causal reference', async () => {
        let artifacts = await Artifacts.anonymous();
        let entityMap = await populateWithHackers(artifacts);

        let query = artifacts.select({
            the: "profile/handle",
            is: {
                type: ValueDataType.String,
                value: "Lord Nikon"
            }
        });


        let artifact;

        for await (const result of query) {
            artifact = result;
        }

        expect(artifact!.cause).to.be.undefined;

        artifact = artifact!.update({
            type: ValueDataType.String,
            value: "Godking Nikon"
        })!;

        expect(artifact.cause).to.be.ok;

        await artifacts.commit([
            {
                type: InstructionType.Assert,
                artifact
            }
        ]);

        query = artifacts.select({
            the: "profile/handle",
            is: {
                type: ValueDataType.String,
                value: "Godking Nikon"
            }
        });

        let descendantArtifact;

        for await (const result of query) {
            descendantArtifact = result;
        }

        expect(descendantArtifact!.cause).to.be.ok;
    });

    it('can use a query result multiple times', async () => {
        let artifacts = await Artifacts.anonymous();
        let entityMap = await populateWithHackers(artifacts);

        let query = artifacts.select({
            the: "profile/name"
        });

        let count = 0;

        for await (const artifact of query) {
            let expectedHandle = entityMap.get(encode(artifact.of))?.name;
            expect(expectedHandle).to.be.ok;
            expect(artifact.is.value).to.be.eq(expectedHandle!)

            count++;
        }

        expect(count).to.be.eq(5);

        for await (const artifact of query) {
            let expectedHandle = entityMap.get(encode(artifact.of))?.name;
            expect(expectedHandle).to.be.ok;
            expect(artifact.is.value).to.be.eq(expectedHandle!)

            count++;
        }

        expect(count).to.be.eq(10);

        for await (const _artifact of query) {
            for await (const _artifact of query) {
                count++;
            }
        }

        expect(count).to.be.eql(35);

        const otherQuery = artifacts.select({
            is: {
                type: ValueDataType.String,
                value: "Acid Burn"
            }
        });

        for await (const _artifact of query) {
            for await (const _artifact of otherQuery) {
                count++;
            }
        }

        expect(count).to.be.eql(40);
    });

    it('pins an iterator at the version where iteration began', async () => {
        let artifacts = await Artifacts.anonymous();
        let entityMap = await populateWithHackers(artifacts);

        let query = artifacts.select({
            the: "profile/name"
        });

        let count = 0;

        for await (const artifact of query) {
            await populateWithHackers(artifacts);

            let expectedHandle = entityMap.get(encode(artifact.of))?.name;
            expect(expectedHandle).to.be.ok;
            expect(artifact.is.value).to.be.eq(expectedHandle!)

            count++;
        }

        expect(count).to.be.eql(5);
    });

    it('gives a 32-byte hash as the revision', async () => {
        let artifacts = await Artifacts.anonymous();
        await populateWithHackers(artifacts);

        let revision = await artifacts.revision();

        expect(revision.length).to.be.eql(32);
    });

    it('round-trips a record value as naked bytes over the JS boundary', async () => {
        // A record value carries an opaque, format-native byte payload (e.g.
        // an automerge document's `save()` output). It must cross the wasm
        // boundary as a naked `Uint8Array` in both directions — no envelope,
        // no tag byte in the payload, byte-identical — distinguished from a
        // plain `Bytes` value only by the `Record` type tag. This is the
        // invariant the automerge-as-record-value integration depends on.
        let artifacts = await Artifacts.anonymous();
        let entity = generateEntity();

        // Bytes chosen to include a leading zero and a byte equal to the
        // Record tag (7), so any accidental prefixing/stripping would corrupt
        // the round trip.
        let document = new Uint8Array([0x00, 0x07, 0x85, 0x6f, 0x4a, 0x83, 0xff]);

        await artifacts.commit([
            {
                type: InstructionType.Assert,
                artifact: {
                    the: 'note/body',
                    of: entity,
                    is: {
                        type: ValueDataType.Record,
                        value: document
                    }
                }
            }
        ]);

        let query = artifacts.select({ the: 'note/body' });

        let results: Artifact[] = [];
        for await (const artifact of query) {
            results.push(artifact);
        }

        expect(results.length).to.be.eq(1);

        let value = results[0].is;
        expect(value.type).to.be.eq(ValueDataType.Record);
        // A record must never be confused with a plain byte buffer.
        expect(value.type).to.not.be.eq(ValueDataType.Bytes);
        expect(value.value).to.be.instanceOf(Uint8Array);

        let bytes = value.value as Uint8Array;
        expect(Array.from(bytes)).to.be.eql(Array.from(document));
    });

    it('keeps record and bytes values distinct across the boundary', async () => {
        // The same payload stored once as `Bytes` and once as `Record` must
        // read back with its distinguishing type tag intact, proving the tag
        // — not the payload — carries the format distinction.
        let artifacts = await Artifacts.anonymous();
        let payload = new Uint8Array([0x01, 0x02, 0x03, 0x04]);

        let bytesEntity = generateEntity();
        let recordEntity = generateEntity();

        await artifacts.commit([
            {
                type: InstructionType.Assert,
                artifact: {
                    the: 'blob/data',
                    of: bytesEntity,
                    is: { type: ValueDataType.Bytes, value: payload }
                }
            },
            {
                type: InstructionType.Assert,
                artifact: {
                    the: 'blob/data',
                    of: recordEntity,
                    is: { type: ValueDataType.Record, value: payload }
                }
            }
        ]);

        let byTag = new Map<ValueDataType, Uint8Array>();
        for await (const artifact of artifacts.select({ the: 'blob/data' })) {
            byTag.set(artifact.is.type, artifact.is.value as Uint8Array);
        }

        expect(byTag.has(ValueDataType.Bytes)).to.be.true;
        expect(byTag.has(ValueDataType.Record)).to.be.true;
        expect(Array.from(byTag.get(ValueDataType.Bytes)!)).to.be.eql(Array.from(payload));
        expect(Array.from(byTag.get(ValueDataType.Record)!)).to.be.eql(Array.from(payload));
    });

    it('can reset to an earlier revision', async () => {
        let artifacts = await Artifacts.anonymous();
        let entityMap = await populateWithHackers(artifacts);

        let revision = await artifacts.revision();

        await populateWithHackers(artifacts);

        await artifacts.reset(revision);

        let query = artifacts.select({
            the: "profile/name"
        });

        let count = 0;

        for await (const artifact of query) {
            count++;
            expect(entityMap.has(encode(artifact.of))).to.be.true;
        }

        expect(count).to.be.eql(5);
    });

});
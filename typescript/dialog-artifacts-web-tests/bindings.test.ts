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

});
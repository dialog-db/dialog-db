import init, { Artifacts, generateEntity, encode, Entity, InstructionType, ValueDataType } from "dialog-artifacts";
import { expect } from "@open-wc/testing";

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
        let artifacts = await Artifacts.open("test");
        let entityMap = await populateWithHackers(artifacts);
        let revision = await artifacts.revision();

        let restored_artifacts = await Artifacts.open("test", revision);

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

    it('can store an artifacts and select them again', async () => {
        let artifacts = await Artifacts.open("test");
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

    it('can use a query result multiple times', async () => {
        let artifacts = await Artifacts.open("test");
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
    });
});
import { Session, Query, fact, Task } from "./self.js";
import { assert } from "@open-wc/testing";
import { alice, bob } from "./constants.js";

describe("experimental", () => {
	it("imports the module", () => {
		assert.equal(typeof Session, "object");
		assert.equal(typeof Query, "object");
	});

	it("can perform basic transactions", async () =>
		Task.spawn(function* () {
			const db = Session.open(alice);
			try {
				const Counter = fact({
					name: String,
					value: Number,
				});

				yield* db.transact([Counter.assert({ name: "test", value: 0 })]);

				const results = yield* Counter().query({ from: db });

				assert.deepEqual(results, [Counter.assert({ name: "test", value: 0 })]);

				yield* db.transact([Counter.assert({ name: "test", value: 5 })]);

				assert.deepEqual(
					yield* Counter().query({ from: db }),
					[
						Counter.assert({ name: "test", value: 0 }),
						Counter.assert({ name: "test", value: 5 }),
					],
					"returns both facts",
				);

				yield* db.transact([
					Counter.assert({ name: "test", value: 0 }).retract(),
				]);

				assert.deepEqual(
					yield* Counter().query({ from: db }),
					[Counter.assert({ name: "test", value: 5 })],
					"one fact was retracted",
				);
			} finally {
				yield* db.clear();
			}
		}));

	it("changes propagate across sessions", async () => {
		const db = await Session.open(alice);
		try {
			const Counter = fact({
				name: String,
				value: Number,
			});
			await db.transact([Counter.assert({ name: "test", value: 10 })]);
			const session = await Session.open(alice);
			assert.deepEqual(
				await Counter().query({ from: session }),
				[Counter.assert({ name: "test", value: 10 })],
				"new session picks up where last lefts off",
			);

			await session.transact([
				Counter.assert({ name: "test", value: 10 }).retract(),
				Counter.assert({ name: "test", value: 15 }),
			]);
			assert.deepEqual(
				await Counter().query({ from: session }),
				[Counter.assert({ name: "test", value: 15 })],
				"transacted state",
			);

			await Task.perform(Task.sleep(100));
			assert.deepEqual(
				await Counter().query({ from: db }),
				[Counter.assert({ name: "test", value: 15 })],
				"change propagated",
			);
		} finally {
			await db.clear();
		}
	});
});

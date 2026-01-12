import { esbuildPlugin } from "@web/dev-server-esbuild";

export default {
	plugins: [esbuildPlugin({ ts: true })],
	testsFinishTimeout: 10000,
	testFramework: {
		config: {
			ui: "bdd",
			timeout: "10000",
		},
	},
};

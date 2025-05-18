import { vitePlugin } from '@remcovaes/web-test-runner-vite-plugin'
import reactRefresh from '@vitejs/plugin-react-refresh'
export default {
  plugins: [
    vitePlugin({
      plugins: [reactRefresh()],
    }),
  ],
  testsFinishTimeout: 10000,
}

import { vitePlugin } from '@remcovaes/web-test-runner-vite-plugin'
import react from '@vitejs/plugin-react'

export default {
  plugins: [
    vitePlugin({
      plugins: [react()],
    }),
  ],
  testsFinishTimeout: 10000,
}

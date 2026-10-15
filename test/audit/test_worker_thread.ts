import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';

if (isMainThread) {
  const worker = new Worker(new URL(import.meta.url), {
    execArgv: ['--experimental-strip-types'],
    workerData: { test: 123 },
  });
  worker.on('message', (msg) => {
    console.log('Main thread received:', msg);
    worker.terminate().then(() => process.exit(0));
  });
} else {
  parentPort?.postMessage({ status: 'ready', data: workerData });
}

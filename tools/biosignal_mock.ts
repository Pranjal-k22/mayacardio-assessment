import * as net from 'node:net';
import { crc32 } from 'node:zlib';
import * as crypto from 'node:crypto';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';

export interface MockOptions {
  mode: 'signal' | 'ramp';
  seed: number;
  duration: number; // in seconds
  port: number;
  host?: string;
  bufferSeconds?: number;
}

export interface InjectedFaults {
  corruptNextCrc?: boolean;
  duplicateNextReply?: boolean;
  dropNextReply?: boolean;
  delayMs?: number;
  injectLoss?: { sensor: number; from_index: number; to_index: number; reason: string } | null;
  forceRestart?: boolean;
}

export class BiosignalMockServer {
  private server: net.Server | null = null;
  private activeSocket: net.Socket | null = null;
  private sessionId: string;
  private startTime: number = 0;
  private startMonotonicMs: number = 0;
  private isGenerationFrozen: boolean = false;
  private options: Required<MockOptions>;
  private faults: InjectedFaults = {};
  private timer: NodeJS.Timeout | null = null;

  // Ring buffers for last 10 seconds: 4000 samples per sensor
  private bufferCapacity: number;
  private sensor1Buffer: number[][] = []; // [ch1, ch2, ch3][]
  private sensor2Buffer: number[][] = []; // [ch1, ch2][]
  private firstBufferedIndex1: number = 0;
  private firstBufferedIndex2: number = 0;
  private totalGeneratedSamples: number = 0;
  private totalServedSamples: { 1: number; 2: number } = { 1: 0, 2: 0 };

  // Track unread tail on restart
  public lastSessionEndReported: boolean = false;

  constructor(opts: MockOptions) {
    this.options = {
      mode: opts.mode || 'signal',
      seed: opts.seed || 7,
      duration: opts.duration || 3600,
      port: opts.port || 7100,
      host: opts.host || '127.0.0.1',
      bufferSeconds: opts.bufferSeconds || 10,
    };
    this.bufferCapacity = this.options.bufferSeconds * 400;
    this.sessionId = this.generateSessionId();
  }

  private generateSessionId(): string {
    return crypto.randomBytes(4).toString('hex');
  }

  public setFaults(faults: InjectedFaults) {
    this.faults = { ...this.faults, ...faults };
  }

  public restartSession() {
    this.sessionId = this.generateSessionId();
    this.sensor1Buffer = [];
    this.sensor2Buffer = [];
    this.firstBufferedIndex1 = 0;
    this.firstBufferedIndex2 = 0;
    this.totalGeneratedSamples = 0;
    this.totalServedSamples = { 1: 0, 2: 0 };
    this.startTime = Date.now();
    this.startMonotonicMs = performance.now();
    this.isGenerationFrozen = false;
  }

  public getSessionId(): string {
    return this.sessionId;
  }

  public getTotalGeneratedSamples(): number {
    return this.totalGeneratedSamples;
  }

  public getTotalServedSamples(): { 1: number; 2: number } {
    return { ...this.totalServedSamples };
  }

  public getStartMonotonicMs(): number {
    return this.startMonotonicMs;
  }

  public getStartTime(): number {
    return this.startTime;
  }

  public stopGeneration(): number {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.generateDueSamples();
    this.isGenerationFrozen = true; // Freeze generation so handleCommand won't generate more
    return this.totalGeneratedSamples;
  }

  public generateDueSamples() {
    if (this.isGenerationFrozen || this.startMonotonicMs === 0) return;
    const elapsedSec = Math.max(0, (performance.now() - this.startMonotonicMs) / 1000);
    const maxSamples = this.options.duration * 400;
    const targetCount = Math.min(maxSamples, Math.floor(elapsedSec * 400));
    const dueCount = targetCount - this.totalGeneratedSamples;
    if (dueCount > 0) {
      this.generateSamples(dueCount);
    }
  }

  public start(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.startTime = Date.now();
      this.startMonotonicMs = performance.now();
      this.isGenerationFrozen = false;

      // Generation loop: monotonic deadline-based scheduling at sustained 400 samples/sec
      // Fired periodically to ensure ring buffer overflows if client is idle,
      // without accumulating recurring timer drift.
      const tickMs = 20;
      this.timer = setInterval(() => {
        this.generateDueSamples();
      }, tickMs);

      this.server = net.createServer((socket) => {
        if (this.activeSocket && !this.activeSocket.destroyed) {
          // Assessment: one client at a time
          socket.write(JSON.stringify({ error: 'busy_only_one_client_allowed' }) + '\n');
          socket.end();
          return;
        }

        this.activeSocket = socket;
        let lineBuffer = '';

        socket.on('data', (chunk) => {
          lineBuffer += chunk.toString('utf-8');
          let newlineIdx: number;
          while ((newlineIdx = lineBuffer.indexOf('\n')) !== -1) {
            const line = lineBuffer.slice(0, newlineIdx).trim();
            lineBuffer = lineBuffer.slice(newlineIdx + 1);
            if (line.length > 0) {
              this.handleCommand(line, socket);
            }
          }
        });

        socket.on('close', () => {
          if (this.activeSocket === socket) {
            this.activeSocket = null;
          }
        });

        socket.on('error', () => {
          // Ignore socket errors on client disconnect
        });
      });

      this.server.listen(this.options.port, this.options.host, () => {
        resolve();
      });

      this.server.on('error', reject);
    });
  }

  public stop(): Promise<void> {
    return new Promise((resolve) => {
      if (this.timer) {
        clearInterval(this.timer);
        this.timer = null;
      }
      this.isGenerationFrozen = true;
      if (this.activeSocket) {
        this.activeSocket.destroy();
        this.activeSocket = null;
      }
      if (this.server) {
        this.server.close(() => resolve());
      } else {
        resolve();
      }
    });
  }

  public generateSamples(count: number) {
    for (let k = 0; k < count; k++) {
      const i = this.totalGeneratedSamples;
      const [s1, s2] = this.computeSample(i);
      this.sensor1Buffer.push(s1);
      this.sensor2Buffer.push(s2);
      this.totalGeneratedSamples++;

      // Enforce 10-second ring buffer capacity
      if (this.sensor1Buffer.length > this.bufferCapacity) {
        this.sensor1Buffer.shift();
        this.firstBufferedIndex1++;
      }
      if (this.sensor2Buffer.length > this.bufferCapacity) {
        this.sensor2Buffer.shift();
        this.firstBufferedIndex2++;
      }
    }
  }

  // Exact math according to Section 6.1
  public computeSample(i: number): [number[], number[]] {
    if (this.options.mode === 'ramp') {
      // Sensor 1 channel c at sample i = (3i + c) mod 8,388,608
      const s1 = [
        (3 * i + 0) % 8388608,
        (3 * i + 1) % 8388608,
        (3 * i + 2) % 8388608,
      ];
      // Sensor 2 channel c = (2i + c) mod 8,388,608
      const s2 = [
        (2 * i + 0) % 8388608,
        (2 * i + 1) % 8388608,
      ];
      return [s1, s2];
    } else {
      // --mode signal
      // Heart rate cycle: 60 bpm for min 0-2, 90 bpm for min 2-4, 72 bpm for min 4-6, repeating (6 min cycle)
      const second = Math.floor(i / 400);
      const minute = Math.floor(second / 60) % 6;
      let bpm = 60;
      if (minute >= 2 && minute < 4) {
        bpm = 90;
      } else if (minute >= 4 && minute < 6) {
        bpm = 72;
      }

      // ECG sample generation: sharp QRS peak once per beat
      const samplesPerBeat = (400 * 60) / bpm;
      const phase = (i % Math.round(samplesPerBeat));
      
      let ecgCh2: number;
      if (phase === 0) {
        ecgCh2 = 2500000; // sharp R-peak
      } else if (phase === 1 || phase === Math.round(samplesPerBeat) - 1) {
        ecgCh2 = -200000; // S/Q dip
      } else if (phase > 20 && phase < 60) {
        ecgCh2 = 300000; // T-wave
      } else {
        ecgCh2 = 10000 + Math.floor(Math.sin(i * 0.05) * 5000); // baseline noise
      }

      const s1 = [
        Math.round(ecgCh2 * 0.6),
        ecgCh2,
        Math.round(ecgCh2 * 1.3),
      ];

      // Sensor 2 has channel 1 near 400,000 and channel 2 near 150,000, slowly varying
      const s2 = [
        400000 + Math.floor(Math.sin(i * 0.001) * 10000),
        150000 + Math.floor(Math.cos(i * 0.001) * 5000),
      ];
      return [s1, s2];
    }
  }

  private handleCommand(line: string, socket: net.Socket) {
    if (this.faults.forceRestart) {
      this.faults.forceRestart = false;
      this.restartSession();
    }

    // Advance generation to monotonic deadline of incoming command
    this.generateDueSamples();

    let cmd: any;
    try {
      cmd = JSON.parse(line);
    } catch {
      socket.write(JSON.stringify({ error: 'invalid_json' }) + '\n');
      return;
    }

    if (cmd.cmd !== 'read' || (cmd.sensor !== 1 && cmd.sensor !== 2)) {
      socket.write(JSON.stringify({ error: 'unknown_command_or_sensor' }) + '\n');
      return;
    }

    const sensor = cmd.sensor as 1 | 2;
    const reqIndex: number | undefined = cmd.from_index;

    const action = () => {
      this.sendReadResponse(sensor, reqIndex, socket);
    };

    if (this.faults.delayMs && this.faults.delayMs > 0) {
      const d = this.faults.delayMs;
      this.faults.delayMs = 0;
      setTimeout(action, d);
    } else {
      action();
    }
  }

  private sendReadResponse(sensor: 1 | 2, fromIndex: number | undefined, socket: net.Socket) {
    if (this.faults.dropNextReply) {
      this.faults.dropNextReply = false;
      return; // Do not answer
    }

    const buffer = sensor === 1 ? this.sensor1Buffer : this.sensor2Buffer;
    const firstBuffered = sensor === 1 ? this.firstBufferedIndex1 : this.firstBufferedIndex2;
    const currentMaxIndex = firstBuffered + buffer.length;

    let targetIndex = fromIndex !== undefined ? fromIndex : firstBuffered;
    const lostRanges: Array<{ from_index: number; to_index: number; reason: string }> = [];

    // Check if requested data is behind the ring buffer (buffer overflow)
    if (targetIndex < firstBuffered) {
      lostRanges.push({
        from_index: targetIndex,
        to_index: firstBuffered - 1,
        reason: 'buffer_overflow',
      });
      targetIndex = firstBuffered;
    }

    if (this.faults.injectLoss && this.faults.injectLoss.sensor === sensor) {
      lostRanges.push({
        from_index: this.faults.injectLoss.from_index,
        to_index: this.faults.injectLoss.to_index,
        reason: this.faults.injectLoss.reason,
      });
      targetIndex = Math.max(targetIndex, this.faults.injectLoss.to_index + 1);
      this.faults.injectLoss = null;
    }

    // Deliver at most 1 second of data (400 samples)
    const availableSamples = currentMaxIndex - targetIndex;
    const countToSend = Math.min(400, Math.max(0, availableSamples));

    const offsetInBuffer = targetIndex - firstBuffered;
    const dataToSend = buffer.slice(offsetInBuffer, offsetInBuffer + countToSend);

    const sessionEnd = this.totalGeneratedSamples >= this.options.duration * 400;

    // Serialize data array exactly to compute CRC32
    const dataText = JSON.stringify(dataToSend);
    let crcHex = crc32(Buffer.from(dataText, 'utf-8')).toString(16).padStart(8, '0');

    if (this.faults.corruptNextCrc) {
      this.faults.corruptNextCrc = false;
      crcHex = 'deadbeef'; // intentionally corrupt CRC
    }

    const replyObj = {
      session_id: this.sessionId,
      sensor: sensor,
      first_index: targetIndex,
      count: countToSend,
      lost: lostRanges,
      session_end: sessionEnd,
      crc32: crcHex,
    };

    // Construct the NDJSON line: ensure `data` matches dataText exactly
    const replyLine = JSON.stringify(replyObj).slice(0, -1) + `,"data":${dataText}}\n`;

    socket.write(replyLine);
    this.totalServedSamples[sensor] += countToSend;

    if (this.faults.duplicateNextReply) {
      this.faults.duplicateNextReply = false;
      // Send the exact same reply twice immediately
      socket.write(replyLine);
    }
  }
}

// Execute when run directly as CLI
const isCli = Boolean(
  process.argv[1] &&
  (path.resolve(process.argv[1]) === fileURLToPath(import.meta.url) ||
   import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))
);

if (isCli) {
  const args = process.argv.slice(2);
  let mode: 'signal' | 'ramp' = 'signal';
  let seed = 7;
  let duration = 3600;
  let port = 7100;
  let host = '127.0.0.1';

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--mode' && args[i + 1]) {
      mode = args[++i] as 'signal' | 'ramp';
    } else if (args[i] === '--seed' && args[i + 1]) {
      seed = parseInt(args[++i], 10);
    } else if (args[i] === '--duration' && args[i + 1]) {
      duration = parseInt(args[++i], 10);
    } else if (args[i] === '--port' && args[i + 1]) {
      port = parseInt(args[++i], 10);
    } else if (args[i] === '--host' && args[i + 1]) {
      host = args[++i];
    }
  }

  const server = new BiosignalMockServer({ mode, seed, duration, port, host });
  server.start().then(() => {
    console.log(`[MOCK] Biosignal Mock Server listening on ${host}:${port} (session: ${server.getSessionId()}, mode: ${mode}, duration: ${duration}s)`);
  }).catch((err) => {
    console.error('[MOCK] Failed to start mock server:', err);
    process.exit(1);
  });
}

/**
 * Automated API & Integration Test Suite for Latency Optimizer
 * Framework: Jest / Node.js
 * Focus: HTTP API Contracts, Real-Time Market Data Endpoints, SSE Streams, and Concurrency
 */

const { spawn } = require('child_process');
const http = require('http');

const TEST_PORT = process.env.TEST_PORT || 8976;
const BASE_URL = `http://127.0.0.1:${TEST_PORT}`;

let serverProcess = null;

// Helper to poll until Go server is accepting HTTP connections
function waitForServerReady(url, maxAttempts = 30, intervalMs = 200) {
  return new Promise((resolve, reject) => {
    let attempts = 0;
    const check = () => {
      attempts++;
      const req = http.get(`${url}/api/orderbook`, (res) => {
        res.resume(); // consume data
        if (res.statusCode === 200) {
          resolve();
        } else {
          retry();
        }
      });
      req.on('error', () => {
        retry();
      });
    };

    const retry = () => {
      if (attempts >= maxAttempts) {
        reject(new Error(`Server failed to start at ${url} after ${maxAttempts} attempts`));
      } else {
        setTimeout(check, intervalMs);
      }
    };

    check();
  });
}

const fs = require('fs');
const path = require('path');

beforeAll(async () => {
  const binaryPath = path.join(__dirname, '..', 'server_test_bin');
  if (!fs.existsSync(binaryPath)) {
    throw new Error('server_test_bin not found – CI must build it first');
  }

  serverProcess = spawn(binaryPath, [], {
    env: { ...process.env, PORT: String(TEST_PORT) },
    stdio: 'ignore'
  });

  await waitForServerReady(BASE_URL);
}, 45000); // give it a bit more time in CI

afterAll(async () => {
  if (serverProcess) {
    serverProcess.kill('SIGTERM');
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
});

describe('Latency Optimizer REST & SSE API Integration Suite', () => {

  test('1. Health Check & Static Dashboard Serving [GET /]', async () => {
    const res = await fetch(`${BASE_URL}/`);
    expect(res.status).toBe(200);
    const text = await res.text();
    expect(text).toContain('Producer-Consumer Architectures in the Go Runtime');
  });

  test('2. Level 2 Order Book State & Depth [GET /api/orderbook]', async () => {
    const res = await fetch(`${BASE_URL}/api/orderbook`);
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toContain('application/json');

    const data = await res.json();
    expect(data).toHaveProperty('orderBook');
    expect(data).toHaveProperty('midPrice');
    expect(data).toHaveProperty('trades');
    expect(data).toHaveProperty('bot');

    const { orderBook, bot } = data;
    expect(Array.isArray(orderBook.topBids)).toBe(true);
    expect(Array.isArray(orderBook.topAsks)).toBe(true);
    expect(typeof orderBook.spread).toBe('number');
    expect(typeof orderBook.obi).toBe('number');
    expect(orderBook.obi).toBeGreaterThanOrEqual(-1.0);
    expect(orderBook.obi).toBeLessThanOrEqual(1.0);

    // Bot portfolio state validation
    expect(typeof bot.cash).toBe('number');
    expect(typeof bot.nav).toBe('number');
    expect(bot.cash).toBeGreaterThan(0);
  });

  test('3. Disruptor Ring Buffer Perimeter & Inspection Slots [GET /api/ring-buffer]', async () => {
    const res = await fetch(`${BASE_URL}/api/ring-buffer`);
    expect(res.status).toBe(200);

    const data = await res.json();
    expect(data).toHaveProperty('writeSeq');
    expect(data).toHaveProperty('botSeq');
    expect(data).toHaveProperty('aiSeq');
    expect(data).toHaveProperty('auditSeq');
    expect(data).toHaveProperty('slots');
    expect(data).toHaveProperty('traces');

    expect(typeof data.writeSeq).toBe('number');
    expect(Array.isArray(data.slots)).toBe(true);
    expect(data.slots.length).toBe(32);

    // Validate circular slot data contract
    const firstSlot = data.slots[0];
    expect(firstSlot).toHaveProperty('index');
    expect(firstSlot).toHaveProperty('tradeId');
    expect(firstSlot).toHaveProperty('price');
    expect(firstSlot).toHaveProperty('side');
    expect(firstSlot).toHaveProperty('venue');
    expect(firstSlot).toHaveProperty('isActive');

    expect(Array.isArray(data.traces)).toBe(true);
  });

  test('4. Quantitative Sentiment Indicators [GET /api/sentiment]', async () => {
    const res = await fetch(`${BASE_URL}/api/sentiment`);
    expect(res.status).toBe(200);

    const data = await res.json();
    expect(data).toHaveProperty('vwap');
    expect(data).toHaveProperty('rsi');
    expect(data).toHaveProperty('ofi');
    expect(data).toHaveProperty('lastUpdated');

    expect(typeof data.vwap).toBe('number');
    expect(typeof data.rsi).toBe('number');
    expect(typeof data.ofi).toBe('number');
  });

  test('5. SSE Benchmark Execution Streaming [GET /api/run-experiment]', async () => {
    const res = await fetch(`${BASE_URL}/api/run-experiment?trades=100&subscribers=10`);
    expect(res.status).toBe(200);
    expect(res.headers.get('content-type')).toContain('text/event-stream');

    const body = await res.text();
    expect(body).toContain('data:');
    expect(body).toContain('[DONE]');
  }, 10000);

  test('6. High-Concurrency Multi-Client Read Simulation', async () => {
    const clientRequests = [];
    const concurrentClients = 10;

    for (let i = 0; i < concurrentClients; i++) {
      const endpoint = i % 2 === 0 ? '/api/orderbook' : '/api/ring-buffer';
      clientRequests.push(
        fetch(`${BASE_URL}${endpoint}`).then(async (res) => {
          expect(res.status).toBe(200);
          return res.json();
        })
      );
    }

    const results = await Promise.all(clientRequests);
    expect(results.length).toBe(concurrentClients);
    for (const payload of results) {
      expect(payload).toBeDefined();
    }
  });

});

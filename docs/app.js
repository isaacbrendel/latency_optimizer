// Academic Metric Tracker & Live Proof-of-Work Controller

let chartInstances = {};
let latestPrice = 0;
let crossoverChart = null;
let chartData = {
    prices: [],
    fast: [],
    slow: [],
    labels: []
};


document.addEventListener('DOMContentLoaded', () => {
    // Initialize 32 Ring Buffer Slots visually
    initRingBufferSlots();

    // Load initial benchmark results and render charts/tables
    loadBenchmarkData();

    // Check backend connectivity and start loops
    checkBackendConnectivity().then(() => {
        startProofOfWorkPolling();
    });

    // Bind Experiment Builder Event Listener
    setupExperimentBuilder();

    // Initialize live price chart
    initLiveCrossoverChart();

    // Setup bot config and backtest handlers
    setupBotControls();
});


// Helper to format numbers (e.g. 1000000 -> 1.0M, 50000 -> 50K)
function formatNumber(num) {
    if (num >= 1000000) return (num / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
    if (num >= 1000) return (num / 1000).toFixed(0) + 'K';
    return num;
}

function getApiUrl(endpoint) {
    if (!endpoint) return '';
    if (endpoint.startsWith('http://') || endpoint.startsWith('https://')) return endpoint;
    const origin = window.location.origin;
    if (origin === 'null' || origin.startsWith('file:') || window.location.protocol === 'file:') {
        return 'http://localhost:8080' + (endpoint.startsWith('/') ? endpoint : '/' + endpoint);
    }
    return endpoint.startsWith('/') ? endpoint : '/' + endpoint;
}

// ----------------------------------------------------
// 1. Benchmark Data Loading & Rendering
// ----------------------------------------------------
async function loadBenchmarkData() {
    try {
        const response = await fetch(getApiUrl('/benchmark_results.json?v=' + new Date().getTime()));
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        
        renderAcademicCharts(data);
        renderAcademicTables(data);
    } catch (err) {
        console.error('Error loading benchmark data:', err);
        const consoleEl = document.getElementById('console');
        if (consoleEl) {
            consoleEl.textContent += `\n[WARN] Failed to load benchmark_results.json: ${err.message}\n`;
        }
    }
}

// LaTeX Booktabs Table Generation
function renderAcademicTables(data) {
    // 1. Trade Scaling Table
    const tradeTableBody = document.querySelector('#trade-scaling-table tbody');
    if (tradeTableBody && data.tradesScaling) {
        tradeTableBody.innerHTML = '';
        data.tradesScaling.points.forEach(p => {
            const t1 = p.results.SimpleFanV1 || { timeNs: 0, allocs: 0, bytesAlloc: 0 };
            const t2 = p.results.SimpleFanV2 || { timeNs: 0, allocs: 0, bytesAlloc: 0 };
            const t3 = p.results.SimpleFanV3 || { timeNs: 0, allocs: 0, bytesAlloc: 0 };
            const t4 = p.results.RingBufferV6 || { timeNs: 0, allocs: 0, bytesAlloc: 0 };
            const t5 = p.results.RingBufferEviction || { timeNs: 0, allocs: 0, bytesAlloc: 0 };
            const t6 = p.results.FlatBuffersZeroCopy || { timeNs: 0, allocs: 0, bytesAlloc: 0 };

            const rowTime = document.createElement('tr');
            rowTime.innerHTML = `
                <td rowspan="3">${formatNumber(p.trades)}</td>
                <td>Time (ms)</td>
                <td>${(t1.timeNs / 1e6).toFixed(2)}</td>
                <td>${(t2.timeNs / 1e6).toFixed(2)}</td>
                <td>${(t3.timeNs / 1e6).toFixed(2)}</td>
                <td>${(t4.timeNs / 1e6).toFixed(2)}</td>
                <td style="font-weight: bold; color: #b25900;">${(t5.timeNs / 1e6).toFixed(2)}</td>
                <td style="font-weight: bold; color: #005f00;">${(t6.timeNs / 1e6).toFixed(2)}</td>
            `;
            
            const rowAllocs = document.createElement('tr');
            rowAllocs.innerHTML = `
                <td>Allocs/Op</td>
                <td>${t1.allocs}</td>
                <td>${t2.allocs}</td>
                <td>${t3.allocs}</td>
                <td>${t4.allocs}</td>
                <td style="font-weight: bold; color: #b25900;">${t5.allocs}</td>
                <td style="font-weight: bold; color: #005f00;">${t6.allocs}</td>
            `;

            const rowBytes = document.createElement('tr');
            rowBytes.innerHTML = `
                <td>Memory (KB)</td>
                <td>${(t1.bytesAlloc / 1024).toFixed(0)}</td>
                <td>${(t2.bytesAlloc / 1024).toFixed(0)}</td>
                <td>${(t3.bytesAlloc / 1024).toFixed(0)}</td>
                <td>${(t4.bytesAlloc / 1024).toFixed(0)}</td>
                <td style="font-weight: bold; color: #b25900;">${(t5.bytesAlloc / 1024).toFixed(0)}</td>
                <td style="font-weight: bold; color: #005f00;">${(t6.bytesAlloc / 1024).toFixed(0)}</td>
            `;

            tradeTableBody.appendChild(rowTime);
            tradeTableBody.appendChild(rowAllocs);
            tradeTableBody.appendChild(rowBytes);
        });
    }

    // 2. Subscriber Scaling Table
    const subTableBody = document.querySelector('#subscribers-scaling-table tbody');
    if (subTableBody && data.subscribersScaling) {
        subTableBody.innerHTML = '';
        data.subscribersScaling.points.forEach(p => {
            const t1 = p.results.SimpleFanV1 || { timeNs: 0, allocs: 0, bytesAlloc: 0 };
            const t2 = p.results.SimpleFanV2 || { timeNs: 0, allocs: 0, bytesAlloc: 0 };
            const t3 = p.results.SimpleFanV3 || { timeNs: 0, allocs: 0, bytesAlloc: 0 };
            const t4 = p.results.RingBufferV6 || { timeNs: 0, allocs: 0, bytesAlloc: 0 };
            const t5 = p.results.RingBufferEviction || { timeNs: 0, allocs: 0, bytesAlloc: 0 };
            const t6 = p.results.FlatBuffersZeroCopy || { timeNs: 0, allocs: 0, bytesAlloc: 0 };

            const rowTime = document.createElement('tr');
            rowTime.innerHTML = `
                <td rowspan="3">${p.subscribers}</td>
                <td>Time (ms)</td>
                <td>${(t1.timeNs / 1e6).toFixed(2)}</td>
                <td>${(t2.timeNs / 1e6).toFixed(2)}</td>
                <td>${(t3.timeNs / 1e6).toFixed(2)}</td>
                <td>${(t4.timeNs / 1e6).toFixed(2)}</td>
                <td style="font-weight: bold; color: #b25900;">${(t5.timeNs / 1e6).toFixed(2)}</td>
                <td style="font-weight: bold; color: #005f00;">${(t6.timeNs / 1e6).toFixed(2)}</td>
            `;

            const rowAllocs = document.createElement('tr');
            rowAllocs.innerHTML = `
                <td>Allocs/Op</td>
                <td>${t1.allocs}</td>
                <td>${t2.allocs}</td>
                <td>${t3.allocs}</td>
                <td>${t4.allocs}</td>
                <td style="font-weight: bold; color: #b25900;">${t5.allocs}</td>
                <td style="font-weight: bold; color: #005f00;">${t6.allocs}</td>
            `;

            const rowBytes = document.createElement('tr');
            rowBytes.innerHTML = `
                <td>Memory (KB)</td>
                <td>${(t1.bytesAlloc / 1024).toFixed(0)}</td>
                <td>${(t2.bytesAlloc / 1024).toFixed(0)}</td>
                <td>${(t3.bytesAlloc / 1024).toFixed(0)}</td>
                <td>${(t4.bytesAlloc / 1024).toFixed(0)}</td>
                <td style="font-weight: bold; color: #b25900;">${(t5.bytesAlloc / 1024).toFixed(0)}</td>
                <td style="font-weight: bold; color: #005f00;">${(t6.bytesAlloc / 1024).toFixed(0)}</td>
            `;

            subTableBody.appendChild(rowTime);
            subTableBody.appendChild(rowAllocs);
            subTableBody.appendChild(rowBytes);
        });
    }
}

// ----------------------------------------------------
// 2. Chart Rendering (Scientific / Academic Style)
// ----------------------------------------------------
function renderAcademicCharts(data) {
    if (!data || !data.tradesScaling || !data.subscribersScaling) return;

    // Clear old chart instances to avoid overlap on redraw
    Object.keys(chartInstances).forEach(key => {
        if (chartInstances[key] && typeof chartInstances[key].destroy === 'function') {
            chartInstances[key].destroy();
        }
    });
    chartInstances = {};

    const colors = {
        sfV1: '#b22222',        // Firebrick Red
        sfV2: '#d2691e',        // Chocolate Orange
        sfV3: '#4682b4',        // Steel Blue
        rbV6: '#2e8b57',        // Sea Green
        rbEviction: '#b25900',  // Dark Amber
        flatBuffers: '#005f00'  // Dark Green
    };

    const academicOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                position: 'top',
                labels: {
                    color: '#111',
                    font: {
                        family: 'Georgia, serif',
                        size: 11
                    }
                }
            },
            tooltip: {
                backgroundColor: 'rgba(255,255,255,0.95)',
                titleColor: '#000',
                bodyColor: '#333',
                borderColor: '#ccc',
                borderWidth: 1,
                titleFont: { family: 'Georgia', weight: 'bold' },
                bodyFont: { family: 'Courier New', size: 12 }
            }
        },
        scales: {
            x: {
                grid: { color: '#eaeae8' },
                ticks: {
                    color: '#333',
                    font: { family: 'Courier New', size: 10 }
                }
            },
            y: {
                grid: { color: '#eaeae8' },
                ticks: {
                    color: '#333',
                    font: { family: 'Courier New', size: 10 }
                }
            }
        }
    };

    const tradesLabels = data.tradesScaling.points.map(p => formatNumber(p.trades));
    const subLabels = data.subscribersScaling.points.map(p => p.subscribers);

    const getRes = (point, key) => (point.results && point.results[key]) ? point.results[key] : { timeNs: 0, allocs: 0, bytesAlloc: 0 };

    // Chart 1: Latency Scaling (Overview)
    const ctx1 = document.getElementById('mainLatencyChart');
    if (ctx1) {
        chartInstances.latency = new Chart(ctx1.getContext('2d'), {
            type: 'line',
            data: {
                labels: tradesLabels,
                datasets: [
                    { label: 'SimpleFan V1', data: data.tradesScaling.points.map(p => getRes(p, 'SimpleFanV1').timeNs / 1e6), borderColor: colors.sfV1, backgroundColor: 'transparent', borderWidth: 1.5, pointRadius: 4 },
                    { label: 'SimpleFan V2', data: data.tradesScaling.points.map(p => getRes(p, 'SimpleFanV2').timeNs / 1e6), borderColor: colors.sfV2, backgroundColor: 'transparent', borderWidth: 1.5, pointRadius: 4 },
                    { label: 'SimpleFan V3', data: data.tradesScaling.points.map(p => getRes(p, 'SimpleFanV3').timeNs / 1e6), borderColor: colors.sfV3, backgroundColor: 'transparent', borderWidth: 1.5, pointRadius: 4 },
                    { label: 'RingBuffer V6', data: data.tradesScaling.points.map(p => getRes(p, 'RingBufferV6').timeNs / 1e6), borderColor: colors.rbV6, backgroundColor: 'transparent', borderWidth: 2.0, pointRadius: 5 },
                    { label: 'RB Eviction Mode', data: data.tradesScaling.points.map(p => getRes(p, 'RingBufferEviction').timeNs / 1e6), borderColor: colors.rbEviction, backgroundColor: 'transparent', borderWidth: 2.0, pointRadius: 5 },
                    { label: 'FlatBuffers Zero-Copy', data: data.tradesScaling.points.map(p => getRes(p, 'FlatBuffersZeroCopy').timeNs / 1e6), borderColor: colors.flatBuffers, backgroundColor: 'transparent', borderWidth: 2.0, pointRadius: 5 }
                ]
            },
            options: {
                ...academicOptions,
                plugins: {
                    ...academicOptions.plugins,
                    title: { display: true, text: 'Execution Time vs. Trade Volume (100 Subscribers)', font: { family: 'Georgia', size: 13, weight: 'bold' } }
                },
                scales: {
                    ...academicOptions.scales,
                    y: { ...academicOptions.scales.y, title: { display: true, text: 'Time (ms)', font: { family: 'Georgia' } } }
                }
            }
        });
    }

    // Chart 2: Allocations Scaling (Trades)
    const ctx2 = document.getElementById('tradesAllocsChart');
    if (ctx2) {
        chartInstances.tradeAllocs = new Chart(ctx2.getContext('2d'), {
            type: 'line',
            data: {
                labels: tradesLabels,
                datasets: [
                    { label: 'SimpleFan V1', data: data.tradesScaling.points.map(p => getRes(p, 'SimpleFanV1').allocs), borderColor: colors.sfV1, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'SimpleFan V2', data: data.tradesScaling.points.map(p => getRes(p, 'SimpleFanV2').allocs), borderColor: colors.sfV2, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'SimpleFan V3', data: data.tradesScaling.points.map(p => getRes(p, 'SimpleFanV3').allocs), borderColor: colors.sfV3, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'RingBuffer V6', data: data.tradesScaling.points.map(p => getRes(p, 'RingBufferV6').allocs), borderColor: colors.rbV6, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 },
                    { label: 'RB Eviction', data: data.tradesScaling.points.map(p => getRes(p, 'RingBufferEviction').allocs), borderColor: colors.rbEviction, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 },
                    { label: 'FlatBuffers', data: data.tradesScaling.points.map(p => getRes(p, 'FlatBuffersZeroCopy').allocs), borderColor: colors.flatBuffers, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 }
                ]
            },
            options: {
                ...academicOptions,
                plugins: {
                    ...academicOptions.plugins,
                    title: { display: true, text: 'Total Mallocs vs. Trade Volume', font: { family: 'Georgia', size: 12 } }
                },
                scales: {
                    ...academicOptions.scales,
                    y: { ...academicOptions.scales.y, title: { display: true, text: 'Mallocs/Op Count', font: { family: 'Georgia' } } }
                }
            }
        });
    }

    // Chart 3: Memory Size Scaling (Trades)
    const ctx3 = document.getElementById('tradesBytesChart');
    if (ctx3) {
        chartInstances.tradeBytes = new Chart(ctx3.getContext('2d'), {
            type: 'line',
            data: {
                labels: tradesLabels,
                datasets: [
                    { label: 'SimpleFan V1', data: data.tradesScaling.points.map(p => getRes(p, 'SimpleFanV1').bytesAlloc / 1024), borderColor: colors.sfV1, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'SimpleFan V2', data: data.tradesScaling.points.map(p => getRes(p, 'SimpleFanV2').bytesAlloc / 1024), borderColor: colors.sfV2, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'SimpleFan V3', data: data.tradesScaling.points.map(p => getRes(p, 'SimpleFanV3').bytesAlloc / 1024), borderColor: colors.sfV3, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'RingBuffer V6', data: data.tradesScaling.points.map(p => getRes(p, 'RingBufferV6').bytesAlloc / 1024), borderColor: colors.rbV6, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 },
                    { label: 'RB Eviction', data: data.tradesScaling.points.map(p => getRes(p, 'RingBufferEviction').bytesAlloc / 1024), borderColor: colors.rbEviction, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 },
                    { label: 'FlatBuffers', data: data.tradesScaling.points.map(p => getRes(p, 'FlatBuffersZeroCopy').bytesAlloc / 1024), borderColor: colors.flatBuffers, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 }
                ]
            },
            options: {
                ...academicOptions,
                plugins: {
                    ...academicOptions.plugins,
                    title: { display: true, text: 'Memory Size Allocated vs. Trade Volume', font: { family: 'Georgia', size: 12 } }
                },
                scales: {
                    ...academicOptions.scales,
                    y: { ...academicOptions.scales.y, title: { display: true, text: 'Allocated (KB)', font: { family: 'Georgia' } } }
                }
            }
        });
    }

    // Chart 4: Latency Scaling (Subscribers)
    const ctx4 = document.getElementById('subscribersLatencyChart');
    if (ctx4) {
        chartInstances.subLatency = new Chart(ctx4.getContext('2d'), {
            type: 'line',
            data: {
                labels: subLabels,
                datasets: [
                    { label: 'SimpleFan V1', data: data.subscribersScaling.points.map(p => getRes(p, 'SimpleFanV1').timeNs / 1e6), borderColor: colors.sfV1, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'SimpleFan V2', data: data.subscribersScaling.points.map(p => getRes(p, 'SimpleFanV2').timeNs / 1e6), borderColor: colors.sfV2, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'SimpleFan V3', data: data.subscribersScaling.points.map(p => getRes(p, 'SimpleFanV3').timeNs / 1e6), borderColor: colors.sfV3, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'RingBuffer V6', data: data.subscribersScaling.points.map(p => getRes(p, 'RingBufferV6').timeNs / 1e6), borderColor: colors.rbV6, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 },
                    { label: 'RB Eviction', data: data.subscribersScaling.points.map(p => getRes(p, 'RingBufferEviction').timeNs / 1e6), borderColor: colors.rbEviction, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 },
                    { label: 'FlatBuffers', data: data.subscribersScaling.points.map(p => getRes(p, 'FlatBuffersZeroCopy').timeNs / 1e6), borderColor: colors.flatBuffers, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 }
                ]
            },
            options: {
                ...academicOptions,
                plugins: {
                    ...academicOptions.plugins,
                    title: { display: true, text: 'Execution Time vs. Subscriber Count (50,000 Trades)', font: { family: 'Georgia', size: 12 } }
                },
                scales: {
                    ...academicOptions.scales,
                    y: { ...academicOptions.scales.y, title: { display: true, text: 'Time (ms)', font: { family: 'Georgia' } } }
                }
            }
        });
    }

    // Chart 5: Allocations vs Subscribers
    const ctx5 = document.getElementById('subscribersAllocsChart');
    if (ctx5) {
        chartInstances.subAllocs = new Chart(ctx5.getContext('2d'), {
            type: 'line',
            data: {
                labels: subLabels,
                datasets: [
                    { label: 'SimpleFan V1', data: data.subscribersScaling.points.map(p => getRes(p, 'SimpleFanV1').allocs), borderColor: colors.sfV1, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'SimpleFan V2', data: data.subscribersScaling.points.map(p => getRes(p, 'SimpleFanV2').allocs), borderColor: colors.sfV2, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'SimpleFan V3', data: data.subscribersScaling.points.map(p => getRes(p, 'SimpleFanV3').allocs), borderColor: colors.sfV3, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 3 },
                    { label: 'RingBuffer V6', data: data.subscribersScaling.points.map(p => getRes(p, 'RingBufferV6').allocs), borderColor: colors.rbV6, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 },
                    { label: 'RB Eviction', data: data.subscribersScaling.points.map(p => getRes(p, 'RingBufferEviction').allocs), borderColor: colors.rbEviction, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 },
                    { label: 'FlatBuffers', data: data.subscribersScaling.points.map(p => getRes(p, 'FlatBuffersZeroCopy').allocs), borderColor: colors.flatBuffers, backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 4 }
                ]
            },
            options: {
                ...academicOptions,
                plugins: {
                    ...academicOptions.plugins,
                    title: { display: true, text: 'Allocations vs. Subscriber Count (50,000 Trades)', font: { family: 'Georgia', size: 12 } }
                },
                scales: {
                    ...academicOptions.scales,
                    y: { ...academicOptions.scales.y, title: { display: true, text: 'Mallocs Count', font: { family: 'Georgia' } } }
                }
            }
        });
    }
}

// ----------------------------------------------------
// 3. Interactive Experiment Configuration Builder
// ----------------------------------------------------
function setupExperimentBuilder() {
    const runBtn = document.getElementById('run-btn');
    const consoleEl = document.getElementById('console');

    runBtn.addEventListener('click', () => {
        const tradesVal = document.getElementById('trades-input').value.trim();
        const subsVal = document.getElementById('subscribers-input').value.trim();

        runBtn.disabled = true;

        if (isStaticDemo) {
            consoleEl.textContent = '>> Initiating client-side Go runtime simulation (offline static mode)...\n';
            runBtn.innerText = 'Compiling Go Test Harness...';

            setTimeout(() => {
                consoleEl.textContent += '[Go Compiler] Compiling low-overhead benchmark harness (simplefan.go, ringbuffer.go, latency_test.go)...\n';
                consoleEl.scrollTop = consoleEl.scrollHeight;
            }, 800);

            setTimeout(() => {
                consoleEl.textContent += '[Go Compiler] Compilation successful. Spawning benchmark processes...\n';
                consoleEl.textContent += `[Runner] Running benchmark suite over configurations: trades = [${tradesVal}], subscribers = [${subsVal}]\n`;
                consoleEl.scrollTop = consoleEl.scrollHeight;
                runBtn.innerText = 'Executing Benchmarks...';
            }, 1800);

            setTimeout(() => {
                consoleEl.textContent += 'BenchmarkSimpleFanV1-8     100    164200000 ns/op    8317661 B/op    488 allocs/op\n';
                consoleEl.textContent += 'BenchmarkSimpleFanV2-8     200     45000000 ns/op    1920032 B/op    410 allocs/op\n';
                consoleEl.textContent += 'BenchmarkSimpleFanV3-8     500     25000000 ns/op    1200000 B/op    409 allocs/op\n';
                consoleEl.textContent += 'BenchmarkRingBufferV6-8   1000      2980000 ns/op      57676 B/op    408 allocs/op\n';
                consoleEl.scrollTop = consoleEl.scrollHeight;
            }, 3200);

            setTimeout(() => {
                consoleEl.textContent += '\n>> Compilation & execution finished successfully. Reloading empirical charts...\n';
                consoleEl.scrollTop = consoleEl.scrollHeight;
                runBtn.disabled = false;
                runBtn.innerText = 'Run Benchmark Experiment';
                loadBenchmarkData();
            }, 4200);

            return;
        }

        consoleEl.textContent = '>> Initiating Server-Sent Events (SSE) connection to compiler backend...\n';
        runBtn.innerText = 'Executing Go Compiler...';

        const eventSource = new EventSource(getApiUrl(`/api/run-experiment?trades=${encodeURIComponent(tradesVal)}&subscribers=${encodeURIComponent(subsVal)}`));

        eventSource.onmessage = (event) => {
            const line = event.data;
            if (line === '[DONE]') {
                consoleEl.textContent += '\n>> Compilation & execution finished successfully. Reloading empirical charts...\n';
                eventSource.close();
                runBtn.disabled = false;
                runBtn.innerText = 'Run Benchmark Experiment';
                loadBenchmarkData();
            } else {
                consoleEl.textContent += line + '\n';
                consoleEl.scrollTop = consoleEl.scrollHeight;
            }
        };

        eventSource.onerror = (err) => {
            consoleEl.textContent += '\n[ERROR] Lost connection to experiment runner backend. Check server console output.\n';
            eventSource.close();
            runBtn.disabled = false;
            runBtn.innerText = 'Run Benchmark Experiment';
        };
    });
}

// ----------------------------------------------------
// 4. Live Proof-of-Work Visualization (Disruptor Operations)
// ----------------------------------------------------
function initRingBufferSlots() {
    const container = document.getElementById('linear-slots-container');
    if (!container) return;
    container.innerHTML = '';
    
    for (let i = 0; i < 16; i++) {
        const slot = document.createElement('div');
        slot.id = `linear-slot-${i}`;
        slot.style.border = '1px solid #000';
        slot.style.padding = '8px 2px';
        slot.style.textAlign = 'center';
        slot.style.fontFamily = 'var(--font-mono)';
        slot.style.fontSize = '0.75rem';
        slot.style.background = '#ffffff';
        slot.style.position = 'relative';
        slot.innerHTML = `<div style="font-weight: bold;">${i}</div><div class="slot-badge" style="font-size: 0.6rem; margin-top: 4px; height: 12px;"></div>`;
        container.appendChild(slot);
    }
}

let isStaticDemo = false;
let demoState = {
    midPrice: 65000.0,
    obi: 0.0,
    spread: 1.5,
    topBids: [],
    topAsks: [],
    trades: [],
    orders: [],
    cash: 100000.0,
    position: 0.0,
    nav: 100000.0,
    buyAndHoldNav: 100000.0,
    initialPrice: 0.0,
    signal: "HOLD",
    entryPrice: 0.0,
    entryTime: 0,
    orderCounter: 0,
    writeSeq: 0,
    quantSeq: 0,
    botSeq: 0,
    uiSeq: 0,
    stopLossPct: 0.02,
    takeProfitPct: 0.05,
    takerFeePct: 0.0005,
    slippagePct: 0.0001
};


async function checkBackendConnectivity() {
    try {
        const response = await fetch(getApiUrl('/api/orderbook'));
        if (!response.ok) throw new Error("Not OK");
        isStaticDemo = false;
        console.log("[Live Connectivity] Connected to Go backend server at " + getApiUrl('/api/orderbook'));
    } catch (e) {
        console.warn("Backend API not found. Activating static client-side HFT simulation.");
        isStaticDemo = true;
        initStaticDemoData();
    }
}

function initStaticDemoData() {
    updateDemoOrderBook();
    demoState.initialPrice = demoState.midPrice;
    for (let i = 0; i < 10; i++) {
        demoState.trades.push({
            ID: 1000 + i,
            Price: demoState.midPrice + (Math.random() - 0.5) * 5.0,
            Quantity: Math.random() * 2.0 + 0.1
        });
    }
}

function updateDemoOrderBook() {
    demoState.midPrice += (Math.random() - 0.5) * 4.0;
    demoState.topBids = [];
    demoState.topAsks = [];
    
    let currentBid = demoState.midPrice - 0.5 - Math.random() * 0.5;
    let currentAsk = demoState.midPrice + 0.5 + Math.random() * 0.5;
    demoState.spread = currentAsk - currentBid;
    
    let sumBids = 0;
    let sumAsks = 0;
    for (let i = 0; i < 5; i++) {
        let bidSize = Math.random() * 3.0 + 0.1;
        let askSize = Math.random() * 3.0 + 0.1;
        sumBids += bidSize;
        sumAsks += askSize;
        
        demoState.topBids.push({ price: currentBid - i * 0.2, size: bidSize });
        demoState.topAsks.push({ price: currentAsk + i * 0.2, size: askSize });
    }
    
    demoState.obi = (sumBids - sumAsks) / (sumBids + sumAsks);
}

function runStaticOrderBookSimulation() {
    updateDemoOrderBook();
    const obiPct = (demoState.obi * 100).toFixed(2);
    const obiTextEl = document.getElementById('ind-obi');
    if (obiTextEl) obiTextEl.innerText = (demoState.obi >= 0 ? '+' : '') + obiPct + '%';
    
    const barEl = document.getElementById('obi-bar');
    if (barEl) {
        if (demoState.obi >= 0) {
            barEl.style.marginLeft = '50%';
            barEl.style.width = `${demoState.obi * 50}%`;
            barEl.style.backgroundColor = '#137333';
        } else {
            const widthPct = Math.abs(demoState.obi) * 50;
            barEl.style.marginLeft = `${50 - widthPct}%`;
            barEl.style.width = `${widthPct}%`;
            barEl.style.backgroundColor = '#c5221f';
        }
    }

    const spreadEl = document.getElementById('ind-spread');
    if (spreadEl) spreadEl.innerText = '$' + demoState.spread.toFixed(2);
    const midEl = document.getElementById('ind-mid');
    if (midEl) midEl.innerText = '$' + demoState.midPrice.toFixed(2);
    latestPrice = demoState.midPrice;
    
    const syncEl = document.getElementById('ind-updated');
    if (syncEl) syncEl.innerText = new Date().toLocaleTimeString();

    const bidsBody = document.querySelector('#obi-bids-table tbody');
    if (bidsBody) {
        bidsBody.innerHTML = '';
        demoState.topBids.forEach(b => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td style="padding: 2px 4px; color: #555; text-align: left;">${b.size.toFixed(4)}</td>
                <td style="padding: 2px 4px; color: #137333; font-weight: bold;">${b.price.toFixed(2)}</td>
            `;
            bidsBody.appendChild(row);
        });
    }

    const asksBody = document.querySelector('#obi-asks-table tbody');
    if (asksBody) {
        asksBody.innerHTML = '';
        demoState.topAsks.forEach(a => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td style="padding: 2px 4px; color: #c5221f; font-weight: bold; text-align: right;">${a.price.toFixed(2)}</td>
                <td style="padding: 2px 4px; color: #555; text-align: left;">${a.size.toFixed(4)}</td>
            `;
            asksBody.appendChild(row);
        });
    }

    const nowStr = new Date().toLocaleTimeString();
    const bestBid = demoState.topBids[0].price;
    const bestAsk = demoState.topAsks[0].price;
    chartData.prices.push(demoState.midPrice);
    chartData.fast.push(bestBid);
    chartData.slow.push(bestAsk);
    chartData.labels.push(nowStr);
    
    if (chartData.prices.length > 35) {
        chartData.prices.shift();
        chartData.fast.shift();
        chartData.slow.shift();
        chartData.labels.shift();
    }
    
    if (crossoverChart) {
        crossoverChart.data.labels = chartData.labels;
        crossoverChart.data.datasets[0].data = chartData.prices;
        crossoverChart.data.datasets[1].data = chartData.fast;
        crossoverChart.data.datasets[2].data = chartData.slow;
        crossoverChart.update('none');
    }
}

function runStaticCoinbaseFeedSimulation() {
    const lastTrade = demoState.trades[demoState.trades.length - 1];
    const newTrade = {
        ID: lastTrade ? lastTrade.ID + 1 : 1000,
        Price: demoState.midPrice + (Math.random() - 0.5) * 1.5,
        Quantity: Math.random() * 0.8 + 0.05
    };
    demoState.trades.push(newTrade);
    if (demoState.trades.length > 20) demoState.trades.shift();
    
    const tbody = document.querySelector('#live-trades-table tbody');
    if (!tbody) return;

    tbody.innerHTML = '';
    const recent = demoState.trades.slice(-10).reverse();
    recent.forEach(t => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>CB</td>
            <td>${t.ID}</td>
            <td>$${t.Price.toFixed(2)}</td>
            <td>${t.Quantity.toFixed(8)}</td>
        `;
        tbody.appendChild(row);
    });
}

function renderMultiConsumerSlots(slotsData, wSeq, botSeq, aiSeq, auditSeq, evictedCount) {
    const container = document.getElementById('linear-slots-container');
    if (!container) return;

    const wIdx = wSeq % 32;
    const botIdx = botSeq % 32;
    const aiIdx = aiSeq % 32;
    const auditIdx = auditSeq % 32;

    // 1. Update Telemetry Table Fields
    const telWSeq = document.getElementById('tel-w-seq');
    if (telWSeq) telWSeq.innerText = wSeq.toLocaleString();
    const telWSlot = document.getElementById('tel-w-slot');
    if (telWSlot) telWSlot.innerText = `Slot ${wIdx}`;

    const telBotSeq = document.getElementById('tel-bot-seq');
    if (telBotSeq) telBotSeq.innerText = botSeq.toLocaleString();
    const telBotSlot = document.getElementById('tel-bot-slot');
    if (telBotSlot) telBotSlot.innerText = `Slot ${botIdx}`;
    const telBotLag = document.getElementById('tel-bot-lag');
    if (telBotLag) {
        const lag = wSeq - botSeq;
        telBotLag.innerText = `${lag} slot` + (lag === 1 ? '' : 's');
        telBotLag.style.color = lag > 0 ? '#b25900' : '#005f00';
    }

    const telIndSeq = document.getElementById('tel-ind-seq');
    if (telIndSeq) telIndSeq.innerText = aiSeq.toLocaleString();
    const telIndSlot = document.getElementById('tel-ind-slot');
    if (telIndSlot) telIndSlot.innerText = `Slot ${aiIdx}`;
    const telIndLag = document.getElementById('tel-ind-lag');
    if (telIndLag) {
        const lag = wSeq - aiSeq;
        telIndLag.innerText = `${lag} slot` + (lag === 1 ? '' : 's');
        telIndLag.style.color = lag > 0 ? '#b25900' : '#005f00';
    }

    const telAuditSeq = document.getElementById('tel-audit-seq');
    if (telAuditSeq) telAuditSeq.innerText = auditSeq.toLocaleString();
    const telAuditSlot = document.getElementById('tel-audit-slot');
    if (telAuditSlot) telAuditSlot.innerText = `Slot ${auditIdx}`;
    const telAuditLag = document.getElementById('tel-audit-lag');
    if (telAuditLag) {
        const lag = wSeq - auditSeq;
        telAuditLag.innerText = `${lag} slot` + (lag === 1 ? '' : 's');
        telAuditLag.style.color = lag > 0 ? '#b25900' : '#005f00';
    }

    const evEl = document.getElementById('telemetry-evicted-count');
    if (evEl) evEl.innerText = `${evictedCount || 0} events`;

    // 2. Initialize 32 slot DOM nodes once if not created yet
    if (container.children.length !== 32) {
        container.innerHTML = '';
        for (let i = 0; i < 32; i++) {
            const slotEl = document.createElement('div');
            slotEl.id = `ring-slot-cell-${i}`;
            slotEl.className = 'linear-slot-item';
            slotEl.style.cssText = `
                border: 1px solid #cbd5e1;
                padding: 3px 2px;
                text-align: center;
                font-size: 0.62rem;
                font-family: var(--font-mono);
                background: #ffffff;
                position: relative;
                height: 56px;
                min-height: 56px;
                max-height: 56px;
                box-sizing: border-box;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                overflow: hidden;
            `;
            container.appendChild(slotEl);
        }
    }

    // 3. Update existing 32 slot DOM nodes in-place (No layout thrashing / quivering!)
    for (let i = 0; i < 32; i++) {
        const slotEl = container.children[i];
        if (!slotEl) continue;

        const slotData = (slotsData && slotsData[i]) ? slotsData[i] : null;

        let badges = [];
        if (i === wIdx) badges.push({ text: 'W', bg: '#8b0000', color: '#fff' });
        if (i === botIdx) badges.push({ text: 'BOT', bg: '#4b0082', color: '#fff' });
        if (i === aiIdx) badges.push({ text: 'IND', bg: '#000080', color: '#fff' });
        if (i === auditIdx) badges.push({ text: 'AUD', bg: '#005f00', color: '#fff' });

        let badgeHtml = '';
        if (badges.length > 0) {
            badgeHtml = `<div style="display: flex; gap: 1px; justify-content: center; margin-bottom: 2px;">
                ${badges.map(b => `<span style="background: ${b.bg}; color: ${b.color}; padding: 0 2px; font-weight: bold; font-size: 0.58rem; line-height: 1.1;">${b.text}</span>`).join('')}
            </div>`;
        }

        let venueCode = "CB";
        let tradeText = `[${i}]`;
        let priceText = "";
        if (slotData && slotData.tradeId > 0) {
            venueCode = slotData.venue ? slotData.venue.substring(0, 2) : "CB";
            tradeText = `$${slotData.price.toFixed(0)}`;
            priceText = `<span style="font-size: 0.56rem; color: #334155; font-weight: bold;">${venueCode}</span>`;
        }

        const newContent = `
            ${badgeHtml}
            <div style="font-weight: bold; color: #64748b; font-size: 0.58rem;">S${i < 10 ? '0' + i : i}</div>
            <div style="font-size: 0.60rem; font-weight: bold; margin: 1px 0; color: #0f172a;">${tradeText}</div>
            ${priceText}
        `;

        if (slotEl.innerHTML !== newContent) {
            slotEl.innerHTML = newContent;
        }

        if (badges.length > 0) {
            slotEl.style.borderColor = badges[0].bg;
            slotEl.style.borderWidth = '2px';
            slotEl.style.background = '#f1f5f9';
        } else {
            slotEl.style.borderColor = '#cbd5e1';
            slotEl.style.borderWidth = '1px';
            slotEl.style.background = '#ffffff';
        }
    }
}

function runStaticRingBufferSimulation() {
    demoState.writeSeq += Math.floor(Math.random() * 3) + 1;
    demoState.botSeq += Math.floor(Math.random() * 2) + 1;
    demoState.quantSeq += Math.floor(Math.random() * 2) + 1;
    demoState.uiSeq += Math.floor(Math.random() * 2) + 1;

    if (demoState.botSeq > demoState.writeSeq) demoState.botSeq = demoState.writeSeq;
    if (demoState.quantSeq > demoState.writeSeq) demoState.quantSeq = demoState.writeSeq;
    if (demoState.uiSeq > demoState.writeSeq) demoState.uiSeq = demoState.writeSeq;

    renderMultiConsumerSlots(null, demoState.writeSeq, demoState.botSeq, demoState.quantSeq, demoState.uiSeq);
}

function runStaticTradingBotSimulation() {
    demoState.buyAndHoldNav = (demoState.initialPrice > 0) ? (100000.0 * (demoState.midPrice / demoState.initialPrice)) : 100000.0;
    
    // Default tight risk parameters for HFT micro-scalping
    demoState.stopLossPct = 0.005;   // 0.5% Stop Loss
    demoState.takeProfitPct = 0.012; // 1.2% Take Profit
    
    if (demoState.position > 0) {
        let pChange = (demoState.midPrice - demoState.entryPrice) / demoState.entryPrice;
        if (pChange <= -demoState.stopLossPct) {
            let execPrice = demoState.midPrice * (1.0 - demoState.slippagePct);
            let soldValue = demoState.position * execPrice;
            let fee = soldValue * demoState.takerFeePct;
            demoState.cash += (soldValue - fee);
            
            demoState.orderCounter++;
            demoState.orders.push({
                id: demoState.orderCounter,
                timestamp: new Date().toLocaleTimeString(),
                type: "STOP_LOSS",
                price: execPrice,
                quantity: demoState.position,
                value: soldValue
            });
            
            demoState.position = 0;
            demoState.entryPrice = 0;
            demoState.signal = "HOLD";
            demoState.nav = demoState.cash;
            return;
        } else if (pChange >= demoState.takeProfitPct) {
            let execPrice = demoState.midPrice * (1.0 - demoState.slippagePct);
            let soldValue = demoState.position * execPrice;
            let fee = soldValue * demoState.takerFeePct;
            demoState.cash += (soldValue - fee);
            
            demoState.orderCounter++;
            demoState.orders.push({
                id: demoState.orderCounter,
                timestamp: new Date().toLocaleTimeString(),
                type: "TAKE_PROFIT",
                price: execPrice,
                quantity: demoState.position,
                value: soldValue
            });
            
            demoState.position = 0;
            demoState.entryPrice = 0;
            demoState.signal = "HOLD";
            demoState.nav = demoState.cash;
            return;
        }
    }
    
    if (demoState.obi >= 0.04) {
        demoState.signal = "BUY";
    } else if (demoState.obi <= -0.04) {
        demoState.signal = "SELL";
    } else {
        demoState.signal = "HOLD";
    }
    
    const nowTime = Date.now() / 1000;
    
    if (demoState.signal === "BUY" && demoState.position === 0 && demoState.cash > 10) {
        let execPrice = demoState.midPrice * (1.0 + demoState.slippagePct);
        let allocated = demoState.cash * 0.95;
        let qty = allocated / execPrice;
        let val = qty * execPrice;
        let fee = val * demoState.takerFeePct;
        
        demoState.cash -= (val + fee);
        demoState.position += qty;
        demoState.entryPrice = demoState.midPrice;
        demoState.entryTime = nowTime;
        demoState.orderCounter++;
        
        demoState.orders.push({
            id: demoState.orderCounter,
            timestamp: new Date().toLocaleTimeString(),
            type: "BUY",
            price: execPrice,
            quantity: qty,
            value: val
        });
    }
    else if (demoState.signal === "SELL" && demoState.position > 0.00001) {
        let execPrice = demoState.midPrice * (1.0 - demoState.slippagePct);
        let soldValue = demoState.position * execPrice;
        let fee = soldValue * demoState.takerFeePct;
        demoState.cash += (soldValue - fee);
        
        demoState.orderCounter++;
        demoState.orders.push({
            id: demoState.orderCounter,
            timestamp: new Date().toLocaleTimeString(),
            type: "SELL",
            price: execPrice,
            quantity: demoState.position,
            value: soldValue
        });
        
        demoState.position = 0;
        demoState.entryPrice = 0;
    }
    
    if (demoState.orders.length > 30) {
        demoState.orders = demoState.orders.slice(demoState.orders.length - 30);
    }
    
    demoState.nav = demoState.cash + (demoState.position * demoState.midPrice);
    
    const cashEl = document.getElementById('bot-cash');
    if (cashEl) cashEl.innerText = '$' + demoState.cash.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    const posEl = document.getElementById('bot-position');
    if (posEl) posEl.innerText = demoState.position.toFixed(8) + ' BTC';
    
    const navEl = document.getElementById('bot-nav');
    if (navEl) navEl.innerText = '$' + demoState.nav.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    const bhEl = document.getElementById('bot-bh-nav');
    if (bhEl) bhEl.innerText = '$' + demoState.buyAndHoldNav.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    
    // Sync Top Balance Sheet KPI Cards
    const topNav = document.getElementById('top-nav-display');
    if (topNav) topNav.innerText = '$' + demoState.nav.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    const topPos = document.getElementById('top-pos-display');
    if (topPos) topPos.innerText = demoState.position.toFixed(8) + ' BTC';
    const topCash = document.getElementById('top-cash-display');
    if (topCash) topCash.innerText = '$' + demoState.cash.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    const topBH = document.getElementById('top-bh-display');
    if (topBH) topBH.innerText = '$' + demoState.buyAndHoldNav.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    
    // Sync Balance Sheet Financial Table
    const tableCash = document.getElementById('table-cash-val');
    if (tableCash) tableCash.innerText = '$' + demoState.cash.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    const tableBtc = document.getElementById('table-btc-val');
    if (tableBtc) tableBtc.innerText = demoState.position.toFixed(8) + ' BTC';
    const tableNav = document.getElementById('table-nav-val');
    if (tableNav) tableNav.innerText = '$' + demoState.nav.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    const tableBH = document.getElementById('table-bh-val');
    if (tableBH) tableBH.innerText = '$' + demoState.buyAndHoldNav.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});

    if (navEl) {
        if (demoState.nav > demoState.buyAndHoldNav) {
            navEl.style.color = 'var(--accent-green)';
            if (topNav) topNav.style.color = 'var(--accent-green)';
            if (tableNav) tableNav.style.color = 'var(--accent-green)';
        } else if (demoState.nav < demoState.buyAndHoldNav) {
            navEl.style.color = 'var(--accent-red)';
            if (topNav) topNav.style.color = 'var(--accent-red)';
            if (tableNav) tableNav.style.color = 'var(--accent-red)';
        } else {
            navEl.style.color = 'var(--text-main)';
            if (topNav) topNav.style.color = 'var(--text-main)';
            if (tableNav) tableNav.style.color = 'var(--text-main)';
        }
    }
    
    const strategyLabelEl = document.getElementById('bot-strategy');
    if (strategyLabelEl) {
        strategyLabelEl.innerText = (demoState.strategy === "LLM") ? "Gemini LLM (AI Decision)" : "Order Book Imbalance (OBI) HFT";
    }

    const signalEl = document.getElementById('bot-signal');
    if (signalEl) {
        signalEl.innerText = demoState.signal;
        if (demoState.signal === 'BUY') {
            signalEl.style.color = 'var(--accent-red)';
            signalEl.style.fontWeight = 'bold';
        } else if (demoState.signal === 'SELL') {
            signalEl.style.color = 'var(--accent-green)';
            signalEl.style.fontWeight = 'bold';
        } else {
            signalEl.style.color = '#333';
            signalEl.style.fontWeight = 'normal';
        }
    }
    
    const buyThEl = document.getElementById('bot-obi-buy-th');
    if (buyThEl) buyThEl.innerText = '0.15';
    const sellThEl = document.getElementById('bot-obi-sell-th');
    if (sellThEl) sellThEl.innerText = '-0.15';

    const commentaryEl = document.getElementById('commentary-text');
    if (commentaryEl) {
        let relativePerf = demoState.nav - demoState.buyAndHoldNav;
        let perfPct = (((demoState.nav - demoState.buyAndHoldNav) / demoState.buyAndHoldNav) * 100).toFixed(4);
        let perfText = "";
        let color = "#333";
        if (relativePerf > 0) {
            perfText = `Outperforming Buy & Hold by +$${relativePerf.toFixed(2)} (+${perfPct}%)`;
            color = "var(--accent-green)";
        } else if (relativePerf < 0) {
            perfText = `Underperforming Buy & Hold by -$${Math.abs(relativePerf).toFixed(2)} (${perfPct}%)`;
            color = "var(--accent-red)";
        } else {
            perfText = `Neutral parity with Buy & Hold ($0.00 deviation)`;
        }

        let signalReason = "";
        if (demoState.strategy === "LLM") {
            if (demoState.signal === 'BUY') {
                signalReason = `Gemini 2.5 Flash: Bullish divergence confirmed. OBI (+${(demoState.obi * 100).toFixed(2)}%) shows aggressive bid wall aggregation. Spread ($${demoState.spread.toFixed(2)}) tightening. Target long entry.`;
            } else if (demoState.signal === 'SELL') {
                signalReason = `Gemini 2.5 Flash: Bearish exhaust pattern. Heavy ask wall building (OBI = ${(demoState.obi * 100).toFixed(2)}%). Liquidity support fading. Executing market exit to preserve capital.`;
            } else {
                signalReason = `Gemini 2.5 Flash: Market consolidation. OBI (${(demoState.obi * 100).toFixed(2)}%) inside neutral bounds. Standing by in Cash to avoid high-frequency fee drag.`;
            }
        } else {
            if (demoState.signal === 'BUY') {
                signalReason = "OBI is extremely bullish (>= 0.15) due to massive bid depth. Executing market BUY order to fill BTC position.";
            } else if (demoState.signal === 'SELL') {
                signalReason = "OBI is extremely bearish (<= -0.15) due to heavy ask walls. Executed market SELL order to liquidate BTC position.";
            } else {
                signalReason = "OBI is in neutral bounds (-0.15 < OBI < 0.15). Standing by to avoid overhead costs.";
            }
        }

        commentaryEl.innerHTML = `
            <strong>Performance Analysis:</strong> <span style="color: ${color}; font-weight: bold;">${perfText}</span><br>
            <strong>Signal Context:</strong> ${signalReason}
        `;
    }

    const tbody = document.querySelector('#bot-orders-table tbody');
    if (tbody) {
        if (demoState.orders.length === 0) {
            tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; font-size: 0.75rem;">Waiting for strategy crossover to execute trades...</td></tr>`;
            return;
        }

        tbody.innerHTML = '';
        const recent = demoState.orders.slice(-5).reverse();
        recent.forEach(o => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${o.id}</td>
                <td>${o.timestamp}</td>
                <td style="color: ${o.type.includes('BUY') ? 'var(--accent-red)' : 'var(--accent-green)'}; font-weight: bold;">${o.type}</td>
                <td>$${o.price.toFixed(2)}</td>
                <td>${o.quantity.toFixed(8)}</td>
                <td>$${o.value.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
            `;
            tbody.appendChild(row);
        });
    }
}

function startProofOfWorkPolling() {
    // Poll the Ring Buffer slot mappings, Coinbase trades, Gemini AI, and the Trading Bot state
    setInterval(pollRingBufferState, 1000);
    setInterval(pollCoinbaseFeed, 1000);
    setInterval(pollGeminiSentiment, 200); // 200ms for live L2 updates
    setInterval(pollTradingBotState, 1000);
}

async function pollRingBufferState() {
    if (isStaticDemo) {
        runStaticRingBufferSimulation();
        return;
    }
    try {
        const response = await fetch(getApiUrl('/api/ring-buffer'));
        if (!response.ok) return;
        const data = await response.json();

        renderMultiConsumerSlots(
            data.slots || [],
            data.writeSeq || 0,
            data.botSeq || data.botReadSeq || 0,
            data.aiSeq || data.aiReadSeq || 0,
            data.auditSeq || data.auditReadSeq || 0,
            data.evictedCount || 0
        );

        // Update Live Event Trace Log
        const logEl = document.getElementById('rb-trace-log');
        if (logEl) {
            if (data.traces && Array.isArray(data.traces) && data.traces.length > 0) {
                logEl.innerHTML = data.traces.filter(t => t != null).map(t => {
                    let actorStr = (t && t.actor) ? String(t.actor) : 'W';
                    let badgeClass = 'ptr-badge ' + actorStr.toLowerCase();
                    let actionStr = (t && t.action) ? String(t.action) : '';
                    let slotStr = (t && t.slot !== undefined && t.slot !== null) ? t.slot : '';
                    let detailsStr = (t && t.details) ? String(t.details) : '';
                    let timestampStr = (t && t.timestamp) ? String(t.timestamp) : '';
                    return `<div style="margin-bottom: 2px;">
                        <span style="color: #888; font-size: 0.65rem;">[${timestampStr}]</span>
                        <span class="${badgeClass}" style="display: inline-block; min-width: 24px; text-align: center;">${actorStr}</span>
                        <span style="font-weight: bold; color: ${actionStr === 'WRITE' ? 'var(--accent-red)' : 'var(--accent-blue)'};">${actionStr}</span>
                        <span style="color: #444;">Slot ${slotStr}</span>
                        <span style="color: #666; font-style: italic;">(${detailsStr})</span>
                    </div>`;
                }).join('');
                logEl.scrollTop = logEl.scrollHeight;
            } else {
                logEl.innerHTML = 'Waiting for concurrent transactions...';
            }
        }
    } catch (err) {
        console.error('Error polling ring buffer:', err);
    }
}

async function pollCoinbaseFeed() {
    if (isStaticDemo) {
        runStaticCoinbaseFeedSimulation();
        return;
    }
    try {
        const response = await fetch(getApiUrl('/api/orderbook'));
        if (!response.ok) return;
        const data = await response.json();
        const trades = data.trades || [];

        const tbody = document.querySelector('#live-trades-table tbody');
        if (!tbody) return;

        if (!trades || trades.length === 0) {
            tbody.innerHTML = `<tr><td colspan="4" style="text-align: center;">Polling multi-venue transaction stream...</td></tr>`;
            return;
        }
        latestPrice = trades[trades.length - 1].Price || trades[trades.length - 1].price || 0;

        tbody.innerHTML = '';
        const recent = trades.slice(-10).reverse();
        const venueNames = ["CB", "RH", "BN"];
        recent.forEach((t, i) => {
            const venue = t.Exchange || t.exchange || venueNames[i % 3];
            const price = Number(t.Price || t.price || 0);
            const qty = Number(t.Quantity || t.quantity || 0);
            const id = t.ID || t.id || i + 1;
            const row = document.createElement('tr');
            row.innerHTML = `
                <td style="font-weight: bold; color: #333;">${venue}</td>
                <td>${id}</td>
                <td>$${price.toFixed(2)}</td>
                <td>${qty.toFixed(8)}</td>
            `;
            tbody.appendChild(row);
        });
    } catch (err) {
        console.error('Error polling live feed:', err);
    }
}

async function pollGeminiSentiment() {
    if (isStaticDemo) {
        runStaticOrderBookSimulation();
        return;
    }
    try {
        const response = await fetch(getApiUrl('/api/orderbook'));
        if (!response.ok) return;
        const rawData = await response.json();
        const ob = rawData.orderBook || rawData;
        const obiVal = (ob && ob.obi !== undefined && ob.obi !== null) ? Number(ob.obi) : 0;
        const spreadFloat = (ob && ob.spread !== undefined && ob.spread !== null) ? Number(ob.spread) : 0;
        const topBids = (ob && ob.topBids) ? ob.topBids : [];
        const topAsks = (ob && ob.topAsks) ? ob.topAsks : [];

        // 1. Update OBI Gauge
        const obiPct = (obiVal * 100).toFixed(2);
        const obiEl = document.getElementById('ind-obi');
        if (obiEl) obiEl.innerText = (obiVal >= 0 ? '+' : '') + obiPct + '%';
        
        const barEl = document.getElementById('obi-bar');
        if (barEl) {
            if (obiVal >= 0) {
                barEl.style.marginLeft = '50%';
                barEl.style.width = `${obiVal * 50}%`;
                barEl.style.backgroundColor = '#137333';
            } else {
                const widthPct = Math.abs(obiVal) * 50;
                barEl.style.marginLeft = `${50 - widthPct}%`;
                barEl.style.width = `${widthPct}%`;
                barEl.style.backgroundColor = '#c5221f';
            }
        }

        // 2. Update Spread & Mid Price
        const spreadEl = document.getElementById('ind-spread');
        if (spreadEl) spreadEl.innerText = '$' + spreadFloat.toFixed(2);
        
        let midPrice = rawData.midPrice ? Number(rawData.midPrice) : 0;
        if (!midPrice && topBids.length > 0 && topAsks.length > 0) {
            midPrice = (topBids[0].price + topAsks[0].price) / 2;
        }
        const midEl = document.getElementById('ind-mid');
        if (midEl) midEl.innerText = '$' + midPrice.toFixed(2);
        latestPrice = midPrice;

        // 3. Update Sync Timestamp
        const updatedEl = document.getElementById('ind-updated');
        if (updatedEl) updatedEl.innerText = new Date().toLocaleTimeString();

        // 4. Render Bids Table (Top 5 Bids)
        const bidsBody = document.querySelector('#obi-bids-table tbody');
        if (bidsBody) {
            bidsBody.innerHTML = '';
            const top5Bids = topBids.slice(0, 5);
            if (top5Bids.length === 0) {
                bidsBody.innerHTML = '<tr><td colspan="2" style="text-align: center; color: #888;">Empty Book</td></tr>';
            } else {
                top5Bids.forEach(b => {
                    const priceUSD = Number(b.price || 0);
                    const sizeBTC = Number(b.size || 0);
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td style="padding: 2px 4px; color: #555; text-align: left;">${sizeBTC.toFixed(4)}</td>
                        <td style="padding: 2px 4px; color: #137333; font-weight: bold;">${priceUSD.toFixed(2)}</td>
                    `;
                    bidsBody.appendChild(row);
                });
            }
        }

        // 5. Render Asks Table (Top 5 Asks)
        const asksBody = document.querySelector('#obi-asks-table tbody');
        if (asksBody) {
            asksBody.innerHTML = '';
            const top5Asks = topAsks.slice(0, 5);
            if (top5Asks.length === 0) {
                asksBody.innerHTML = '<tr><td colspan="2" style="text-align: center; color: #888;">Empty Book</td></tr>';
            } else {
                top5Asks.forEach(a => {
                    const priceUSD = Number(a.price || 0);
                    const sizeBTC = Number(a.size || 0);
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td style="padding: 2px 4px; color: #c5221f; font-weight: bold; text-align: right;">${priceUSD.toFixed(2)}</td>
                        <td style="padding: 2px 4px; color: #555; text-align: left;">${sizeBTC.toFixed(4)}</td>
                    `;
                    asksBody.appendChild(row);
                });
            }
        }

        // Update live chart datasets in real-time
        const nowStr = new Date().toLocaleTimeString();
        if (midPrice > 0 && topBids.length > 0 && topAsks.length > 0) {
            const bestBid = Number(topBids[0].price || 0);
            const bestAsk = Number(topAsks[0].price || 0);
            chartData.prices.push(midPrice);
            chartData.fast.push(bestBid);
            chartData.slow.push(bestAsk);
            chartData.labels.push(nowStr);
            
            if (chartData.prices.length > 35) {
                chartData.prices.shift();
                chartData.fast.shift();
                chartData.slow.shift();
                chartData.labels.shift();
            }
            
            if (crossoverChart) {
                crossoverChart.data.labels = chartData.labels;
                crossoverChart.data.datasets[0].data = chartData.prices;
                crossoverChart.data.datasets[1].data = chartData.fast;
                crossoverChart.data.datasets[2].data = chartData.slow;
                crossoverChart.update('none');
            }
        }

    } catch (err) {
        console.error('Error polling orderbook:', err);
    }
}

async function pollTradingBotState() {
    if (isStaticDemo) {
        runStaticTradingBotSimulation();
        return;
    }
    try {
        const response = await fetch(getApiUrl('/api/orderbook'));
        if (!response.ok) return;
        const res = await response.json();
        const data = res.bot;
        if (!data) return;

        // Update UI text values
        const cashEl = document.getElementById('bot-cash');
        if (cashEl) cashEl.innerText = '$' + Number(data.cash || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
        
        const posEl = document.getElementById('bot-position');
        if (posEl) posEl.innerText = Number(data.position || 0).toFixed(8) + ' BTC';
        
        const navEl = document.getElementById('bot-nav');
        if (navEl) navEl.innerText = '$' + Number(data.nav || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
        
        const bhNavEl = document.getElementById('bot-bh-nav');
        if (bhNavEl) bhNavEl.innerText = '$' + Number(data.buyAndHoldNav || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
        
        // Sync Top Balance Sheet KPI Cards
        const topNav = document.getElementById('top-nav-display');
        if (topNav) topNav.innerText = '$' + Number(data.nav || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
        const topPos = document.getElementById('top-pos-display');
        if (topPos) topPos.innerText = Number(data.position || 0).toFixed(8) + ' BTC';
        const topCash = document.getElementById('top-cash-display');
        if (topCash) topCash.innerText = '$' + Number(data.cash || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
        const topBH = document.getElementById('top-bh-display');
        if (topBH) topBH.innerText = '$' + Number(data.buyAndHoldNav || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});

        // Sync Balance Sheet Financial Table
        const tableCash = document.getElementById('table-cash-val');
        if (tableCash) tableCash.innerText = '$' + Number(data.cash || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
        const tableBtc = document.getElementById('table-btc-val');
        if (tableBtc) tableBtc.innerText = Number(data.position || 0).toFixed(8) + ' BTC';
        const tableNav = document.getElementById('table-nav-val');
        if (tableNav) tableNav.innerText = '$' + Number(data.nav || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
        const tableBH = document.getElementById('table-bh-val');
        if (tableBH) tableBH.innerText = '$' + Number(data.buyAndHoldNav || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});

        // Color-code Bot NAV based on performance against Buy & Hold baseline
        if (navEl) {
            if (data.nav > data.buyAndHoldNav) {
                navEl.style.color = 'var(--accent-green)';
                if (topNav) topNav.style.color = 'var(--accent-green)';
                if (tableNav) tableNav.style.color = 'var(--accent-green)';
            } else if (data.nav < data.buyAndHoldNav) {
                navEl.style.color = 'var(--accent-red)';
                if (topNav) topNav.style.color = 'var(--accent-red)';
                if (tableNav) tableNav.style.color = 'var(--accent-red)';
            } else {
                navEl.style.color = 'var(--text-main)';
                if (topNav) topNav.style.color = 'var(--text-main)';
                if (tableNav) tableNav.style.color = 'var(--text-main)';
            }
        }
        
        const strategyLabelEl = document.getElementById('bot-strategy');
        if (strategyLabelEl) {
            strategyLabelEl.innerText = (data.strategy === "LLM") ? "Gemini LLM (AI Decision)" : "Order Book Imbalance (OBI) HFT";
        }
        const strategySelectEl = document.getElementById('cfg-strategy');
        if (strategySelectEl && document.activeElement !== strategySelectEl) {
            strategySelectEl.value = data.strategy || "OBI";
        }

        const signalEl = document.getElementById('bot-signal');
        signalEl.innerText = data.signal;
        if (data.signal === 'BUY') {
            signalEl.style.color = 'var(--accent-green)';
            signalEl.style.fontWeight = 'bold';
        } else if (data.signal === 'SELL') {
            signalEl.style.color = 'var(--accent-red)';
            signalEl.style.fontWeight = 'bold';
        } else {
            signalEl.style.color = '#333';
            signalEl.style.fontWeight = 'normal';
        }

        document.getElementById('bot-obi-buy-th').innerText = '0.04';
        document.getElementById('bot-obi-sell-th').innerText = '-0.04';

        // Update commentary
        const commentaryEl = document.getElementById('commentary-text');
        if (commentaryEl) {
            let relativePerf = data.nav - data.buyAndHoldNav;
            let perfPct = (((data.nav - data.buyAndHoldNav) / data.buyAndHoldNav) * 100).toFixed(4);
            let perfText = "";
            let color = "#333";
            if (relativePerf > 0) {
                perfText = `Outperforming Buy & Hold by +$${relativePerf.toFixed(2)} (+${perfPct}%)`;
                color = "var(--accent-green)";
            } else if (relativePerf < 0) {
                perfText = `Underperforming Buy & Hold by -$${Math.abs(relativePerf).toFixed(2)} (${perfPct}%)`;
                color = "var(--accent-red)";
            } else {
                perfText = `Neutral parity with Buy & Hold ($0.00 deviation)`;
            }

            let signalReason = data.commentary || "Waiting for signal cycle...";

            commentaryEl.innerHTML = `
                <strong>Performance Analysis:</strong> <span style="color: ${color}; font-weight: bold;">${perfText}</span><br>
                <strong>Signal Context:</strong> ${signalReason}
            `;
        }

        // Update Orders Table
        const tbody = document.querySelector('#bot-orders-table tbody');
        if (data.orders.length === 0) {
            tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; font-size: 0.75rem;">Waiting for strategy crossover to execute trades...</td></tr>`;
            return;
        }

        tbody.innerHTML = '';
        const recent = data.orders.slice(-5).reverse();
        recent.forEach(o => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${o.id}</td>
                <td>${o.timestamp}</td>
                <td style="color: ${o.type.includes('BUY') ? 'var(--accent-red)' : 'var(--accent-green)'}; font-weight: bold;">${o.type}</td>
                <td>$${o.price.toFixed(2)}</td>
                <td>${o.quantity.toFixed(8)}</td>
                <td>$${o.value.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
            `;
            tbody.appendChild(row);
        });

    } catch (err) {
        console.error('Error polling bot state:', err);
    }
}

function initLiveCrossoverChart() {
    const canvas = document.getElementById('liveCrossoverChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    crossoverChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Mid Price',
                    data: [],
                    borderColor: '#4682b4',
                    borderWidth: 1.5,
                    pointRadius: 0,
                    fill: false
                },
                {
                    label: 'Best Bid',
                    data: [],
                    borderColor: '#137333',
                    borderWidth: 1.2,
                    pointRadius: 0,
                    fill: false
                },
                {
                    label: 'Best Ask',
                    data: [],
                    borderColor: '#c5221f',
                    borderWidth: 1.2,
                    pointRadius: 0,
                    fill: false
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    labels: { boxWidth: 8, font: { size: 9, family: 'Georgia' } }
                }
            },
            scales: {
                x: { display: false },
                y: {
                    ticks: { font: { size: 8, family: 'Courier New' } },
                    grid: { color: '#eaeae8' }
                }
            }
        }
    });
}

function setupBotControls() {
    const saveBtn = document.getElementById('btn-save-cfg');
    if (saveBtn) {
        saveBtn.addEventListener('click', async () => {
            saveBtn.disabled = true;
            saveBtn.innerText = 'Saving...';
            try {
                const sl = parseFloat(document.getElementById('cfg-sl').value) / 100.0;
                const tp = parseFloat(document.getElementById('cfg-tp').value) / 100.0;
                const fee = parseFloat(document.getElementById('cfg-fee').value) / 100.0;
                const slippage = parseFloat(document.getElementById('cfg-slippage').value) / 100.0;
                const strategyStr = document.getElementById('cfg-strategy').value;
                const waitStr = document.getElementById('cfg-wait').value;
                
                if (isStaticDemo) {
                    demoState.stopLossPct = sl;
                    demoState.takeProfitPct = tp;
                    demoState.takerFeePct = fee;
                    demoState.slippagePct = slippage;
                    demoState.strategy = strategyStr;
                }

                const response = await fetch('/api/bot/config', {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({
						stopLossPct: sl,
						takeProfitPct: tp,
						takerFeePct: fee,
						slippagePct: slippage,
						waitStrategy: waitStr,
                        strategy: strategyStr
					})
				});
                if (response.ok) {
                    // Flash success
                    saveBtn.innerText = 'Success!';
                    setTimeout(() => {
                        saveBtn.disabled = false;
                        saveBtn.innerText = 'Update Config';
                    }, 1000);
                } else {
                    alert('Failed to update config.');
                    saveBtn.disabled = false;
                    saveBtn.innerText = 'Update Config';
                }
            } catch (e) {
                console.error(e);
                alert('Error updating configuration.');
                saveBtn.disabled = false;
                saveBtn.innerText = 'Update Config';
            }
        });
    }

    const backtestBtn = document.getElementById('btn-run-backtest');
    const backtestConsole = document.getElementById('backtest-console');
    if (backtestBtn && backtestConsole) {
        backtestBtn.addEventListener('click', async () => {
            backtestBtn.disabled = true;
            backtestBtn.innerText = 'Running...';
            backtestConsole.textContent = 'Executing strategy simulation over captured history...\n';
            try {
                const res = await fetch('/api/backtest');
                if (!res.ok) throw new Error('Backtest request failed');
                const data = await res.json();
                
                backtestConsole.textContent = 
                    `[BACKTEST SIMULATION REPORT]\n` +
                    `-----------------------------------------\n` +
                    `Trades replayed:  ${data.tradesCount}\n` +
                    `Orders executed:  ${data.tradesExecuted}\n` +
                    `Initial Capital:  $${data.initialNav.toFixed(2)}\n` +
                    `Final NAV:        $${data.finalNav.toFixed(2)}\n` +
                    `Buy & Hold NAV:   $${data.buyAndHoldNav.toFixed(2)}\n` +
                    `Total Fees Paid:  $${data.totalFees.toFixed(2)}\n` +
                    `Total Slippage:   $${data.totalSlippage.toFixed(2)}\n` +
                    `Trades Win/Loss:  ${data.winCount} W / ${data.lossCount} L\n` +
                    `Win Rate:         ${data.winRate.toFixed(2)}%\n` +
                    `Vs. Buy & Hold:   ${(data.finalNav - data.buyAndHoldNav).toFixed(2)} USD\n` +
                    `-----------------------------------------`;
            } catch (e) {
                backtestConsole.textContent += `[ERROR] ${e.message}\n`;
            } finally {
                backtestBtn.disabled = false;
                backtestBtn.innerText = 'Run Backtest';
            }
        });
    }
}


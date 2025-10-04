[![CI](https://github.com/fa993/mqtt-sql-connector/actions/workflows/rust.yml/badge.svg)](https://github.com/fa993/mqtt-sql-connector/actions/workflows/rust.yml)

# Immediate Goals

- [x] Stress test
- [x] Add some form of batching logic for db write
- [x] Make all hard coded values from .env
- [ ] Use an explicit logger and add more logs (as revealed by stress test)
- [ ] Better error printing (can be taken up as part of logger)
- [ ] Structure eventloops cleanly
- [ ] Generify the mapper functions

# 🧪 Stress Test Result

**Test Environment**

- **Device:** M1 MacBook Air (2020)  
- **Execution Flavor:** `current_thread`  
- **Duration:** 60 seconds per scenario  
- **Observation:** Flame graph analysis revealed that **87% of CPU time** is spent in the poll loop.

---

## 📊 Latency Summary by Message Rate

| Rate          | P99 (s)       | P95 (s)       | P90 (s)       | P50 (s)   | Avg Latency (s)        | Max (s)   | Min (s)   |
|----------------|---------------|---------------|---------------|-----------|------------------------|-----------|-----------|
| **10 msg/sec**    | 0.002203 | 0.001365 | 0.001205 | 0.000578 | 0.000710 | 0.009699 | 0.000175 |
| **100 msg/sec**   | 0.007711 | 0.000732 | 0.000512 | 0.000264 | 0.000536 | 0.056326 | 0.000098 |
| **1000 msg/sec**  | 0.005324 | 0.001122 | 0.000774 | 0.000170 | 0.000503 | 0.062094 | 0.000051 |
| **10000 msg/sec** | 0.052992 | 0.003011 | 0.001252 | 0.000596 | 0.002255 | 0.286005 | 0.000042 |

---

## 🔍 Observations

- **Low message rates (10–100 msg/sec)** maintain sub-millisecond latency across all percentiles.  

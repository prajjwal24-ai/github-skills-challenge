# AIOps Operational Data & Event Streaming Assessment

## 1. AIOps Scenario Explanation
This project simulates an event-driven AIOps monitoring pipeline for an application service (`payment-service`). Telemetry data containing system performance metrics and log records is continuously analyzed by an anomaly detector. When anomalous behavior or errors are detected, structured anomaly events are generated, published to an event streaming topic, consumed by a subscriber, and processed for operational insights.

---

## 2. Description of Operational Data
The operational data (`data/service_data.json`) consists of 10 chronological telemetry records containing both metric and log fields:
- **Metric Fields:** `response_time_ms`, `cpu_percent`, `memory_percent`
- **Log Fields:** `timestamp`, `service`, `log_level`, `message`

---

## 3. Observations from Logs and Metrics
1. **Metric Fields:** Quantifiable performance indicators including service latency (`response_time_ms`), CPU usage (`cpu_percent`), and RAM usage (`memory_percent`).
2. **Log Information Fields:** Event context including `timestamp`, service name (`service`), log severity level (`log_level`), and textual event status (`message`).
3. **Timestamp Usage:** Timestamps formatted in ISO 8601 (`YYYY-MM-DDTHH:MM:SS`) chronologically sequence telemetry events, allowing correlation between metric spikes and log errors over time.
4. **Normal Behaviour:** Records from `10:00:00` to `10:04:00` and `10:07:00` to `10:09:00` exhibit nominal performance (`response_time_ms` ~120–150ms, CPU ~42–50%, Memory ~51–57%, `log_level == "INFO"`).
5. **Unusual Behaviour:** 
   - `2026-09-20T10:05:00`: High response time (610 ms) with log level `ERROR` ("Payment service timeout").
   - `2026-09-20T10:06:00`: Severe performance degradation with response time 640 ms, CPU at 94%, Memory at 91%, and log level `ERROR` ("Database connection timeout").

---

## 4. Anomaly Detection Findings
- **Anomalies Detected:** Two records were correctly identified as anomalous:
  1. `10:05:00`: Response time exceeded threshold (610ms > 500ms) and error log present.
  2. `10:06:00`: Response time (640ms), CPU (94%), and Memory (91%) all exceeded thresholds (500ms, 80%, 80% respectively) along with an error log.
- **Missed / False Events:** Initially, `ERROR` level logs were missed due to a bug in the detector logic checking only for `"WARNING"`. Once corrected, all anomalies were captured without false positives on normal records.
- **Limitation / Improvement:** Static rule-based thresholds fail to adapt to varying workloads. Implementing dynamic thresholding (e.g., standard deviation over a rolling window) or machine learning models (such as Isolation Forest) would reduce false positives during heavy legitimate traffic.

---

## 5. Description of Event-Processing Flow
1. **Operational Data:** Telemetry JSON ingested sequentially.
2. **Anomaly Detection:** `AnomalyDetector` evaluates metrics and logs against defined thresholds.
3. **Event Generation:** Detected anomalies create structured payload dictionary objects.
4. **Producer (`EventProducer`):** Publishes anomaly events to the designated event topic.
5. **Topic (`EventTopic`):** Serves as an in-memory queue holding published events.
6. **Consumer (`EventConsumer`):** Consumes messages from the topic.
7. **Downstream AIOps:** Consumed events are formatted and presented for operational investigation.

---

## 6. Final Workflow Execution Result
Running `python src/aiops_pipeline.py` produced the following result:
- **Records Processed:** 10
- **Anomalies Detected:** 2
- **Events Consumed:** 2

---

## 7. Issues Identified and Corrected
1. **Bug 1 — Anomaly Detector (`src/anomaly_detector.py`):**
   - *Affected Component:* `AnomalyDetector`
   - *Cause:* Log level check evaluated `record["log_level"] == "WARNING"`, ignoring `"ERROR"` entries.
   - *Correction:* Updated logic to `if record["log_level"] in ["WARNING", "ERROR"]:`.
2. **Bug 2 — Pipeline Topic Mismatch (`src/aiops_pipeline.py`):**
   - *Affected Component:* Event streaming system pipeline (`EventProducer` and `EventConsumer`).
   - *Cause:* Producer and Consumer were instantiated with separate `EventTopic` instances (`service-events` and `anomaly-events`), causing published events to be lost.
   - *Correction:* Configured producer and consumer to share a single `EventTopic("anomaly-events")` instance.

---

## 8. Limitation or Possible Improvement
- **Static Thresholding Limitation:** Hardcoded thresholds (e.g., CPU > 80%) do not adjust to normal operational load patterns and can cause false alarms during scheduled batch processing.
- **Proposed Improvement:** Implement dynamic adaptive baselining using rolling statistics or an ML anomaly detection algorithm.

---

## 9. Steps to Reproduce
1. Execute the main AIOps end-to-end pipeline script:
   ```bash
   python src/aiops_pipeline.py
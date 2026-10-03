---
name: drasiwake-sensor-reading-summary
kind: meta
description: Summarize sensor readings returned by the DrasiWake sensor query.
triggers:
  - summarize sensor readings
  - sensor reading summary
  - summarize sensor data
meta_priority: 50
always: false
final_text_mode: "step:summarize"
composition:
  steps:
    - id: summarize
      kind: llm_chat
      with:
        system: >-
          Summarize the supplied sensor-reading JSON in one or two concise sentences.
          Report only sensor identifiers, temperatures, and humidity values present
          in the input. Do not infer thresholds, anomalies, causes, trends, or actions.
          If a field is absent, say it was not provided. Do not invent readings.
        task: "{{ input | xml_escape | truncate(4000) }}"
---

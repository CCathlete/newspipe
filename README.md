Newspipe is an AI-based autonomous news ingestion and streaming platform, all open source and free. It polls geopolitics RSS feeds, enriches each article with an LLM summary and dashboard tags, and lands data + metadata as one unit in the data lake's bronze layer.

Scala 3 + sbt rewrite (feat-1): RSS feeds replace website scraping, `opencode run` CLI calls replace LLM HTTP calls, and Delta Lake replaces JSON bronze.

Infra -
https://github.com/CCathlete/pipeline_infra

Business logic (RSS ingestion + lakehouse loading) -
https://github.com/CCathlete/newspipe


            ┌──────────────┐
            │  RSS feeds   │
            │ geopolitics  │
            └──────┬───────┘
                   │
              raw_rss (Kafka)
                   │
                   ▼
         ┌──────────────────┐
         │ opencode run CLI │
         │ summary + tags   │
         │ countries,       │
         │ geo-area, topics │
         └────────┬─────────┘
                  │
                  ▼
         ┌──────────────────┐
         │  Delta bronze    │
         │  one row/article │
         │  data + metadata │
         └──────────────────┘

Hexagonal layout under `src/main/scala/newspipe/`: domain > application > infrastructure > control.

Run it:

```sh
sbt compile   # build
sbt test      # full suite (proves feat-1 criteria 1-7)
sbt run       # poll feeds -> Kafka -> enrich -> Delta bronze
```

Configure feeds, the opencode model, Kafka, and the bronze path in `src/main/resources/application.conf`. Design spec: `.LLMDD/spec-driven/features/feat-1-scala-rss-opencode-bronze-pipeline.md`.


The setup itself is running on a a few mobile devices performing as micro servers and a laptop that does the heavy lifting.
It's an ai based autonomous web scraper + data ingestion and streaming platform, all open source and free.

Infra - 
https://github.com/CCathlete/pipeline_infra

Business logic (stream scraping + lakehouse loading) -
https://github.com/CCathlete/newspipe



            ┌──────────────┐
            │ discovery app│
            │ asyncio      │
            └──────┬───────┘
                   │
            discovery_queue
                   │
                   ▼
             crawler + chunker
                   │
               raw_chunks
                   │
                   ▼
            ┌──────────────┐
            │ ingestion app│
            │ blocking     │
            │ Spark owner  │
            └──────┬───────┘
                   │
                 lakehouse


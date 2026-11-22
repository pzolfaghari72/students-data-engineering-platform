# System context

```mermaid
flowchart LR
    DBA["Source DBA"] -->|Approves access and CDC prerequisites| SQL[("External SQL Server")]
    SQL --> P["Student Data Engineering Platform"]
    O["Platform operator"] -->|Triggers batch and monitors CDC| P
    P -->|Published academic metrics| A["Analytics reader"]
    P -->|Publication references and lag| O
```

A source DBA owns change capture, access and source retention. A platform operator owns deployment, workflow runs and recovery within the local environment. An analytics reader sees published measures. Shared storage and the common dashboard are inside the platform boundary, with independently published batch and CDC models.

For detailed views, see the [deployment](deployment-architecture.md), [batch sequence](batch-pipeline.md), [streaming sequence](streaming-pipeline.md), and [source and warehouse models](../data-model.md).

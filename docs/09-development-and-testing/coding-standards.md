# Coding and Configuration Standards

The repository uses small Python modules with explicit environment reads,
standard logging, type hints in core loader/stream modules, SQL DDL and
Compose YAML. Follow existing component boundaries.

- Use required environment settings for connection endpoints and secrets;
  fail explicitly when missing. Do not add host/database/credential
  fallbacks to application code.
- Keep snapshot extraction, CDC event normalization, Spark batch and Spark
  streaming responsibilities separate.
- Preserve source keys, LSN metadata, Kafka position and load/generation IDs
  through processing.
- Raise on invalid envelopes or failed correctness checks; do not silently
  discard records.
- Keep operational logs free of credentials and personal student data.
- Add focused unit tests for new normalization, ordering, retry or publication
  behavior; integration claims need live validation evidence.
- Put schema changes in the corresponding DDL and update the
  [data model](../02-architecture/data-model.md).

Do not treat the current local Compose stack as a security baseline for
production. TLS, secrets management, least privilege and backup policy need
separate review.

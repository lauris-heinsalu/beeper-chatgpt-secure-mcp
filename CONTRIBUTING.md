# Contributing

Issues and focused pull requests are welcome.

The central architectural constraint is deliberate: the Events sidecar must remain isolated from Beeper's stable native MCP tool path. Changes should not turn the sidecar into a proxy for search, conversation reads, or message sends unless the project direction explicitly changes.

Before opening a pull request:

1. Keep credentials, private message content, account IDs, chat IDs, sender IDs, tunnel IDs, host details, and runtime databases out of commits and test fixtures.
2. Add or update tests for behavior changes, especially around idempotency, retries, reconciliation, authentication, callback validation, and concurrency.
3. Run:

   ```bash
   pytest -q -W error
   ruff check src tests
   mypy --no-incremental --ignore-missing-imports src
   ```

4. Keep deployment examples illustrative and location-independent. Use placeholders rather than real usernames, tunnel IDs, credentials, or host paths.
5. Prefer small correctness or maintainability improvements over stylistic churn.

For security-sensitive findings, follow [SECURITY.md](SECURITY.md) rather than opening a detailed public issue.

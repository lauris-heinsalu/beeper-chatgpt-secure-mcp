# Security policy

## Supported versions

Security fixes are applied to the latest release line. At initial publication, that is `v1.0.x`.

## Reporting a vulnerability

Please do not post credentials, private message content, exploit details, host identifiers, or other sensitive deployment data in a public issue.

If GitHub's private vulnerability reporting is available for this repository, use **Security → Report a vulnerability**. If it is not available, open a minimal issue stating that you need a private channel for a security report, without including sensitive details.

For ordinary non-sensitive bugs, use the public issue tracker.

## Security boundary

This repository is a reference implementation for a single-owner private deployment. In particular:

- the MCP listeners are intended to remain loopback-only behind OpenAI Secure MCP Tunnel;
- local bearer and tunnel credentials must stay outside the repository with owner-only filesystem permissions;
- webhook callbacks are expected to use HTTPS and globally routable destinations;
- event payloads intentionally omit message text; and
- a multi-user deployment would require a real authenticated principal model and per-user authorization controls.

Publishing this repository does not publish or expose any live Beeper, messaging, tunnel, or ChatGPT connection.

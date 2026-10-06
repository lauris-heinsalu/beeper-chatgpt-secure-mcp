# A malformed key can start at the password prompt

For a separately reproduced reboot/startup-order problem and its tested fix, see the [README's service section](README.md#5-run-the-tunnel-as-a-service).

During the original setup, a Windows Python `getpass()` / Ctrl+V problem was reported alongside:

```text
control plane API key is malformed
```

The original secret bytes were not recovered, so this write-up does not claim to reconstruct the exact malformed value. The observed setup history is nevertheless stronger than a purely hypothetical explanation: the failing helper ran under **Python 3.13.14** and used Windows `getpass.win_getpass()`, whose input path reads characters through `msvcrt.getwch()`. A first revised helper that simply ignored Ctrl+V produced an empty credential file; a second revision handled Ctrl+V by reading the Windows clipboard directly, after which `tunnel-client doctor --profile beeper --explain` immediately passed.

## Verified mechanism

On Windows, `getpass.win_getpass()` reads character-by-character and does not itself implement clipboard paste semantics. If Ctrl+V reaches it as U+0016 rather than as pasted clipboard text, that control character can be retained in the entered value. The original Python 3.13.14 behavior and helper-fix sequence above are the practical evidence from this setup. Separately, a synthetic test on Windows Python **3.12.14**, replacing the character input function with dummy input, confirmed that an injected U+0016 survives the password reader and `.strip()`. The [CPython implementation](https://github.com/python/cpython/blob/3.13/Lib/getpass.py) provides the source-level explanation.

The installed tunnel client independently rejects characters outside its permitted API-key character set before attempting authentication. Its [validator at the inspected revision](https://github.com/openai/tunnel-client/blob/a390c168ff1b2d14e73a95991c186c6aba3ff5a0/pkg/runtimeconfig/config.go#L93) returns the error above for malformed input. That makes a retained control character a sufficient explanation for this error, but not proof of what happened in the original terminal. Other malformed values produce the same message.

## Diagnose without leaking the key

Validate the value in memory, and report only a pass/fail result:

```python
import re

def check_runtime_key(value):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("Unexpected key characters; re-enter the credential.")
```

This mirrors the inspected client's character restriction; it is not a general OpenAI key-format contract and does not check authorization. Do not print `repr(key)`, the key's prefix, character dumps, or the file contents. Reject corrupted input instead of silently deleting characters and hoping the result is the intended secret.

If paste behavior is uncertain, test the terminal with an obviously fake value first. Use a terminal-supported paste action or a trusted secret-entry workflow, then verify that the value was accepted without exposing it. Do not assume every Windows terminal or every Python release handles Ctrl+V the same way.

### Harpoon loopback warning

The inspected setup also emitted a Harpoon auto-registration warning because Beeper's OAuth metadata referenced a loopback `http://127.0.0.1` origin. The **main Beeper MCP channel still initialized and worked successfully** with the static Authorization header. Do not enable plaintext-HTTP Harpoon options merely to silence that warning unless you actually need Harpoon/OAuth behavior and have reviewed the security implications.

## Keep error layers separate

| Symptom | First check |
|---|---|
| Local malformed-key validation | The OpenAI key's input/storage path; accidental whitespace or control characters |
| OpenAI HTTP authentication or permission failure | Runtime credential, tunnel-use role, organization context |
| Beeper MCP HTTP `401` | Beeper token and its local Authorization header |
| Healthy tunnel but failed discovery | Native MCP initialization, configured URL, port, and headers |
| Unreadable encrypted chats | Beeper sign-in, device verification, and initial sync |

The practical lesson is to diagnose the failing layer before changing unrelated configuration. A native endpoint returning `401` establishes an authentication boundary; successful authenticated MCP initialization and tool discovery establish considerably more.

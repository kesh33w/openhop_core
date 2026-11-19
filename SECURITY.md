# Security

Report suspected vulnerabilities in this fork privately, using GitHub's **Report a vulnerability** button on the Security tab, not in a public issue. For problems in upstream code that this fork didn't change, also tell [openhop-dev](https://github.com/openhop-dev).

The library never logs channel secrets or identity private keys. The reception capture file is opened with `O_NOFOLLOW` and mode 0600. Never commit keys, credentials or live configuration; use placeholders in tests and docs.

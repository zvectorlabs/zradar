# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| Latest release | Yes |
| Older releases | No — please upgrade |

## Reporting a vulnerability

**DO NOT open a public GitHub issue for security vulnerabilities.**

Use GitHub's private vulnerability reporting — this creates a draft advisory visible only to maintainers, with no public trace:

**[Report a vulnerability →](https://github.com/zvectorlabs/zradar/security/advisories/new)**

You can describe the issue, attach a proof-of-concept, and collaborate on a fix entirely in private. We coordinate the public advisory after the patch ships.

If you cannot use GitHub's reporting, email **security@zvectorlabs.com** as a fallback.

Please include:

- Description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept
- Affected versions
- Any suggested mitigations you are aware of

**Response timeline:** 48-hour acknowledgement → 5-business-day assessment → coordinated fix and public advisory within 90 days (sooner when a patch is ready faster).

## Scope

This policy covers the zradar server binary and all crates in this repository. It does not cover third-party dependencies — please report those upstream.

## Out of scope

- Denial-of-service attacks requiring extraordinary resource consumption
- Attacks that require physical access to the host
- Social engineering attacks against contributors or maintainers

## Recognition

We appreciate responsible disclosure. Contributors who report valid security vulnerabilities will be credited in the release advisory unless they prefer to remain anonymous.

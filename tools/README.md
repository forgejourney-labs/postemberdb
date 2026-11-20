# PostemberDB tools

This directory contains two independent operator applications.

| Application | Purpose | UI |
|---|---|---|
| [Browser](browser/README.md) | Native desktop database browser | Compact classic desktop UI inspired by 2014–2015 tools |
| [Admin](admin/README.md) | Local web administration panel | Browser-based, phpMyAdmin-style layout |

Both use the Rust PostemberDB library for embedded/local access. Remote connections are a separate milestone and must use a stable authenticated protocol with TLS, not the current development JSONL/1 transport.

## Safety boundary

- Admin binds to loopback only.
- Do not expose development protocols to untrusted networks.
- Remote support requires TLS, authentication, authorization, bounded requests, and explicit permission checks.
- Neither application silently migrates a database or runs destructive maintenance operations.

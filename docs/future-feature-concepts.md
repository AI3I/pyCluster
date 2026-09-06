# Future Feature Concepts

Updated: 2026-09-05

This document preserves design ideas for later evaluation. It is not a release
commitment. GitHub issues and milestones remain authoritative for scheduled
work, and the changelog records only implemented behavior.

## Governing Principles

- Standalone operation must remain complete and first-class.
- Participation in shared infrastructure must be explicit and reversible.
- Nodes authenticate identities but retain local authorization policy.
- Telnet and web must use the same database-backed account, preference, and
  filter records.
- Credentials, password hashes, TOTP secrets, user directories, private mail,
  and registration queues must never be exchanged through `PY`.
- Failure of optional infrastructure must not prevent local authentication or
  ordinary node operation.

## pycluster.net Node Directory

Offer verified operators stable names such as
`ai3i-15.pycluster.net`. Naming is independently useful and should precede any
identity federation.

Potential service features:

- token-authenticated dynamic DNS updates
- A and AAAA records, with CNAME support where appropriate
- callsign and control-of-node verification
- explicit handling or rejection of portable calls containing `/`
- rapid revocation and an acceptable-use policy for delegated names
- DNS-01 ACME delegation so node operators obtain certificates without sharing
  private keys with the directory operator

A worldwide login name should not be an unmanaged DNS round-robin. Pool
membership should be separately opt-in and require health checks, current
software, and a usable new-user policy. Regional names such as
`na.pycluster.net` and `eu.pycluster.net` could use health-checked or
latency-aware answers. A dead node must be removed automatically rather than
remaining a random DNS result until its TTL expires.

Open decisions:

- DNS provider and health-check implementation
- required node verification and renewal interval
- eligibility requirements for regional and worldwide pools
- privacy policy and data-retention requirements
- whether naming is available to non-pyCluster DX nodes

## Optional Federated Identity

The preferred model is optional identity assertions, not a mandatory master
user database. The provider asserts that a subject controls a callsign and
states an assurance level. Each node independently decides whether to accept
that assertion and which local access it grants.

Required boundaries:

- Existing local accounts cannot be linked or replaced silently.
- Linking an existing account requires a fully authenticated local session.
- New-account provisioning follows the destination node's registration policy.
- External identity can never grant System Operator privilege.
- Assertions are audience-bound to one destination node.
- Nodes never send local passwords or password hashes to a home node or central
  service.
- There is no bulk user-directory endpoint.
- Local passwords continue working when the provider is unavailable.
- Solo nodes make no provider requests.

`PY01` can advertise a participating node's public HTTPS endpoint, but identity
assertions and profiles should travel over a separately authenticated HTTPS
protocol. `PY` topology gossip is not an authentication channel.

Potential assurance values include e-mail verified, manually verified, and
license-record verified. Callsign lookup data alone does not prove that the
person logging in controls that callsign.

Implementation should use a maintained OAuth/OIDC implementation when full
federation is justified. Do not implement OAuth, OIDC, JWT validation, or a
device authorization grant from scratch. Opaque, node-audience credentials may
be useful during an early trial, but still require expiration, revocation,
transport security, and strict destination binding.

Open decisions:

- operator demand and acceptable central-service responsibility
- identity-provider software and hosting model
- callsign verification and revocation policy
- minimal profile fields and GDPR deletion/export obligations
- browser login, telnet device flow, and outage behavior
- whether `home_node` remains mail-routing metadata only

## Compatible Authentication Modes

Some telnet clients support only `login:` and `password:` prompts. Interactive
MFA currently introduces another prompt, while unattended clients such as Ham
Radio Deluxe cannot supply a rotating code at reconnect time.

Proposed explicit account modes:

| Mode | Password-field input | Intended use |
| --- | --- | --- |
| `password` | ordinary password | Default and unattended legacy clients |
| `pin_totp` | four-digit PIN followed by six-digit TOTP | Interactive users requiring two factors in two prompts |
| `app_password` | long revocable client credential | Named unattended applications |

The mode must be stored explicitly. Credential shape must not silently select a
weaker authentication path, and an ambiguous `either` mode should be avoided.
The literal `password:` prompt should remain stable for client compatibility.

### PIN And TOTP Enrollment

Before `pin_totp` can ship:

1. Require a fully authenticated session and verified recovery e-mail.
2. Collect and confirm a nontrivial four-digit PIN.
3. Generate a pending TOTP secret rather than enabling it immediately.
4. Verify one complete PIN-plus-TOTP credential.
5. Atomically promote the secret, activate `pin_totp`, and disable the ordinary
   password credential.
6. Generate single-use recovery codes and display them once.

The PIN must use a keyed hash with a server secret stored outside SQLite. That
secret must survive install, upgrade, repair, migration, and disaster recovery,
must be excluded from redacted support bundles, and must have a documented
secure-backup procedure. Losing it would lock every `pin_totp` account.

Failed unauthenticated TOTP attempts must never disenroll the authenticator.
Apply source-IP throttling and Fail2Ban-compatible events, a short response
delay, temporary account cooldowns rather than permanent attacker-triggered
locks, replay prevention, and a daily attempt ceiling. Runtime health should
surface material UTC clock skew before users depend exclusively on TOTP.

### Application Passwords

Application passwords solve unattended reconnects; PIN plus TOTP does not.
Each credential should have a public lookup identifier, a high-entropy secret,
a label, creation and last-use timestamps, last source path, scope, and an
individual revocation state.

Application passwords should:

- be created only from a fully authenticated web session
- be shown once
- be accepted only for ordinary cluster login
- never authenticate public web or the System Operator console
- remain subject to blocks and the local access matrix
- use constant-time verification of a stored cryptographic digest
- produce useful but non-secret audit events

## Graphical Filter Management

Joe, N9JR, supplied a browser mock-up in
`Cluster Filter Generator.zip`. Its useful design goal is making traditional
cluster filters approachable while showing the equivalent telnet command.

The current authenticated Rules panel already reads and writes the same SQLite
`filter_rules` records used by telnet. It supports normal spot and RBN families,
accept/reject actions, slots, callsigns, spotters, bands, CQ and ITU zones,
continents, DXCC entities, comment or mode text, compound expressions, and
advanced expressions. System Operators can also maintain node-wide bad-DX,
bad-spotter, bad-node, and bad-word records.

Next improvements:

- validate expressions before saving and report unsupported syntax
- provide a test/preview using a sample spot and explain the matched rule
- build compound rules graphically while displaying the equivalent telnet form
- offer multi-select lists for zones, continents, states, and DXCC entities
- include spotter-state matching where location data is sufficiently reliable
- make RBN scope and subscription state obvious
- prevent quick filters from unexpectedly replacing a manually managed slot 8
- load access and rule state once per request or live batch rather than once per
  candidate spot
- preserve deterministic slot and accept/reject precedence across web and
  telnet

The mock-up is a design input, not drop-in production code. A second browser-only
filter store would violate the shared-state architecture.

## PY Security Advisories

A separate proposed `PY` capability can exchange short-lived reports that a
source address is attacking a node. Reports should be informational by default,
retained locally for about 72 hours, rate-limited, signed or otherwise tied to
an authenticated direct peer, and never cause automatic network-wide blocking.
The System Operator may inspect evidence and explicitly apply a local block.

This mechanism must account for NAT, shared carriers, IPv4 and IPv6 prefixes,
spoofed or mistaken reports, conflicting observations, expiry, and malicious
participating nodes. It must not become a distributed automatic firewall.

## Suggested Sequence

1. Complete graphical filter validation, preview, and request-level caching.
2. Correct telnet TOTP enrollment so pending secrets require verification.
3. Design and implement local app passwords.
4. Design `pin_totp`, recovery secrets, lockout behavior, and clock health.
5. Prototype verified pycluster.net DNS/DDNS and ACME delegation independently.
6. Measure operator adoption before building health-checked login pools.
7. Write a separate threat model and protocol specification before federated
   identity or distributed security advisories enter production code.


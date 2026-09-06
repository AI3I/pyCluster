# Public Web UI

The public web UI is the user-facing browser frontend for pyCluster.

Default local URL:

- `http://127.0.0.1:8081/`

## Backend State

Logged-in public web controls use the same backend database as telnet and the
System Operator console:

- spots, stats, leaderboards, bulletins, propagation, and cluster views are read from the shared store or shared runtime state
- profile fields write to `user_registry` and related `user_prefs`
- public filter presets, watch profiles, local watch rules, and recent watch matches are stored in the user's `public.presets` preference
- watch seeds are derived from database-backed buddy entries and positive spot filters
- common spot filter controls write a single compound rule to the same stored spot-filter table used by telnet `accept/spots` and `reject/spots`
- the authenticated `Rules` panel lists and edits slot-preserving `accept`/`reject` records for both spots and RBN; it does not maintain a separate browser-only rule model
- system operators also see separately labeled node-wide `baddx`, `badspotter`, `badnode`, and `badword` controls backed by the global deny-rule table
- registration requests validate the callsign before creating registry or review-queue records
- RBN visibility is controlled through the same access matrix and stored user preferences used elsewhere
- spots sourced from the configured RBN source node are treated as RBN traffic even when the comment text does not include a Skimmer-style marker
- raw PC11/PC61 RBN markers are honored even when the visible spot comment contains only a mode such as FT8
- RBN reports are delivered over a best-effort local socket and kept in a bounded in-memory window; they are not part of durable spot history or statistics and are not forwarded to cluster peers
- persisted request-time policy and integration settings, including SMTP and MFA, reload when the primary or local override configuration changes; listener addresses, ports, and the database path remain restart-bound
- authenticated websocket updates, spot history, statistics, and leaderboards all apply the same stored RBN preference and accept/reject filters

Browser storage is used only as a convenience cache or for anonymous,
not-yet-authenticated UI state such as display toggles and temporary map/QTH
inputs. After login, the map uses the stored profile grid square to seed the
QTH marker when the browser does not already have a local QTH override.

## Main Pages

## Mobile Layout

The public web UI is responsive across desktop, phone, tablet, and foldable
layouts. On narrower screens:

- the spot map/table and sidebar stack vertically
- the stacked spot page scrolls so filters, metrics, watch, and operate panels remain reachable
- footer controls switch to a touch-friendly grid
- spot popups use a safe-area-aware position above the mobile footer
- profile, login, and registration modals scroll when the viewport is short

### Spots

Core live spot view.

Features:

- filter by band, mode, activity, DX continent, spotter continent, DX CQ zone, spotter CQ zone, comment tags, and text
- time-range filtering for `1h`, `3h`, `6h`, `12h`, `18h`, and `24h`

The quick-filter panel intentionally omits ITU-zone controls. The authenticated `Rules` panel supports the shared telnet expressions for callsign, band, CQ zone, ITU zone, continent, DXCC entity, comment/mode, and advanced compound rules. Rules retain their family, action, and slot in `filter_rules`, so telnet and web sessions share the same effective state. An already-connected telnet session refreshes externally edited rules within two seconds. Slot 8 is also used by the quick-filter panel and may be replaced when those controls change.

State/province filtering is not inferred from callsign call areas. It will only be exposed when the shared spot record and evaluator have a reliable state field; guessing would make web and telnet results disagree.
- filter by spotter continent for logged-in operators; this is based on the station that posted the spot, not the spotted DX entity
- saved filter presets for logged-in users
- count of filtered vs total spots
- `RARE` badge support for selected entities
- `All` reset button when a filter is active
- QTH marker support from the local map profile or, after login, the user's stored grid square

### Stats

Activity and leaderboard view for recent cluster traffic.

Shows:

- spot-rate chart
- daily spot history
- band-by-hour heat matrix
- band and continent activity bars
- top DX entities, spotters, and spotted DX calls

The page supports `1h`, `3h`, `6h`, `12h`, `18h`, and `24h` ranges. Longer
48-hour and 72-hour counters are intentionally not exposed because the rolling
spot totals are less useful at that scale in the current UI.

### Bulletins

Recent non-spot traffic from the cluster.

Shows:

- announcements
- chat/talk traffic
- WX bulletins
- WCY bulletins
- WWV bulletins

Features:

- category tabs for quick filtering
- most-recent-first ordering
- sender, time, and scope metadata
- automatic refresh after posting and on interval reloads

### Cluster

Cluster overview without pretending to know a global topology it has not observed.

Shows:

- direct links
- network summary
- known nodes
- family and version when explicitly learned

The network summary lists pyCluster nodes by callsign and lists legacy peers by
callsign with their learned family, rather than only showing aggregate family
counts.

### Propagation

Solar and geomagnetic dashboard.

Shows:

- solar flux, sunspots, A index, K index, X-ray class, and solar wind
- band-condition summaries from the solar feed, grouped as HF day, HF night, and VHF
- expanded VHF path labels such as `Sporadic E`, `Tropospheric`, and `Meteor Scatter`
- seven-day Planetary Kp values served through `/api/kp`

### Watch

Authenticated user watch/alerting page.

Features:

- only available after login
- cluster-backed watch seeds from buddies and positive spot filters
- local saved watch rules layered on top of those inherited cluster watch seeds
- recent matches
- hit counts
- saved watch profiles
- per-rule toast and sound controls

Logged-in watch rules, watch profiles, and recent watch matches are saved back
to the node database. Loading the public web from another browser will reload
those account-backed watch settings after login.

### Operate

Authenticated posting and user actions.

Features:

- available only after login
- permission-aware posting controls
- footer `Log In` popup for authentication
- footer `Register` popup for verified account setup or new registration requests
- login-modal password reset for verified accounts; the exact callsign/SSID and its matching verified email address are both required
- login-modal MFA reset using the same exact-account and verified-email proof without changing the password or bypassing node policy
- footer `Edit Profile` popup after login

Profile fields:

- name
- location (QTH)
- grid square
- home node
- email address

MFA controls:

- view current MFA status and effective policy
- switch between email and authenticator-app MFA when both are available
- enable authenticator-app MFA with a QR code plus a manual setup key for authenticator apps that cannot scan the QR code
- verify the active MFA method with a code
- disable user-level MFA and clear outstanding challenges
- read the profile-modal MFA notice explaining whether login codes come from email or an authenticator app and how fallback/recovery works

When email MFA is selected, `Verify` sends a code to the profile email address and asks for that code. When authenticator-app MFA is selected, `Verify` asks for the current app code. Authenticator enrollment keeps email OTP as a fallback for the exact logged-in callsign or SSID. `Disable` turns off both authenticator and email MFA for that exact account. Login prompts say whether the code came from email or from the authenticator app.

Password reset:

- available from the web login popup
- requires a verified email address already stored on the account
- sends a reset code through the configured SMTP path
- requires the reset code plus matching new-password fields
- clears the failed-password lock state after a successful reset

MFA reset:

- is a separate action in the web login popup
- requires the exact callsign/SSID, matching verified email, and an emailed recovery code
- removes authenticator secrets, MFA failure state, and outstanding challenges without changing the password
- does not clear password or administrative locks
- retains verified email OTP when node policy requires MFA

The spot map uses the standard OpenStreetMap tile service with visible contributor attribution. Dark mode is derived from the same tiles in the browser, so node operators do not need a third-party map API key.

When node policy requires registration, the verified `Register` submission remains pending until a System Operator approves it. When registration approval is disabled, the same email-verification flow creates the account immediately. Existing telnet identities without a web password may complete web account setup only by verifying the email already stored on that callsign.

## Posting Controls

Depending on node policy, a user may be allowed to post:

- DX spots
- RBN spots
- chat/talk
- announce
- WX
- WWV

Disabled actions in the UI reflect local policy.
## Rule Preview

The Rules editor can combine two conditions with AND and displays the equivalent
telnet command. Its sample preview evaluates the stored rules plus the current
editor draft, replacing only the same family/action/slot in memory. It does not
save a rule, insert a spot, or transmit traffic.

The result includes the winning rule or explains that no accept rule matched.
RBN samples also check the user's web RBN access and subscription. This is a
rule/policy preview, not a guarantee of delivery: feed connectivity, ingestion
policy, browser display filters, and live throttling are outside its scope.

Quick filters ask before replacing existing slot-8 rules. Web saves reject
oversized or incomplete recognized conditions instead of truncating them.
Advanced expressions retain the existing matching syntax, including literal
text matching; a successful preview is not exhaustive grammar validation.

# Public Web UI

The public web UI is the user-facing browser frontend for pyCluster.

Default local URL:

- `http://127.0.0.1:8081/`

## Main Pages

### Spots

Core live spot view.

Features:

- filter by band, mode, activity, continent, zone, and text
- count of filtered vs total spots
- `RARE` badge support for selected entities
- `All` reset button when a filter is active

### Cluster

Cluster overview without pretending to know a global topology it has not observed.

Shows:

- direct links
- network summary
- known nodes
- family and version when explicitly learned

### Watch

User watch/alerting page.

Features:

- rule-based watch entries
- recent matches
- hit counts
- saved watch profiles
- per-rule toast and sound controls

### Operate

Authenticated posting and user actions.

Features:

- callsign/password login
- permission-aware posting controls
- footer `Edit Profile` popup

Profile fields:

- name
- location (QTH)
- grid square
- home node

## Posting Controls

Depending on node policy, a user may be allowed to post:

- DX spots
- chat/talk
- announce
- WX
- WCY
- WWV

Disabled actions in the UI reflect local policy.

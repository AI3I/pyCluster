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

### Operate

Authenticated posting and user actions.

Features:

- available only after login
- permission-aware posting controls
- footer `Log In` popup for authentication
- footer `Register` popup for new registration requests
- footer `Edit Profile` popup after login

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

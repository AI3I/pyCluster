from __future__ import annotations

from .models import Spot, normalize_call


DEFAULT_DEDUPE_TTL_SECONDS = 900
DEFAULT_DEDUPE_MAX_ENTRIES = 50000
# Nodes round frequencies independently, so the same signal can arrive as
# 7003.5 from one path and 7003.6 from another. Two tenths of a kHz either way
# absorbs that jitter without merging genuinely adjacent signals.
DEFAULT_FREQ_TOLERANCE_TENTHS = 2
# Peers with skewed clocks are common on the cluster network. Anything claiming
# to be further ahead than this is treated as "now" rather than trusted.
DEFAULT_MAX_EPOCH_SKEW_SECONDS = 300
# One identity can legitimately recur over a long import; remember enough
# timestamps per key to tell those apart from genuine repeats.
DEFAULT_MAX_EPOCHS_PER_KEY = 16
DEFAULT_MAX_COMMENTS_PER_KEY = 8


def clamp_spot_epoch(
    epoch: object,
    now_epoch: int,
    *,
    max_skew_seconds: int = DEFAULT_MAX_EPOCH_SKEW_SECONDS,
) -> int:
    """Clamp a peer-supplied spot timestamp against local time.

    A future-dated spot from one badly-set peer clock must not be able to shift
    windows that every other peer's spots are measured against.
    """
    now = int(now_epoch)
    try:
        value = int(epoch)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return now
    return now if value > now + max(0, int(max_skew_seconds)) else value


def freq_tenths(freq_khz: object) -> int:
    try:
        return round(float(freq_khz) * 10)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def normalize_comment(info: object) -> str:
    return " ".join(str(info or "").split()).casefold()


def spot_identity_key(spot: Spot) -> tuple[str, str, int]:
    """Identify the spot itself, independent of how it reached us.

    The comment is deliberately excluded: every node reformats it, and this node
    rewrites it too (see ``rbn_summary_info``), so keying on it means the same
    spot arriving by two paths yields two keys. The timestamp is excluded for
    the same reason - it is reconstructed at minute resolution on the PC61 path
    and taken from arrival time on the RBN feed. What stays constant across
    paths is who spotted what, and where.
    """
    return (
        normalize_call(spot.dx_call),
        normalize_call(spot.spotter),
        freq_tenths(spot.freq_khz),
    )


class SpotDeduper:
    """Shared suppression of spots already seen, across every ingest lane.

    Each entry records the spot timestamps seen for one identity plus the time
    they last arrived. The duplicate decision compares spot timestamps, so a
    historical import spanning days is not collapsed into a handful of rows;
    expiry uses arrival time, so a peer with a skewed clock cannot evict entries
    belonging to every other peer.
    """

    def __init__(
        self,
        *,
        ttl_seconds: int = DEFAULT_DEDUPE_TTL_SECONDS,
        max_entries: int = DEFAULT_DEDUPE_MAX_ENTRIES,
        freq_tolerance_tenths: int = DEFAULT_FREQ_TOLERANCE_TENTHS,
        max_epochs_per_key: int = DEFAULT_MAX_EPOCHS_PER_KEY,
        max_comments_per_key: int = DEFAULT_MAX_COMMENTS_PER_KEY,
    ) -> None:
        self.enabled = True
        self.ttl_seconds = max(0, int(ttl_seconds))
        self.max_entries = max(1, int(max_entries))
        self.freq_tolerance_tenths = max(0, int(freq_tolerance_tenths))
        self.max_epochs_per_key = max(1, int(max_epochs_per_key))
        self.max_comments_per_key = max(1, int(max_comments_per_key))
        # key -> (spot epochs seen, arrival of the most recent, comments seen)
        self._seen: dict[tuple[str, str, int], tuple[list[int], int, list[str]]] = {}

    def __len__(self) -> int:
        return len(self._seen)

    def prune(self, arrival_epoch: int) -> None:
        if not self._seen:
            return
        cutoff = int(arrival_epoch) - self.ttl_seconds
        for key in [k for k, entry in self._seen.items() if entry[1] < cutoff]:
            self._seen.pop(key, None)

    def _enforce_capacity(self) -> None:
        overflow = len(self._seen) - self.max_entries
        if overflow <= 0:
            return
        for key in sorted(self._seen, key=lambda k: self._seen[k][1])[:overflow]:
            self._seen.pop(key, None)

    def check(self, spot: Spot, arrival_epoch: int, *, match_comment: bool = False) -> bool:
        """Return True when this spot repeats one already seen, else remember it.

        ``match_comment`` is for spots this node originated. A locally typed
        comment is stable - no relay has reformatted it - so an operator
        correcting their own comment is honoured while an exact repeat is still
        suppressed. Network spots must never key on the comment: see
        ``spot_identity_key``.
        """
        if not self.enabled:
            return False
        arrival = int(arrival_epoch)
        self.prune(arrival)
        dx_call, spotter, tenths = spot_identity_key(spot)
        spot_epoch = int(spot.epoch)
        comment = normalize_comment(spot.info)
        tolerance = self.freq_tolerance_tenths
        for offset in range(-tolerance, tolerance + 1):
            entry = self._seen.get((dx_call, spotter, tenths + offset))
            if entry is None:
                continue
            if not any(abs(spot_epoch - seen) <= self.ttl_seconds for seen in entry[0]):
                continue
            if match_comment and comment not in entry[2]:
                continue
            return True
        self.remember(spot, arrival)
        return False

    def remember(self, spot: Spot, arrival_epoch: int) -> None:
        key = spot_identity_key(spot)
        entry = self._seen.get(key)
        epochs = list(entry[0]) if entry else []
        comments = list(entry[2]) if entry else []
        epochs.append(int(spot.epoch))
        if len(epochs) > self.max_epochs_per_key:
            # Keep the newest observations; older ones are already outside any
            # window a future spot could still fall into.
            epochs = sorted(epochs)[-self.max_epochs_per_key:]
        comment = normalize_comment(spot.info)
        if comment in comments:
            comments.remove(comment)
        comments.append(comment)
        del comments[: -self.max_comments_per_key]
        self._seen[key] = (epochs, int(arrival_epoch), comments)
        self._enforce_capacity()

    def clear(self) -> int:
        count = len(self._seen)
        self._seen.clear()
        return count

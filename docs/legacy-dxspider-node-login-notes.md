# Legacy DXSpider Node Login Notes

These notes capture the behavior observed on the live legacy DXSpider host at `dxcluster.ai3i.net` on March 11, 2026 while preparing `AI3I-16` for interoperability testing.

## What is documented publicly

Public DXSpider docs describe:

- user telnet login flow (`login:` and optional `password:` prompts)
- outbound node connect scripts under `/spider/connect/<callsign>`
- local script syntax such as `client <peer> telnet`

What they do **not** clearly document is the rule that decides whether an incoming telnet login becomes:

- a normal user/command session, or
- a node-to-node protocol session

## What the live source showed

On the legacy host, the deciding logic is in `perl/cluster.pl` and `perl/DXUser.pm`.

Observed dispatch behavior from `cluster.pl`:

```perl
if ($user->is_node) {
    $dxchan = DXProt->new($call, $conn, $user);
} elsif ($user->is_user) {
    $dxchan = DXCommandmode->new($call, $conn, $user);
}
```

Observed type logic from `DXUser.pm`:

```perl
sub is_node
{
    my $self = shift;
    return $self->{sort} =~ /[ACRSX]/;
}

sub is_user
{
    my $self = shift;
    return $self->{sort} eq 'U';
}
```

That means an incoming telnet login becomes a node session only if the stored DXUser record is considered node-like by `is_node()`. A normal user record (`sort => 'U'`) is routed into `DXCommandmode`, not `DXProt`.

## Related login flow

Observed from `perl/ExtMsg.pm`:

- new telnet connections receive `login: `
- if password policy applies, DXSpider prompts for `password: `
- after successful authentication, `to_connected($call, 'A', $sort)` is called
- channel creation then depends on the stored DXUser record type, not just the callsign string used at login

## What we observed live with AI3I-16

On the live `AI3I-15` node:

- `AI3I-16` initially existed as a normal user record with `sort => 'U'`
- logging in as `AI3I-16` produced a normal operator session:
  - greeting banner
  - cluster status line
  - prompt `AI3I-15>`
- it did **not** enter node-protocol mode

We also confirmed that `connect/wb3ffv-2` contains:

```text
'login: ' 'AI3I-15'
client wb3ffv-2 telnet
```

The important detail is that `client wb3ffv-2 telnet` is local DXSpider connect-script syntax. It is not a command accepted at a normal remote telnet prompt.

## Practical implication for pyCluster interop

For a legacy DXSpider node to treat `AI3I-16` as a node peer instead of a normal user login:

1. the DXSpider user database must contain a node-style record for `AI3I-16`
2. the running DXSpider process must actually see that record state
3. only then will incoming login dispatch choose `DXProt` instead of `DXCommandmode`

This behavior is based on live source inspection and runtime probing, not on a single public DXSpider document.

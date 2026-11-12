scripts
=======

```
npm ci
npm run typecheck
```

CDN maps cleanup
----------------

With springfiles fallback disabled, the CDN should only hold maps that are
actually used. Cleanup is done in two steps.

### 1. Generate list of used maps (offline)

`gen_used_maps.ts` builds `used_maps.json`: every map that was ever

- played, per [data-processing](https://github.com/beyond-all-reason/data-processing)
  dumps (`matches.parquet` `map` column, or raw `replay_maps.parquet` pgdump
  `scriptName` column), tagged `replay`,
- referenced in any revision of `map_list.yaml` in
  [maps-metadata](https://github.com/beyond-all-reason/maps-metadata),
  tagged `maps-metadata`.

It doesn't access the network, fetch inputs first. The maps-metadata clone
must have full history (no `--depth`/`--filter`):

```
curl -O https://data-marts.beyondallreason.dev/matches.parquet
git clone https://github.com/beyond-all-reason/maps-metadata
node gen_used_maps.ts --replays matches.parquet --maps-metadata maps-metadata --out used_maps.json
```

`--replays` can be passed multiple times. Each entry in the output has
`sources` and, per source, match count and last played time, or first/last
`map_list.yaml` commit date and whether the map is in its current revision.

### 2. Delete unused maps from KV and R2

`cleanup_cdn_maps.ts` lists `from_name/map/*` keys in the assets KV and all
objects in the R2 buckets, and deletes KV keys of maps not in
`used_maps.json` (springname compared case insensitively) and R2 objects
referenced only by those keys. R2 objects referenced by no KV key (orphans) are
reported, and deleted only with `--delete-orphans` if older than 24h.
Non-map KV keys (engines) are never touched.

Uses the same environment variables as `cacher` (see `../cacher/README.md`).
The R2 access key needs object read and write permission on all buckets.

```
node cleanup_cdn_maps.ts --used-maps used_maps.json             # dry run
node cleanup_cdn_maps.ts --used-maps used_maps.json --no-dry-run
```

Dry run is the default. Each run writes `cleanup_plan.json` (`--plan-out`) with
everything it would delete, including full KV values of deleted keys so they
can be restored. Deleted R2 objects can't be restored. KV keys are deleted
before R2 objects. The fetcher caches KV reads (up to 8h) and `/file/`
responses (Cache API, until evicted), so deleted maps can still be served for
a while after cleanup.

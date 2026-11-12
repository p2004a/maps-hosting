// Generates the list of maps that were ever used, from two offline sources:
//
//  - replays: Parquet dumps from https://github.com/beyond-all-reason/data-processing,
//    e.g. https://data-marts.beyondallreason.dev/matches.parquet (`map` column),
//    or the raw replay_maps.parquet pgdump (`scriptName` column).
//  - maps-metadata: every revision of map_list.yaml in a local clone of
//    https://github.com/beyond-all-reason/maps-metadata.
//
// Nothing is fetched from the network, all inputs must be available locally.
//
// Usage:
//   node gen_used_maps.ts --replays matches.parquet --maps-metadata ../../maps-metadata --out used_maps.json

import * as child_process from 'node:child_process';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { parseArgs } from 'node:util';
import { asyncBufferFromFile, parquetMetadataAsync, parquetReadObjects, parquetSchema } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';
import * as YAML from 'yaml';

export type MapSource = 'maps-metadata' | 'replay';

export interface UsedMap {
    springname: string,
    sources: MapSource[],
    replay?: {
        matches: number,
        // Only when the dump has a start_time column.
        last_played?: string,
    },
    maps_metadata?: {
        first_seen: string,
        last_seen: string,
        in_current: boolean,
    },
}

export interface UsedMapsFile {
    generated_at: string,
    inputs: {
        replays: Array<{ file: string, column: string, rows: number }>,
        maps_metadata?: { repo: string, ref: string, commit: string, revisions: number },
    },
    maps: UsedMap[],
}

// Columns holding map springname in known data-processing dumps, in order of
// preference.
const REPLAY_MAP_COLUMNS = ['map', 'scriptName'];
const REPLAY_TIME_COLUMNS = ['start_time', 'startTime'];
const MAP_LIST_PATH = 'map_list.yaml';

function toIsoDate(v: unknown): string | undefined {
    if (v instanceof Date) return v.toISOString();
    if (typeof v === 'string') return new Date(v).toISOString();
    return undefined;
}

async function scanReplays(file: string, used: Map<string, UsedMap>) {
    const buf = await asyncBufferFromFile(file);
    const metadata = await parquetMetadataAsync(buf);
    const columns = parquetSchema(metadata).children.map(c => c.element.name);
    const mapColumn = REPLAY_MAP_COLUMNS.find(c => columns.includes(c));
    if (!mapColumn) {
        throw new Error(`${file}: none of the columns ${REPLAY_MAP_COLUMNS.join(', ')} found, got: ${columns.join(', ')}`);
    }
    const timeColumn = REPLAY_TIME_COLUMNS.find(c => columns.includes(c));

    let rowStart = 0;
    for (const rg of metadata.row_groups) {
        const rowEnd = rowStart + Number(rg.num_rows);
        const rows = await parquetReadObjects({
            file: buf,
            metadata,
            compressors,
            columns: timeColumn ? [mapColumn, timeColumn] : [mapColumn],
            rowStart,
            rowEnd,
        });
        for (const row of rows) {
            const name = row[mapColumn];
            if (typeof name !== 'string' || name === '') continue;
            const m = getOrAdd(used, name);
            m.replay ??= { matches: 0 };
            m.replay.matches++;
            const t = timeColumn ? toIsoDate(row[timeColumn]) : undefined;
            if (t && (!m.replay.last_played || t > m.replay.last_played)) {
                m.replay.last_played = t;
            }
        }
        rowStart = rowEnd;
    }
    console.error(`${file}: ${rowStart} rows, column '${mapColumn}'`);
    return { file: path.basename(file), column: mapColumn, rows: rowStart };
}

function getOrAdd(used: Map<string, UsedMap>, springname: string): UsedMap {
    let m = used.get(springname);
    if (!m) {
        m = { springname, sources: [] };
        used.set(springname, m);
    }
    return m;
}

function git(repo: string, args: string[]): string {
    return child_process.execFileSync('git', ['-C', repo, ...args], { encoding: 'utf8', maxBuffer: 64 << 20 });
}

// Reads `<rev>:<path>` blobs with a single `git cat-file --batch` process.
// Missing objects (e.g. revision deleting the file) are returned as null.
function catFiles(repo: string, specs: string[]): Array<string | null> {
    const out = child_process.execFileSync('git', ['-C', repo, 'cat-file', '--batch'], {
        input: specs.join('\n') + '\n',
        maxBuffer: 4 << 30,
    });
    const result: Array<string | null> = [];
    let pos = 0;
    for (const spec of specs) {
        const nl = out.indexOf(0x0a, pos);
        const header = out.subarray(pos, nl).toString('utf8');
        pos = nl + 1;
        if (header.endsWith(' missing') || header.endsWith(' ambiguous')) {
            result.push(null);
            continue;
        }
        const size = parseInt(header.split(' ')[2]);
        if (isNaN(size)) throw new Error(`Unexpected git cat-file header for ${spec}: ${header}`);
        result.push(out.subarray(pos, pos + size).toString('utf8'));
        pos += size + 1;  // content is followed by LF
    }
    return result;
}

// map_list.yaml had 2 formats over time: initially a list of objects with
// `springname`, later a map from rowy ID to object with `springName`.
export function springNamesFromMapList(content: string): string[] {
    let doc: unknown;
    try {
        doc = YAML.parse(content);
    } catch (e) {
        // Fall back to a line based scan, it's better to keep too much than
        // too little.
        console.error(`Failed to parse map_list.yaml revision, falling back to regex: ${e}`);
        return [...content.matchAll(/^\s*(?:-\s*)?spring[nN]ame:\s*(.+?)\s*$/gm)]
            .map(m => m[1].replace(/^(['"])(.*)\1$/, '$2'));
    }
    if (doc === null || typeof doc !== 'object') return [];
    const entries: unknown[] = Array.isArray(doc) ? doc : Object.values(doc);
    const names: string[] = [];
    for (const e of entries) {
        if (e === null || typeof e !== 'object') continue;
        const o = e as Record<string, unknown>;
        let name = o.springName ?? o.springname;
        // YAML could parse e.g. `springName: 1.5` as a number.
        if (typeof name === 'number') name = String(name);
        if (typeof name === 'string' && name !== '') names.push(name);
    }
    return names;
}

function scanMapsMetadata(repo: string, ref: string, used: Map<string, UsedMap>) {
    const commit = git(repo, ['rev-parse', '--verify', `${ref}^{commit}`]).trim();
    // Oldest first, so the first time we see map is its first_seen.
    const revisions = git(repo, ['log', '--reverse', '--format=%H %cI', commit, '--', MAP_LIST_PATH])
        .trim().split('\n').filter(l => l).map(l => {
            const [hash, date] = l.split(' ');
            return { hash, date: new Date(date).toISOString() };
        });
    if (revisions.length === 0) {
        throw new Error(`No revisions of ${MAP_LIST_PATH} found in ${repo} at ${ref}`);
    }
    const contents = catFiles(repo, revisions.map(r => `${r.hash}:${MAP_LIST_PATH}`));
    const current = new Set(springNamesFromMapList(catFiles(repo, [`${commit}:${MAP_LIST_PATH}`])[0] ?? ''));
    for (let i = 0; i < revisions.length; ++i) {
        const content = contents[i];
        if (content === null) continue;
        for (const name of springNamesFromMapList(content)) {
            const m = getOrAdd(used, name);
            m.maps_metadata ??= { first_seen: revisions[i].date, last_seen: revisions[i].date, in_current: current.has(name) };
            m.maps_metadata.last_seen = revisions[i].date;
        }
    }
    console.error(`${repo}: ${revisions.length} revisions of ${MAP_LIST_PATH} up to ${commit}`);
    return { repo: path.basename(path.resolve(repo)), ref, commit, revisions: revisions.length };
}

async function main() {
    const { values } = parseArgs({
        options: {
            'replays': { type: 'string', multiple: true, default: [] },
            'maps-metadata': { type: 'string' },
            'maps-metadata-ref': { type: 'string', default: 'HEAD' },
            'out': { type: 'string', default: 'used_maps.json' },
            'help': { type: 'boolean', short: 'h' },
        },
    });
    if (values.help || (values.replays.length === 0 && !values['maps-metadata'])) {
        console.error('Usage: node gen_used_maps.ts [--replays FILE.parquet]... [--maps-metadata REPO_DIR] [--maps-metadata-ref REF] [--out used_maps.json]');
        process.exit(values.help ? 0 : 2);
    }

    const used = new Map<string, UsedMap>();
    const result: UsedMapsFile = {
        generated_at: new Date().toISOString(),
        inputs: { replays: [] },
        maps: [],
    };
    for (const file of values.replays) {
        result.inputs.replays.push(await scanReplays(file, used));
    }
    if (values['maps-metadata']) {
        result.inputs.maps_metadata = scanMapsMetadata(values['maps-metadata'], values['maps-metadata-ref'], used);
    }

    for (const m of used.values()) {
        if (m.maps_metadata) m.sources.push('maps-metadata');
        if (m.replay) m.sources.push('replay');
    }
    result.maps = [...used.values()].sort((a, b) => a.springname < b.springname ? -1 : a.springname > b.springname ? 1 : 0);

    await fs.writeFile(values.out, JSON.stringify(result, null, 2) + '\n');
    const count = (s: MapSource[]) => result.maps.filter(m => m.sources.join() === s.join()).length;
    console.error(`Wrote ${result.maps.length} maps to ${values.out}: `
        + `${count(['maps-metadata', 'replay'])} both, `
        + `${count(['replay'])} replay only, `
        + `${count(['maps-metadata'])} maps-metadata only`);
}

await main();

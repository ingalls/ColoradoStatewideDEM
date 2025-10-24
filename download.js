import { fetch } from 'undici';
import { pipeline } from 'node:stream/promises';
import { PromisePool } from '@supercharge/promise-pool';
import fs from 'node:fs';

// node download.js <output dir>

//const formats = await downloadFormats();
const datasets = await downloadDatasets();

if (process.argv[2] && !process.argv[2].endsWith('/')) {
    throw new Error('Output directory must end with a slash (/)');
}

const downloadDir = new URL('file://' + process.argv[2]) || new URL('./data', import.meta.url);

//console.log('Supported Formats: ', Object.values(formats).map(f => f.name).join(', '));
// While the api reports TIFF support, they aren't actually generated/available

const formats = [
    '36ac1747-105e-4eef-9be4-2d06f218d861', // ADF
    'cab5dd42-cbfc-4bdc-8f20-46be55fbb415', // TIF
    '30993bf7-abc8-4339-978a-5e61cd692768', // IMG
    'db1174ae-ff1a-4cef-8c78-cd1bb8048749' // ASC
]

for (const dataset of datasets.keys()) {
    console.log(`Dataset: ${dataset}`);

    fs.mkdirSync(`${downloadDir.pathname}${dataset}`, { recursive: true });

    const tileIndex = await downloadTileIndex(dataset);

    const { errors } = await PromisePool
        .for(tileIndex)
        .withConcurrency(25)
        .process(async (tileid) => {
            console.log(`Dataset: ${dataset}, Tile: ${tileid}`);

            if (fs.existsSync(`${downloadDir.pathname}${dataset}/${tileid}.zip`)) {
                console.log(`Dataset: ${dataset}, Tile: ${tileid} already exists, skipping`);
                return;
            }

            await downloadTile(dataset, tileid, formats);
        });

    if (errors.length) throw new Error(`Failed to download some tiles: ${errors.map(e => e.message).join(', ')}`);
}

async function downloadTileIndex(dataset) {
    const res = await fetch('https://coloradohazardmapping.com/api/lidar/tileSummaries', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify([ dataset ])
    });

    if (!res.ok) {
        throw new Error(`Failed to fetch tile index for dataset ${dataset}: ${res.status} ${res.statusText}`);
    }

    const tileIndex = await res.json();
    return new Set(Object.keys(tileIndex[dataset]))
}

async function downloadDatasets() {
    const res = await fetch('https://coloradohazardmapping.com/api/lidar/datasets');

    if (!res.ok) {
        throw new Error(`Failed to fetch datasets: ${res.status} ${res.statusText}`);
    }

    const datasets = await res.json();

    const map = new Map();

    for (const dataset of Object.keys(datasets)) {
        map.set(dataset, datasets[dataset]);
    }

    return map;
}

async function downloadFormats() {
    const res = await fetch('https://coloradohazardmapping.com/api/lidar/formats');

    if (!res.ok) {
        throw new Error(`Failed to fetch datasets: ${res.status} ${res.statusText}`);
    }

    return await res.json();
}

async function downloadTile(dataset, tileid, formatids) {
    const res = await fetch(`https://coloradohazardmapping.com/api/lidar/files/tile/download`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            email: "",
            tiles: [ tileid ],
            formats: formatids
        })
    });

    if (!res.ok) {
        throw new Error(`Failed to fetch tile ${tileid}: ${res.status} ${await res.text()}`);
    }

    await pipeline(
        res.body,
        fs.createWriteStream(`${downloadDir.pathname}${dataset}/${tileid}.zip`)
    )
}

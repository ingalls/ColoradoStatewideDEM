<h1 align=center>Colorado Statewide DEM</h1>

This repository contains a script to download and build a statewide DEM for Colorado at ~1m resolution.

Please note these scripts are rough operational code and are not intended for production use. The final output
will be hosted by the State of Colorado

### Run

```
npm install
```

```
node download.js
```

### Validate

Determine if there are any corrupted zip files:
```
find . -type f -name "*.zip" -print0 | parallel -0 -j 25 'unzip -tq {} > /dev/null || echo "{}"'
```

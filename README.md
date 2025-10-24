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

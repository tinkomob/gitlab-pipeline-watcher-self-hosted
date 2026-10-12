# Build

For a Windows release, place the distributable executable at `dist/pipeline-watcher.exe`:

```powershell
cargo build --release
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Copy-Item -Force target\release\pipeline-watcher.exe dist\pipeline-watcher.exe
```

Cargo keeps its intermediate build files in `target`; the app executable delivered from a release build belongs in `dist`.

# tbar.Optimizely.FallbackBlobProvider for Optimizely CMS 12

Ever restored the Production database to your developer environment and got a website without images? 

Transparent blob fallback for Optimizely/EPiServer: if a blob is missing in the primary Azure container (azurite probably), serve it from a fallback container immediately and copy it to primary in the background (deduped).

## What it does
- Primary-first reads; on miss, returns the real blob from fallback.
- Background copy backfills the primary container (one copy per blob key at a time).
- Robust existence checks and safe delete (single id or by prefix).
- Clean, structured logging (no secrets) for initialization, fallbacks, copy dedupe, deletes, and missing-both.

## Install (drop-in)
1) Add `FallbackBlobProvider.cs` to your web project (e.g., `Infrastructure/Initialization`).
2) Register in `Startup.ConfigureServices`:

```csharp
if (bool.TryParse(Configuration["UseFallbackProvider"], out var useFallback) && useFallback)
{
    services.AddFallbackBlobProvider(Configuration); // binds "AzureBlob" by default
}
else
{
    // your existing provider registration here
}
```

## Configuration (appsettings.Development.json)
```json
{
  "AzureBlob": {
    "ConnectionString": "UseDevelopmentStorage=true",
    "ContainerName": "sitemedia-local",
    "FallbackConnectionString": "<fallback-connection-string>",
    "FallbackContainerName": "sitemedia"
  },
  "UseFallbackProvider": true
}
```
Notes:
- `FallbackContainerName` is optional; defaults to `ContainerName` when omitted.
- If `FallbackConnectionString` is empty, fallback is disabled automatically.

## How it works
- Existence checks prefer `AzureBlob` wrapper methods (Exists/ExistsAsync); otherwise, we attempt `OpenRead` and treat 404/BlobNotFound as not found.
- Background copy is deduped using the blob AbsoluteUri string key (avoids subtle `Uri` equality issues).
- Delete behavior:
  - If the id has more than two URI segments, it’s treated as a single blob delete.
  - Otherwise it deletes by prefix (folder-style cleanup).

## Logging you’ll see
- Info: initialization summary, “Serving blob from fallback … and scheduling background copy.”
- Info: “Background copy deduped …” when concurrent requests happen.
- Info: one-time per id when blob is missing in both primary and fallback.
- Info/Warn around deletes; Debug for decision details.

## Limitations
- Uses EPiServer Azure internals (`AzureBlob`, `AzureBlobContainerFactory`); behavior may vary by package version.
- No retry/backoff on the background copy by default (keep it lean). Add if your environment is unstable.

## Quick test checklist
- Seed only the fallback with a blob and request it:
  - Expect: served from fallback immediately; background copy; subsequent requests from primary.
- Request a non-existent blob:
  - Expect: one-time Info log about missing in both.
- Hit the same missing blob concurrently:
  - Expect: a single background copy with an Info log showing dedupe for later callers.

## Tips
- Keep logging at Info for operations, Debug for verbose tracing. Secrets are never logged (connection strings are scrubbed).
- If you disable fallback (missing/empty `FallbackConnectionString`), the provider behaves like a regular primary-only Azure blob provider.

## Acknowledgements

This problem is not new and [Luc Gosso solved it nicely for CMS < 12](https://github.com/LucGosso/Gosso.EPiServerAddOn.DownloadIfMissingFileBlob) - thanks! 


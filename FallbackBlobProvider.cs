
using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using EPiServer.Azure.Blobs.Internal;
using EPiServer.Framework.Blobs;
using EPiServer.Web;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using System.ComponentModel.DataAnnotations;

#nullable enable
namespace tbar.Optimizely.Providers;

/// <summary>
/// <para>A <see cref="BlobProvider"/> that first serves blobs from a primary Azure container and, on a 404, transparently
/// fetches the blob from a fallback container, copies it to the primary container, and then returns the stream.</para>
/// <para>
/// Register the provider with <c>services.AddFallbackBlobProvider(Configuration)</c>.
/// Configuration keys are read from the <c>AzureBlob</c> section by default; see
/// <see cref="FallbackBlobProviderOptions"/> for the accepted keys.
/// </para>
/// </summary>
public partial class FallbackBlobProvider(IMimeTypeResolver mimeTypeResolver, IOptions<FallbackBlobProviderOptions> options, ILogger<FallbackBlobProvider> log)
    : BlobProvider, IAsyncDisposable
{
    // Use string key (AbsoluteUri) to avoid subtle Uri normalization/casing issues on dictionary keys.
    private static readonly ConcurrentDictionary<string, Lazy<Task>> _copyTasks = new();
    // Track first-time "absent in both" logs to avoid flooding.
    private static readonly ConcurrentDictionary<string, byte> _missingLogged = new();
    private IAzureBlobContainer? _container;
    private IAzureBlobContainer? _fallbackContainer;
    private IAzureBlobContainerFactory? _containerFactory;
    private readonly IMimeTypeResolver _mimeTypeResolver = mimeTypeResolver;
    private readonly FallbackBlobProviderOptions _options = options.Value;

    /// <summary>
    /// Removes secrets from a connection string before logging.
    /// </summary>
    private static string ScrubConnectionString(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return string.Empty;

        // Dev‑storage strings are safe to echo.
        if (connectionString.StartsWith("UseDevelopmentStorage=", StringComparison.OrdinalIgnoreCase))
            return "UseDevelopmentStorage";

        // Prefer exposing only the account name.
        foreach (var part in connectionString.Split(';'))
        {
            var kvp = part.Split('=', 2, StringSplitOptions.TrimEntries);
            if (kvp.Length == 2 && kvp[0].Equals("AccountName", StringComparison.OrdinalIgnoreCase))
                return $"AccountName={kvp[1]}";
        }
        return "(redacted)";
    }

    private IAzureBlobContainer Container => _container ??= GetContainer();
    private IAzureBlobContainer FallbackContainer => _fallbackContainer ??= GetContainer(true);

    // cap for the one-time missing log cache to avoid unbounded growth
    private const int MissingLogCacheMax = 5000;
    private const int MissingLogCacheTrimTo = 2500;

    /// <summary>
    /// Gets or sets the <see cref="IAzureBlobContainerFactory"/> used to create containers.
    /// Override for unit tests or to plug in a custom factory.
    /// </summary>
    public IAzureBlobContainerFactory ContainerFactory
    {
        get => _containerFactory ??= new DefaultAzureBlobContainerFactory();
        set => _containerFactory = value;
    }

    /// <summary>
    /// Validates the supplied connection information and initialises the primary container synchronously.
    /// Consumers normally call the async counterpart, but Episerver infrastructure requires this override.
    /// </summary>
    /// <param name="connectionString">Storage account connection string.</param>
    /// <param name="containerName">Blob container name.</param>
    public virtual void Initialize(string connectionString, string containerName)
    {
        log.LogDebug("Initialize called with connectionString='{ConnectionString}', containerName='{ContainerName}'",
            ScrubConnectionString(connectionString), containerName);
        _container = ContainerFactory.GetContainer(connectionString, containerName);
    }

    public override async Task InitializeAsync()
    {
        log.LogDebug("InitializeAsync called, ensuring container exists.");
        await Container.CreateIfNotExistAsync();
        log.LogDebug("Container ensure completed.");
        log.LogInformation(
            "FallbackBlobProvider initialized. Primary container '{PrimaryContainer}'. Fallback enabled: {FallbackEnabled}. Fallback container: '{FallbackContainer}'.",
            _options.ContainerName,
            _options.Enabled,
            string.IsNullOrWhiteSpace(_options.FallbackContainerName) ? _options.ContainerName : _options.FallbackContainerName);
    }

    public override Blob CreateBlob(Uri id, string extension)
    {
        log.LogDebug("CreateBlob called with id='{Id}', extension='{Extension}'", id, extension);
        return GetBlob(Blob.NewBlobIdentifier(id, extension));
    }

    public override void Delete(Uri id)
    {
        log.LogDebug("Delete called for id='{Id}'", id);
        ThrowIfNotAbsoluteUri(id);
        if (id.Segments.Length > 2)
        {
            log.LogDebug("Deleting single blob for id='{Id}'", id);
            try
            {
                Container.GetBlob(id).DeleteIfExists();
                log.LogInformation("Deleted blob id='{Id}' from primary container '{Container}'.", id, _options.ContainerName);
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Failed to delete blob id='{Id}'.", id);
            }
        }
        else
        {
            var prefix = id.Segments[1];
            log.LogDebug("Deleting blobs by prefix '{Prefix}' for id='{Id}'", prefix, id);
            log.LogInformation("Deleting blobs with prefix '{Prefix}' from primary container '{Container}'.", prefix, _options.ContainerName);
            try
            {
                Container.DeleteByPrefix(prefix);
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Failed to delete blobs by prefix '{Prefix}'.", prefix);
            }
        }
    }

    public override Blob GetBlob(Uri id)
    {
        log.LogDebug("GetBlob called for id='{Id}'", id);
        ThrowIfNotAbsoluteUri(id);
        AzureBlob blob = Container.GetBlob(id);

        // Prefer an existence check over OpenRead to avoid unnecessary GETs.
        if (Exists(blob))
        {
            blob.ContentType = _mimeTypeResolver.GetMimeMapping(Path.GetFileName(id.AbsolutePath));
            return blob;
        }

        if (TryReturnFromFallbackOrLog(id, out var fromFallback))
        {
            return fromFallback!;
        }

        LogMissingOnce(id);
        log.LogDebug("Primary and fallback missing or disabled for id='{Id}'. Returning primary handle (may 404 on read).", id);
        blob.ContentType = _mimeTypeResolver.GetMimeMapping(Path.GetFileName(id.AbsolutePath));
        return blob;
    }

    private bool TryReturnFromFallbackOrLog(Uri id, out AzureBlob? returned)
    {
        returned = null;
        if (_options.Enabled &&
            !string.IsNullOrWhiteSpace(_options.FallbackConnectionString) &&
            FallbackContainer != null)
        {
            log.LogDebug("Primary blob missing, checking fallback for id='{Id}'", id);
            var fallbackBlob = FallbackContainer.GetBlob(id);
            if (Exists(fallbackBlob))
            {
                log.LogDebug("Blob exists in fallback; returning it and scheduling background copy for id='{Id}'", id);
                returned = GetFallbackBlob(id, CancellationToken.None);
                return true;
            }
            log.LogDebug("Blob not found in fallback for id='{Id}'", id);
        }
        return false;
    }

    private AzureBlob GetFallbackBlob(Uri id, CancellationToken token = default)
    {
        log.LogDebug("Attempting fallback blob for id='{Id}'", id);
        var src = FallbackContainer.GetBlob(id);
        src.ContentType = _mimeTypeResolver.GetMimeMapping(Path.GetFileName(id.AbsolutePath));

        log.LogInformation("Serving blob from fallback for id='{Id}' and scheduling background copy.", id);

        var key = id.AbsoluteUri;
        var blobId = id; // capture for closures

        // Try to schedule a new background copy; if another thread already scheduled it, we dedupe.
        var newLazy = new Lazy<Task>(() => Task.Run(async () =>
        {
            try
            {
                var dst = Container.GetBlob(blobId);
                dst.ContentType = _mimeTypeResolver.GetMimeMapping(Path.GetFileName(blobId.AbsolutePath));
                log.LogDebug("Starting background copy for id='{Id}' from fallback to primary", blobId);
                await StreamCopyAsync(src, dst, token);
                log.LogInformation("Blob fallback was successful for Blob '{Id}'", blobId);
            }
            catch (Exception exception)
            {
                log.LogWarning(exception, "Copy of {Id} failed.", blobId);
            }
            finally
            {
                _copyTasks.TryRemove(key, out Lazy<Task>? _);
            }
        }));

        var lazyCopy = _copyTasks.GetOrAdd(key, _ => newLazy);
        if (!ReferenceEquals(lazyCopy, newLazy))
        {
            log.LogDebug("Background copy already in progress for id='{Id}', deduping", blobId);
            log.LogInformation("Background copy deduped for id='{Id}'.", blobId);
        }

        _ = lazyCopy.Value; // kick off background copy

        return src;
    }

    private void LogMissingOnce(Uri id)
    {
        var key = id.AbsoluteUri;
        // Trim cache if it grows too large
        if (_missingLogged.Count > MissingLogCacheMax)
        {
            foreach (var k in _missingLogged.Keys.Take(MissingLogCacheMax - MissingLogCacheTrimTo))
            {
                _missingLogged.TryRemove(k, out _);
            }
        }
        if (_missingLogged.TryAdd(key, 1))
        {
            log.LogInformation("Blob not found in primary or fallback for id='{Id}'.", id);
        }
    }

    private static void ThrowIfNotAbsoluteUri(Uri id)
    {
        if (!id.IsAbsoluteUri)
        {
            throw new ArgumentException("Given Uri identifier must be an absolute Uri");
        }
    }

    private IAzureBlobContainer GetContainer(bool isFallbackContainer = false)
    {
        if (isFallbackContainer)
        {
            log.LogDebug("Resolving fallback container '{ContainerName}'", _options.FallbackContainerName);
            return ContainerFactory.GetContainer(_options.FallbackConnectionString!, _options.FallbackContainerName!);
        }

        log.LogDebug("Resolving primary container '{ContainerName}'", _options.ContainerName);
        return ContainerFactory.GetContainer(_options.ConnectionString, _options.ContainerName);
    }

    private static bool IsNotFound(Exception ex)
    {
        if (ex is FileNotFoundException)
            return true;
        if (ex is Azure.RequestFailedException rfe && (rfe.ErrorCode == "BlobNotFound" || rfe.Status == 404))
            return true;
#pragma warning disable CS0618
        if (ex.GetType().FullName == "Microsoft.Azure.Storage.StorageException" && ex.Message.Contains("404"))
            return true;
#pragma warning restore CS0618
        return false;
    }

    private static bool Exists(AzureBlob blob)
    {
        // Prefer a public Exists/ExistsAsync on the wrapper if available
        if (TryWrapperExists(blob, out var exists))
            return exists;

        // Fallback: attempt to open for read and catch not found.
        try
        {
            using var _ = blob.OpenRead();
            return true;
        }
        catch (Exception ex) when (IsNotFound(ex))
        {
            return false;
        }
    }

    private static bool TryWrapperExists(AzureBlob blob, out bool exists)
    {
        exists = false;
        try
        {
            var wrapperExists = typeof(AzureBlob).GetMethod("Exists", Type.EmptyTypes)
                               ?? typeof(AzureBlob).GetMethod("ExistsAsync", Type.EmptyTypes);
            if (wrapperExists != null)
            {
                var result = wrapperExists.Invoke(blob, null);
                if (result is bool b)
                {
                    exists = b; return true;
                }
                if (result is Task<bool> tb)
                {
                    exists = tb.GetAwaiter().GetResult(); return true;
                }
            }
        }
        catch
        {
            // ignore and let caller fallback
        }
        return false;
    }

    private static async Task StreamCopyAsync(AzureBlob src, AzureBlob dst, CancellationToken token)
    {
        await using var inStream = src.OpenRead();
        await using var outStream = dst.OpenWrite();
        await inStream.CopyToAsync(outStream, token);
    }

    // Placeholder seeding removed.

    public async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        if (_container is IAsyncDisposable asyncContainer)
            await asyncContainer.DisposeAsync().ConfigureAwait(false);
        else if (_container is IDisposable syncContainer)
            syncContainer.Dispose();

        if (_fallbackContainer is IAsyncDisposable asyncFallback)
            await asyncFallback.DisposeAsync().ConfigureAwait(false);
        else if (_fallbackContainer is IDisposable syncFallback)
            syncFallback.Dispose();

        // Await outstanding background copies.
        foreach (var lazy in _copyTasks.Values.ToArray())
        {
            try { await lazy.Value.ConfigureAwait(false); }
            catch { /* swallow exceptions on shutdown */ }
        }
    }
}

/// <summary>
/// Strong‑typed options for <see cref="FallbackBlobProvider"/>. Bind from configuration
/// via <c>IConfiguration.GetSection("AzureBlob")</c> or any custom section name.
/// </summary>
public class FallbackBlobProviderOptions
{
    /// <summary>Connection string to the fallback storage account.</summary>
    public string? FallbackConnectionString { get; set; }

    /// <summary>Name of the blob container in the fallback account. If empty, <see cref="ContainerName"/> is reused.</summary>
    [MinLength(3), MaxLength(63)]
    [RegularExpression("^[a-z0-9]+(-[a-z0-9]+)*$")]
    public string? FallbackContainerName { get; set; }

    /// <summary>Primary storage account connection string. Defaults to Azurite (<c>UseDevelopmentStorage=true</c>).</summary>
    [Required]
    public string ConnectionString { get; set; } = "UseDevelopmentStorage=true";

    /// <summary>Name of the primary blob container. Defaults to <c>sitemedia-local</c>.</summary>
    [Required, MinLength(3), MaxLength(63)]
    [RegularExpression("^[a-z0-9]+(-[a-z0-9]+)*$")]
    public string ContainerName { get; set; } = "sitemedia-local";

    /// <summary>Set to <c>false</c> to disable fallback behaviour entirely.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Default section for Binding.</summary>    
    public const string DefaultSection = "AzureBlob";
}

public static partial class FallbackBlobProviderExtensions
{
    private static readonly Regex ContainerNameRegex = MyRegex();

    /// <summary>Registers the provider itself and related services once.</summary>
    private static void RegisterProviderServices(IServiceCollection services)
    {
        services.TryAddSingleton<IAzureBlobContainerFactory, DefaultAzureBlobContainerFactory>();

        services.Configure<BlobProvidersOptions>(opts =>
        {
            opts.DefaultProvider = "fallback";
            opts.AddProvider<FallbackBlobProvider>("fallback");
        });

        services.TryAddEnumerable(Microsoft.Extensions.DependencyInjection.ServiceDescriptor.Singleton<
            IPostConfigureOptions<FallbackBlobProviderOptions>,
            FallbackBlobProviderOptionsConfigurer>());
    }

    internal class FallbackBlobProviderOptionsConfigurer() : IPostConfigureOptions<FallbackBlobProviderOptions>
    {
        public void PostConfigure(string? name, FallbackBlobProviderOptions? options)
        {
            if (options != null)
            {
                if (string.IsNullOrWhiteSpace(options.ConnectionString))
                {
                    options.ConnectionString = "UseDevelopmentStorage=true";
                }

                if (string.IsNullOrWhiteSpace(options.FallbackConnectionString))
                {
                    options.Enabled = false;
                    options.FallbackConnectionString = "disabled";
                }

                if (string.IsNullOrWhiteSpace(options.FallbackContainerName))
                {
                    options.FallbackContainerName = options.ContainerName;
                }
            }
        }
    }

    /// <summary>
    /// Registers <see cref="FallbackBlobProvider"/> as the default provider and optionally configures its options.
    /// </summary>
    /// <param name="services">The DI container.</param>
    /// <param name="configureOptions">Optional delegate used to configure <see cref="FallbackBlobProviderOptions"/>.</param>
    /// <returns>The same <paramref name="services"/> instance for chaining.</returns>
    public static IServiceCollection AddFallbackBlobProvider(
        this IServiceCollection services,
        Action<FallbackBlobProviderOptions>? configureOptions = null)
    {
        var builder = services.AddOptions<FallbackBlobProviderOptions>()
                      .ValidateDataAnnotations()
                      .Validate(o => ContainerNameRegex.IsMatch(o.ContainerName ?? string.Empty), "Invalid container name")
                      .Validate(o => string.IsNullOrWhiteSpace(o.FallbackContainerName) || ContainerNameRegex.IsMatch(o.FallbackContainerName), "Invalid fallback container name");

        if (configureOptions is not null)
            builder.Configure(configureOptions);

        RegisterProviderServices(services);
        return services;
    }

    /// <summary>
    /// Convenience overload that binds <see cref="FallbackBlobProviderOptions"/> from a configuration section
    /// (defaults to <see cref="FallbackBlobProviderOptions.DefaultSection"/>) and registers the provider.
    /// </summary>
    /// <param name="services">The DI container.</param>
    /// <param name="configuration">Application configuration.</param>
    /// <param name="sectionName">Configuration section to bind; defaults to <c>AzureBlob</c>.</param>
    /// <returns>The same <paramref name="services"/> instance for chaining.</returns>
    public static IServiceCollection AddFallbackBlobProvider(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = FallbackBlobProviderOptions.DefaultSection)
    {
        services.AddOptions<FallbackBlobProviderOptions>()
                    .Bind(configuration.GetSection(sectionName))
                    .ValidateDataAnnotations()
                    .Validate(o => ContainerNameRegex.IsMatch(o.ContainerName ?? string.Empty), "Invalid container name")
                    .Validate(o => string.IsNullOrWhiteSpace(o.FallbackContainerName) || ContainerNameRegex.IsMatch(o.FallbackContainerName), "Invalid fallback container name");

        RegisterProviderServices(services);
        return services;
    }

    [GeneratedRegex("^[a-z0-9]+(-[a-z0-9]+)*$", RegexOptions.Compiled)]
    private static partial Regex MyRegex();
}


/// <summary>
/// Abstraction for creating <see cref="IAzureBlobContainer"/> instances.
/// </summary>
public interface IAzureBlobContainerFactory
{
    IAzureBlobContainer GetContainer(string connectionString, string containerName);
}

/// <summary>
/// Default factory implementation that delegates to Episerver's internal <c>AzureBlobContainerFactory</c>.
/// </summary>
internal sealed class DefaultAzureBlobContainerFactory : IAzureBlobContainerFactory
{
    private readonly AzureBlobContainerFactory _inner = new();

    public IAzureBlobContainer GetContainer(string connectionString, string containerName) =>
        _inner.GetContainer(connectionString, containerName);
}
#nullable disable

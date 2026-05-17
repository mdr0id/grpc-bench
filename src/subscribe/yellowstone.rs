//! Thin wrapper around the generated `GeyserClient` from
//! [`yellowstone_grpc_proto`]. We bypass `yellowstone-grpc-client` because
//! its `subscribe_once` returns a stream wrapped in an `AutoReconnect`
//! adapter and feeds the request through an `mpsc::channel(1000)`, which
//! holds the send-half of the bidirectional stream open after the single
//! `SubscribeRequest` has been delivered. Some provider tiers
//! (observed in production with Quicknode's smaller Yellowstone plans)
//! count the still-open send-half as a "pending filter add" against the
//! per-token filter quota and reject the request with
//! `Max amount of filters reached, only 1 allowed` — even though the
//! request body itself contains exactly one filter.
//!
//! Using `futures::stream::iter(vec![req])` closes the send-half
//! immediately, matching the behaviour of `grpcurl` and `protoc`-generated
//! Go/Python clients.
//!
//! Timing model for arrival timestamps:
//!
//! - **Linux** with the spec's full precision posture would expose the
//!   underlying TCP socket fd, call
//!   [`crate::timing::kernel_ts::enable_so_timestampns`] on it, and read
//!   `SCM_TIMESTAMPNS` cmsgs per `recvmsg`. `tonic` 0.14 does not surface
//!   raw fds through its high-level `Channel`/`Endpoint` API. Wiring a
//!   custom `tonic` connector that owns the `TcpStream` and routes
//!   per-frame timestamps to this layer is a meaningful refactor and is
//!   tracked as a follow-up; see `PRECISION.md`.
//! - **Default today** (Linux + macOS dev) is fallback:
//!   `EventTimestamp` is captured via [`ClockOrigin::now_user_space`]
//!   immediately after the protobuf decode returns.

use std::time::Duration;

use futures::stream::{self, Stream, StreamExt};
use thiserror::Error;
use tonic::{
    metadata::{errors::InvalidMetadataValue, AsciiMetadataValue},
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status,
};
use yellowstone_grpc_proto::geyser::{
    geyser_client::GeyserClient, GetVersionRequest, GetVersionResponse, SubscribeRequest,
    SubscribeUpdate,
};

use crate::{
    config::EndpointSpec,
    proto::{evaluate, Compatibility, EndpointVersion},
    timing::{ClockOrigin, EventTimestamp},
};

/// Default per-connection timeout for the initial gRPC `connect` and the
/// `GetVersion` ping.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Default per-`GetVersion` request timeout.
pub const DEFAULT_GET_VERSION_TIMEOUT: Duration = Duration::from_secs(10);

/// Errors emitted by the subscribe wrapper.
#[derive(Debug, Error)]
pub enum YellowstoneError {
    /// gRPC endpoint URL/string was invalid.
    #[error("invalid endpoint URL {url:?}: {source}")]
    InvalidEndpoint {
        /// Offending URL string.
        url: String,
        /// Underlying tonic transport error.
        #[source]
        source: tonic::transport::Error,
    },
    /// `x-token` could not be parsed into an ASCII metadata value.
    #[error("invalid x-token for {url:?}: {source}")]
    InvalidXToken {
        /// Endpoint label.
        url: String,
        /// Underlying tonic metadata error.
        #[source]
        source: InvalidMetadataValue,
    },
    /// TCP/TLS connect failed.
    #[error("failed to connect to {url:?}: {source}")]
    Connect {
        /// Endpoint label.
        url: String,
        /// Underlying tonic transport error.
        #[source]
        source: tonic::transport::Error,
    },
    /// `GetVersion` RPC failed.
    #[error("GetVersion against {url:?} failed: {source}")]
    GetVersion {
        /// Endpoint label.
        url: String,
        /// Underlying tonic status.
        #[source]
        source: tonic::Status,
    },
    /// Server proto / plugin version was rejected by [`crate::proto::evaluate`].
    #[error("server proto version refused for {url:?}: {reason}")]
    VersionRefused {
        /// Endpoint label.
        url: String,
        /// Reason text built from the [`Compatibility`] variant.
        reason: String,
    },
    /// `Subscribe` bidirectional stream open failed.
    #[error("Subscribe stream open failed for {url:?}: {source}")]
    SubscribeOpen {
        /// Endpoint label.
        url: String,
        /// Underlying tonic status.
        #[source]
        source: tonic::Status,
    },
}

/// Interceptor that attaches the `x-token` metadata header to every
/// outbound RPC.
#[derive(Clone)]
pub struct XTokenInterceptor {
    token: AsciiMetadataValue,
}

impl Interceptor for XTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("x-token", self.token.clone());
        Ok(request)
    }
}

/// Connected client + the intercepted service it wraps. The client is
/// `Clone` so the caller can use it for multiple RPCs (e.g. `GetVersion`
/// followed by `Subscribe`) without re-establishing TCP.
pub type Client = GeyserClient<InterceptedService<Channel, XTokenInterceptor>>;

/// Per-message decode cap if no override is supplied. Sized for
/// worst-case Solana mainnet blocks with `--with-blocks` (observed
/// 4-5 MiB during pump.fun surges) plus headroom.
pub const DEFAULT_MAX_DECODE_BYTES: usize = 64 * 1024 * 1024;

/// Build a connected [`Client`] for an [`EndpointSpec`].
///
/// TLS is auto-detected from the scheme (`https://`) and force-enabled
/// when `endpoint.tls_forced` is set, using the native root store. The
/// x-token metadata interceptor is installed on the channel so every RPC
/// carries it. The per-message decode cap is set via
/// [`Client::max_decoding_message_size`] so full-block streams don't
/// trigger tonic's default 4 MiB limit.
///
/// # Errors
/// See [`YellowstoneError`] variants:
/// - [`YellowstoneError::InvalidEndpoint`] for unparseable URLs.
/// - [`YellowstoneError::InvalidXToken`] for malformed tokens.
/// - [`YellowstoneError::Connect`] for TCP/TLS failures.
pub async fn connect(endpoint: &EndpointSpec) -> Result<Client, YellowstoneError> {
    connect_with_decode_limit(endpoint, DEFAULT_MAX_DECODE_BYTES).await
}

/// Variant of [`connect`] that takes an explicit per-message decode cap.
///
/// # Errors
/// Same conditions as [`connect`].
pub async fn connect_with_decode_limit(
    endpoint: &EndpointSpec,
    max_decode_bytes: usize,
) -> Result<Client, YellowstoneError> {
    let url = endpoint.url.clone();
    let token: AsciiMetadataValue = endpoint
        .x_token
        .parse()
        .map_err(|source| YellowstoneError::InvalidXToken {
            url: url.clone(),
            source,
        })?;

    let mut ep = Endpoint::from_shared(url.clone()).map_err(|source| {
        YellowstoneError::InvalidEndpoint {
            url: url.clone(),
            source,
        }
    })?;

    if endpoint.tls_forced || url.starts_with("https://") {
        ep = ep
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|source| YellowstoneError::Connect {
                url: url.clone(),
                source,
            })?;
    }

    ep = ep.connect_timeout(DEFAULT_CONNECT_TIMEOUT);

    let channel = ep
        .connect()
        .await
        .map_err(|source| YellowstoneError::Connect {
            url: url.clone(),
            source,
        })?;

    let interceptor = XTokenInterceptor { token };
    let client = GeyserClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(max_decode_bytes);
    Ok(client)
}

/// Call `GetVersion` and evaluate the response against the harness's
/// built-against proto version (the proto policy).
///
/// # Errors
/// - [`YellowstoneError::GetVersion`] if the RPC itself fails.
/// - [`YellowstoneError::VersionRefused`] if the server reports a proto
///   version that the harness refuses to operate against.
pub async fn fetch_and_evaluate_version(
    client: &mut Client,
    url: &str,
) -> Result<EndpointVersion, YellowstoneError> {
    let request = Request::new(GetVersionRequest {});
    let resp: GetVersionResponse = tokio::time::timeout(
        DEFAULT_GET_VERSION_TIMEOUT,
        client.get_version(request),
    )
    .await
    .map_err(|_| YellowstoneError::GetVersion {
        url: url.to_string(),
        source: Status::deadline_exceeded("GetVersion timed out"),
    })?
    .map_err(|source| YellowstoneError::GetVersion {
        url: url.to_string(),
        source,
    })?
    .into_inner();

    let evaluation = evaluate(&resp.version);
    if let Compatibility::RefuseOlderMajor { harness, server } = &evaluation.compatibility {
        return Err(YellowstoneError::VersionRefused {
            url: url.to_string(),
            reason: format!(
                "server proto major {server} is older than harness major {harness}"
            ),
        });
    }
    if let Compatibility::RefuseBaseline {
        package,
        minimum,
        reported,
    } = &evaluation.compatibility
    {
        return Err(YellowstoneError::VersionRefused {
            url: url.to_string(),
            reason: format!(
                "server plugin {package} reports {reported} which is below the supported \
                 minimum {minimum}"
            ),
        });
    }
    Ok(evaluation)
}

/// One decoded gRPC frame with the user-space `(mono, wall)` timestamp
/// captured immediately after decode (the precision posture fallback path).
#[derive(Debug)]
pub struct TimedUpdate {
    /// Arrival timestamp pair.
    pub ts: EventTimestamp,
    /// Decoded protobuf payload.
    pub update: SubscribeUpdate,
}

/// Open a `Subscribe` bidirectional stream and return it adapted to yield
/// [`TimedUpdate`] values.
///
/// The send-half closes immediately after the single `SubscribeRequest`
/// is delivered, because `stream::iter` is a finite stream — this avoids
/// some provider tiers' filter-quota accounting from misreading an
/// open send-half as a pending filter addition.
///
/// # Errors
/// [`YellowstoneError::SubscribeOpen`] when the bidirectional stream
/// can't be opened.
pub async fn open_subscription(
    client: &mut Client,
    request: SubscribeRequest,
    url: &str,
    clock: ClockOrigin,
) -> Result<impl Stream<Item = Result<TimedUpdate, Status>>, YellowstoneError> {
    // Wire-level dump for diagnostics — at debug level so production
    // runs aren't noisy. Includes filter-key inventory so operators can
    // cross-check what the server is being asked to do.
    tracing::debug!(
        url,
        slots_keys = ?request.slots.keys().collect::<Vec<_>>(),
        accounts_keys = ?request.accounts.keys().collect::<Vec<_>>(),
        transactions_keys = ?request.transactions.keys().collect::<Vec<_>>(),
        transactions_status_keys = ?request.transactions_status.keys().collect::<Vec<_>>(),
        blocks_keys = ?request.blocks.keys().collect::<Vec<_>>(),
        blocks_meta_keys = ?request.blocks_meta.keys().collect::<Vec<_>>(),
        entry_keys = ?request.entry.keys().collect::<Vec<_>>(),
        commitment = ?request.commitment,
        from_slot = ?request.from_slot,
        accounts_data_slice_count = request.accounts_data_slice.len(),
        ping = request.ping.is_some(),
        "opening Subscribe with request body"
    );
    tracing::trace!(?request, "full SubscribeRequest");

    // Single-item finite stream: closes the send-half immediately after
    // delivery. Equivalent to `grpcurl -d '<one json>'` which EOFs after
    // the single message.
    let request_stream = stream::iter(vec![request]);
    let response = client
        .subscribe(Request::new(request_stream))
        .await
        .map_err(|source| YellowstoneError::SubscribeOpen {
            url: url.to_string(),
            source,
        })?;
    let stream = response.into_inner();
    Ok(stream.map(move |item| {
        // Precision posture fallback: capture the (mono, wall) pair immediately
        // after the decode returns.
        let ts = clock.now_user_space();
        item.map(|update| TimedUpdate { ts, update })
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EndpointSpec;

    #[test]
    fn invalid_url_is_invalid_endpoint_error() {
        // Build an endpoint string that the URI parser will reject.
        let endpoint = EndpointSpec {
            url: String::new(),
            x_token: "tok".into(),
            tls_forced: false,
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        match rt.block_on(connect(&endpoint)) {
            Ok(_) => panic!("expected InvalidEndpoint, got Ok"),
            Err(YellowstoneError::InvalidEndpoint { .. }) => {}
            Err(other) => panic!("expected InvalidEndpoint, got {other:?}"),
        }
    }

    #[test]
    fn version_refused_when_plugin_below_baseline() {
        let raw = r#"{"package":"yellowstone-grpc-geyser","version":"12.1.0","proto":"12.1.0"}"#;
        let eval = crate::proto::evaluate(raw);
        assert!(eval.compatibility.is_refusal());
    }
}

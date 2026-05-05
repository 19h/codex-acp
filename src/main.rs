use codex_acp::{
    CodexAgent, FsBridge,
    agent::ClientOp::{ReadTextFile, RequestPermission, WriteTextFile},
};

use agent_client_protocol::{AgentSideConnection, Client, Error};
use anyhow::{Result, bail};
use codex_core::config::{self, Config};
use codex_utils_absolute_path::AbsolutePathBuf;
use std::env;
use tokio::{
    io,
    sync::mpsc,
    task::{self, LocalSet},
};
use tokio_util::compat::{TokioAsyncReadCompatExt as _, TokioAsyncWriteCompatExt as _};
use tracing::error;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    codex_acp::init_from_env()?;

    if env::args().nth(1).as_deref() == Some("--acp-fs-mcp") {
        return codex_acp::fs::run_mcp_server().await;
    }
    if env::args().nth(1).as_deref() == Some("--acp-sse-mcp") {
        return codex_acp::mcp_sse_proxy::run_from_env();
    }

    let outgoing = io::stdout().compat_write();
    let incoming = io::stdin().compat();

    let local_set = LocalSet::new();
    local_set.run_until(async move {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (client_tx, mut client_rx) = mpsc::unbounded_channel();

        // Load config and profiles in a single pass to avoid redundant I/O.
        // We load the TOML first to get profiles, then construct Config from it.
        let config = Config::load_with_cli_overrides(vec![]).await?;
        let cwd_abs = AbsolutePathBuf::from_absolute_path(&config.cwd)
            .map_err(|e| anyhow::anyhow!("failed to resolve absolute path for cwd: {}", e))?;

        let config_toml = config::load_config_as_toml_with_cli_overrides(
            &config.codex_home,
            &cwd_abs,
            vec![],
        ).await?;

        let profiles = config_toml.profiles;
        let cwd_path = config.cwd.clone();
        let fs_bridge = FsBridge::start(client_tx.clone(), cwd_path).await?;
        let agent = CodexAgent::with_config(tx, client_tx, config, profiles, Some(fs_bridge));
        let session_manager = agent.session_manager().clone();
        let (conn, handle_io) = AgentSideConnection::new(agent, outgoing, incoming, |fut| {
            task::spawn_local(fut);
        });

        task::spawn_local(async move {
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some((session_notification, tx)) => {
                                let result = conn.session_notification(session_notification).await;
                                if let Err(e) = result { error!(error = ?e, "failed to send session notification"); break; }
                                let _ = tx.send(());
                            }
                            None => break,
                        }
                    }
                    op = client_rx.recv() => {
                        match op {
                            Some(RequestPermission { request: req, response_tx: tx }) => {
                                let res = conn.request_permission(req).await;
                                let _ = tx.send(res);
                            }
                            Some(ReadTextFile { request: mut req, response_tx: tx }) => {
                                match session_manager.resolve_acp_session_id(&req.session_id) {
                                    Some(resolved_id) => {
                                        req.session_id = resolved_id;
                                        let res = conn.read_text_file(req).await;
                                        let _ = tx.send(res);
                                    }
                                    None => {
                                        let err = Error::invalid_params()
                                            .data("unknown session for read_text_file");
                                        let _ = tx.send(Err(err));
                                    }
                                }
                            }
                            Some(WriteTextFile { request: mut req, response_tx: tx }) => {
                                match session_manager.resolve_acp_session_id(&req.session_id) {
                                    Some(resolved_id) => {
                                        req.session_id = resolved_id.clone();
                                        if session_manager.is_read_only(&resolved_id) {
                                            let err = Error::invalid_params()
                                                .data("write_text_file is disabled while session mode is read-only");
                                            let _ = tx.send(Err(err));
                                        } else {
                                            let res = conn.write_text_file(req).await;
                                            let _ = tx.send(res);
                                        }
                                    }
                                    None => {
                                        let err = Error::invalid_params()
                                            .data("unknown session for write_text_file");
                                        let _ = tx.send(Err(err));
                                    }
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        match handle_io.await {
            Ok(()) => Ok(()),
            Err(e) => bail!(e),
        }
    }).await
}

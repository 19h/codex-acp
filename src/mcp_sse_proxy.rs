use std::{
    collections::HashMap,
    env,
    io::{self, BufRead, BufReader, Write},
    net::TcpStream,
    sync::Arc,
    thread,
};

use anyhow::{Context, Result, anyhow, bail};
use tracing::{debug, warn};

const URL_ENV: &str = "ACP_MCP_SSE_URL";
const HEADERS_ENV: &str = "ACP_MCP_SSE_HEADERS";

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpUrl {
    host: String,
    port: u16,
    path: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SseEvent {
    event: String,
    data: String,
}

/// Run a stdio MCP transport proxy for legacy MCP-over-SSE servers.
///
/// Codex Core only speaks stdio and streamable HTTP. ACP clients can still
/// provide legacy SSE MCP servers, so this process bridges Codex's stdio MCP
/// traffic to a client SSE endpoint.
pub fn run_from_env() -> Result<()> {
    crate::init_from_env()?;

    let url = env::var(URL_ENV).with_context(|| format!("{URL_ENV} is required"))?;
    let headers = env::var(HEADERS_ENV)
        .ok()
        .map(|raw| serde_json::from_str::<HashMap<String, String>>(&raw))
        .transpose()
        .context("failed to parse ACP_MCP_SSE_HEADERS")?
        .unwrap_or_default();

    run(&url, headers)
}

fn run(url: &str, headers: HashMap<String, String>) -> Result<()> {
    let mut sse_reader = connect_sse(url, &headers)?;
    let endpoint = read_endpoint_event(&mut sse_reader)?;
    debug!(endpoint = %endpoint, "connected legacy SSE MCP proxy");

    let headers = Arc::new(headers);
    let endpoint_for_stdin = endpoint.clone();
    let headers_for_stdin = Arc::clone(&headers);
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(line) if line.trim().is_empty() => {}
                Ok(line) => {
                    if let Err(err) = post_json(&endpoint_for_stdin, &headers_for_stdin, &line) {
                        warn!(error = %err, "failed to forward MCP stdin message to SSE endpoint");
                    }
                }
                Err(err) => {
                    warn!(error = %err, "failed reading MCP stdin");
                    break;
                }
            }
        }
    });

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    while let Some(event) = read_sse_event(&mut sse_reader)? {
        if event.event == "message" || event.event.is_empty() {
            stdout.write_all(event.data.as_bytes())?;
            stdout.write_all(b"\n")?;
            stdout.flush()?;
        }
    }

    Ok(())
}

fn connect_sse(url: &str, headers: &HashMap<String, String>) -> Result<BufReader<TcpStream>> {
    let parsed = parse_http_url(url)?;
    let mut stream = TcpStream::connect((parsed.host.as_str(), parsed.port))
        .with_context(|| format!("failed to connect to MCP SSE server at {url}"))?;

    let request = build_http_request(
        "GET",
        &parsed,
        headers,
        &[
            ("Accept", "text/event-stream"),
            ("Cache-Control", "no-cache"),
        ],
        None,
    )?;
    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    let status = read_status_and_headers(&mut reader)?;
    if !(200..300).contains(&status) {
        bail!("SSE connect failed with HTTP status {status}");
    }
    Ok(reader)
}

fn read_endpoint_event(reader: &mut BufReader<TcpStream>) -> Result<String> {
    while let Some(event) = read_sse_event(reader)? {
        if event.event == "endpoint" {
            if event.data.trim().is_empty() {
                bail!("MCP SSE endpoint event did not include a data URL");
            }
            return Ok(event.data.trim().to_owned());
        }
    }
    bail!("MCP SSE stream closed before endpoint event")
}

fn post_json(url: &str, headers: &HashMap<String, String>, body: &str) -> Result<()> {
    let parsed = parse_http_url(url)?;
    let mut stream = TcpStream::connect((parsed.host.as_str(), parsed.port))
        .with_context(|| format!("failed to connect to MCP SSE post endpoint at {url}"))?;
    let content_length = body.len().to_string();
    let request = build_http_request(
        "POST",
        &parsed,
        headers,
        &[
            ("Content-Type", "application/json"),
            ("Content-Length", &content_length),
        ],
        Some(body),
    )?;
    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    let status = read_status_and_headers(&mut reader)?;
    if !(200..300).contains(&status) {
        bail!("MCP SSE post failed with HTTP status {status}");
    }
    Ok(())
}

fn build_http_request(
    method: &str,
    url: &HttpUrl,
    headers: &HashMap<String, String>,
    extra_headers: &[(&str, &str)],
    body: Option<&str>,
) -> Result<String> {
    let mut request = format!(
        "{method} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
        url.path, url.host
    );

    for (name, value) in extra_headers {
        append_header(&mut request, name, value)?;
    }
    for (name, value) in headers {
        append_header(&mut request, name, value)?;
    }

    request.push_str("\r\n");
    if let Some(body) = body {
        request.push_str(body);
    }
    Ok(request)
}

fn append_header(request: &mut String, name: &str, value: &str) -> Result<()> {
    if name.contains(['\r', '\n', ':']) || value.contains(['\r', '\n']) {
        bail!("invalid HTTP header for MCP SSE proxy");
    }
    request.push_str(name);
    request.push_str(": ");
    request.push_str(value);
    request.push_str("\r\n");
    Ok(())
}

fn read_status_and_headers(reader: &mut BufReader<TcpStream>) -> Result<u16> {
    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow!("missing HTTP status"))?
        .parse::<u16>()
        .context("invalid HTTP status")?;

    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 || line == "\r\n" || line == "\n" {
            break;
        }
    }

    Ok(status)
}

fn read_sse_event<R: BufRead>(reader: &mut R) -> Result<Option<SseEvent>> {
    let mut event = SseEvent::default();
    let mut saw_field = false;
    let mut line = String::new();

    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            return if saw_field { Ok(Some(event)) } else { Ok(None) };
        }

        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            if saw_field {
                return Ok(Some(event));
            }
            continue;
        }
        if trimmed.starts_with(':') {
            continue;
        }

        saw_field = true;
        let (field, value) = trimmed
            .split_once(':')
            .map(|(field, value)| (field, value.strip_prefix(' ').unwrap_or(value)))
            .unwrap_or((trimmed, ""));
        match field {
            "event" => event.event = value.to_owned(),
            "data" => {
                if !event.data.is_empty() {
                    event.data.push('\n');
                }
                event.data.push_str(value);
            }
            _ => {}
        }
    }
}

fn parse_http_url(url: &str) -> Result<HttpUrl> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| anyhow!("legacy MCP SSE proxy only supports http:// URLs"))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() {
        bail!("missing host in URL");
    }

    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() => (host, port.parse::<u16>()?),
        _ => (authority, 80),
    };

    Ok(HttpUrl {
        host: host.to_owned(),
        port,
        path: format!("/{path}"),
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn parse_http_url_defaults_port_and_preserves_query() {
        let parsed = parse_http_url("http://127.0.0.1/sse?x=1").unwrap();
        assert_eq!(
            parsed,
            HttpUrl {
                host: "127.0.0.1".into(),
                port: 80,
                path: "/sse?x=1".into(),
            }
        );
    }

    #[test]
    fn parse_http_url_reads_explicit_port() {
        let parsed = parse_http_url("http://localhost:4567/message?sessionId=abc").unwrap();
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.port, 4567);
        assert_eq!(parsed.path, "/message?sessionId=abc");
    }

    #[test]
    fn read_sse_event_collects_multiline_data() {
        let mut reader = Cursor::new("event: message\ndata: one\ndata: two\n\n");
        let event = read_sse_event(&mut reader).unwrap().unwrap();
        assert_eq!(event.event, "message");
        assert_eq!(event.data, "one\ntwo");
    }

    #[test]
    fn read_sse_event_ignores_keepalives() {
        let mut reader = Cursor::new(": keepalive\n\nevent: endpoint\ndata: http://x\n\n");
        let event = read_sse_event(&mut reader).unwrap().unwrap();
        assert_eq!(event.event, "endpoint");
        assert_eq!(event.data, "http://x");
    }
}

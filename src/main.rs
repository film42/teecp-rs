use serde::{Deserialize, Serialize};
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{
    tcp::{ReadHalf, WriteHalf},
    TcpListener, TcpStream,
};

#[derive(Deserialize, Serialize, Debug)]
struct Config {
    bind: String,
    proxy: String,
    tees: Vec<String>,
}

static EXAMPLE_CONFIG: &str = r#"
{
  "bind": "127.0.0.1:10000",
  "proxy": "127.0.0.1:6379",
  "tees": [
    "127.0.0.1:6379",
    "127.0.0.1:6379",
    "127.0.0.1:6379",
    "127.0.0.1:6379",
    "127.0.0.1:6379"
  ]
}
"#;

async fn copy_to_all_writers<R, W>(mut reader: R, mut writers: Vec<W>) -> io::Result<()>
where
    R: AsyncRead + std::marker::Unpin,
    W: AsyncWrite + std::marker::Unpin,
{
    let mut buffer = [0; 8096];
    loop {
        let n = reader.read(&mut buffer[..]).await?;
        if n == 0 {
            // The reader has no more bytes to consume.
            break;
        }

        // Do a full copy of the buffer to each writer.
        // If the write fails, we should bail early.
        for writer in writers.iter_mut() {
            writer.write_all(&buffer[..n]).await?;
        }
    }

    Ok(())
}

async fn sink<'a>(mut reader: ReadHalf<'a>) -> io::Result<()> {
    let mut buffer = [0; 8096];
    loop {
        let n = reader.read(&mut buffer[..]).await?;
        if n == 0 {
            // The reader has no more bytes to consume.
            break;
        }
    }
    Ok(())
}

async fn handle_client(
    mut client: TcpStream,
    upstream_proxy: SocketAddr,
    upstream_tees: Vec<SocketAddr>,
) -> io::Result<()> {
    let mut proxy = TcpStream::connect(upstream_proxy.clone()).await?;

    let mut tees: Vec<TcpStream> = futures::future::try_join_all(
        upstream_tees
            .iter()
            .map(|tee_addr| TcpStream::connect(tee_addr)),
    )
    .await?;

    let (tee_readers, mut tee_writers): (Vec<ReadHalf>, Vec<WriteHalf>) = tees.iter_mut().fold(
        (vec![], vec![]),
        |(mut readers, mut writers), tcp_stream| {
            let (reader, writer) = tcp_stream.split();
            readers.push(reader);
            writers.push(writer);
            (readers, writers)
        },
    );

    let (mut client_reader, mut client_writer) = client.split();
    let (mut proxy_reader, proxy_writer) = proxy.split();
    tee_writers.push(proxy_writer);

    // Write all proxy bytes back to the client. Drop all tee response bytes.
    // Wait for any of the readers or writers to exit. Drop the entire combined
    // future if it happens.
    futures::future::select(
        Box::pin(async { tokio::io::copy(&mut proxy_reader, &mut client_writer).await }),
        futures::future::select(
            Box::pin(async { copy_to_all_writers(&mut client_reader, tee_writers).await }),
            Box::pin(async {
                let sinks = tee_readers.into_iter().map(|reader| sink(reader));
                futures::future::join_all(sinks).await
            }),
        ),
    )
    .await;

    Ok(())
}

// 1. Listen for a connection.
// 2. Connect to the upstream proxy.
// 3. Connect to tee'd upstreams.
// 4. Reply with all bytes from upstream proxy.
// 5. Drop all bytes from upstream tees.
#[tokio::main]
async fn main() -> io::Result<()> {
    let config: Config = serde_json::from_str(EXAMPLE_CONFIG)?;
    println!("Starting server with config: {:?}", config);

    let bind_addr = config.bind.parse::<SocketAddr>().unwrap();
    let upstream_proxy = config.proxy.parse::<SocketAddr>().unwrap();
    let upstream_tees: Vec<SocketAddr> = config
        .tees
        .into_iter()
        .map(|addr| addr.parse::<SocketAddr>().unwrap())
        .collect();

    println!("Listening on: {:?}", bind_addr);
    let mut listener = TcpListener::bind(bind_addr).await?;
    loop {
        let (client, _) = listener.accept().await?;
        let client_info = format!("{:?}", client);
        println!("Client connected: {:?}", client_info);
        tokio::spawn({
            let upstream_tees = upstream_tees.clone();

            async move {
                // All client bytes should be sent to proxy and tees.
                if let Err(err) = handle_client(client, upstream_proxy, upstream_tees).await {
                    println!("Error: {:?}", err);
                }
                println!("Client connection cleaned up: {:?}", client_info);
            }
        });
    }
}

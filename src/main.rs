use serde::{Deserialize, Serialize};
use serde_json::Result;

use std::io::{self, Write};
use std::net::SocketAddr; // Shutdown

use futures::future::join_all;
use tokio::io::copy; // shutdown
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

#[derive(Debug)]
struct WriteProxy;

impl WriteProxy {
    pub fn sink() -> WriteProxy {
        WriteProxy
    }
}

impl Write for WriteProxy {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        println!("Sink write: {:?}", std::str::from_utf8(buf));
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl AsyncWrite for WriteProxy {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        Ok(().into())
    }
}

// Multi Writer
#[derive(Debug)]
struct MultiWriter<T>
where
    T: Sized,
{
    writers: Vec<Box<T>>,
}

impl<T> MultiWriter<T>
where
    T: AsyncWrite,
{
    pub fn new() -> MultiWriter<T> {
        MultiWriter { writers: vec![] }
    }

    fn push(&mut self, writer: Box<T>) {
        self.writers.push(writer);
    }
}

impl<T> Write for MultiWriter<T>
where
    T: AsyncWrite,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        for writer in self.writers.iter_mut() {
            let n = writer.write(buf)?;
            if n != buf.len() {
                return Err(io::Error::new(io::ErrorKind::Other, "Short write!"));
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        for writer in self.writers.iter_mut() {
            writer.flush()?;
        }
        Ok(())
    }
}

impl<T> AsyncWrite for MultiWriter<T>
where
    T: AsyncWrite,
{
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        for writer in self.writers.iter_mut() {
            writer.shutdown()?;
        }
        Ok(tokio::prelude::Async::Ready(()))
    }
}

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

fn proxy_copy<R1, W1, R2, W2>(
    src_reader: R1,
    dest_writer: W1,
    dest_reader: R2,
    src_writer: W2,
) -> tokio::prelude::future::Select2<tokio::io::Copy<R1, W1>, tokio::io::Copy<R2, W2>>
where
    R1: AsyncRead,
    R2: AsyncRead,
    W1: AsyncWrite,
    W2: AsyncWrite,
{
    let src_to_dest = copy(dest_reader, src_writer);
    let dest_to_src = copy(src_reader, dest_writer);
    dest_to_src.select2(src_to_dest)
}

fn main() -> Result<()> {
    let c: Config = serde_json::from_str(EXAMPLE_CONFIG)?;
    let bind_addr = c.bind.to_string().parse::<SocketAddr>().unwrap();
    let proxy_addr = c.proxy.to_string().parse::<SocketAddr>().unwrap();
    println!("{:?}, {:?}", c, proxy_addr);

    let tee_addrs: Vec<SocketAddr> = c
        .tees
        .iter()
        .map(|s| s.to_string().parse::<SocketAddr>().unwrap())
        .collect();
    println!("Tees: {:?}", tee_addrs);

    let listener = TcpListener::bind(&bind_addr).unwrap();

    let done = listener
        .incoming()
        .map_err(|e| println!("Error accepting connection: {}", e))
        .for_each(move |client| {
            let proxy_conn = TcpStream::connect(&proxy_addr);
            let mut connect_futures = vec![proxy_conn];

            for tee_addr in tee_addrs.iter() {
                connect_futures.push(TcpStream::connect(tee_addr))
            }

            let lifecycle = join_all(connect_futures).and_then(|mut conns| {
                println!("Conns: {:?}", conns);

                let (clt_reader, clt_writer) = client.split();
                let srv_stream = conns.remove(0);
                let (srv_reader, srv_writer) = srv_stream.split();

                let mut multi_writer = MultiWriter::new();
                multi_writer.push(Box::new(srv_writer));

                let tee_reader_copy_futures: Vec<
                    tokio::io::Copy<tokio::io::ReadHalf<TcpStream>, WriteProxy>,
                > = conns
                    .drain(..)
                    .map(|tee_conn| {
                        let (tee_reader, tee_writer) = tee_conn.split();
                        multi_writer.push(Box::new(tee_writer));
                        copy(tee_reader, WriteProxy::sink())
                    })
                    .collect();

                proxy_copy(
                    Box::new(clt_reader),
                    Box::new(multi_writer),
                    Box::new(srv_reader),
                    Box::new(clt_writer),
                )
                .select2(tokio::prelude::future::select_all(tee_reader_copy_futures))
                .then(|x| {
                    x.map(|_| println!("It completed!"))
                        .map_err(|err| eprintln!("An error occurred during the proxy: {:?}", err))
                        .expect("never errors");

                    // Do this because the type system doesn't want an io::Error here?
                    future::ok(())
                })
            });

            tokio::spawn(
                lifecycle
                    .map(|_| println!("The proxy is done"))
                    .map_err(|err| eprintln!("Proxy err: {:?}", err)),
            )
        });

    tokio::run(done);
    Ok(())
}

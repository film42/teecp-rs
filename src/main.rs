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

// fn proxy(
//     src: TcpStream,
//     dest: TcpStream,
// ) -> tokio::prelude::future::Select<
//     tokio::io::Copy<tokio::io::ReadHalf<TcpStream>, tokio::io::WriteHalf<TcpStream>>,
//     tokio::io::Copy<tokio::io::ReadHalf<TcpStream>, tokio::io::WriteHalf<TcpStream>>,
// > {
//     let (src_reader, src_writer) = src.split();
//     let (dest_reader, dest_writer) = dest.split();
//     let src_to_dest = copy(dest_reader, src_writer);
//     let dest_to_src = copy(src_reader, dest_writer);
//     dest_to_src.select(src_to_dest)
// }

// #[derive(Debug)]
// struct WriteProxy<T> {
//     writer: Option<T>,
// }

// impl<T> WriteProxy<T>
// where
//     T: AsyncWrite + Write + Send + Sync,
// {
//     pub fn sink() -> WriteProxy<T> {
//         WriteProxy { writer: None }
//     }

//     pub fn new(writer: T) -> WriteProxy<T> {
//         WriteProxy {
//             writer: Some(writer),
//         }
//     }
// }

// impl<T: Write> Write for WriteProxy<T> {
//     fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
//         match &mut self.writer {
//             Some(ref mut writer) => writer.write(buf),
//             None => Ok(buf.len()),
//         }
//     }
//     fn flush(&mut self) -> io::Result<()> {
//         match &mut self.writer {
//             Some(ref mut writer) => writer.flush(),
//             None => Ok(()),
//         }
//     }
// }
// impl<T: AsyncWrite> AsyncWrite for WriteProxy<T> {
//     fn shutdown(&mut self) -> Poll<(), io::Error> {
//         match &mut self.writer {
//             Some(ref mut writer) => writer.shutdown(),
//             None => Ok(().into()),
//         }
//     }
// }

// // Multi-writer type.
// struct MultiWriterStream(Vec<ProxyTcpStream>);

// impl Write for MultiWriterStream {
//     fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
//         for stream in self.0.iter_mut() {
//             let res = stream.write(buf);
//             match res {
//                 Ok(len) => {
//                     if len != buf.len() {
//                         return Err(io::Error::new(io::ErrorKind::Other, "short write"))
//                     }
//                 },
//                 _ => return res
//             }
//         }
//         return Ok(buf.len())
//     }

//     fn flush(&mut self) -> io::Result<()> {
//         Ok(())
//     }
// }

// // This is a custom type used to have a custom implementation of the
// // `AsyncWrite::shutdown` method which actually calls `TcpStream::shutdown` to
// // notify the remote end that we're done writing.
// #[derive(Clone)]
// struct ProxyTcpStream(Arc<Mutex<TcpStream>>);

// impl Read for ProxyTcpStream {
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//         self.0.lock().unwrap().read(buf)
//     }
// }

// impl Write for ProxyTcpStream {
//     fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
//         self.0.lock().unwrap().write(buf)
//     }

//     fn flush(&mut self) -> io::Result<()> {
//         Ok(())
//     }
// }

// impl AsyncRead for ProxyTcpStream {}

// impl AsyncWrite for ProxyTcpStream {
//     fn shutdown(&mut self) -> Poll<(), io::Error> {
//         self.0.lock().unwrap().shutdown(Shutdown::Write);
//         Ok(().into())
//     }
// }

// fn connect_and_proxy(srcConn: ProxyTcpStream, proxyConn: ProxyTcpStream, c: &Config) {
//     let tees = join_all(c.tees.iter().map(|addr_str| {
//         let addr = addr_str.parse::<SocketAddr>().unwrap();
//         TcpStream::connect(&addr).and_then(move |tee_conn| {
//             futures::future::ok::<ProxyTcpStream, std::io::Error>(
//                 ProxyTcpStream(Arc::new(Mutex::new(tee_conn))))
//         })
//     }));

//     tees.map(|conns| {
//         let teeWriter = MultiWriterStream(conns);
//         let proxyToTees = copy(proxyConn, srcConn);
//         let teeFutures = conns.iter().map(|teeConn| {
//             let sink = SinkWriter{};
//             copy(teeConn.clone(), sink)
//         });

//     });

//     //.map_err(|e| println!("error connecting to tee: {}", e) );

//     // tee_conns + 1;

//     // let tee_conns = join_all();

//     // tee_conns.and_then(|x| x);

//     // tee_conns.map(|conns| {
//     //     let x: Vec<ProxyTcpStream> = conns.into_iter().map(|conn| {
//     //         ProxyTcpStream(Arc::new(Mutex::new(conn)))
//     //     }).collect();
//     //     x
//     // }).map_err(|e| println!("error connecting to tee: {}", e) );
// }

use serde::{Deserialize, Serialize};
use serde_json::Result;

//use std::io::{self, Read, Write};
use std::net::SocketAddr; // Shutdown
                          //use std::sync::{Arc, Mutex};

//use futures::future::join_all;
use tokio::io::copy; // shutdown
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

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
    "127.0.0.1:6379"
  ]
}
"#;

fn main() -> Result<()> {
    //let
    let c: Config = serde_json::from_str(EXAMPLE_CONFIG)?;
    let bind_addr = c.bind.to_string().parse::<SocketAddr>().unwrap();
    let proxy_addr = c.proxy.to_string().parse::<SocketAddr>().unwrap();
    println!("{:?}, {:?}", c, proxy_addr);

    let listener = TcpListener::bind(&bind_addr).unwrap();

    let done = listener
        .incoming()
        .map_err(|e| println!("Error accepting connection: {}", e))
        .for_each(move |client| {
            let proxy_conn = TcpStream::connect(&proxy_addr);

            let lifecycle = proxy_conn.and_then(move |server| {
                println!("{:?}", server);

                let (client_reader, client_writer) = client.split();
                let (server_reader, server_writer) = server.split();

                let client_to_server = copy(server_reader, client_writer);
                let server_to_client = copy(client_reader, server_writer);

                server_to_client.select(client_to_server).then(|x| {
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
                    .map_err(|err| eprintln!("Proxy err: {}", err)),
            )
        });

    tokio::run(done);
    Ok(())
}

// struct SinkWriter;

// impl Write for SinkWriter {
//     fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
//         Ok(buf.len())
//     }
//     fn flush(&mut self) -> io::Result<()> {
//         Ok(())
//     }
// }
// impl AsyncWrite for SinkWriter {
//     fn shutdown(&mut self) -> Poll<(), io::Error> {
//         Ok(().into())
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

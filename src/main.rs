#[macro_use]
extern crate failure;
#[macro_use]
extern crate futures;

use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use futures::future::{select_all, try_select};
use futures::lock::Mutex;
use serde::{Deserialize, Serialize};
//use std::io::Write;
use futures::channel::mpsc;
use std::net::SocketAddr; // Shutdown
use std::sync::Arc;
use tokio::net::tcp::split::WriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::watch;

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

struct ByteForwarder {
    senders: Vec<futures::channel::mpsc::UnboundedSender<Vec<u8>>>,
}

impl ByteForwarder {
    fn new() -> ByteForwarder {
        ByteForwarder { senders: vec![] }
    }

    fn push_sender(&mut self, sender: futures::channel::mpsc::UnboundedSender<Vec<u8>>) {
        self.senders.push(sender)
    }
}

impl AsyncWrite for ByteForwarder {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context,
        mut buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let mut copy_of_bytes: Vec<u8> = vec![];
        // TODO: Check n-bytes copied to verify?
        std::io::copy(&mut buf, &mut copy_of_bytes)?;

        for sender in self.senders.iter() {
            eprintln!(
                "ByteForwarder: Forwarding bytes: {:?}",
                std::str::from_utf8(&copy_of_bytes)
            );

            // TODO: Be smart here.
            match sender.unbounded_send(copy_of_bytes.clone()) {
                Ok(_) => {}
                Err(e) => eprintln!("Error: {:?}", e),
            };
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(().into()))
    }
}

struct ChannelReader {
    channel: futures::channel::mpsc::UnboundedReceiver<Vec<u8>>,
}

impl AsyncRead for ChannelReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        mut buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        match Pin::new(&mut self.channel).poll_next(cx) {
            Poll::Ready(Some(new_bytes)) => {
                if buf.len() < new_bytes.len() {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Receiving buffer too small!",
                    )));
                }
                eprintln!(
                    "ChannelReader: Data received: {:?}",
                    std::str::from_utf8(&new_bytes)
                );

                match std::io::copy(&mut new_bytes.as_slice(), &mut buf) {
                    Ok(n) => Poll::Ready(Ok(n as usize)),
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
            Poll::Ready(None) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "channel closed",
            ))),
            Poll::Pending => Poll::Pending,
        }

        // match futures::ready!(self.channel.next()) {
        //     Some(new_bytes) => {
        //         if buf.len() < new_bytes.len() {
        //             return Poll::Ready(Err(std::io::Error::new(
        //                 std::io::ErrorKind::Other,
        //                 "Receiving buffer too small!",
        //             )));
        //         }
        //         eprintln!("ChannelReader: Data received: {:?}", std::str::from_utf8(new_bytes.as_ref()));

        //         match std::io::copy(&mut new_bytes.as_slice(), &mut buf) {
        //             Ok(n) => Poll::Ready(Ok(n as usize)),
        //             Err(e) => Poll::Ready(Err(e)),
        //         }
        //     },
        //     None => Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, "channel closed")))
        // }
    }
}

struct ClientRelay {
    // // Idea here is that we can send this via the closer channel
    // // and the client knows which connection broke.
    // id: String,
    // // This is our communication back to the client that we're
    // // all done and need to be torn down.
    // closer_channel: futures::channel::mpsc::UnboundedSender<String>,

    // This should contain bytes to forward to the tee.
    incoming_channel: futures::channel::mpsc::UnboundedReceiver<Vec<u8>>,
}

async fn connect_to_tees(
    tee_addrs: &Arc<Mutex<Vec<SocketAddr>>>,
) -> Result<Option<Vec<TcpStream>>, Box<dyn std::error::Error>> {
    let mut tees = vec![];
    for tee_addr in tee_addrs.lock().await.iter() {
        let tee_conn = TcpStream::connect(tee_addr).await?;
        println!("Was here -- tee : {:?}", tee_conn);
        tees.push(tee_conn);
    }
    Ok(Some(tees))
}

// async fn proxy_channel_receiver_to_async_writer<'a>(
//     relay: &mut ClientRelay,
//     mut writer: WriteHalf<'a>,
// ) -> Result<(), failure::Error> {
//     loop {
//         match relay.incoming_channel.next().await {
//             Some(mut buf) => writer.write_all(&mut buf).await?,
//             None => bail!("channel closed"),
//         };
//     }
// }

async fn spawn_proxy_for_tee(
    mut channel_reader: ChannelReader,
    mut tee_conn: TcpStream,
) -> Result<(), failure::Error> {
    let (mut tee_reader, tee_writer) = tee_conn.split();
    let mut sink = WriteProxy::sink();
    let mut writer = MultiWriter::new();
    writer.push(Box::new(WriteContainer::new_write_half(tee_writer)));

    match futures::future::try_join(channel_reader.copy(&mut writer), tee_reader.copy(&mut sink))
        .await
    {
        _ => Ok(()),
        Err(e) => bail!(e),
    }
}

// async fn spawn_proxy_for_tee(client_relay: ClientRelay, tee_conn: TcpStream) -> Result {
//     let (mut tee_reader, mut tee_writer) = tee_conn.split();
//     let mut sink = WriteProxy::sink();

//     try_join!(
//         proxy_channel_to_writer(client_relay.incoming, mut tee_writer),
//         tee_reader.copy(&mut sink),
//     )
//     .await?;
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // //let (tx, mut rx) = watch::channel("sender hung up");
    // let (tx, mut rx) = mpsc::unbounded();
    // //let tx = Arc::new(Mutex::new(tx));

    // use std::time::Duration;
    // use tokio::timer::delay;

    // for i in 1..10 {
    //     let mut tx = tx.clone();
    //     tokio::spawn(async move {
    //         println!("Starting for {}", i);
    //         delay(tokio::clock::now() + Duration::from_millis(100)).await;

    //         //tx.broadcast("leet haxor");
    //         tx.unbounded_send("closed");
    //         tx.disconnect();
    //         println!("Stopping for {}", i);
    //     });
    // }

    // match rx.next().await {
    //     Some(value) => println!("received stop = {:?} / shutting down", value),
    //     None => {}
    // };

    // println!("Moving on...");

    // -----------

    let c: Config = serde_json::from_str(EXAMPLE_CONFIG)?;
    let bind_addr = c.bind.to_string().parse::<SocketAddr>().unwrap();
    let proxy_addr = c.proxy.to_string().parse::<SocketAddr>().unwrap();
    println!("{:?}, {:?}, {:?}", c, bind_addr, proxy_addr);

    let tee_addrs: Vec<SocketAddr> = c
        .tees
        .iter()
        .map(|s| s.to_string().parse::<SocketAddr>().unwrap())
        .collect();

    let tee_addrs_mu = Arc::new(Mutex::new(tee_addrs));
    //println!("Tees: {:?}", tee_addrs);

    let mut listener = TcpListener::bind(&bind_addr).await?;

    loop {
        let (mut client, _) = listener.accept().await?;
        let mut proxy_conn = TcpStream::connect(&proxy_addr).await.unwrap();
        let tee_addrs_mu = tee_addrs_mu.clone();

        tokio::spawn(async move {
            let mut multi_writer = MultiWriter::new();

            let mut tee_conns = connect_to_tees(&tee_addrs_mu).await.unwrap().unwrap();

            println!("Attempt to proxy: {:?}, {:?}", proxy_conn, client);

            let (mut clt_reader, clt_writer) = client.split();
            let mut clt_multi_writer = MultiWriter::new();
            clt_multi_writer.push(Box::new(WriteContainer::new_write_half(clt_writer)));

            let (mut srv_reader, srv_writer) = proxy_conn.split();
            multi_writer.push(Box::new(WriteContainer::new_write_half(srv_writer)));

            let mut copy_futures = vec![];
            // let mut multi_writer = MultiWriter::new();
            // let mut sinks = vec![];

            let mut byte_forwarder = ByteForwarder::new();
            for tee_conn in tee_conns.drain(..) {
                let (tx, rx) = mpsc::unbounded();
                byte_forwarder.push_sender(tx);

                let relay = ChannelReader { channel: rx };
                tokio::spawn(async move {
                    eprintln!(
                        "Tee finished: {:?}",
                        spawn_proxy_for_tee(relay, tee_conn).await
                    );
                });
            }

            // LAST THING TO FIX:
            multi_writer.push(Box::new(WriteContainer::new_byte_forwarder(byte_forwarder)));

            // for tee_conn in tee_conns.iter_mut() {
            //     let (mut tee_reader, tee_writer) = tee_conn.split();
            //     multi_writer.push(Box::new(tee_writer));

            //     sinks.push(MultiWriter::sink());
            //     let sink = sinks.last_mut().unwrap();
            //     copy_futures.push(tee_reader.copy(sink));
            // }

            // let mut copy_futures: Vec<_> = tee_conns.into_iter().map(|mut tee_conn| {
            //     let (mut tee_reader, tee_writer) = tee_conn.split();
            //     multi_writer.push(Box::new(tee_writer));
            //     let mut sink = MultiWriter::sink();
            //     tee_reader.copy(&mut sink)
            // }).collect();

            // let mut copy_futures: Vec<_> = copy_futures1.into_iter().map(|(_,_,future)| future).collect();

            //let mut tee_conn = tee_conns.drain(..).next().unwrap();

            // }

            copy_futures.push(clt_reader.copy(&mut multi_writer));
            copy_futures.push(srv_reader.copy(&mut clt_multi_writer));

            // select_all(
            //     tee_conns
            //         .drain(..)
            //         .map(|tee_conn| {
            //             let (mut tee_reader, tee_writer) = tee_conn.split();
            //             multi_writer.push(Box::new(tee_writer));
            //             let mut sink = WriteProxy::sink();
            //             tee_reader.copy(&mut sink)
            //         })
            //         .collect(),
            // ),
            let (res, _, _) = select_all(copy_futures).await;
            res.unwrap();

            println!("All done");
        });
    }

    //     let done = listener
    //         .incoming()
    //         .map_err(|e| println!("Error accepting connection: {}", e))
    //         .for_each(move |client| {
    //             let proxy_conn = TcpStream::connect(&proxy_addr);
    //             let mut connect_futures = vec![proxy_conn];

    //             for tee_addr in tee_addrs.iter() {
    //                 connect_futures.push(TcpStream::connect(tee_addr))
    //             }

    //             let lifecycle = join_all(connect_futures).and_then(|mut conns| {
    //                 println!("Conns: {:?}", conns);

    //                 let (clt_reader, clt_writer) = client.split();
    //                 let srv_stream = conns.remove(0);
    //                 let (srv_reader, srv_writer) = srv_stream.split();

    //                 let mut multi_writer = MultiWriter::new();
    //                 multi_writer.push(Box::new(srv_writer));

    //                 let tee_reader_copy_futures: Vec<
    //                     tokio::io::Copy<tokio::io::ReadHalf<TcpStream>, WriteProxy>,
    //                 > = conns
    //                     .drain(..)
    //                     .map(|tee_conn| {
    //                         let (tee_reader, tee_writer) = tee_conn.split();
    //                         multi_writer.push(Box::new(tee_writer));
    //                         copy(tee_reader, WriteProxy::sink())
    //                     })
    //                     .collect();

    //                 proxy_copy(
    //                     Box::new(clt_reader),
    //                     Box::new(multi_writer),
    //                     Box::new(srv_reader),
    //                     Box::new(clt_writer),
    //                 )
    //                 .select2(tokio::prelude::future::select_all(tee_reader_copy_futures))
    //                 .then(|x| {
    //                     x.map(|_| println!("It completed!"))
    //                         .map_err(|err| eprintln!("An error occurred during the proxy: {:?}", err))
    //                         .expect("never errors");

    //                     // Do this because the type system doesn't want an io::Error here?
    //                     future::ok(())
    //                 })
    //             });

    //             tokio::spawn(
    //                 lifecycle
    //                     .map(|_| println!("The proxy is done"))
    //                     .map_err(|err| eprintln!("Proxy err: {:?}", err)),
    //             )
    //         });

    //     tokio::run(done);

    // Ok(())
}

// BELOW IS THE OLD IMPL USING FUTURES 0.1

// use serde::{Deserialize, Serialize};
// use serde_json::Result;

// use std::io::{self, Write};
// use std::net::SocketAddr; // Shutdown

// use futures::future::join_all;
// use tokio::io::copy; // shutdown
// use tokio::net::{TcpListener, TcpStream};
// use tokio::prelude::*;

#[derive(Debug)]
struct WriteProxy;

impl WriteProxy {
    pub fn sink() -> WriteProxy {
        WriteProxy
    }
}

// impl Write for WriteProxy {
//     fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
//         println!("Sink write: {:?}", std::str::from_utf8(buf));
//         Ok(buf.len())
//     }
//     fn flush(&mut self) -> std::io::Result<()> {
//         Ok(())
//     }
// }

impl AsyncWrite for WriteProxy {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(().into()))
    }
}

struct WriteContainer<'a> {
    write_half: Option<WriteHalf<'a>>,
    byte_forwarder: Option<ByteForwarder>,
}

impl<'a> WriteContainer<'a> {
    fn new_write_half(write_half: WriteHalf<'a>) -> WriteContainer<'a> {
        WriteContainer {
            write_half: Some(write_half),
            byte_forwarder: None,
        }
    }

    fn new_byte_forwarder(byte_forwarder: ByteForwarder) -> WriteContainer<'a> {
        WriteContainer {
            write_half: None,
            byte_forwarder: Some(byte_forwarder),
        }
    }
}

impl AsyncWrite for WriteContainer<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let mut me = Pin::new(&mut *self);
        if let Some(writer) = &mut me.write_half {
            return Pin::new(writer).poll_write(cx, buf);
        }
        if let Some(writer) = &mut me.byte_forwarder {
            return Pin::new(writer).poll_write(cx, buf);
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), std::io::Error>> {
        let mut me = Pin::new(&mut *self);
        if let Some(writer) = &mut me.write_half {
            return Pin::new(writer).poll_flush(cx);
        }
        if let Some(writer) = &mut me.byte_forwarder {
            return Pin::new(writer).poll_flush(cx);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut me = Pin::new(&mut *self);
        if let Some(writer) = &mut me.write_half {
            return Pin::new(writer).poll_shutdown(cx);
        }
        if let Some(writer) = &mut me.byte_forwarder {
            return Pin::new(writer).poll_shutdown(cx);
        }
        Poll::Ready(Ok(().into()))
    }
}

// enum WriterContainer {
//     WriteHalf(WriteHalf),
//     ChannelWriter(ChannelWriter),
// };

// Multi Writer
// #[derive(Debug)]
struct MultiWriter<'a> {
    writers: Vec<Box<WriteContainer<'a>>>,
}

impl<'a> MultiWriter<'a> {
    pub fn new() -> MultiWriter<'a> {
        MultiWriter { writers: vec![] }
    }

    pub fn sink() -> MultiWriter<'a> {
        MultiWriter { writers: vec![] }
    }

    fn push(&mut self, writer: Box<WriteContainer<'a>>) {
        self.writers.push(writer);
    }
}

impl AsyncWrite for MultiWriter<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        println!(
            "Poll write for buf (len: {}): {:?} {:?}",
            self.writers.len(),
            cx,
            std::str::from_utf8(buf)
        );

        for mut writer in self.writers.iter_mut() {
            Pin::new(&mut writer).poll_write(cx, buf)?;
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), std::io::Error>> {
        for mut writer in self.writers.iter_mut() {
            Pin::new(&mut writer).poll_flush(cx)?;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<(), std::io::Error>> {
        for mut writer in self.writers.iter_mut() {
            Pin::new(&mut writer).poll_shutdown(cx)?;
        }
        Poll::Ready(Ok(().into()))
    }
}

// impl<T> Write for MultiWriter<T>
// where
//     T: Write,
// {
//     fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
//         for writer in self.writers.iter_mut() {
//             let n = writer.write(buf)?;
//             if n != buf.len() {
//                 return Err(std::io::Error::new(
//                     std::io::ErrorKind::Other,
//                     "Short write!",
//                 ));
//             }
//         }
//         Ok(buf.len())
//     }
//     fn flush(&mut self) -> std::io::Result<()> {
//         for writer in self.writers.iter_mut() {
//             writer.flush()?;
//         }
//         Ok(())
//     }
// }

// #[derive(Deserialize, Serialize, Debug)]
// struct Config {
//     bind: String,
//     proxy: String,
//     tees: Vec<String>,
// }

// static EXAMPLE_CONFIG: &str = r#"
// {
//   "bind": "127.0.0.1:10000",
//   "proxy": "127.0.0.1:6379",
//   "tees": [
//     "127.0.0.1:6379",
//     "127.0.0.1:6379",
//     "127.0.0.1:6379",
//     "127.0.0.1:6379",
//     "127.0.0.1:6379"
//   ]
// }
// "#;

// fn proxy_copy<R1, W1, R2, W2>(
//     src_reader: R1,
//     dest_writer: W1,
//     dest_reader: R2,
//     src_writer: W2,
// ) -> tokio::prelude::future::Select2<tokio::io::Copy<R1, W1>, tokio::io::Copy<R2, W2>>
// where
//     R1: AsyncRead,
//     R2: AsyncRead,
//     W1: AsyncWrite,
//     W2: AsyncWrite,
// {
//     let src_to_dest = copy(dest_reader, src_writer);
//     let dest_to_src = copy(src_reader, dest_writer);
//     dest_to_src.select2(src_to_dest)
// }

// fn main() -> Result<()> {
//     let c: Config = serde_json::from_str(EXAMPLE_CONFIG)?;
//     let bind_addr = c.bind.to_string().parse::<SocketAddr>().unwrap();
//     let proxy_addr = c.proxy.to_string().parse::<SocketAddr>().unwrap();
//     println!("{:?}, {:?}", c, proxy_addr);

//     let tee_addrs: Vec<SocketAddr> = c
//         .tees
//         .iter()
//         .map(|s| s.to_string().parse::<SocketAddr>().unwrap())
//         .collect();
//     println!("Tees: {:?}", tee_addrs);

//     let listener = TcpListener::bind(&bind_addr).unwrap();

//     let done = listener
//         .incoming()
//         .map_err(|e| println!("Error accepting connection: {}", e))
//         .for_each(move |client| {
//             let proxy_conn = TcpStream::connect(&proxy_addr);
//             let mut connect_futures = vec![proxy_conn];

//             for tee_addr in tee_addrs.iter() {
//                 connect_futures.push(TcpStream::connect(tee_addr))
//             }

//             let lifecycle = join_all(connect_futures).and_then(|mut conns| {
//                 println!("Conns: {:?}", conns);

//                 let (clt_reader, clt_writer) = client.split();
//                 let srv_stream = conns.remove(0);
//                 let (srv_reader, srv_writer) = srv_stream.split();

//                 let mut multi_writer = MultiWriter::new();
//                 multi_writer.push(Box::new(srv_writer));

//                 let tee_reader_copy_futures: Vec<
//                     tokio::io::Copy<tokio::io::ReadHalf<TcpStream>, WriteProxy>,
//                 > = conns
//                     .drain(..)
//                     .map(|tee_conn| {
//                         let (tee_reader, tee_writer) = tee_conn.split();
//                         multi_writer.push(Box::new(tee_writer));
//                         copy(tee_reader, WriteProxy::sink())
//                     })
//                     .collect();

//                 proxy_copy(
//                     Box::new(clt_reader),
//                     Box::new(multi_writer),
//                     Box::new(srv_reader),
//                     Box::new(clt_writer),
//                 )
//                 .select2(tokio::prelude::future::select_all(tee_reader_copy_futures))
//                 .then(|x| {
//                     x.map(|_| println!("It completed!"))
//                         .map_err(|err| eprintln!("An error occurred during the proxy: {:?}", err))
//                         .expect("never errors");

//                     // Do this because the type system doesn't want an io::Error here?
//                     future::ok(())
//                 })
//             });

//             tokio::spawn(
//                 lifecycle
//                     .map(|_| println!("The proxy is done"))
//                     .map_err(|err| eprintln!("Proxy err: {:?}", err)),
//             )
//         });

//     tokio::run(done);
//     Ok(())
// }

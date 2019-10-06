#[macro_use]
extern crate failure;
extern crate futures;

use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use futures::channel::mpsc;
use futures::future::select_all;
use futures::lock::Mutex;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr; // Shutdown
use std::sync::Arc;
use tokio::net::tcp::split::WriteHalf;
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
    "127.0.0.1:6379",
    "127.0.0.1:6379",
    "127.0.0.1:6379",
    "127.0.0.1:6379"
  ]
}
"#;

struct ByteForwarder {
    senders: Vec<futures::channel::mpsc::Sender<Vec<u8>>>,
}

impl ByteForwarder {
    fn new() -> ByteForwarder {
        ByteForwarder { senders: vec![] }
    }

    fn push_sender(&mut self, sender: futures::channel::mpsc::Sender<Vec<u8>>) {
        self.senders.push(sender)
    }
}

impl AsyncWrite for ByteForwarder {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context,
        mut buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let mut copy_of_bytes: Vec<u8> = vec![];
        // TODO: Check n-bytes copied to verify?
        std::io::copy(&mut buf, &mut copy_of_bytes)?;

        for sender in self.senders.iter_mut() {
            eprintln!(
                "ByteForwarder: Forwarding bytes: {:?}",
                std::str::from_utf8(&copy_of_bytes)
            );

            // TODO: Be smart here.
            match sender.try_send(copy_of_bytes.clone()) {
                Ok(_) => {}
                Err(e) => eprintln!("ByteForwarder error: {:?}", e),
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
    channel: futures::channel::mpsc::Receiver<Vec<u8>>,
}

impl AsyncRead for ChannelReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        mut buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        // if self.channel.is_none() {
        //     return Poll::Ready(Ok(0));
        // }

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
            Poll::Ready(None) => {
                // Poll::Ready(Err(std::io::Error::new(
                //     std::io::ErrorKind::Other,
                //     "channel closed",
                //     )))

                Poll::Ready(Ok(0))
            }
            Poll::Pending => Poll::Pending,
        }
    }
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
        Ok(_) => Ok(()),
        Err(e) => bail!("spawned tee: {:?}", e),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

            let mut byte_forwarder = ByteForwarder::new();
            for tee_conn in tee_conns.drain(..) {
                // Setting this to 0 means each sender gets 1 buffer slot. Is this OK? Do we need to be unbounded here?
                let (tx, rx) = mpsc::channel(0);
                byte_forwarder.push_sender(tx);

                let relay = ChannelReader { channel: rx };
                tokio::spawn(async move {
                    eprintln!(
                        "Tee finished: {:?}",
                        spawn_proxy_for_tee(relay, tee_conn).await
                    );
                });
            }

            multi_writer.push(Box::new(WriteContainer::new_byte_forwarder(byte_forwarder)));

            copy_futures.push(clt_reader.copy(&mut multi_writer));
            copy_futures.push(srv_reader.copy(&mut clt_multi_writer));

            let (res, _, _) = select_all(copy_futures).await;
            res.unwrap();

            println!("All done");
        });
    }
}

#[derive(Debug)]
struct WriteProxy;

impl WriteProxy {
    pub fn sink() -> WriteProxy {
        WriteProxy
    }
}

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

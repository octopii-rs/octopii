use bytes::Bytes;
use octopii::transport::QuicTransport;
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::test]
async fn test_transport_basic_connection() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = QuicTransport::new(addr1).await.unwrap();
    let transport2 = QuicTransport::new(addr2).await.unwrap();

    let actual_addr1 = transport1.local_addr().unwrap();
    let actual_addr2 = transport2.local_addr().unwrap();

    assert_ne!(actual_addr1, actual_addr2);

    // Close transports
    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_transport_send_receive() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Spawn acceptor on transport2
    let t2_clone = Arc::clone(&transport2);
    let receiver_handle = tokio::spawn(async move {
        let (_, peer) = t2_clone.accept().await.unwrap();
        let data = peer.recv().await.unwrap().unwrap();
        data
    });

    // Give acceptor time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Send from transport1 to transport2
    let message = Bytes::from("hello from transport1");
    transport1
        .send(actual_addr2, message.clone())
        .await
        .unwrap();

    // Wait for receiver
    let received = receiver_handle.await.unwrap();
    assert_eq!(received, message);

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_transport_multiple_connections() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Spawn acceptor
    let t2_clone = Arc::clone(&transport2);
    tokio::spawn(async move {
        for _ in 0..3 {
            let (_, peer) = t2_clone.accept().await.unwrap();
            tokio::spawn(async move { while let Ok(Some(_)) = peer.recv().await {} });
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Connect multiple times
    for i in 0..3 {
        let msg = Bytes::from(format!("message_{}", i));
        transport1.send(actual_addr2, msg).await.unwrap();
    }

    transport1.close();
    transport2.close();
}

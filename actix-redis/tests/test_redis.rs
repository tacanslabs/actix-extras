#[macro_use]
extern crate redis_async;

use actix_redis::{Command, Error, RedisActor, RespValue};

#[actix_web::test]
async fn test_error_connect() {
    let addr = RedisActor::start("localhost:54000");
    let _addr2 = addr.clone();

    let res = addr.send(Command(resp_array!["GET", "test"])).await;
    match res {
        Ok(Err(Error::NotConnected)) => (),
        _ => panic!("Should not happen {:?}", res),
    }
}

#[actix_web::test]
async fn test_redis() {
    env_logger::init();

    let addr = RedisActor::start("127.0.0.1:6379");
    let res = addr
        .send(Command(resp_array!["SET", "test", "value"]))
        .await;

    match res {
        Ok(Ok(resp)) => {
            assert_eq!(resp, RespValue::SimpleString("OK".to_owned()));

            let res = addr.send(Command(resp_array!["GET", "test"])).await;
            match res {
                Ok(Ok(resp)) => {
                    println!("RESP: {resp:?}");
                    assert_eq!(resp, RespValue::BulkString((&b"value"[..]).into()));
                }
                _ => panic!("Should not happen {:?}", res),
            }
        }
        _ => panic!("Should not happen {:?}", res),
    }
}


#[test]
fn parse_naked_redis_address() {
    let addr = "127.0.0.1:6378";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, None);
    assert_eq!(index, None);



}
#[test]
fn parse_redis_address_with_index() {
    let address = "redis://127.0.0.1:6378/5";

    let (host_port, password, index) = RedisActor::parse_url(address.to_string());
    assert_eq!(host_port, "127.0.0.1:6378".to_string());
    assert_eq!(password, None);
    assert_eq!(index, Some("5".to_string()));

    let (host_port, password, index) =
        RedisActor::parse_url(address.trim_start_matches("redis://").to_string());
    assert_eq!(host_port, "127.0.0.1:6378".to_string());
    assert_eq!(password, None);
    assert_eq!(index, Some("5".to_string()));
}

#[test]
fn parse_redis_address_with_password() {
    let address = "redis://:68831eec-e7dc-4ac3-9292-b5e2107bb46d@127.0.0.1:6378";

    let (host_port, password, index) = RedisActor::parse_url(address.to_string());
    assert_eq!(host_port, "127.0.0.1:6378".to_string());
    assert_eq!(
        password,
        Some("68831eec-e7dc-4ac3-9292-b5e2107bb46d".to_string())
    );
    assert_eq!(index, None);

    let (host_port, password, index) =
        RedisActor::parse_url(address.trim_start_matches("redis://").to_string());
    assert_eq!(host_port, "127.0.0.1:6378".to_string());
    assert_eq!(
        password,
        Some("68831eec-e7dc-4ac3-9292-b5e2107bb46d".to_string())
    );
    assert_eq!(index, None);
}

#[test]
fn parse_redis_address_with_password_and_index() {
    let address = "redis://:68831eec-e7dc-4ac3-9292-b5e2107bb46d@127.0.0.1:6378/5";

    let (host_port, password, index) = RedisActor::parse_url(address.to_string());
    assert_eq!(host_port, "127.0.0.1:6378".to_string());
    assert_eq!(
        password,
        Some("68831eec-e7dc-4ac3-9292-b5e2107bb46d".to_string())
    );
    assert_eq!(index, Some("5".to_string()));

    let (host_port, password, index) =
        RedisActor::parse_url(address.trim_start_matches("redis://").to_string());
    assert_eq!(host_port, "127.0.0.1:6378".to_string());
    assert_eq!(
        password,
        Some("68831eec-e7dc-4ac3-9292-b5e2107bb46d".to_string())
    );
    assert_eq!(index, Some("5".to_string()));
}

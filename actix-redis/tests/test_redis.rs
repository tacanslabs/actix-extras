#[macro_use]
extern crate redis_async;

use actix_redis::{Command, Error, RedisActor, RespValue};

#[actix_web::test]
async fn test_error_connect() {
    let addr = RedisActor::start("127.0.0.1:54000");
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
fn parse_host_port_redis_address() {
    let addr = "127.0.0.1:6378";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, None);
    assert_eq!(index, None);
}


#[test]
fn parse_scheme_host_port_redis_address() {
    let addr = "redis://127.0.0.1:6378";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, None);
    assert_eq!(index, None);
}

#[test]
fn parse_host_port_index_redis_address() {
    let addr = "127.0.0.1:6378/5";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, None);
    assert_eq!(index, Some("5".to_string()));
}

#[test]
fn parse_scheme_host_port_index_redis_address() {
    let addr = "redis://127.0.0.1:6378/5";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, None);
    assert_eq!(index, Some("5".to_string()));
}

#[test]
fn parse_password_host_port_redis_address() {
    let addr = "password123@127.0.0.1:6378";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, Some("password123".to_string()));
    assert_eq!(index, None);

    let addr = ":password123@127.0.0.1:6378";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, Some("password123".to_string()));
    assert_eq!(index, None);
}

#[test]
fn parse_password_host_port_index_redis_address() {
    let addr = "password123@127.0.0.1:6378/5";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, Some("password123".to_string()));
    assert_eq!(index, Some("5".to_string()));

    let addr = ":password123@127.0.0.1:6378/5";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, Some("password123".to_string()));
    assert_eq!(index, Some("5".to_string()));
}

#[test]
fn parse_scheme_password_host_port_redis_address() {
    let addr = "redis://password123@127.0.0.1:6378";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, Some("password123".to_string()));
    assert_eq!(index, None);

    let addr = "redis://:password123@127.0.0.1:6378";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, Some("password123".to_string()));
    assert_eq!(index, None);
}

#[test]
fn parse_scheme_password_host_port_index_redis_address() {
    let addr = "redis://password123@127.0.0.1:6378/5";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, Some("password123".to_string()));
    assert_eq!(index, Some("5".to_string()));

    let addr = "redis://:password123@127.0.0.1:6378/5";

    let (addr, password, index) = RedisActor::parse_url(addr.into());

    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, Some("password123".to_string()));
    assert_eq!(index, Some("5".to_string()));
}

#[macro_use]
extern crate redis_async;

use actix_redis::{Command, Error, RedisActor, RespValue};

#[actix_web::test]
async fn test_error_connect() {
    let addr = "10.13.0.52:6379";
    let redis = RedisActor::start(addr);
    let response = redis.send(
        Command(resp_array!["AUTH", "z&#kL8oXQDzrGgsB8L6Jg7ZJs3&GVAB8upoNCr5uDdaRn4HaB^P6$esHXj"])
    ).await.unwrap();


    println!("{:?}", response);

    let response = redis.send(
        Command(resp_array!["SELECT", "21"])
    ).await.unwrap();

    println!("{:?}", response);

    let response = redis.send(
        Command(resp_array!["HGETALL", "services_last_synced_block"])
    ).await.unwrap();

    println!("{:?}", response);
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
fn parse_redis_address_with_index() {
    let address = "redis://127.0.0.1:6378/5";

    let actix_redis::RedisUrl { addr, password, index } = RedisActor::parse_url(address.to_string());
    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, None);
    assert_eq!(index, Some("5".to_string()));

    let actix_redis::RedisUrl { addr, password, index } =
        RedisActor::parse_url(address.trim_start_matches("redis://").to_string());
    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(password, None);
    assert_eq!(index, Some("5".to_string()));
}

#[test]
fn parse_redis_address_with_password() {
    let address = "redis://:68831eec-e7dc-4ac3-9292-b5e2107bb46d@127.0.0.1:6378";

    let actix_redis::RedisUrl { addr, password, index } = RedisActor::parse_url(address.to_string());
    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(
        password,
        Some("68831eec-e7dc-4ac3-9292-b5e2107bb46d".to_string())
    );
    assert_eq!(index, None);

    let actix_redis::RedisUrl { addr, password, index } =
        RedisActor::parse_url(address.trim_start_matches("redis://").to_string());
    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(
        password,
        Some("68831eec-e7dc-4ac3-9292-b5e2107bb46d".to_string())
    );
    assert_eq!(index, None);
}

#[test]
fn parse_redis_address_with_password_and_index() {
    let address = "redis://:68831eec-e7dc-4ac3-9292-b5e2107bb46d@127.0.0.1:6378/5";

    let actix_redis::RedisUrl { addr, password, index } = RedisActor::parse_url(address.to_string());
    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(
        password,
        Some("68831eec-e7dc-4ac3-9292-b5e2107bb46d".to_string())
    );
    assert_eq!(index, Some("5".to_string()));

    let actix_redis::RedisUrl { addr, password, index } =
        RedisActor::parse_url(address.trim_start_matches("redis://").to_string());
    assert_eq!(addr, "127.0.0.1:6378".to_string());
    assert_eq!(
        password,
        Some("68831eec-e7dc-4ac3-9292-b5e2107bb46d".to_string())
    );
    assert_eq!(index, Some("5".to_string()));
}

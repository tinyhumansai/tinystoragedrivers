use super::*;

use tinystoragedrivers_types::ErrorKind;

#[test]
fn plaintext_credentials_to_a_remote_host_are_refused_without_echoing_them() {
    for url in [
        "amqp://u:p@broker.example/",
        "amqp://u:p@broker.example:5672/vhost",
        "AMQP://u:p@broker.example/",
        "amqp://u@broker.example/",
        "amqp://u:p@10.0.0.7/",
        "amqp://u:p@[2001:db8::1]:5672/",
        "amqp://u:p@localhost.example/",
        "amqp://u:p@broker.example/?insecure=false",
    ] {
        let error = check_broker_url(url).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{url}");
        let message = error.message();
        assert!(message.contains("amqps://"), "{url}: {message}");
        assert!(!message.contains("u:p"), "{url}: {message}");
        assert!(!message.contains("broker.example"), "{url}: {message}");
    }
}

#[test]
fn tls_loopback_and_credential_free_urls_pass_unchanged() {
    for url in [
        "amqps://u:p@broker.example/",
        "amqps://u:p@broker.example:5671/vhost?heartbeat=10",
        "amqp://u:p@localhost/",
        "amqp://u:p@LOCALHOST:5672/",
        "amqp://u:p@127.0.0.1/",
        "amqp://u:p@127.8.9.10:5672/vhost",
        "amqp://u:p@[::1]:5672/",
        "amqp://broker.example/",
        "amqp://broker.example:5672/vhost?heartbeat=10",
        "amqp://@broker.example/",
    ] {
        assert_eq!(check_broker_url(url).unwrap(), url);
    }
}

#[test]
fn the_insecure_opt_in_allows_plaintext_and_never_reaches_the_broker() {
    assert_eq!(
        check_broker_url("amqp://u:p@broker.example/vhost?insecure=true").unwrap(),
        "amqp://u:p@broker.example/vhost"
    );
    assert_eq!(
        check_broker_url("amqp://u:p@broker.example/?heartbeat=10&insecure=true&frame_max=8192")
            .unwrap(),
        "amqp://u:p@broker.example/?heartbeat=10&frame_max=8192"
    );
    assert_eq!(
        check_broker_url("amqp://u:p@broker.example/?insecure=true#frag").unwrap(),
        "amqp://u:p@broker.example/#frag"
    );
    // A refused value is still stripped from any URL that is allowed.
    assert_eq!(
        check_broker_url("amqps://u:p@broker.example/?insecure=false&insecure").unwrap(),
        "amqps://u:p@broker.example/"
    );
    for url in [
        "amqp://u:p@broker.example/?insecure=true",
        "amqps://u:p@broker.example/?a=1&insecure=true",
        "amqp://u:p@localhost/?insecure=1",
    ] {
        let connect_with = check_broker_url(url).unwrap();
        assert!(!connect_with.contains(INSECURE), "{url} -> {connect_with}");
    }
}

#[test]
fn urls_without_a_scheme_or_another_scheme_are_left_to_the_connection() {
    assert_eq!(check_broker_url("broker").unwrap(), "broker");
    assert_eq!(
        check_broker_url("https://u:p@broker.example/").unwrap(),
        "https://u:p@broker.example/"
    );
    assert_eq!(
        check_broker_url("amqp://u:p@").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        check_broker_url("amqp://u:p@[::1/").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

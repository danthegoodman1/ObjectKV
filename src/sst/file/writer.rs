pub struct Writer {
  s3_client: aws_sdk_s3::Client,
}

impl Writer {
  pub async fn new() -> Self {
    let writer  = Writer {
      s3_client: aws_sdk_s3::Client::new(&aws_config::load_from_env().await)
    };

    return writer
  }
}

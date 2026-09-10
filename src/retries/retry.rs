use rand::RngExt;
use std::time::Duration;

pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl RetryPolicy {
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let exp = self
            .base_delay
            .saturating_mul(1u32.checked_shl(attempt).unwrap_or(u32::MAX));
        let capped = exp.min(self.max_delay);

        // Full jitter: uniformly between 0 and the capped exponential delay.
        let jittered_millis = rand::rng().random_range(0..=capped.as_millis() as u64);
        Duration::from_millis(jittered_millis)
    }
}

pub async fn retry_if<F, Fut, T, E>(
    policy: &RetryPolicy,
    mut op: F,
    should_retry: impl Fn(&E) -> bool,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) if attempt + 1 < policy.max_attempts && should_retry(&e) => {
                let delay = policy.delay_for_attempt(attempt);
                glommio_ng::timer::sleep(delay).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

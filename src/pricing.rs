use std::collections::HashSet;

use bigdecimal::{BigDecimal, RoundingMode, ToPrimitive};
use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{Error, Result};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PriceBook {
    timezone: String,
    versions: Vec<Version>,
    #[serde(flatten)]
    calendar: Calendar,
}

impl PriceBook {
    fn parse(pricing: &Value) -> Result<Self> {
        let book =
            Self::deserialize(pricing).map_err(|e| Error::bad(format!("invalid pricing: {e}")))?;
        if book.timezone != "UTC" {
            return Err(Error::bad("pricing timezone must be UTC"));
        }
        Ok(book)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Version {
    effective_from: Value,
    default_tiers: Vec<Tier>,
    #[serde(default)]
    periods: Vec<Period>,
    #[serde(flatten)]
    calendar: Calendar,
}

#[derive(Deserialize)]
struct Calendar {
    holiday_dates: Option<Vec<String>>,
    covered_years: Option<Vec<i32>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Period {
    weekdays: Vec<u8>, // ISO weekday, Monday = 1.
    start_minute: u16,
    end_minute: u16,
    #[serde(default)]
    exclude_holidays: bool,
    tiers: Vec<Tier>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Tier {
    up_to_input_tokens: Option<i64>,
    rates: Rates,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Rates {
    input: Option<String>,
    cache_read: Option<String>,
    cache_write: Option<String>,
    output: Option<String>,
}

fn effective_from(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(ms) = value.as_i64() {
        return DateTime::from_timestamp_millis(ms);
    }
    DateTime::parse_from_rfc3339(value.as_str()?)
        .ok()
        .map(|at| at.to_utc())
}

impl Calendar {
    fn validate(&self) -> Result<()> {
        let years = self.covered_years.as_deref().unwrap_or_default();
        if years.iter().collect::<HashSet<_>>().len() != years.len() {
            return Err(Error::bad("duplicate pricing covered_years"));
        }
        let mut seen = HashSet::new();
        for text in self.holiday_dates.iter().flatten() {
            let date = text
                .parse::<NaiveDate>()
                .map_err(|_| Error::bad("invalid pricing holiday date"))?;
            if date.to_string() != *text || !years.contains(&date.year()) || !seen.insert(date) {
                return Err(Error::bad(
                    "pricing holiday_dates must be unique YYYY-MM-DD dates in covered_years",
                ));
            }
        }
        Ok(())
    }

    fn is_holiday(&self, date: NaiveDate) -> Option<bool> {
        if !self.covered_years.as_ref()?.contains(&date.year()) {
            return None;
        }
        let today = date.to_string();
        Some(self.holiday_dates.iter().flatten().any(|day| day == &today))
    }
}

fn decimal(text: &str) -> Option<BigDecimal> {
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    let (whole, fraction) = text.split_once('.').unwrap_or((text, "0"));
    if digits(whole) && digits(fraction) {
        text.parse().ok()
    } else {
        None
    }
}

fn validate_tiers(tiers: &[Tier]) -> Result<()> {
    if tiers
        .last()
        .is_none_or(|tier| tier.up_to_input_tokens.is_some())
    {
        return Err(Error::bad("pricing tiers must end with an unlimited tier"));
    }
    let mut previous = -1;
    for (i, tier) in tiers.iter().enumerate() {
        match tier.up_to_input_tokens {
            Some(limit) if limit > previous => previous = limit,
            None if i + 1 == tiers.len() => (),
            _ => {
                return Err(Error::bad(
                    "pricing tier limits must be nonnegative and strictly increasing",
                ));
            }
        }
        for rate in [
            &tier.rates.input,
            &tier.rates.cache_read,
            &tier.rates.cache_write,
            &tier.rates.output,
        ]
        .into_iter()
        .flatten()
        {
            if decimal(rate).is_none() {
                return Err(Error::bad(
                    "pricing rates must be nonnegative decimal strings",
                ));
            }
        }
    }
    Ok(())
}

pub fn validate(pricing: &Value) -> Result<()> {
    if pricing.is_null() {
        return Ok(());
    }
    let book = PriceBook::parse(pricing)?;
    book.calendar.validate()?;
    if book.versions.is_empty() {
        return Err(Error::bad("pricing requires at least one version"));
    }
    let mut starts = HashSet::new();
    for version in book.versions {
        let start = effective_from(&version.effective_from).ok_or_else(|| {
            Error::bad("pricing effective_from must be an RFC3339 instant or Unix milliseconds")
        })?;
        if !starts.insert(start) {
            return Err(Error::bad("duplicate pricing effective_from"));
        }
        version.calendar.validate()?;
        validate_tiers(&version.default_tiers)?;
        let mut weekdays: [Vec<(u16, u16)>; 7] = Default::default();
        for period in version.periods {
            if period.weekdays.is_empty()
                || period.start_minute >= period.end_minute
                || period.end_minute > 1440
            {
                return Err(Error::bad(
                    "pricing periods need weekdays and a nonempty minute range within 0..1440",
                ));
            }
            validate_tiers(&period.tiers)?;
            for day in period.weekdays {
                if !(1..=7).contains(&day) {
                    return Err(Error::bad(
                        "pricing weekdays must be ISO weekday numbers 1 through 7",
                    ));
                }
                weekdays[usize::from(day - 1)].push((period.start_minute, period.end_minute));
            }
        }
        for intervals in &mut weekdays {
            intervals.sort_unstable();
            if intervals.windows(2).any(|w| w[0].1 > w[1].0) {
                return Err(Error::bad(
                    "overlapping pricing periods on the same weekday",
                ));
            }
        }
    }
    Ok(())
}

pub fn snapshot(pricing: &Value, at_ms: i64) -> Option<Value> {
    let book = PriceBook::parse(pricing).ok()?;
    let at = DateTime::from_timestamp_millis(at_ms)?;
    let (_, version) = book
        .versions
        .iter()
        .filter_map(|v| Some((effective_from(&v.effective_from)?, v)))
        .filter(|(start, _)| *start <= at)
        .max_by_key(|(start, _)| *start)?;
    let weekday = at.weekday().number_from_monday() as u8;
    let minute = (at.hour() * 60 + at.minute()) as u16;
    let mut period_index = None;
    let mut period_rule = Value::Null;
    let mut tiers = &version.default_tiers;
    for (i, period) in version.periods.iter().enumerate() {
        if !period.weekdays.contains(&weekday)
            || !(period.start_minute..period.end_minute).contains(&minute)
        {
            continue;
        }
        if period.exclude_holidays {
            let calendar = if version.calendar.covered_years.is_some()
                || version.calendar.holiday_dates.is_some()
            {
                &version.calendar
            } else {
                &book.calendar
            };
            if calendar.is_holiday(at.date_naive())? {
                continue;
            }
        }
        period_index = Some(i);
        period_rule = json!({
            "weekdays": period.weekdays,
            "start_minute": period.start_minute,
            "end_minute": period.end_minute,
            "exclude_holidays": period.exclude_holidays,
        });
        tiers = &period.tiers;
        break;
    }
    Some(json!({
        "timezone": book.timezone,
        "effective_from": version.effective_from,
        "period_index": period_index,
        "period": period_rule,
        "tiers": tiers,
        "matched_tier_index": Value::Null,
    }))
}

#[derive(Default)]
pub struct Settlement {
    pub snapshot: Option<Value>,
    pub input_uncached_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_write_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cost_nano_usd: Option<i64>,
}

fn count(value: &Value) -> Option<i64> {
    value.as_i64().filter(|n| *n >= 0)
}

impl Settlement {
    fn cost(&self, rates: &Rates) -> Option<i64> {
        let mut total = BigDecimal::from(0);
        for (rate, quantity) in [
            (&rates.input, self.input_uncached_tokens?),
            (&rates.cache_read, self.cache_read_tokens?),
            (&rates.cache_write, self.cache_write_tokens?),
            (&rates.output, self.output_tokens?),
        ] {
            if quantity > 0 {
                total += decimal(rate.as_deref()?)? * BigDecimal::from(quantity);
            }
        }
        // USD per million tokens to nano-USD per token is a factor of 1000.
        (total * BigDecimal::from(1000))
            .with_scale_round(0, RoundingMode::HalfUp)
            .to_i64()
    }
}

fn match_tier(snapshot: &mut Value, input: i64) -> Option<Rates> {
    for (i, value) in snapshot["tiers"].as_array()?.iter().enumerate() {
        let tier = Tier::deserialize(value).ok()?;
        if tier.up_to_input_tokens.is_none_or(|limit| input <= limit) {
            snapshot["matched_tier_index"] = json!(i);
            return Some(tier.rates);
        }
    }
    None
}

pub fn settle(snapshot: Option<Value>, usage: Option<&Value>) -> Settlement {
    let mut result = Settlement {
        snapshot,
        ..Default::default()
    };
    let Some(usage) = usage else { return result };
    result.output_tokens = count(&usage["output_tokens"]);
    let input = count(&usage["input_tokens"]);
    let rates = result
        .snapshot
        .as_mut()
        .and_then(|saved| match_tier(saved, input?));
    let separate_write_charge = rates
        .as_ref()
        .and_then(|r| r.cache_write.as_deref())
        .and_then(decimal)
        .is_some_and(|rate| rate > 0);
    let details = &usage["input_tokens_details"];
    result.cache_read_tokens = count(&details["cached_tokens"]);
    if input == Some(0) && details.is_null() {
        result.cache_read_tokens = Some(0);
    }
    // DeepSeek-style Responses reports cache hits, with writes billed as ordinary input.
    // A tier that charges separately for writes needs an explicit write count, even for zero.
    let standard_input = input.is_some() && result.cache_read_tokens.is_some();
    result.cache_write_tokens = match details.get("cache_write_tokens") {
        None | Some(Value::Null) => (standard_input && !separate_write_charge).then_some(0),
        Some(written) => count(written),
    };
    let Some(input) = input else { return result };
    if let (Some(read), Some(written)) = (result.cache_read_tokens, result.cache_write_tokens) {
        result.input_uncached_tokens = input
            .checked_sub(read)
            .and_then(|remaining| remaining.checked_sub(written))
            .filter(|n| *n >= 0);
    }
    result.cost_nano_usd = rates.as_ref().and_then(|rates| result.cost(rates));
    result
}

use skein_fuzz::{run_campaign, CampaignOptions, FuzzError};
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(success) => {
            if success {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => {
            eprintln!("skein-fuzz: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, FuzzError> {
    let options = parse_options(std::env::args().skip(1))?;
    let report = run_campaign(options)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report.json())
            .map_err(|error| FuzzError::new(format!("failed to encode report: {error}")))?
    );
    Ok(report.success())
}

fn parse_options(args: impl IntoIterator<Item = String>) -> Result<CampaignOptions, FuzzError> {
    let mut options = CampaignOptions::default();
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--seed" => {
                let value = args.next().ok_or_else(|| FuzzError::new(usage()))?;
                options.seed = value
                    .parse()
                    .map_err(|_| FuzzError::new("--seed must be an unsigned 64-bit integer"))?;
            }
            "--cases" => {
                let value = args.next().ok_or_else(|| FuzzError::new(usage()))?;
                options.case_count = value
                    .parse()
                    .map_err(|_| FuzzError::new("--cases must be a non-negative integer"))?;
            }
            "--help" | "-h" => return Err(FuzzError::new(usage())),
            _ => {
                return Err(FuzzError::new(format!(
                    "unknown argument '{argument}'\n{}",
                    usage()
                )));
            }
        }
    }
    Ok(options)
}

fn usage() -> &'static str {
    "usage: skein-fuzz [--seed <u64>] [--cases <usize>]"
}

#[cfg(test)]
mod tests {
    use super::parse_options;

    #[test]
    fn parses_seed_and_case_count() {
        let options = parse_options([
            "--seed".to_string(),
            "7".to_string(),
            "--cases".to_string(),
            "12".to_string(),
        ])
        .unwrap();

        assert_eq!(options.seed, 7);
        assert_eq!(options.case_count, 12);
    }
}

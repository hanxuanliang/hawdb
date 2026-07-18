#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct RequiredProperties {
    pub distribution: Distribution,
    pub ordering: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub struct PhysicalProperties {
    pub distribution: Distribution,
    pub ordering: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Hash)]
pub enum Distribution {
    #[default]
    Any,
    Single,
    Hash(Vec<String>),
}

impl PhysicalProperties {
    pub fn satisfies(&self, required: &RequiredProperties) -> bool {
        distribution_satisfies(&self.distribution, &required.distribution)
            && ordering_satisfies(&self.ordering, &required.ordering)
    }
}

impl Distribution {
    pub fn as_str(&self) -> &'static str {
        match self {
            Distribution::Any => "any",
            Distribution::Single => "single",
            Distribution::Hash(_) => "hash",
        }
    }
}

fn distribution_satisfies(actual: &Distribution, required: &Distribution) -> bool {
    match required {
        Distribution::Any => true,
        Distribution::Single => actual == required,
        Distribution::Hash(keys) => {
            matches!(actual, Distribution::Hash(actual_keys) if actual_keys == keys)
        }
    }
}

fn ordering_satisfies(actual: &[String], required: &[String]) -> bool {
    required.is_empty()
        || (actual.len() >= required.len()
            && actual
                .iter()
                .zip(required.iter())
                .all(|(actual, required)| actual == required))
}

#[cfg(test)]
mod tests {
    use super::{Distribution, PhysicalProperties, RequiredProperties};

    #[test]
    fn empty_required_properties_accept_any_plan() {
        let actual = PhysicalProperties {
            distribution: Distribution::Hash(vec!["space_id".to_string()]),
            ordering: vec!["updated_at".to_string()],
        };

        assert!(actual.satisfies(&RequiredProperties::default()));
    }

    #[test]
    fn ordering_requirement_accepts_prefix_match() {
        let actual = PhysicalProperties {
            distribution: Distribution::Single,
            ordering: vec!["space_id".to_string(), "updated_at".to_string()],
        };
        let required = RequiredProperties {
            distribution: Distribution::Single,
            ordering: vec!["space_id".to_string()],
        };

        assert!(actual.satisfies(&required));
    }

    #[test]
    fn distribution_exposes_stable_names() {
        assert_eq!(Distribution::Any.as_str(), "any");
        assert_eq!(Distribution::Single.as_str(), "single");
        assert_eq!(
            Distribution::Hash(vec!["space_id".to_string()]).as_str(),
            "hash"
        );
    }
}

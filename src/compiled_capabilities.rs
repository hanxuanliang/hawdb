use crate::RuntimeCapabilities;

pub const fn compiled_runtime_capabilities() -> RuntimeCapabilities {
    RuntimeCapabilities {
        full_text_search: cfg!(feature = "full-text-search"),
        vector_search: cfg!(feature = "vector-search"),
        graph_analytics: cfg!(feature = "graph-analytics"),
        background_maintenance: cfg!(feature = "background-maintenance"),
    }
}

pub(crate) const fn effective_runtime_capabilities(
    requested: RuntimeCapabilities,
) -> RuntimeCapabilities {
    requested.intersection(compiled_runtime_capabilities())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeCapability;

    #[test]
    fn compiled_matrix_matches_enabled_cargo_features() {
        let capabilities = compiled_runtime_capabilities();

        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::FullTextSearch),
            cfg!(feature = "full-text-search")
        );
        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::VectorSearch),
            cfg!(feature = "vector-search")
        );
        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::GraphAnalytics),
            cfg!(feature = "graph-analytics")
        );
        assert_eq!(
            capabilities.is_enabled(RuntimeCapability::BackgroundMaintenance),
            cfg!(feature = "background-maintenance")
        );
    }
}

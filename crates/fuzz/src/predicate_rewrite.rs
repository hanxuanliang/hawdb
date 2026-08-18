pub(crate) const PREDICATE_REWRITE_SHAPES: [&str; 4] = [
    "double_negation",
    "conjunction_idempotence",
    "disjunction_idempotence",
    "null_totality",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PredicateRewriteKind {
    DoubleNegation,
    ConjunctionIdempotence,
    DisjunctionIdempotence,
    NullTotality,
}

impl PredicateRewriteKind {
    pub(crate) const fn for_case(index: usize) -> Self {
        match index % PREDICATE_REWRITE_SHAPES.len() {
            0 => Self::DoubleNegation,
            1 => Self::ConjunctionIdempotence,
            2 => Self::DisjunctionIdempotence,
            _ => Self::NullTotality,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DoubleNegation => PREDICATE_REWRITE_SHAPES[0],
            Self::ConjunctionIdempotence => PREDICATE_REWRITE_SHAPES[1],
            Self::DisjunctionIdempotence => PREDICATE_REWRITE_SHAPES[2],
            Self::NullTotality => PREDICATE_REWRITE_SHAPES[3],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_selection_covers_every_rewrite_deterministically() {
        let observed = (0..PREDICATE_REWRITE_SHAPES.len())
            .map(|index| PredicateRewriteKind::for_case(index).as_str())
            .collect::<Vec<_>>();

        assert_eq!(observed, PREDICATE_REWRITE_SHAPES);
        assert_eq!(PredicateRewriteKind::for_case(4).as_str(), observed[0]);
    }
}

use super::{Database, QueryAccessControlContext};
use skein_core::RuntimeCapability;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessControlPolicyReadiness {
    pub ready: bool,
    pub access_control_capability_enabled: bool,
    pub required_policy_epoch: u64,
    pub observed_policy_epoch: Option<u64>,
    pub stale_policy_state: bool,
    pub blocker_codes: Vec<String>,
}

impl Database {
    pub fn access_control_policy_readiness(
        &self,
        required_policy_epoch: u64,
        observed_policy: Option<&QueryAccessControlContext>,
    ) -> AccessControlPolicyReadiness {
        let access_control_capability_enabled = self
            .config
            .runtime_capabilities
            .is_enabled(RuntimeCapability::AccessControl);
        let observed_policy_epoch = observed_policy.map(QueryAccessControlContext::policy_epoch);
        let mut blocker_codes = Vec::new();

        if !access_control_capability_enabled {
            blocker_codes.push("access_control_capability_disabled".to_string());
        }
        if required_policy_epoch == 0 {
            blocker_codes.push("access_control_required_policy_epoch_missing".to_string());
        }

        match observed_policy {
            Some(policy) => {
                if policy.validate().is_err() {
                    blocker_codes.push("access_control_policy_invalid".to_string());
                }
                if required_policy_epoch != 0 && policy.policy_epoch() < required_policy_epoch {
                    blocker_codes.push("access_control_policy_stale".to_string());
                }
            }
            None => blocker_codes.push("access_control_policy_missing".to_string()),
        }

        let stale_policy_state = blocker_codes
            .iter()
            .any(|code| code == "access_control_policy_stale");

        AccessControlPolicyReadiness {
            ready: blocker_codes.is_empty(),
            access_control_capability_enabled,
            required_policy_epoch,
            observed_policy_epoch,
            stale_policy_state,
            blocker_codes,
        }
    }
}

-------------------- MODULE SkeinMemoryTierRelease --------------------
EXTENDS FiniteSets

(***************************************************************************)
(* The release gate keeps the desktop policy, explicit 512 MiB capability  *)
(* policy, representative production read, and constrained capability read *)
(* as four independent obligations. Every obligation is bound to the exact *)
(* release identity. Advancing the identity makes retained evidence stale;  *)
(* stale or incomplete evidence can never publish a ready release.          *)
(***************************************************************************)

CONSTANT Identities

ASSUME Identities # {}

NoEvidence == "none"
EvidenceIdentities == Identities \cup {NoEvidence}

VARIABLES
    currentIdentity,
    desktopPolicyIdentity,
    capabilityPolicyIdentity,
    productionReadIdentity,
    capabilityReadIdentity,
    releaseReady

vars == <<
    currentIdentity,
    desktopPolicyIdentity,
    capabilityPolicyIdentity,
    productionReadIdentity,
    capabilityReadIdentity,
    releaseReady
>>

Init ==
    /\ currentIdentity \in Identities
    /\ desktopPolicyIdentity = NoEvidence
    /\ capabilityPolicyIdentity = NoEvidence
    /\ productionReadIdentity = NoEvidence
    /\ capabilityReadIdentity = NoEvidence
    /\ releaseReady = FALSE

RecordDesktopPolicy ==
    /\ desktopPolicyIdentity' = currentIdentity
    /\ UNCHANGED <<
        currentIdentity,
        capabilityPolicyIdentity,
        productionReadIdentity,
        capabilityReadIdentity,
        releaseReady
       >>

RecordCapabilityPolicy ==
    /\ capabilityPolicyIdentity' = currentIdentity
    /\ UNCHANGED <<
        currentIdentity,
        desktopPolicyIdentity,
        productionReadIdentity,
        capabilityReadIdentity,
        releaseReady
       >>

RecordProductionRead ==
    /\ productionReadIdentity' = currentIdentity
    /\ UNCHANGED <<
        currentIdentity,
        desktopPolicyIdentity,
        capabilityPolicyIdentity,
        capabilityReadIdentity,
        releaseReady
       >>

RecordCapabilityRead ==
    /\ capabilityReadIdentity' = currentIdentity
    /\ UNCHANGED <<
        currentIdentity,
        desktopPolicyIdentity,
        capabilityPolicyIdentity,
        productionReadIdentity,
        releaseReady
       >>

AdvanceIdentity ==
    /\ \E nextIdentity \in Identities \ {currentIdentity}:
          currentIdentity' = nextIdentity
    /\ releaseReady' = FALSE
    /\ UNCHANGED <<
        desktopPolicyIdentity,
        capabilityPolicyIdentity,
        productionReadIdentity,
        capabilityReadIdentity
       >>

AllEvidenceCurrent ==
    /\ desktopPolicyIdentity = currentIdentity
    /\ capabilityPolicyIdentity = currentIdentity
    /\ productionReadIdentity = currentIdentity
    /\ capabilityReadIdentity = currentIdentity

AdmitRelease ==
    /\ ~releaseReady
    /\ AllEvidenceCurrent
    /\ releaseReady' = TRUE
    /\ UNCHANGED <<
        currentIdentity,
        desktopPolicyIdentity,
        capabilityPolicyIdentity,
        productionReadIdentity,
        capabilityReadIdentity
       >>

Next ==
    \/ RecordDesktopPolicy
    \/ RecordCapabilityPolicy
    \/ RecordProductionRead
    \/ RecordCapabilityRead
    \/ AdvanceIdentity
    \/ AdmitRelease

TypeOK ==
    /\ currentIdentity \in Identities
    /\ desktopPolicyIdentity \in EvidenceIdentities
    /\ capabilityPolicyIdentity \in EvidenceIdentities
    /\ productionReadIdentity \in EvidenceIdentities
    /\ capabilityReadIdentity \in EvidenceIdentities
    /\ releaseReady \in BOOLEAN

ReadyRequiresBothPolicies ==
    releaseReady =>
        /\ desktopPolicyIdentity = currentIdentity
        /\ capabilityPolicyIdentity = currentIdentity

ReadyRequiresBothWorkloads ==
    releaseReady =>
        /\ productionReadIdentity = currentIdentity
        /\ capabilityReadIdentity = currentIdentity

Spec == Init /\ [][Next]_vars

=============================================================================

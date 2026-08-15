---- MODULE SkeinGraphIndexQualification ----
EXTENDS Naturals, TLC

CONSTANTS IndexClasses, Generations

VARIABLES
    rowGeneration,
    indexGeneration,
    differentialGeneration,
    recoveryGeneration,
    cacheEvidence,
    productionGeneration,
    activationGeneration,
    corruptGeneration,
    readOutcome

vars == <<
    rowGeneration,
    indexGeneration,
    differentialGeneration,
    recoveryGeneration,
    cacheEvidence,
    productionGeneration,
    activationGeneration,
    corruptGeneration,
    readOutcome
>>

Outcomes == {"Idle", "Selected", "Fallback", "Failed"}
CachePhases == {"None", "Cold", "Warm", "Cancelled"}
CacheEvidenceValues == [
    generation : Generations \cup {0},
    phase : CachePhases
]
CacheState(generation, phase) == [
    generation |-> generation,
    phase |-> phase
]

TypeOK ==
    /\ rowGeneration \in Generations
    /\ indexGeneration \in Generations
    /\ differentialGeneration \in [IndexClasses -> Generations \cup {0}]
    /\ recoveryGeneration \in [IndexClasses -> Generations \cup {0}]
    /\ cacheEvidence \in [IndexClasses -> CacheEvidenceValues]
    /\ productionGeneration \in [IndexClasses -> Generations \cup {0}]
    /\ activationGeneration \in [IndexClasses -> Generations \cup {0}]
    /\ corruptGeneration \in [IndexClasses -> Generations \cup {0}]
    /\ readOutcome \in [IndexClasses -> Outcomes]

Aligned == rowGeneration = indexGeneration

CacheEvidenceReady(class) ==
    cacheEvidence[class] = CacheState(rowGeneration, "Cancelled")

EvidenceReady(class) ==
    /\ Aligned
    /\ differentialGeneration[class] = rowGeneration
    /\ recoveryGeneration[class] = rowGeneration
    /\ CacheEvidenceReady(class)
    /\ productionGeneration[class] = rowGeneration

Activated(class) ==
    /\ activationGeneration[class] = rowGeneration
    /\ EvidenceReady(class)

Init ==
    /\ rowGeneration = 1
    /\ indexGeneration = 1
    /\ differentialGeneration = [class \in IndexClasses |-> 0]
    /\ recoveryGeneration = [class \in IndexClasses |-> 0]
    /\ cacheEvidence = [class \in IndexClasses |-> CacheState(0, "None")]
    /\ productionGeneration = [class \in IndexClasses |-> 0]
    /\ activationGeneration = [class \in IndexClasses |-> 0]
    /\ corruptGeneration = [class \in IndexClasses |-> 0]
    /\ readOutcome = [class \in IndexClasses |-> "Idle"]

PublishRowsOnly(nextGeneration) ==
    /\ nextGeneration \in Generations
    /\ nextGeneration > rowGeneration
    /\ rowGeneration' = nextGeneration
    /\ readOutcome' = [class \in IndexClasses |-> "Idle"]
    /\ UNCHANGED <<
        indexGeneration,
        differentialGeneration,
        recoveryGeneration,
        cacheEvidence,
        productionGeneration,
        activationGeneration,
        corruptGeneration
        >>

PublishMatchingIndex ==
    /\ indexGeneration # rowGeneration
    /\ indexGeneration' = rowGeneration
    /\ readOutcome' = [class \in IndexClasses |-> "Idle"]
    /\ UNCHANGED <<
        rowGeneration,
        differentialGeneration,
        recoveryGeneration,
        cacheEvidence,
        productionGeneration,
        activationGeneration,
        corruptGeneration
        >>

PublishAlignedCheckpoint(nextGeneration) ==
    /\ nextGeneration \in Generations
    /\ nextGeneration > rowGeneration
    /\ rowGeneration' = nextGeneration
    /\ indexGeneration' = nextGeneration
    /\ readOutcome' = [class \in IndexClasses |-> "Idle"]
    /\ UNCHANGED <<
        differentialGeneration,
        recoveryGeneration,
        cacheEvidence,
        productionGeneration,
        activationGeneration,
        corruptGeneration
        >>

RecordDifferential(class) ==
    /\ class \in IndexClasses
    /\ Aligned
    /\ differentialGeneration' = [differentialGeneration EXCEPT ![class] = rowGeneration]
    /\ UNCHANGED <<
        rowGeneration,
        indexGeneration,
        recoveryGeneration,
        cacheEvidence,
        productionGeneration,
        activationGeneration,
        corruptGeneration,
        readOutcome
        >>

RecordRecovery(class) ==
    /\ class \in IndexClasses
    /\ Aligned
    /\ recoveryGeneration' = [recoveryGeneration EXCEPT ![class] = rowGeneration]
    /\ UNCHANGED <<
        rowGeneration,
        indexGeneration,
        differentialGeneration,
        cacheEvidence,
        productionGeneration,
        activationGeneration,
        corruptGeneration,
        readOutcome
        >>

RecordColdConstrainedRead(class) ==
    /\ class \in IndexClasses
    /\ Aligned
    /\ cacheEvidence[class] \notin {
        CacheState(rowGeneration, "Cold"),
        CacheState(rowGeneration, "Warm"),
        CacheState(rowGeneration, "Cancelled")
        }
    /\ cacheEvidence' = [cacheEvidence EXCEPT
        ![class] = CacheState(rowGeneration, "Cold")]
    /\ UNCHANGED <<
        rowGeneration,
        indexGeneration,
        differentialGeneration,
        recoveryGeneration,
        productionGeneration,
        activationGeneration,
        corruptGeneration,
        readOutcome
        >>

RecordWarmRead(class) ==
    /\ class \in IndexClasses
    /\ Aligned
    /\ cacheEvidence[class] = CacheState(rowGeneration, "Cold")
    /\ cacheEvidence' = [cacheEvidence EXCEPT
        ![class] = CacheState(rowGeneration, "Warm")]
    /\ UNCHANGED <<
        rowGeneration,
        indexGeneration,
        differentialGeneration,
        recoveryGeneration,
        productionGeneration,
        activationGeneration,
        corruptGeneration,
        readOutcome
        >>

RecordCancellation(class) ==
    /\ class \in IndexClasses
    /\ Aligned
    /\ cacheEvidence[class] = CacheState(rowGeneration, "Warm")
    /\ cacheEvidence' = [cacheEvidence EXCEPT
        ![class] = CacheState(rowGeneration, "Cancelled")]
    /\ UNCHANGED <<
        rowGeneration,
        indexGeneration,
        differentialGeneration,
        recoveryGeneration,
        productionGeneration,
        activationGeneration,
        corruptGeneration,
        readOutcome
        >>

RecordProduction(class) ==
    /\ class \in IndexClasses
    /\ Aligned
    /\ productionGeneration' = [productionGeneration EXCEPT ![class] = rowGeneration]
    /\ UNCHANGED <<
        rowGeneration,
        indexGeneration,
        differentialGeneration,
        recoveryGeneration,
        cacheEvidence,
        activationGeneration,
        corruptGeneration,
        readOutcome
        >>

Activate(class) ==
    /\ class \in IndexClasses
    /\ EvidenceReady(class)
    /\ corruptGeneration[class] # rowGeneration
    /\ activationGeneration' = [activationGeneration EXCEPT ![class] = rowGeneration]
    /\ UNCHANGED <<
        rowGeneration,
        indexGeneration,
        differentialGeneration,
        recoveryGeneration,
        cacheEvidence,
        productionGeneration,
        corruptGeneration,
        readOutcome
        >>

Read(class) ==
    /\ class \in IndexClasses
    /\ readOutcome' = [readOutcome EXCEPT
        ![class] = IF Activated(class)
            THEN IF corruptGeneration[class] = rowGeneration
                THEN "Failed"
                ELSE "Selected"
            ELSE "Fallback"]
    /\ UNCHANGED <<
        rowGeneration,
        indexGeneration,
        differentialGeneration,
        recoveryGeneration,
        cacheEvidence,
        productionGeneration,
        activationGeneration,
        corruptGeneration
        >>

CorruptSelectedPage(class) ==
    /\ class \in IndexClasses
    /\ Aligned
    /\ corruptGeneration' = [corruptGeneration EXCEPT ![class] = rowGeneration]
    /\ readOutcome' = [readOutcome EXCEPT
        ![class] = IF Activated(class) THEN "Failed" ELSE @]
    /\ UNCHANGED <<
        rowGeneration,
        indexGeneration,
        differentialGeneration,
        recoveryGeneration,
        cacheEvidence,
        productionGeneration,
        activationGeneration
        >>

Next ==
    \/ \E nextGeneration \in Generations: PublishRowsOnly(nextGeneration)
    \/ PublishMatchingIndex
    \/ \E nextGeneration \in Generations: PublishAlignedCheckpoint(nextGeneration)
    \/ \E class \in IndexClasses: RecordDifferential(class)
    \/ \E class \in IndexClasses: RecordRecovery(class)
    \/ \E class \in IndexClasses: RecordColdConstrainedRead(class)
    \/ \E class \in IndexClasses: RecordWarmRead(class)
    \/ \E class \in IndexClasses: RecordCancellation(class)
    \/ \E class \in IndexClasses: RecordProduction(class)
    \/ \E class \in IndexClasses: Activate(class)
    \/ \E class \in IndexClasses: Read(class)
    \/ \E class \in IndexClasses: CorruptSelectedPage(class)

ActivationRequiresAllEvidence ==
    \A class \in IndexClasses:
        activationGeneration[class] = rowGeneration =>
            /\ differentialGeneration[class] = activationGeneration[class]
            /\ recoveryGeneration[class] = activationGeneration[class]
            /\ cacheEvidence[class] = CacheState(
                activationGeneration[class],
                "Cancelled"
                )
            /\ productionGeneration[class] = activationGeneration[class]

SelectedReadUsesCurrentQualifiedGeneration ==
    \A class \in IndexClasses:
        readOutcome[class] = "Selected" =>
            /\ Activated(class)
            /\ corruptGeneration[class] # rowGeneration

CorruptActivatedReadFailsClosed ==
    \A class \in IndexClasses:
        /\ Activated(class)
        /\ corruptGeneration[class] = rowGeneration
        => readOutcome[class] \in {"Idle", "Failed"}

StaleOrUnqualifiedReadNeverSelects ==
    \A class \in IndexClasses:
        ~Activated(class) => readOutcome[class] # "Selected"

====

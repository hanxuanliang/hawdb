---- MODULE SkeinGraphIndexQualification ----
EXTENDS Naturals, TLC

CONSTANTS IndexClasses, Generations

VARIABLES
    rowGeneration,
    indexGeneration,
    differentialGeneration,
    recoveryGeneration,
    cacheGeneration,
    productionGeneration,
    activationGeneration,
    corruptGeneration,
    readOutcome

vars == <<
    rowGeneration,
    indexGeneration,
    differentialGeneration,
    recoveryGeneration,
    cacheGeneration,
    productionGeneration,
    activationGeneration,
    corruptGeneration,
    readOutcome
>>

Outcomes == {"Idle", "Selected", "Fallback", "Failed"}

TypeOK ==
    /\ rowGeneration \in Generations
    /\ indexGeneration \in Generations
    /\ differentialGeneration \in [IndexClasses -> Generations \cup {0}]
    /\ recoveryGeneration \in [IndexClasses -> Generations \cup {0}]
    /\ cacheGeneration \in [IndexClasses -> Generations \cup {0}]
    /\ productionGeneration \in [IndexClasses -> Generations \cup {0}]
    /\ activationGeneration \in [IndexClasses -> Generations \cup {0}]
    /\ corruptGeneration \in [IndexClasses -> Generations \cup {0}]
    /\ readOutcome \in [IndexClasses -> Outcomes]

Aligned == rowGeneration = indexGeneration

EvidenceReady(class) ==
    /\ Aligned
    /\ differentialGeneration[class] = rowGeneration
    /\ recoveryGeneration[class] = rowGeneration
    /\ cacheGeneration[class] = rowGeneration
    /\ productionGeneration[class] = rowGeneration

Activated(class) ==
    /\ activationGeneration[class] = rowGeneration
    /\ EvidenceReady(class)

Init ==
    /\ rowGeneration = 1
    /\ indexGeneration = 1
    /\ differentialGeneration = [class \in IndexClasses |-> 0]
    /\ recoveryGeneration = [class \in IndexClasses |-> 0]
    /\ cacheGeneration = [class \in IndexClasses |-> 0]
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
        cacheGeneration,
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
        cacheGeneration,
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
        cacheGeneration,
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
        cacheGeneration,
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
        cacheGeneration,
        productionGeneration,
        activationGeneration,
        corruptGeneration,
        readOutcome
        >>

RecordCacheBudget(class) ==
    /\ class \in IndexClasses
    /\ Aligned
    /\ cacheGeneration' = [cacheGeneration EXCEPT ![class] = rowGeneration]
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
        cacheGeneration,
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
        cacheGeneration,
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
        cacheGeneration,
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
        cacheGeneration,
        productionGeneration,
        activationGeneration
        >>

Next ==
    \/ \E nextGeneration \in Generations: PublishRowsOnly(nextGeneration)
    \/ PublishMatchingIndex
    \/ \E nextGeneration \in Generations: PublishAlignedCheckpoint(nextGeneration)
    \/ \E class \in IndexClasses: RecordDifferential(class)
    \/ \E class \in IndexClasses: RecordRecovery(class)
    \/ \E class \in IndexClasses: RecordCacheBudget(class)
    \/ \E class \in IndexClasses: RecordProduction(class)
    \/ \E class \in IndexClasses: Activate(class)
    \/ \E class \in IndexClasses: Read(class)
    \/ \E class \in IndexClasses: CorruptSelectedPage(class)

ActivationRequiresAllEvidence ==
    \A class \in IndexClasses:
        activationGeneration[class] = rowGeneration =>
            /\ differentialGeneration[class] = activationGeneration[class]
            /\ recoveryGeneration[class] = activationGeneration[class]
            /\ cacheGeneration[class] = activationGeneration[class]
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

---------------- MODULE SkeinGraphDescriptorPaging ----------------
EXTENDS FiniteSets, Naturals

(***************************************************************************)
(* Graph descriptor pages are immutable physical copies. A candidate root  *)
(* becomes selectable only after every referenced page is durable. Opening  *)
(* a shadow reader pins one complete generation but does not warm descriptor  *)
(* pages. Publishing this root does not activate the production serving path. *)
(* A lookup admits at most MaxResidentPages and pins the selected page until *)
(* completion. Corruption fails the reader closed and poisons the handle.    *)
(***************************************************************************)

CONSTANTS Pages, Generations, MaxResidentPages

PageCopies == Generations \X Pages
NoPage == <<0, 0>>
CompleteGeneration(generation) == {<<generation, page>> : page \in Pages}

ReaderStates == {"closed", "open", "reading", "succeeded", "failed"}

VARIABLES
    durablePages,
    candidateGeneration,
    candidatePages,
    shadowGeneration,
    shadowPages,
    servingGeneration,
    servingPages,
    readerGeneration,
    readerPages,
    readerState,
    residentPages,
    pinnedPage,
    corruptPages,
    poisoned

vars == <<
    durablePages,
    candidateGeneration,
    candidatePages,
    shadowGeneration,
    shadowPages,
    servingGeneration,
    servingPages,
    readerGeneration,
    readerPages,
    readerState,
    residentPages,
    pinnedPage,
    corruptPages,
    poisoned
>>

Init ==
    /\ durablePages = {}
    /\ candidateGeneration = 0
    /\ candidatePages = {}
    /\ shadowGeneration = 0
    /\ shadowPages = {}
    /\ servingGeneration = 0
    /\ servingPages = {}
    /\ readerGeneration = 0
    /\ readerPages = {}
    /\ readerState = "closed"
    /\ residentPages = {}
    /\ pinnedPage = NoPage
    /\ corruptPages = {}
    /\ poisoned = FALSE

StartCandidate ==
    /\ candidateGeneration = 0
    /\ \E generation \in Generations:
          /\ generation # shadowGeneration
          /\ candidateGeneration' = generation
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        durablePages, shadowGeneration, shadowPages, servingGeneration,
        servingPages, readerGeneration,
        readerPages, readerState, residentPages, pinnedPage, corruptPages,
        poisoned
       >>

WriteCandidatePage ==
    /\ candidateGeneration \in Generations
    /\ candidatePages # CompleteGeneration(candidateGeneration)
    /\ \E copy \in CompleteGeneration(candidateGeneration) \ candidatePages:
          /\ candidatePages' = candidatePages \union {copy}
          /\ durablePages' = durablePages \union {copy}
    /\ UNCHANGED <<
        candidateGeneration, shadowGeneration, shadowPages,
        servingGeneration, servingPages,
        readerGeneration, readerPages, readerState, residentPages, pinnedPage,
        corruptPages, poisoned
       >>

PublishCandidateRoot ==
    /\ candidateGeneration \in Generations
    /\ candidatePages = CompleteGeneration(candidateGeneration)
    /\ candidatePages \subseteq durablePages
    /\ shadowGeneration' = candidateGeneration
    /\ shadowPages' = candidatePages
    /\ candidateGeneration' = 0
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        durablePages, servingGeneration, servingPages, readerGeneration,
        readerPages, readerState,
        residentPages, pinnedPage, corruptPages, poisoned
       >>

CrashCandidate ==
    /\ candidateGeneration \in Generations
    /\ candidateGeneration' = 0
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        durablePages, shadowGeneration, shadowPages, servingGeneration,
        servingPages, readerGeneration,
        readerPages, readerState, residentPages, pinnedPage, corruptPages,
        poisoned
       >>

OpenPinnedReader ==
    /\ readerState = "closed"
    /\ ~poisoned
    /\ shadowGeneration \in Generations
    /\ readerGeneration' = shadowGeneration
    /\ readerPages' = shadowPages
    /\ readerState' = "open"
    /\ pinnedPage' = NoPage
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        shadowGeneration, shadowPages, servingGeneration, servingPages,
        residentPages, corruptPages,
        poisoned
       >>

BeginDemandRead ==
    /\ readerState = "open"
    /\ ~poisoned
    /\ \E copy \in readerPages:
          /\ pinnedPage' = copy
          /\ residentPages' = {copy}
    /\ readerState' = "reading"
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        shadowGeneration, shadowPages, servingGeneration, servingPages,
        readerGeneration, readerPages,
        corruptPages, poisoned
       >>

CompleteDemandRead ==
    /\ readerState = "reading"
    /\ pinnedPage \notin corruptPages
    /\ readerState' = "succeeded"
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        shadowGeneration, shadowPages, servingGeneration, servingPages,
        readerGeneration, readerPages,
        residentPages, pinnedPage, corruptPages, poisoned
       >>

RejectCorruptPage ==
    /\ readerState = "reading"
    /\ pinnedPage \in corruptPages
    /\ readerState' = "failed"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        shadowGeneration, shadowPages, servingGeneration, servingPages,
        readerGeneration, readerPages,
        residentPages, pinnedPage, corruptPages
       >>

ReleaseSuccessfulRead ==
    /\ readerState = "succeeded"
    /\ readerState' = "open"
    /\ pinnedPage' = NoPage
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        shadowGeneration, shadowPages, servingGeneration, servingPages,
        readerGeneration, readerPages,
        residentPages, corruptPages, poisoned
       >>

CloseReader ==
    /\ readerState = "open"
    /\ readerState' = "closed"
    /\ readerGeneration' = 0
    /\ readerPages' = {}
    /\ pinnedPage' = NoPage
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        shadowGeneration, shadowPages, servingGeneration, servingPages,
        residentPages, corruptPages,
        poisoned
       >>

InjectCorruption ==
    /\ readerState # "succeeded"
    /\ corruptPages # durablePages
    /\ \E copy \in durablePages \ corruptPages:
          corruptPages' = corruptPages \union {copy}
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        shadowGeneration, shadowPages, servingGeneration, servingPages,
        readerGeneration, readerPages,
        readerState, residentPages, pinnedPage, poisoned
       >>

Next ==
    \/ StartCandidate
    \/ WriteCandidatePage
    \/ PublishCandidateRoot
    \/ CrashCandidate
    \/ OpenPinnedReader
    \/ BeginDemandRead
    \/ CompleteDemandRead
    \/ RejectCorruptPage
    \/ ReleaseSuccessfulRead
    \/ CloseReader
    \/ InjectCorruption

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ durablePages \subseteq PageCopies
    /\ candidateGeneration \in Generations \union {0}
    /\ candidatePages \subseteq PageCopies
    /\ shadowGeneration \in Generations \union {0}
    /\ shadowPages \subseteq PageCopies
    /\ servingGeneration \in Generations \union {0}
    /\ servingPages \subseteq PageCopies
    /\ readerGeneration \in Generations \union {0}
    /\ readerPages \subseteq PageCopies
    /\ readerState \in ReaderStates
    /\ residentPages \subseteq PageCopies
    /\ pinnedPage \in PageCopies \union {NoPage}
    /\ corruptPages \subseteq PageCopies
    /\ poisoned \in BOOLEAN

ShadowRootIsComplete ==
    shadowGeneration = 0
        \/ (shadowPages = CompleteGeneration(shadowGeneration)
            /\ shadowPages \subseteq durablePages)

CandidateIsNeverVisible ==
    candidateGeneration = 0
        \/ shadowGeneration # candidateGeneration

ShadowPublicationDoesNotActivateServing ==
    /\ servingGeneration = 0
    /\ servingPages = {}

PinnedReaderIsComplete ==
    readerGeneration = 0
        \/ (readerPages = CompleteGeneration(readerGeneration)
            /\ readerPages \subseteq durablePages)

ResidentPagesAreBounded == Cardinality(residentPages) <= MaxResidentPages

PinnedPageIsResident == pinnedPage = NoPage \/ pinnedPage \in residentPages

SuccessfulReadIsClean ==
    readerState # "succeeded" \/ pinnedPage \notin corruptPages

PoisonedReaderCannotRead ==
    ~poisoned \/ readerState \notin {"open", "reading", "succeeded"}

=============================================================================

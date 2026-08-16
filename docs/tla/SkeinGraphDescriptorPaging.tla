---------------- MODULE SkeinGraphDescriptorPaging ----------------
EXTENDS FiniteSets, Naturals

(***************************************************************************)
(* Graph descriptor pages are immutable physical copies. A candidate root  *)
(* becomes published only after every referenced page is durable. The outer *)
(* durable manifest may then atomically select that exact published root for *)
(* serving. Opening a production reader pins the selected generation without *)
(* warming descriptor pages; a later selection cannot move that reader. A   *)
(* lookup admits at most MaxResidentPages and pins one selected page until   *)
(* completion. Corruption fails the read closed and poisons the handle.      *)
(* The model is instantiated per descriptor class. Property-projection       *)
(* shadow publication currently refines only candidate/write/publish/crash;  *)
(* its ActivatePublishedRoot action is reserved for the serving contract.    *)
(***************************************************************************)

CONSTANTS Pages, Generations, MaxResidentPages

PageCopies == Generations \X Pages
NoPage == <<0, 0>>
CompleteGeneration(generation) == {<<generation, page>> : page \in Pages}

ReaderStates == {"closed", "open", "reading", "succeeded", "failed"}

VARIABLES
    durablePages,
    usedGenerations,
    candidateGeneration,
    candidatePages,
    publishedGeneration,
    publishedPages,
    publishedGenerations,
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
    usedGenerations,
    candidateGeneration,
    candidatePages,
    publishedGeneration,
    publishedPages,
    publishedGenerations,
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
    /\ usedGenerations = {}
    /\ candidateGeneration = 0
    /\ candidatePages = {}
    /\ publishedGeneration = 0
    /\ publishedPages = {}
    /\ publishedGenerations = {}
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
    /\ \E generation \in Generations \ usedGenerations:
          /\ candidateGeneration' = generation
          /\ usedGenerations' = usedGenerations \union {generation}
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        durablePages, publishedGeneration, publishedPages,
        publishedGenerations, servingGeneration, servingPages,
        readerGeneration, readerPages, readerState, residentPages, pinnedPage,
        corruptPages, poisoned
       >>

WriteCandidatePage ==
    /\ candidateGeneration \in Generations
    /\ candidatePages # CompleteGeneration(candidateGeneration)
    /\ \E copy \in CompleteGeneration(candidateGeneration) \ candidatePages:
          /\ candidatePages' = candidatePages \union {copy}
          /\ durablePages' = durablePages \union {copy}
    /\ UNCHANGED <<
        usedGenerations, candidateGeneration, publishedGeneration,
        publishedPages, publishedGenerations, servingGeneration, servingPages,
        readerGeneration, readerPages, readerState, residentPages, pinnedPage,
        corruptPages, poisoned
       >>

PublishCandidateRoot ==
    /\ candidateGeneration \in Generations
    /\ candidatePages = CompleteGeneration(candidateGeneration)
    /\ candidatePages \subseteq durablePages
    /\ publishedGeneration' = candidateGeneration
    /\ publishedPages' = candidatePages
    /\ publishedGenerations' =
        publishedGenerations \union {candidateGeneration}
    /\ candidateGeneration' = 0
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        durablePages, usedGenerations, servingGeneration, servingPages,
        readerGeneration, readerPages, readerState, residentPages, pinnedPage,
        corruptPages, poisoned
       >>

ActivatePublishedRoot ==
    /\ publishedGeneration \in publishedGenerations
    /\ publishedPages = CompleteGeneration(publishedGeneration)
    /\ publishedPages \subseteq durablePages
    /\ servingGeneration' = publishedGeneration
    /\ servingPages' = publishedPages
    /\ UNCHANGED <<
        durablePages, usedGenerations, candidateGeneration, candidatePages,
        publishedGeneration, publishedPages, publishedGenerations,
        readerGeneration, readerPages, readerState, residentPages, pinnedPage,
        corruptPages, poisoned
       >>

CrashCandidate ==
    /\ candidateGeneration \in Generations
    /\ candidateGeneration' = 0
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        durablePages, usedGenerations, publishedGeneration, publishedPages,
        publishedGenerations, servingGeneration, servingPages,
        readerGeneration, readerPages, readerState, residentPages, pinnedPage,
        corruptPages, poisoned
       >>

OpenPinnedReader ==
    /\ readerState = "closed"
    /\ ~poisoned
    /\ servingGeneration \in Generations
    /\ readerGeneration' = servingGeneration
    /\ readerPages' = servingPages
    /\ readerState' = "open"
    /\ pinnedPage' = NoPage
    /\ UNCHANGED <<
        durablePages, usedGenerations, candidateGeneration, candidatePages,
        publishedGeneration, publishedPages, publishedGenerations,
        servingGeneration, servingPages, residentPages, corruptPages, poisoned
       >>

BeginDemandRead ==
    /\ readerState = "open"
    /\ ~poisoned
    /\ \E copy \in readerPages:
          /\ pinnedPage' = copy
          /\ residentPages' = {copy}
    /\ readerState' = "reading"
    /\ UNCHANGED <<
        durablePages, usedGenerations, candidateGeneration, candidatePages,
        publishedGeneration, publishedPages, publishedGenerations,
        servingGeneration, servingPages, readerGeneration, readerPages,
        corruptPages, poisoned
       >>

CompleteDemandRead ==
    /\ readerState = "reading"
    /\ pinnedPage \notin corruptPages
    /\ readerState' = "succeeded"
    /\ UNCHANGED <<
        durablePages, usedGenerations, candidateGeneration, candidatePages,
        publishedGeneration, publishedPages, publishedGenerations,
        servingGeneration, servingPages, readerGeneration, readerPages,
        residentPages, pinnedPage, corruptPages, poisoned
       >>

RejectCorruptPage ==
    /\ readerState = "reading"
    /\ pinnedPage \in corruptPages
    /\ readerState' = "failed"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        durablePages, usedGenerations, candidateGeneration, candidatePages,
        publishedGeneration, publishedPages, publishedGenerations,
        servingGeneration, servingPages, readerGeneration, readerPages,
        residentPages, pinnedPage, corruptPages
       >>

ReleaseSuccessfulRead ==
    /\ readerState = "succeeded"
    /\ readerState' = "open"
    /\ pinnedPage' = NoPage
    /\ UNCHANGED <<
        durablePages, usedGenerations, candidateGeneration, candidatePages,
        publishedGeneration, publishedPages, publishedGenerations,
        servingGeneration, servingPages, readerGeneration, readerPages,
        residentPages, corruptPages, poisoned
       >>

CloseReader ==
    /\ readerState = "open"
    /\ readerState' = "closed"
    /\ readerGeneration' = 0
    /\ readerPages' = {}
    /\ pinnedPage' = NoPage
    /\ UNCHANGED <<
        durablePages, usedGenerations, candidateGeneration, candidatePages,
        publishedGeneration, publishedPages, publishedGenerations,
        servingGeneration, servingPages, residentPages, corruptPages, poisoned
       >>

InjectCorruption ==
    /\ readerState # "succeeded"
    /\ corruptPages # durablePages
    /\ \E copy \in durablePages \ corruptPages:
          corruptPages' = corruptPages \union {copy}
    /\ UNCHANGED <<
        durablePages, usedGenerations, candidateGeneration, candidatePages,
        publishedGeneration, publishedPages, publishedGenerations,
        servingGeneration, servingPages, readerGeneration, readerPages,
        readerState, residentPages, pinnedPage, poisoned
       >>

Next ==
    \/ StartCandidate
    \/ WriteCandidatePage
    \/ PublishCandidateRoot
    \/ ActivatePublishedRoot
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
    /\ usedGenerations \subseteq Generations
    /\ candidateGeneration \in Generations \union {0}
    /\ candidatePages \subseteq PageCopies
    /\ publishedGeneration \in Generations \union {0}
    /\ publishedPages \subseteq PageCopies
    /\ publishedGenerations \subseteq Generations
    /\ servingGeneration \in Generations \union {0}
    /\ servingPages \subseteq PageCopies
    /\ readerGeneration \in Generations \union {0}
    /\ readerPages \subseteq PageCopies
    /\ readerState \in ReaderStates
    /\ residentPages \subseteq PageCopies
    /\ pinnedPage \in PageCopies \union {NoPage}
    /\ corruptPages \subseteq PageCopies
    /\ poisoned \in BOOLEAN

PublishedRootIsComplete ==
    \/ /\ publishedGeneration = 0
       /\ publishedPages = {}
    \/ /\ publishedGeneration \in publishedGenerations
       /\ publishedPages = CompleteGeneration(publishedGeneration)
       /\ publishedPages \subseteq durablePages

AllPublishedRootsAreDurable ==
    \A generation \in publishedGenerations:
        CompleteGeneration(generation) \subseteq durablePages

CandidateIsNeverSelectable ==
    candidateGeneration = 0
        \/ /\ candidateGeneration \notin publishedGenerations
           /\ candidateGeneration # publishedGeneration
           /\ candidateGeneration # servingGeneration

ServingRootIsPublished ==
    \/ /\ servingGeneration = 0
       /\ servingPages = {}
    \/ /\ servingGeneration \in publishedGenerations
       /\ servingPages = CompleteGeneration(servingGeneration)
       /\ servingPages \subseteq durablePages

PinnedReaderIsComplete ==
    \/ /\ readerGeneration = 0
       /\ readerPages = {}
    \/ /\ readerGeneration \in publishedGenerations
       /\ readerPages = CompleteGeneration(readerGeneration)
       /\ readerPages \subseteq durablePages

ResidentPagesAreBounded == Cardinality(residentPages) <= MaxResidentPages

PinnedPageIsResident == pinnedPage = NoPage \/ pinnedPage \in residentPages

SuccessfulReadIsClean ==
    readerState # "succeeded" \/ pinnedPage \notin corruptPages

PoisonedReaderCannotRead ==
    ~poisoned \/ readerState \notin {"open", "reading", "succeeded"}

=============================================================================

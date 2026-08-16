---------------- MODULE SkeinGraphDescriptorPaging ----------------
EXTENDS FiniteSets, Naturals

(***************************************************************************)
(* Graph descriptor pages are immutable physical copies. A candidate root  *)
(* becomes selectable only after every referenced page is durable. Opening  *)
(* a reader pins one complete generation but does not warm descriptor pages. *)
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
    selectedGeneration,
    selectedPages,
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
    selectedGeneration,
    selectedPages,
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
    /\ selectedGeneration = 0
    /\ selectedPages = {}
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
          /\ generation # selectedGeneration
          /\ candidateGeneration' = generation
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        durablePages, selectedGeneration, selectedPages, readerGeneration,
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
        candidateGeneration, selectedGeneration, selectedPages,
        readerGeneration, readerPages, readerState, residentPages, pinnedPage,
        corruptPages, poisoned
       >>

PublishCandidateRoot ==
    /\ candidateGeneration \in Generations
    /\ candidatePages = CompleteGeneration(candidateGeneration)
    /\ candidatePages \subseteq durablePages
    /\ selectedGeneration' = candidateGeneration
    /\ selectedPages' = candidatePages
    /\ candidateGeneration' = 0
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        durablePages, readerGeneration, readerPages, readerState,
        residentPages, pinnedPage, corruptPages, poisoned
       >>

CrashCandidate ==
    /\ candidateGeneration \in Generations
    /\ candidateGeneration' = 0
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        durablePages, selectedGeneration, selectedPages, readerGeneration,
        readerPages, readerState, residentPages, pinnedPage, corruptPages,
        poisoned
       >>

OpenPinnedReader ==
    /\ readerState = "closed"
    /\ ~poisoned
    /\ selectedGeneration \in Generations
    /\ readerGeneration' = selectedGeneration
    /\ readerPages' = selectedPages
    /\ readerState' = "open"
    /\ pinnedPage' = NoPage
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        selectedGeneration, selectedPages, residentPages, corruptPages,
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
        selectedGeneration, selectedPages, readerGeneration, readerPages,
        corruptPages, poisoned
       >>

CompleteDemandRead ==
    /\ readerState = "reading"
    /\ pinnedPage \notin corruptPages
    /\ readerState' = "succeeded"
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        selectedGeneration, selectedPages, readerGeneration, readerPages,
        residentPages, pinnedPage, corruptPages, poisoned
       >>

RejectCorruptPage ==
    /\ readerState = "reading"
    /\ pinnedPage \in corruptPages
    /\ readerState' = "failed"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        selectedGeneration, selectedPages, readerGeneration, readerPages,
        residentPages, pinnedPage, corruptPages
       >>

ReleaseSuccessfulRead ==
    /\ readerState = "succeeded"
    /\ readerState' = "open"
    /\ pinnedPage' = NoPage
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        selectedGeneration, selectedPages, readerGeneration, readerPages,
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
        selectedGeneration, selectedPages, residentPages, corruptPages,
        poisoned
       >>

InjectCorruption ==
    /\ readerState # "succeeded"
    /\ corruptPages # durablePages
    /\ \E copy \in durablePages \ corruptPages:
          corruptPages' = corruptPages \union {copy}
    /\ UNCHANGED <<
        durablePages, candidateGeneration, candidatePages,
        selectedGeneration, selectedPages, readerGeneration, readerPages,
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
    /\ selectedGeneration \in Generations \union {0}
    /\ selectedPages \subseteq PageCopies
    /\ readerGeneration \in Generations \union {0}
    /\ readerPages \subseteq PageCopies
    /\ readerState \in ReaderStates
    /\ residentPages \subseteq PageCopies
    /\ pinnedPage \in PageCopies \union {NoPage}
    /\ corruptPages \subseteq PageCopies
    /\ poisoned \in BOOLEAN

SelectedRootIsComplete ==
    selectedGeneration = 0
        \/ (selectedPages = CompleteGeneration(selectedGeneration)
            /\ selectedPages \subseteq durablePages)

CandidateIsNeverVisible ==
    candidateGeneration = 0
        \/ selectedGeneration # candidateGeneration

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

-------------------- MODULE SkeinSourceSegmentPublication -------------------
EXTENDS Integers, Naturals

CONSTANT Readers, MaxEpoch

VARIABLES graphEpoch, manifestEpoch, segmentEpoch, buildEpoch, builder, readerEpoch, readerPath

vars == <<graphEpoch, manifestEpoch, segmentEpoch, buildEpoch, builder, readerEpoch, readerPath>>

Init ==
    /\ graphEpoch = 0
    /\ manifestEpoch = 0
    /\ segmentEpoch = 0
    /\ buildEpoch = 0
    /\ builder = "none"
    /\ readerEpoch = [reader \in Readers |-> -1]
    /\ readerPath = [reader \in Readers |-> "none"]

CommitGraph ==
    /\ builder = "none"
    /\ graphEpoch < MaxEpoch
    /\ graphEpoch' = graphEpoch + 1
    /\ UNCHANGED <<manifestEpoch, segmentEpoch, buildEpoch, builder, readerEpoch, readerPath>>

BeginBuild ==
    /\ builder = "none"
    /\ buildEpoch' = graphEpoch
    /\ builder' = "staged"
    /\ UNCHANGED <<graphEpoch, manifestEpoch, segmentEpoch, readerEpoch, readerPath>>

MakeSegmentDurable ==
    /\ builder = "staged"
    /\ segmentEpoch' = buildEpoch
    /\ builder' = "durable"
    /\ UNCHANGED <<graphEpoch, manifestEpoch, buildEpoch, readerEpoch, readerPath>>

PublishManifest ==
    /\ builder = "durable"
    /\ segmentEpoch = buildEpoch
    /\ manifestEpoch' = buildEpoch
    /\ buildEpoch' = 0
    /\ builder' = "none"
    /\ UNCHANGED <<graphEpoch, segmentEpoch, readerEpoch, readerPath>>

BeginRead(reader) ==
    /\ readerEpoch[reader] = -1
    /\ readerEpoch' = [readerEpoch EXCEPT ![reader] = graphEpoch]
    /\ readerPath' = [readerPath EXCEPT ![reader] = "none"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, segmentEpoch, buildEpoch, builder>>

UseSegment(reader) ==
    /\ readerEpoch[reader] >= 0
    /\ readerEpoch[reader] = manifestEpoch
    /\ manifestEpoch = segmentEpoch
    /\ readerPath' = [readerPath EXCEPT ![reader] = "segment"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, segmentEpoch, buildEpoch, builder, readerEpoch>>

FallbackToGraph(reader) ==
    /\ readerEpoch[reader] >= 0
    /\ readerEpoch[reader] # manifestEpoch \/ manifestEpoch # segmentEpoch
    /\ readerPath' = [readerPath EXCEPT ![reader] = "graph"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, segmentEpoch, buildEpoch, builder, readerEpoch>>

EndRead(reader) ==
    /\ readerEpoch[reader] >= 0
    /\ readerEpoch' = [readerEpoch EXCEPT ![reader] = -1]
    /\ readerPath' = [readerPath EXCEPT ![reader] = "none"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, segmentEpoch, buildEpoch, builder>>

Crash ==
    /\ builder' = "none"
    /\ buildEpoch' = 0
    /\ readerEpoch' = [reader \in Readers |-> -1]
    /\ readerPath' = [reader \in Readers |-> "none"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, segmentEpoch>>

Next ==
    \/ CommitGraph
    \/ BeginBuild
    \/ MakeSegmentDurable
    \/ PublishManifest
    \/ \E reader \in Readers: BeginRead(reader)
    \/ \E reader \in Readers: UseSegment(reader)
    \/ \E reader \in Readers: FallbackToGraph(reader)
    \/ \E reader \in Readers: EndRead(reader)
    \/ Crash

ManifestNeverReferencesUndurableSegment == manifestEpoch <= segmentEpoch
SegmentReadMatchesPinnedGraph ==
    \A reader \in Readers:
        readerPath[reader] = "segment" =>
            /\ readerEpoch[reader] = manifestEpoch
            /\ manifestEpoch = segmentEpoch
StaleSegmentUsesFallback ==
    \A reader \in Readers:
        /\ readerEpoch[reader] >= 0
        /\ (readerEpoch[reader] # manifestEpoch \/ manifestEpoch # segmentEpoch)
        => readerPath[reader] # "segment"
CrashRetainsPublishedManifest == manifestEpoch <= segmentEpoch

Spec == Init /\ [][Next]_vars

=============================================================================

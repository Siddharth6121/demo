#r "nuget: Akka.FSharp, 1.5.13"
#r "nuget: Akka, 1.5.13"

open System
open Akka.FSharp
open System.Threading

// Hop counter //
type HopCountMsg =
    | InreaseConvergedNode of int * int * int

let hopCount numNodes (mailbox: Actor<_>) = 
    let mutable numberOfHopsAllNodes = 0
    let mutable totalnumRequest = 0
    let mutable totalConvergedNodes = 0
    let rec system () = actor {
        let! message = mailbox.Receive ()
        match message with
        | InreaseConvergedNode (nodeID, hopCount, numRequest) ->
            printfn "NodeID %d converged with hopCount %d, numRequest %d" nodeID hopCount numRequest
            numberOfHopsAllNodes <- numberOfHopsAllNodes + hopCount
            totalnumRequest <- totalnumRequest + numRequest
            totalConvergedNodes <- totalConvergedNodes + 1
            if(totalConvergedNodes = numNodes) then
                printfn "Total number of hops: %d" numberOfHopsAllNodes
                printfn "Total number of requests: %d" totalnumRequest
                printfn "Average number of hops: %f" ((float numberOfHopsAllNodes) / (float totalnumRequest))
                mailbox.Context.System.Terminate() |> ignore
        
        return! system ()
    }
    system ()

// Node //
type NodeMessage =
    | Create
    | Join of int
    | FindingNextNode of int
    | ReceivingNextNode of int
    | StabilizeSystem
    | FindingPreviousNode
    | ReceivingPreviousNode of int
    | NotifingTheNodes of int
    | FixingFingerTable
    | FindingNextFinger of int * int * int
    | UpdatingFinger of int * int   
    | StartQuerying
    | QueryMessage
    | FindigNextKey of int * int * int
    | FoundKey of int

let getActorPath s =
    let actorPath = @"akka://chord/user/" + string s
    actorPath

let withoutLeftWithoutRight hashSpace left value right =
    let correctedRight = if(right < left) then right + hashSpace else right
    let correctedValue = if((value < left) && (left > right)) then (value + hashSpace) else value
    (left = right) || ((correctedValue > left) && (correctedValue < correctedRight))

let withoutLeftWithRight hashSpace left value right =
    let correctedRight = if(right < left) then right + hashSpace else right
    let correctedValue = if((value < left) && (left > right)) then (value + hashSpace) else value
    (left = right) || ((correctedValue > left) && (correctedValue <= correctedRight))

let myActor (nodeID: int) m maxNumRequests hopCountRef (mailbox: Actor<_>) =
    printfn "[INFO] Creating node %d" nodeID
    let hashSpace = int (Math.Pow(2.0, float m))
    let mutable prevNodeID = -1
    let mutable fingerTable = Array.create m -1
    let mutable next = 0
    let mutable totalHopCount = 0
    let mutable numRequests = 0

    let rec system () = actor {
        let! message = mailbox.Receive ()
        let sender = mailbox.Sender ()
        match message with
        | Create ->
            prevNodeID <- -1
            for i = 0 to m - 1 do
                fingerTable.[i] <- nodeID
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                TimeSpan.FromSeconds(0.0),
                TimeSpan.FromSeconds(1.0),
                mailbox.Self,
                StabilizeSystem
            )
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromSeconds(0.0),
                    TimeSpan.FromSeconds(1.0),
                    mailbox.Self,
                    FixingFingerTable
                )

        | Join (nDash) ->
            prevNodeID <- -1
            let nDashPath = getActorPath nDash
            let nDashRef = mailbox.Context.ActorSelection nDashPath
            nDashRef <! FindingNextNode (nodeID)

        | FindingNextNode (id) ->
            if(withoutLeftWithRight hashSpace nodeID id fingerTable.[0]) then
                let newNodePath = getActorPath id
                let newNodeRef = mailbox.Context.ActorSelection newNodePath
                newNodeRef <! ReceivingNextNode (fingerTable.[0])
            else
                let mutable i = m - 1
                while(i >= 0) do
                    if(withoutLeftWithoutRight hashSpace nodeID fingerTable.[i] id) then
                        let nearestPrevNodeID = fingerTable.[i]
                        let nearestPrevNodePath = getActorPath nearestPrevNodeID
                        let nearestPrevNodeReference = mailbox.Context.ActorSelection nearestPrevNodePath
                        nearestPrevNodeReference <! FindingNextNode (id)
                        i <- -1
                    i <- i - 1

        | ReceivingNextNode (nextNodeID) ->
            for i = 0 to m - 1 do
                fingerTable.[i] <- nextNodeID
            // Start stabilize and schedulers
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                TimeSpan.FromSeconds(0.0),
                TimeSpan.FromSeconds(1.0),
                mailbox.Self,
                StabilizeSystem
            )
            mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromSeconds(0.0),
                    TimeSpan.FromSeconds(1.0),
                    mailbox.Self,
                    FixingFingerTable
                )

        | StabilizeSystem ->
            let nextNodeID = fingerTable.[0]
            let nextNodePath = getActorPath nextNodeID
            let nextNodeReference = mailbox.Context.ActorSelection nextNodePath
            nextNodeReference <! FindingPreviousNode

        | FindingPreviousNode ->
            sender <! ReceivingPreviousNode (prevNodeID)

        | ReceivingPreviousNode (x) ->
            if((x <> -1) && (withoutLeftWithoutRight hashSpace nodeID x fingerTable.[0])) then
                fingerTable.[0] <- x
            let nextNodeID = fingerTable.[0]
            let nextNodePath = getActorPath nextNodeID
            let nextNodeReference = mailbox.Context.ActorSelection nextNodePath
            nextNodeReference <! NotifingTheNodes (nodeID)

        | NotifingTheNodes (nDash) ->
            if((prevNodeID = -1) || (withoutLeftWithoutRight hashSpace prevNodeID nDash nodeID)) then
                prevNodeID <- nDash

        | FixingFingerTable ->
            next <- next + 1
            if(next >= m) then
                next <- 0
            let fingerValue = nodeID + int (Math.Pow(2.0, float (next)))
            mailbox.Self <! FindingNextFinger (nodeID, next, fingerValue)

        | FindingNextFinger (originNodeID, next, id) ->
            if(withoutLeftWithRight hashSpace nodeID id fingerTable.[0]) then
                let originNodePath = getActorPath originNodeID
                let originNodeReference = mailbox.Context.ActorSelection originNodePath
                originNodeReference <! UpdatingFinger (next, fingerTable.[0])
            else
                let mutable i = m - 1
                while(i >= 0) do
                    if(withoutLeftWithoutRight hashSpace nodeID fingerTable.[i] id) then
                        let nearestPrevNodeID = fingerTable.[i]
                        let nearestPrevNodePath = getActorPath nearestPrevNodeID
                        let nearestPrevNodeReference = mailbox.Context.ActorSelection nearestPrevNodePath
                        nearestPrevNodeReference <! FindingNextFinger (originNodeID, next, id)
                        i <- -1
                    i <- i - 1

        | UpdatingFinger (next, fingerSuccessor) ->
            fingerTable.[next] <- fingerSuccessor

        | StartQuerying ->
            if(numRequests < maxNumRequests) then
                mailbox.Self <! QueryMessage
                mailbox.Context.System.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.), mailbox.Self, StartQuerying)
            else
                // Done with querying, send the current status to hop counter
                hopCountRef <! InreaseConvergedNode (nodeID, totalHopCount, numRequests)

        | QueryMessage ->
            let key = (System.Random()).Next(hashSpace)
            mailbox.Self <! FindigNextKey (nodeID, key, 0)

        // Scalable key lookup
        | FindigNextKey (originNodeID, id, numHops) ->
            if(id = nodeID) then
                let originNodePath = getActorPath originNodeID
                let originNodeReference = mailbox.Context.ActorSelection originNodePath
                originNodeReference <! FoundKey (numHops)
            elif(withoutLeftWithRight hashSpace nodeID id fingerTable.[0]) then
                let originNodePath = getActorPath originNodeID
                let originNodeReference = mailbox.Context.ActorSelection originNodePath
                originNodeReference <! FoundKey (numHops)
            else
                let mutable i = m - 1
                while(i >= 0) do
                    if(withoutLeftWithoutRight hashSpace nodeID fingerTable.[i] id) then
                        let nearestPrevNodeID = fingerTable.[i]
                        let nearestPrevNodePath = getActorPath nearestPrevNodeID
                        let nearestPrevNodeReference = mailbox.Context.ActorSelection nearestPrevNodePath
                        nearestPrevNodeReference <! FindigNextKey (originNodeID, id, numHops + 1)
                        i <- -1
                    i <- i - 1

        | FoundKey (hopCount) ->
            if(numRequests < maxNumRequests) then
                totalHopCount <- totalHopCount + hopCount
                numRequests <- numRequests + 1

        return! system ()
    }
    system ()

let main() =
    // Create system
    let system = System.create "chord" (Configuration.load())

    // Parse command line arguments
    let numNodes = fsi.CommandLineArgs.[1] |> int
    let numRequests = fsi.CommandLineArgs.[2] |> int

    // m-bit identifier
    let m = 20
    let hashSpace = int (Math.Pow(2.0, float m))

    // Spawn hopCounter
    let hopCounterRef = spawn system "hopCounter" (hopCount numNodes)
  
    // Generate random node IDs
    let nodeIDs = Array.init numNodes (fun _ -> (Random()).Next(hashSpace))
  
    let sortedNodeIDs = Array.sort nodeIDs 
    // Generate the nodes
    let nodeIDs = Array.create numNodes -1;
    let nodeRefs = Array.create numNodes null;
    let mutable i = 0;
    while(i < numNodes) do
        try
            let nodeID  = sortedNodeIDs.[i]
            
            nodeRefs.[i] <- spawn system (string nodeID) (myActor nodeID m numRequests hopCounterRef)
            if(i = 0) then
                nodeRefs.[i] <! Create
            else
                nodeRefs.[i] <! Join(sortedNodeIDs.[0])
            i <- i + 1
        with _ -> ()

    // Wait for system stabilization
    printfn "Waiting for 30 seconds to get system stabilized"
    Thread.Sleep(30000)

    // Start querying
    for nodeRef in nodeRefs do
        nodeRef <! StartQuerying
        Thread.Sleep(500)

    // Wait until all the actors terminate
    system.WhenTerminated.Wait()

    0 // return an integer exit code


main()
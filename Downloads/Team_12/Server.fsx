#r "System.dll"
#r "System.Net.dll"
#r "System.Net.Sockets.dll"

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading

let port = 8081

let clientCounter = ref 0  // Counter to keep track of connected clients
let validOperations = ["add"; "subtract"; "multiply"; "bye"; "terminate"]
let handleClient (clientSocket: TcpClient) =
    async {
        let reader = new StreamReader(clientSocket.GetStream())
        let writer = new StreamWriter(clientSocket.GetStream())

        writer.AutoFlush <- true

        // Send #Hello! to the client
        writer.WriteLine("Hello!")
        // Increment the client counter and fetch the current client number
        let clientId = Interlocked.Increment(clientCounter)
        printfn "Client %d connected" clientId
        let rec processRequests () =
            async {
                try
                    let clientRequest = reader.ReadLine()
                    printfn "Received: %s" (clientRequest)
                    if String.IsNullOrWhiteSpace(clientRequest) then
                        return () // Terminate the function if the request is empty

                    // Split the input into operation and arguments
                    let parts = clientRequest.Split(' ')
                    let op = parts.[0]
                    let args = parts.[1..]

                    // Process client request
                    let response: int =
                        match op with
                        | "add" when args.Length >= 2 && args.Length <= 4 ->
                            let numbers = args |> Array.map (fun arg ->
                                match Int32.TryParse(arg) with
                                | true, n -> n
                                | _ -> -4 // One or more of the inputs contain(s) non-number(s)
                            )
                            if Array.exists ((>) 0) numbers then
                                -4 // One or more of the inputs contain(s) non-number(s)
                            else
                                Array.sum numbers
                        | "subtract" when args.Length >= 2 && args.Length <= 4 ->
                            let numbers = args |> Array.map (fun arg ->
                                match Int32.TryParse(arg) with
                                | true, n -> n
                                | _ -> -4 // One or more of the inputs contain(s) non-number(s)
                            )
                            if Array.exists ((>) 0) numbers then
                                -4 // One or more of the inputs contain(s) non-number(s)
                            else
                                Array.reduce (-) numbers
                        | "multiply" when args.Length >= 2 && args.Length <= 4 ->
                            let numbers = args |> Array.map (fun arg ->
                                match Int32.TryParse(arg) with
                                | true, n -> n
                                | _ -> -4 // One or more of the inputs contain(s) non-number(s)
                            )
                            if Array.exists ((>) 0) numbers then
                                -4 // One or more of the inputs contain(s) non-number(s)
                            else
                                Array.reduce (*) numbers
                        | "bye" -> -5
                        | "terminate"->
                            -5 // Exit
                        | _ -> -1 // Incorrect operation command

                    // Check for error codes -2 and -3
                    if response = -1 then
                        if not (List.contains op validOperations) then
                            writer.WriteLine(-1)
                            printfn "incorrect operation command"
                        elif args.Length < 2 then
                            writer.WriteLine(-2) // Number of inputs is less than two
                            printfn "number of inputs is less than two."
                        elif args.Length > 4 then
                            writer.WriteLine(-3) // Number of inputs is more than four
                            printfn "number of inputs is more than four"
                        else
                            // Send the response to the client
                            writer.WriteLine(response)
                    else
                        // Send the response to the client
                        writer.WriteLine(response)
                        printfn "Responding to client %d with result: %d" clientId response
                       // Recursively process the next request
                    do! processRequests ()
                with
                    | ex -> Console.WriteLine(ex.Message)

                // Close the client socket
                clientSocket.Close()
            }

        do! processRequests ()

        return ()
    }

let listener = new TcpListener(IPAddress.Loopback, port)
listener.Start()

printfn "Server is running and listening on port %d." port

let rec acceptClients () =
    async {
        let clientSocket = listener.AcceptTcpClient()
        async { do! handleClient clientSocket } |> Async.Start
        do! acceptClients ()
    }

Async.RunSynchronously (acceptClients())
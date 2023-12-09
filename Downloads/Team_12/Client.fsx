#r "System.dll"
#r "System.Net.dll"
#r "System.Net.Sockets.dll"

open System
open System.IO
open System.Net
open System.Net.Sockets

let serverHost = "localhost"
let serverPort = 8081

let connectToServer () =
    async {
        let client = new TcpClient(serverHost, serverPort)
        let reader = new StreamReader(client.GetStream())
        let writer = new StreamWriter(client.GetStream())

        writer.AutoFlush <- true

        // Receive and print the #Hello! message from the server
        let helloMessage = reader.ReadLine()
        printfn "Server says: %s" helloMessage

        let rec sendCommands () =
            async {
                printfn "Sending command: "
                let command = Console.ReadLine()

                // Send the command to the server
                writer.WriteLine(command)

                // Receive and print the response from the server
                let response = reader.ReadLine()
                printfn "Server response: %s" response

                // Handle termination commands
                if command = "bye" || command = "terminate" then
                    printfn "exit"
                else
                    do! sendCommands ()
            }

        do! sendCommands ()

        // Close the client socket
        client.Close()
    }

Async.RunSynchronously (connectToServer())
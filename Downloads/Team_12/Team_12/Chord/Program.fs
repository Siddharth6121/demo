open System.Diagnostics
open System

let mutable numNodes = 0
let mutable numRequests = 0

let args = System.Environment.GetCommandLineArgs()

// Check if there are at least three arguments
if args.Length >= 3 then
    
    numNodes <- int args[1]
    numRequests <-  int args[2]

else
    printfn "Usage: dotnet run <numNodes> <numRequests>"
    Environment.Exit 0
let startScriptWithArguments scriptPath args =
    let psi = new ProcessStartInfo("dotnet", sprintf "fsi %s %s" scriptPath args)
    psi.RedirectStandardOutput <- true // Enable redirection

    let process = new Process()
    process.StartInfo <- psi
    process.Start()
    let output = process.StandardOutput.ReadToEnd() // Read standard output
    process.WaitForExit()
    output // Return the output

let scriptPath = "C:\\Users\\Dell\\Documents\\DOSP\\chord\\chord.fsx"

// Print the script output

let args1 = sprintf "%d %d" numNodes numRequests

let scriptOutput = startScriptWithArguments scriptPath args1

printfn "Script Output:\n%s" scriptOutput
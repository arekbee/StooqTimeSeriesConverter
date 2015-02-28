// #region Open references
open System
open System.Collections
open System.Collections.Generic
open System.Collections.Specialized
open System.Configuration
open System.Configuration.Assemblies
open System.Diagnostics
open System.Dynamic
open System.Globalization
open System.IO
open System.IO.Compression
open System.IO.Pipes
open System.IO.Ports
open System.Linq
open System.Linq.Expressions
open System.Media
open System.Net
open System.Net.Configuration
open System.Net.Mail
open System.Net.Mime
open System.Net.NetworkInformation
open System.Net.Security
open System.Net.Sockets
open System.Numerics
open System.Reflection
open System.Reflection.Emit
open System.Resources
open System.Runtime
open System.Runtime.Hosting
open System.Runtime.InteropServices
open System.Runtime.Remoting
open System.Runtime.Serialization
open System.Runtime.Versioning
open System.Security
open System.Security.AccessControl
open System.Security.Authentication
open System.Security.Cryptography
open System.Security.Permissions
open System.Security.Policy
open System.Text
open System.Text.RegularExpressions
open System.Threading
open System.Threading.Tasks
open System.Timers
// #endregion


    let outputFileSeperator = ","
    let dateTimeFormat = "yyyyMMdd"
    let roundingPrcision  = 3
    
    [<DebuggerDisplay("{Name}{Date}")>] 
    type StockValue = 
        { 
            Name :string;
            ID : Guid  ;
            Date: DateTime ;
            Open: decimal;
            High: decimal ;
            Low: decimal ;
            Close: decimal;
            Volume :decimal ;
        }
    
    [<DebuggerDisplay("{Name} {DateFrom} -> {DateTo}")>] 
    type AggStockValue = 
        { 
            Name :string seq;
            DateFrom: DateTime ;
            DateTo: DateTime ;
            
            Open: decimal;
            High: decimal ;
            Low: decimal ;
            Close: decimal;
            
            VolumeLow :decimal ;
            VolumeHight :decimal ;
            VolumeOpen :decimal ;
            VolumeClose :decimal ;
            VolumeSum :decimal ;
            Count :int;
        }

    let readLine (filePath:string) = seq {
        let underlierName = Path.GetFileNameWithoutExtension filePath
        use sr= new StreamReader (filePath)
        while not sr.EndOfStream do
            yield (underlierName, sr.ReadLine())
        }

    let splitCommas (l:string) =
        l.Split(',')

    let toStockValue (stockName:string) (valueStr:string) =
        let values = splitCommas valueStr             
        {
            Name =stockName; 
            ID=Guid.NewGuid(); 
            Date = DateTime.ParseExact(values.[0],dateTimeFormat,null); 
            Open  = decimal(values.[1]);
            High  = decimal(values.[2]);
            Low  = decimal(values.[3]);
            Close  = decimal(values.[4]);
            Volume  = decimal(values.[5]);
        }


    let toAggregateStockValues (svs : StockValue seq ) = 
        
        let rec loop togoList asv = 
            match  togoList with
                | h :: t -> loop t asv
                | [] -> asv

        let svsList = List.ofSeq   svs           
        let res = loop svsList None
        res 

    let tuple3ToStr (t1 :'a, t2 , t3)  =
         String.Join(outputFileSeperator, t1, t2, t3 )

    let deleteFileOrDir path = 
        if Directory.Exists(path) then 
            Directory.Delete(path)
        if File.Exists(path) then
            File.Delete(path)

    let saveValue (path :String) (seqToSave :seq<String>)   (headers: seq<String>) = 
        let header = String.Join(outputFileSeperator,headers )

        deleteFileOrDir path

        use fileWriter = new StreamWriter(path)
        fileWriter.WriteLine(header)
        seqToSave |> Seq.iter fileWriter.WriteLine


    let clearDir dir = 
        let files =  Directory.GetFiles(dir)
        if files.Any() then
            files |> Seq.iter  File.Delete  |> ignore

    let checkDir dir = 
        if Directory.Exists(dir) |> not then 
            Directory.CreateDirectory(dir) |> ignore

    let getIndexUntileDiffPath (path1 :string) (path2 :string) =  
        let maxchars = max path1.Length path2.Length
        let diffs = seq {for i=0 to maxchars do if path2.[i] <>  path2.[i] then yield  i}
        if diffs.Any() then 
            diffs |> Seq.head 
        else
            -1
        
    let getDirParetns (dir:string)= 
        dir.Split([| Path.DirectorySeparatorChar |], StringSplitOptions.RemoveEmptyEntries)

    let getSameFolders path1 path2 =
        let dirName1 = getDirParetns path1 |> Seq.toList
        let dirName2 = getDirParetns path2 |> Seq.toList
        let rec loop p1  p2 (pathbase :string list) = 
            match (p1, p2)  with
                | (head1 :: tail1 , head2 :: tail2 ) when head1 = head2 ->   loop tail1 tail2  ( pathbase @ [head1] )
                | ( _, _)  ->  pathbase
        let res = loop dirName1  dirName2 []
        String.Join(Path.DirectorySeparatorChar.ToString(),  (res |> List.toArray ) )

    let getFolderhSubtraction path1 path2 =
        let sameFolders = getSameFolders path1 path2
        path1.Replace(sameFolders + "/", "")
       
    let getOutputLocation intputFilePath baseLocationInput baseLocationOutput  = 
        let substraction = getFolderhSubtraction intputFilePath baseLocationInput
        baseLocationOutput +  substraction

    let readStockValues file = 
         readLine file |> Seq.skip 1 |> Seq.map (fun (n,v) -> toStockValue n v ) |> Seq.sortBy (fun x->  x.Date)
    
    let getGrow v1 v2 = 
        (v2 - v1 )  / v1
    
    let round (x :Decimal) = 
        Math.Round(x, roundingPrcision)


    let log0 (x :Decimal) =
        let fx = float(x)
        match fx  with 
            | fx when fx <= 0. -> 0M
            | fx -> Math.Log( fx ,10.) |> decimal
    
    let floorLog0  (x :Decimal)  = 
       x |> log0 |> Math.Floor

    let countEdges (edges : seq<Decimal * Decimal> ) =
            edges |> Seq.groupBy (fun (p,c) -> (p,c) )
                |> Seq.map (fun (k,s) -> (fst k, snd k, s.Count() ) ) |> Seq.sort


    

    // #region Converters
    let convertToGraph (feeds : StockValue seq) : (string seq * string array) = 
        let firstFeed = feeds.First()
        let prevPoint = firstFeed.Open |> ref 
        let edges = seq {for feed in feeds do
            yield (!prevPoint, feed.Close, feed.Date.ToString(dateTimeFormat))
            prevPoint := feed.Close }
            
        let res = edges |> Seq.map (fun x -> tuple3ToStr x)
        (res, [|"From";"To";"Date"|])
    
    let convertToIntGraph (feeds : StockValue seq) : (string seq * string array)= 
        let firstFeed = feeds.First()
        let prevPoint = int(firstFeed.Open) |> ref 
        let edges = seq {for feed in feeds do
            yield (!prevPoint, int(feed.Close) , feed.Date.ToString(dateTimeFormat))
            prevPoint := int(feed.Close) }


        let res = edges |> Seq.map (fun x -> tuple3ToStr x)
        (res, [|"From";"To";"Date"|])

    let convertToGrowthGraph (feeds : StockValue seq) : (string seq * string array) = 
        let firstFeed = feeds.First()
        let prevPoint = firstFeed.Close |> ref 
        let edges = seq {for feed in feeds do
            let grow =  if !prevPoint = 0M then 0M else (getGrow feed.Close !prevPoint |> round)
            yield (!prevPoint, grow , feed.Date.ToString(dateTimeFormat))
            prevPoint := grow }
            
        let res = edges |> Seq.map (fun x -> tuple3ToStr x)
        (res, [|"From";"To";"Date"|])
    

    let convertToVolumeGraph (feeds : StockValue seq) : (string seq * string array) = 
        let firstFeed = feeds.First()
        let prevPoint = firstFeed.Volume |> ref 
        let edges = seq {for feed in feeds do
            yield (!prevPoint, feed.Volume, feed.Date.ToString(dateTimeFormat))
            prevPoint := feed.Volume}
            
        let res = edges |> Seq.map (fun x -> tuple3ToStr x)
        (res, [|"From";"To";"Date"|])
    
    let convertToLogVolumeGraph (feeds : StockValue seq) : (string seq * string array) = 
        let firstFeed = feeds.First()
        let prevPoint = firstFeed.Volume |> floorLog0 |> ref 
        let edges = seq {for feed in feeds do
            let curVolume = feed.Volume |> floorLog0
            yield (!prevPoint, curVolume, feed.Date.ToString(dateTimeFormat))
            prevPoint := curVolume }
            
        let res = edges |> Seq.map (fun x -> tuple3ToStr x)
        (res, [|"From";"To";"Date"|])


    let convertToLogVolumeGraphCount (feeds : StockValue seq) : (string seq * string array) = 
        let firstFeed = feeds.First()
        let prevPoint = firstFeed.Volume |> floorLog0 |> ref 
        let edges = seq {for feed in feeds do
            let curVolume = feed.Volume |> floorLog0
            yield (!prevPoint, curVolume)
            prevPoint := curVolume }
            
        let res = edges |> countEdges                
                |> Seq.map (fun x -> tuple3ToStr x)

        (res, [|"From";"To";"Count"|]) 


    let convertToGrowthGraphCount (feeds : StockValue seq) : (string seq * string array) = 
        let firstFeed = feeds.First()
        let prevPoint = firstFeed.Close |> ref 
        let edges = seq {for feed in feeds do
            let grow =  if !prevPoint = 0M then 0M else (getGrow feed.Close !prevPoint |> round)
            yield (!prevPoint, grow )
            prevPoint := grow }
            
        let res = edges  |> countEdges  |> Seq.map (fun x -> tuple3ToStr x)
        (res, [|"From";"To";"Date"|])    

    

    let convertToGrowthTimeSerie (feeds : StockValue seq) : (string seq * string array)  = 
        let prevPoint = feeds.First() |> ref 
        let res = seq {for feed in feeds do
            let prev = !prevPoint
            let openV = getGrow prev.Open feed.Open |> round 
            let HighV = getGrow prev.High feed.High |> round 
            let LowV = getGrow prev.Low feed.Low |> round 
            let CloseV= getGrow prev.Close feed.Close |> round 
            let VolumeV = if feed.Volume = 0M then 0M else (getGrow prev.Volume  feed.Volume |> round )
            let str = String.Join(outputFileSeperator, feed.Date.ToString(dateTimeFormat), openV,HighV,LowV,CloseV,VolumeV)
            yield str
            prevPoint := feed 
        }
        (res, [|"Date"; "Open"; "High"; "Low"; "Close"; "Volume"  |])
    
    let convertToIntTimeSerie (feeds : StockValue seq) : (string seq * string array)  = 
        let res = seq {for feed in feeds do
            let openV = feed.Open |> int 
            let HighV = feed.High |> int 
            let LowV = feed.Low |> int 
            let CloseV= feed.Close |> int 
            let VolumeV = feed.Volume |> int 
            yield String.Join(outputFileSeperator, feed.Date.ToString(dateTimeFormat), openV,HighV,LowV,CloseV,VolumeV)
        }
        (res, [|"Date"; "Open"; "High"; "Low"; "Close"; "Volume"  |])
    

// #endregion

     let convertAndSave file baseLocationInput baseLocationOutput (method :string) (outputExtensionFile :string) = 
            let locationOfOutput =  getOutputLocation file baseLocationInput baseLocationOutput

            let svName = Path.GetFileNameWithoutExtension(file)
            let feeds =  readStockValues file
            let strs, headers = match method with 
                | "graph" -> convertToGraph feeds
                | "intgraph" -> convertToIntGraph feeds
                | "growthgraph" -> convertToGrowthGraph feeds
                | "volumegraph" -> convertToVolumeGraph feeds
                | "logvolumegraph" -> convertToLogVolumeGraph feeds

                | "logvolumegraphcount" -> convertToLogVolumeGraphCount feeds
                | "growthgraphcount" ->  convertToGrowthGraphCount   feeds

                | "growthts" -> convertToGrowthTimeSerie feeds
                | "intts" -> convertToIntTimeSerie feeds

                | _ -> ArgumentException("Wrong convertion method") |> raise

            Path.GetDirectoryName(locationOfOutput) |> checkDir  
            let outputFile = Path.ChangeExtension(locationOfOutput , outputExtensionFile )
            saveValue outputFile strs headers


        
    type MessageFileConverter =
        | Conv of  string * string


[<EntryPoint>]
let main argv = 
    let locationOfInput  ="/Users/arekbee/Data/git/StooqHistoricalDataDaily/" 
    let locationOfBaseInput = "/Users/arekbee/Data/git/StooqHistoricalDataDaily/" 
    let locationOfBaseOutput = "/Users/arekbee/Data/git/StooqTimeSeriesConverter-Data/Data"
    let extensionFile = ".csv"
    let converters  = [| "graph"; "intgraph"; "growthgraph"; "volumegraph"; "logvolumegraph"; "logvolumegraphcount"; "growthgraphcount"; "growthts"; "intts"  |] 
    let clen = converters.Length

    let files = Directory.GetFiles(locationOfInput,"*.txt", SearchOption.AllDirectories)
     
    
    let agents =[ for i in 0 .. (files.Length * clen) -> 
        MailboxProcessor<MessageFileConverter>.Start(fun inbox -> async {
                        while true do 
                            let! message= inbox.Receive()
                            match message with 
                                | Conv(file, convertTo) -> 
                                    convertAndSave file locationOfBaseInput (Path.Combine(locationOfBaseOutput,convertTo)) convertTo extensionFile
                                    printfn "%s for file: %s" convertTo file
                            ignore
    }) ] 
     
    for c=0 to (clen - 1) do
        let convertTo = converters.[c]
        for i=0 to files.Length - 1 do
            agents.[c * clen +  i].Post( Conv(files.[i], convertTo ) )





   
    Console.ReadKey() |> ignore



    0 // return an integer exit code


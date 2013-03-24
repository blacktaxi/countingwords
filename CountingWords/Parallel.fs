module Parallel

module private Impl =
    open System
    open System.Text
    open System.Text.RegularExpressions
    open System.Linq
    open System.IO

    open Microsoft.FSharp.Collections

    open CountingWords.Common

    /// A partial result of word counting problem
    type PartialResult = {
            PartialChart : FreqDict
            PartOffset : int
            PartLength : int
            LeftPiece : Option<string>
            RightPiece : Option<string>
        }
        with
            static member Empty = {
                    PartialChart = new FreqDict()
                    PartOffset = 0; PartLength = 0
                    LeftPiece = None; RightPiece = None
                }

    /// A part of input text
    type TextPart = { Text : seq<char>; Offset : int }

    /// Explodes a text part in tokens/words, returning a tuple:
    ///   (leftmost word, [words inbetween], rightmost word)
    let parsePart (text : seq<char>) =
        let all = parseText (text) |> Array.ofSeq

        // we can't work with a part that's just one huge word.
        // it could be done, but I cba to implement.
        assert (all.Length > 1)

        // see if part starts and ends with a word character
        let haveLeft = isWordChar <| text.First()
        let haveRight = isWordChar <| text.Last()

        // some "complicated" array splitting
        (if haveLeft then Some <| all.First() else None),
        (all.Skip(if haveLeft then 1 else 0)
            .Take(all.Length - (if haveLeft && haveRight then 2 
                                else if haveLeft || haveRight then 1 
                                else 0))),
        (if haveRight then Some <| all.Last() else None)

    /// Processes a text part, returning a partial result
    let processPart ({ Text = text; Offset = offset } : TextPart) =
        let left, middle, right = parsePart text

        {
            PartialResult.PartOffset = offset
            PartLength = Seq.length text
            LeftPiece = left
            RightPiece = right
            PartialChart = addWordsToDict (new FreqDict()) middle
        }

    /// Combines partial resulsts
    let combineResults (parts : seq<PartialResult>) =
        // check that part sequence is valid, that is part offsets and lengths
        // match up.
        assert (Seq.forall 
                    (fun (l, r) -> r.PartOffset = l.PartOffset + l.PartLength) 
                <| Seq.pairwise parts)

        let d = new FreqDict()

        // merge in partial charts
        for p in parts do 
            for KeyValue (k, v) in p.PartialChart do 
                d.[k] <- d.GetOrDefault(k, 0) + v

        // recover splitted words (the word is split when text part cut position
        // was chosen to be inside it)
        let recoveredWords = [
            for (left, right) in Seq.pairwise parts do
                let inline toStr (x : Option<string>) = if x.IsSome then x.Value else ""
                yield (toStr left.RightPiece) + (toStr right.LeftPiece)
        ]

        // add those words to general chart
        ignore <| addWordsToDict d recoveredWords

        let leftest = Seq.head parts
        let rightest = Seq.last parts

        {
            PartialResult.PartOffset = leftest.PartOffset
            PartLength = Seq.sumBy (fun x -> x.PartLength) parts
            LeftPiece = leftest.LeftPiece
            RightPiece = rightest.RightPiece
            PartialChart = d
        }

    let processBuffers (parts : seq<TextPart>) = 
        async {
            let! partialResults = 
                Async.Parallel <| [| for p in parts -> async { return processPart p } |]

//            // @NOTE doesn't work!
//            let! partialResults = 
//                Async.Parallel <| seq { for p in parts -> async { return processPart p } }

            let finalResult =
                Seq.concat [ 
                    [| PartialResult.Empty |]; 
                    partialResults
                ] |> combineResults
            
            let finalDict = finalResult.PartialChart

            // add leftovers to chart
            for w in [finalResult.LeftPiece; finalResult.RightPiece] do
                ignore <| addWordsToDict finalDict (Option.toList w)

            return finalDict
        }

    let chartFromDict (d : FreqDict) =
        Seq.map (fun (KeyValue x) -> x) d

    /// Chunk a stream in a sequence of buffers
    let readBuffers stream bufferSize =
        seq {
            use rdr = new BinaryReader(stream)
            let reading = ref true
            let currentOffset = ref 0

            while !reading do
                let chars = rdr.ReadChars(bufferSize)
                reading := chars.Length > 0
                if !reading then
                    yield { Text = chars; Offset = !currentOffset }
                currentOffset := !currentOffset + chars.Length
        }

    let topWords howMany chart =
        List.ofSeq chart
        |> List.sortBy (fun (k, v) -> v)
        |> List.rev
        |> Seq.truncate howMany

    let processStream bufferSize stream =
        readBuffers stream bufferSize
        |> processBuffers
        |> Async.RunSynchronously
        |> chartFromDict
        |> topWords 10
    
let processStream = Impl.processStream


module CountingWords.Common

open System
open System.Text
open System.Text.RegularExpressions
open System.IO
open System.Linq

type System.Collections.Generic.Dictionary<'a, 'b> with
    member inline x.GetOrDefault(key : 'a, defaultVal : 'b) =
        let v = ref Unchecked.defaultof<'b>
        if x.TryGetValue(key, v) then !v
        else defaultVal

type FreqDict = Collections.Generic.Dictionary<string, int>

let inline isWordChar c = ('a' <= c) && (c <= 'z')

/// Parses a text into a sequence of lowercase words
let parseText (text : seq<char>) =
    seq {
        let b = new StringBuilder()
        for char in text do
            let char = Char.ToLowerInvariant(char)
            if isWordChar char then ignore <| b.Append(char)
            else 
                if b.Length > 0 then 
                    yield b.ToString()
                    ignore <| b.Clear()

        if b.Length > 0 then 
            yield b.ToString()
    }

/// Appends word sequence to freqency dictionary
let addWordsToDict (dict : FreqDict) (words : seq<string>) =
    for word in words do dict.[word] <- dict.GetOrDefault(word, 0) + 1
    dict
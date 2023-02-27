using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Threading.Tasks;
using Nethereum.BlockchainProcessing.BlockStorage.Entities;
using Nethereum.Hex.HexConvertors.Extensions;
using Nethereum.Hex.HexTypes;
using Nethereum.JsonRpc.Client;
using Nethereum.RPC.Eth.DTOs;
using Nethereum.Web3;

namespace NethereumSample
{

    class Program
    {
        public static readonly byte[] EofPrefix = { 0xEF, 0x00 };
        static Web3? web3 = new Nethereum.Web3.Web3("http://127.0.0.1:8545");
        static int BlockchainHeight = 19040000;
        static Stopwatch timer = new Stopwatch();
        static ConcurrentBag<BigInteger> FailedBlocks = new();
        static ConcurrentDictionary<BigInteger, bool> HandledBlocks = new();
        static FileStream ErrorStream = File.Open("error.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        static (StreamWriter, StreamReader) ErrorStreamIO = (
            new StreamWriter(ErrorStream),
            new StreamReader(ErrorStream)
        );
        static FileStream HandledStream = File.Open("handled.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        static (StreamWriter, StreamReader) HandledStreamIO = (
            new StreamWriter(HandledStream),
            new StreamReader(HandledStream)
        );
        static FileStream ReceiptStream = File.Open("receipts.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        static (StreamWriter, StreamReader) ReceiptStreamIO = (
            new StreamWriter(ReceiptStream),
            new StreamReader(ReceiptStream)
        );
        static FileStream ContractsStream = File.Open("contracts.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        static (StreamWriter, StreamReader) ContractsStreamIO = (
            new StreamWriter(ContractsStream),
            new StreamReader(ContractsStream)
        );
        
        static FileStream ResultsStream = File.Open("results.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        static (StreamWriter, StreamReader) ResultsStreamIO = (
            new StreamWriter(ResultsStream),
            new StreamReader(ResultsStream)
        );
        static async Task Main(string[] args)
        {
            try {
                BigInteger startBatch = args.Length > 0 ? int.Parse(args[0]) / 1000: 0;
                try {
                    FailedBlocks = new ConcurrentBag<BigInteger>(
                        ErrorStreamIO.Item2.ReadToEnd().Split("\n").Where(line => !String.IsNullOrWhiteSpace(line))
                            .Select(line => line.Split('-')[0])
                            .Select(BigInteger.Parse)
                            .ToList()
                    );
                    HandledStreamIO.Item2.ReadToEnd().Split("\n").Where(line => !String.IsNullOrWhiteSpace(line)).Select(line => line.Split('-')).ToList()
                        .ForEach(line => HandledBlocks.TryAdd(BigInteger.Parse(line[0]), bool.Parse(line[1]))); 
                }catch(Exception e) {
                    Console.WriteLine(e.Message);
                    throw;
                }

                static BigInteger Max(BigInteger a, BigInteger b) => a > b ? a : b;
                var batchSize= 10000;
                var biggestKey = (HandledBlocks.IsEmpty ? 0 : HandledBlocks.Keys.Max() / batchSize);
                var biggestError = (FailedBlocks.IsEmpty ? 0 : FailedBlocks.Max() / batchSize);
                startBatch = Max(Max(biggestKey, biggestError), startBatch);
                Console.WriteLine($"Starting from batch {startBatch}");
                bool StartWithEofPrefixTx(byte[] bytecode) => bytecode.AsSpan().StartsWith(EofPrefix);
                List<BlockWithTransactions> blocks = new(); 
                int len = BlockchainHeight / batchSize;

                var AnyEofsFound = false;
                for(BigInteger i = startBatch; i <= len; i++) {
                    var start = i * batchSize;
                    var end = (i + 1) * batchSize;
                    AnyEofsFound |= await IsThereAnyEofInBlock(start, end, StartWithEofPrefixTx);
                }
                // open error.txt and add the failed blocks to the FailedBlocks list
                var failedBlocks = FailedBlocks.ToList();
                if(failedBlocks.Count > 0) {
                    Console.WriteLine("Handling failed blocks");
                    AnyEofsFound |= await HandleRogueSet(failedBlocks, StartWithEofPrefixTx);
                    Console.WriteLine("Done handling failed blocks");

                    
                    ErrorStreamIO.Item1.Flush();
                    ErrorStreamIO.Item1.Dispose();
                    ErrorStreamIO.Item2.Dispose();
                    ErrorStream.Dispose();

                    File.Delete("error.txt");
                }

                // make sure iota iteration didnt skip any blocks
                var missingBlocks = Enumerable.Range(0, BlockchainHeight).Select(i => (BigInteger)i).Except(HandledBlocks.Keys).ToList();
                if(missingBlocks.Count > 0) {
                    Console.WriteLine("Handling ignored blocks");
                    AnyEofsFound |= await HandleRogueSet(missingBlocks, StartWithEofPrefixTx);
                    Console.WriteLine("Done handling ignored blocks");
                }

                string message = AnyEofsFound ? "EOF found" : "No EOF found";
                Console.WriteLine(message);
                File.WriteAllText("found.txt", message);
            } finally {
                ErrorStreamIO.Item1.Flush();
                ErrorStreamIO.Item1.Dispose();
                ErrorStreamIO.Item2.Dispose();
                ErrorStream.Dispose();
                HandledStreamIO.Item1.Flush();
                HandledStreamIO.Item1.Dispose();
                HandledStreamIO.Item2.Dispose();
                HandledStream.Dispose();
                ReceiptStreamIO.Item1.Flush();
                ReceiptStreamIO.Item1.Dispose();
                ReceiptStreamIO.Item2.Dispose();
                ReceiptStream.Dispose();
                ContractsStreamIO.Item1.Flush();
                ContractsStreamIO.Item1.Dispose();
                ContractsStreamIO.Item2.Dispose();
                ContractsStream.Dispose();
                ResultsStreamIO.Item1.Flush();
                ResultsStreamIO.Item1.Dispose();
                ResultsStreamIO.Item2.Dispose();
                ResultsStream.Dispose();
            }
        }

        async static Task<bool> HandleRogueSet(List<BigInteger> blocks, Func<byte[], bool> Check) {
            bool hasAnyEof = false;
            int? index = null;
            do {
                index ??= (blocks.Count - 1); 
                var block = blocks[index.Value];
                var result = await HandleBlockNumber(block, Check, true, -1);
                if(result is null) {
                    continue;
                }
                index--;
                hasAnyEof |= result.Value;
            }
            while (index >= 0);
            return hasAnyEof;
        }

        static async Task<bool?> HandleBlockNumber(BigInteger i, Func<byte[], bool> Check, bool force, int retries) {
            Func<Task<bool?>> process = async () => {
                Console.WriteLine($"Handling Block Number {i}");
                if(HandledBlocks.ContainsKey(i)) {
                    return HandledBlocks[i];
                }

                if(!force && FailedBlocks.Contains(i)) {
                    return null;
                }

                BlockWithTransactions block = await web3.Eth.Blocks.GetBlockWithTransactionsByNumber.SendRequestAsync(new BlockParameter(i.ToHexBigInteger()));
                        // get the receipts of the create transactions
                var TxReceipts = await Task.WhenAll(
                    block.Transactions
                        .Select(tx => web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(tx.TransactionHash)).ToArray()); // get the receipt of the create transaction
                
                lock(ReceiptStreamIO.Item1) {
                    var batchrec = String.Join("\n", TxReceipts.Select(r => $"Block: {i} Tx: {r.TransactionHash} From: {r.From} To: {r.To} Contract: {r.ContractAddress} Status: {r.Status.Value} GasUsed: {r.GasUsed.Value} CumulativeGasUsed: {r.CumulativeGasUsed.Value}"));
                    ReceiptStreamIO.Item1.WriteLine(batchrec);
                }
                
                // get the bytecode of the deployed contracts
                var deployedContractsBytecode = await Task.WhenAll(
                    TxReceipts
                        .Select(r => r.ContractAddress != null ? web3.Eth.GetCode.SendRequestAsync(r.ContractAddress) : Task.FromResult(string.Empty)).ToArray()); // get the bytecode of the deployed contract
                
                lock(ContractsStreamIO.Item1) {
                    string batchdeped = String.Join("\n", deployedContractsBytecode.Where(add => !String.IsNullOrWhiteSpace(add)).Select(r => $"Block: {i} Contract: {r}"));
                    ContractsStreamIO.Item1.WriteLine(batchdeped);
                }

                // check if the bytecode starts with the EOF prefix
                var deployedContracts = deployedContractsBytecode
                    .Select((HexCode, idx) => (HexCode, idx))
                    .Where(pair => Check(pair.HexCode.HexToByteArray()))
                    .ToList();

                bool hasEofContracts = deployedContracts.Any();
                if(hasEofContracts) {
                    lock(ResultsStreamIO.Item1) {
                        var eofbatch = String.Join("\n", 
                            deployedContracts.Select(pair => (TxReceipts[pair.idx].ContractAddress, pair.HexCode))
                                .Select(c => $"Contract : {c.ContractAddress} Code : {c.HexCode}"));
                        ResultsStreamIO.Item1.WriteLine(eofbatch);
                    }
                }

                HandledBlocks.AddOrUpdate(i, hasEofContracts, (k, v) => hasEofContracts);

                lock(HandledStreamIO.Item1) {
                    HandledStreamIO.Item1.WriteLine($"{i}-{hasEofContracts}");
                }
                return hasEofContracts;
            };

            while(retries != 0) {
                try {
                    return await process();
                } catch(Exception e) {
                    Console.WriteLine($"Error handling block {i}: {e.Message}");                    
                    if(retries == 0) {
                        throw;
                    }
                    retries--;
                }
            }
            return null;
        }

        static async Task<bool> IsThereAnyEofInBlock(BigInteger start, BigInteger end, Func<byte[], bool> Check) {
            int subbatchSize = 1000;
            Console.WriteLine("Handling batch: " + start + " - " + end);
            for(BigInteger i = start; i < end; i+=subbatchSize) {
                var results = await Task.WhenAll(Enumerable.Range(0, subbatchSize).Select(async j => {
                    try {
                        var result = await HandleBlockNumber(i + j, Check, false, 10);
                        if(result ?? false) {
                            return true;
                        }
                    } catch(Exception e) {
                        lock(ErrorStreamIO.Item1){
                            ErrorStreamIO.Item1.WriteLine($"{i + j}-{e.Message}");
                        }
                        FailedBlocks.Add(i);
                    }
                    return false;
                }));
                if(results.Any(r => r)) {
                    return true;
                }
            }
            Console.WriteLine("Done handling batch: " + start + " - " + end);
            return false;
        }


    }
}

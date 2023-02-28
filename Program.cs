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
        public static readonly int BytecodeMeteredLen = 0x6000;
        public static readonly  Func<byte[], bool>[] Checks = new Func<byte[], bool>[] { 
            /*Eip3540, Eip3541*/ (byte[] bytecode) => bytecode.AsSpan().StartsWith(EofPrefix),
            /*Eip-170*/ (byte[] bytecode) => bytecode.Length > BytecodeMeteredLen
        };

        static Web3? web3 = new Nethereum.Web3.Web3("http://127.0.0.1:8545");
        static (int OfBatch, int OfSubBatch) Size = (10000, 1000);
        static Stopwatch timer = new Stopwatch();
        static ConcurrentBag<BigInteger> FailedBlocks = new();
        static ConcurrentDictionary<BigInteger, bool> HandledBlocks = new();

        static FileStreamOptions StreamOptions = new FileStreamOptions {
            Mode = FileMode.OpenOrCreate | FileMode.Append,
            Access = FileAccess.Write,
            Options = FileOptions.Asynchronous
        };
        static volatile StreamWriter ReceiptStream;
        static volatile StreamWriter ContractsStream;
        static volatile StreamWriter ResultsStream;
        static volatile StreamWriter ErrorStream;
        static volatile StreamWriter ProgressStream;

        static async Task Setup() {
            try {
                Task.WaitAll(
                    Task.Run(async () => {
                        File.OpenText("errors.txt").ReadToEnd().Split("\n").Where(line => !String.IsNullOrWhiteSpace(line))
                            .Select(line => line.Split('-')[0])
                            .Select(BigInteger.Parse)
                            .ToList().ForEach(FailedBlocks.Add);
                    }),
                    Task.Run(async () => {
                        File.OpenText("progress.txt").ReadToEnd().Split("\n").Where(line => !String.IsNullOrWhiteSpace(line)).Select(line => line.Split('-')).ToList()
                            .ForEach(line => HandledBlocks.TryAdd(BigInteger.Parse(line[0]), bool.Parse(line[1])));
                    })
                );

                ReceiptStream = new("receipts.txt", StreamOptions);
                ContractsStream = new("contracts.txt", StreamOptions);
                ResultsStream = new("results.txt", StreamOptions);
                ErrorStream = new("errors.txt", StreamOptions);
                ProgressStream = new("progress.txt", StreamOptions);
            }catch(Exception e) {
                Console.WriteLine(e.Message);
                throw;
            }
        }

        static async Task Main(string[] args)
        {
            try {
                await Setup();

                int BlockchainHeight = (int)(await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync()).Value;
                var failedTheChecks = false;

                Console.WriteLine("Handling first pass blocks");
                var batches = await Task.WhenAll(Enumerable.Range(0, BlockchainHeight)
                    .Select(i => (BigInteger)i)
                    .Chunk(Size.OfBatch)
                    .Select(b => HandleBlockBatch(b, false, 20)));
                failedTheChecks |= batches.Any(e => e);
                Console.WriteLine("Handling first pass blocks");

                Console.WriteLine("Handling failed blocks");
                var err_results = await Task.WhenAll(FailedBlocks.Chunk(Size.OfBatch)
                    .Select(chunk => HandleBlockBatch(chunk, true, -1)));
                failedTheChecks |= err_results.Any(e => e);
                Console.WriteLine("Done handling failed blocks");

                Console.WriteLine("Handling ignored blocks");
                var corr_results = await Task.WhenAll(Enumerable.Range(0, BlockchainHeight).Select(i => (BigInteger)i)
                    .Except(HandledBlocks.Keys).Chunk(Size.OfBatch)
                    .Select(chunk => HandleBlockBatch(chunk, true, -1)));
                failedTheChecks |= corr_results.Any(e => e);
                Console.WriteLine("Done handling ignored blocks");

                string message = failedTheChecks ? "Eip:[3540-170] conflicts found" : "No conflicts found";
                Console.WriteLine(message);
                File.WriteAllText("found.txt", message);
            } catch(Exception e) {
                Console.WriteLine(e.Message);
                throw;
            } finally {
                ReceiptStream.Close();
                ContractsStream.Close();
                ResultsStream.Close();
                ErrorStream.Close();
                ProgressStream.Close();
            }
        }

        static async Task<bool?> HandleBlockNumber(BigInteger i, bool force, int retries) {
            Func<Task<bool?>> process = async () => {
                if(HandledBlocks.ContainsKey(i)) {
                    return HandledBlocks[i];
                }

                if(!force && FailedBlocks.Contains(i)) {
                    return null;
                }


                await Task.Delay(System.Random.Shared.Next(0, 1000));
                BlockWithTransactions block = await web3.Eth.Blocks.GetBlockWithTransactionsByNumber.SendRequestAsync(new BlockParameter(i.ToHexBigInteger()));
                        // get the receipts of the create transactions
                var TxReceipts = await Task.WhenAll(
                    block.Transactions
                        .Select(tx => web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(tx.TransactionHash)).ToArray()); // get the receipt of the create transaction
                
                lock(ReceiptStream) {
                    var batchrec = String.Join("\n", TxReceipts.Select(r => $"Block: {i} Tx: {r.TransactionHash} From: {r.From} To: {r.To} Contract: {r.ContractAddress} Status: {r.Status.Value} GasUsed: {r.GasUsed.Value} CumulativeGasUsed: {r.CumulativeGasUsed.Value}"));
                    ReceiptStream.WriteLine(batchrec);
                }
                
                // get the bytecode of the deployed contracts
                var deployedContractsBytecode = await Task.WhenAll(
                    TxReceipts
                        .Select(r => r.ContractAddress != null ? web3.Eth.GetCode.SendRequestAsync(r.ContractAddress) : Task.FromResult(string.Empty)).ToArray()); // get the bytecode of the deployed contract
                
                lock(ContractsStream) {
                    string batchdeped = String.Join("\n", deployedContractsBytecode.Where(add => !String.IsNullOrWhiteSpace(add)).Select(r => $"Block: {i} Contract: {r}"));
                    ContractsStream.WriteLine(batchdeped);
                    ContractsStream.Flush();
                }

                // check if the bytecode starts with the EOF prefix
                var deployedContracts = deployedContractsBytecode
                    .Select((HexCode, idx) => (HexCode, idx))
                    .Where(pair => {
                        byte[] bytecode = pair.HexCode.HexToByteArray();
                        return Checks.Any(check => check(bytecode));
                    })
                    .ToList();

                bool failedTheFilter = deployedContracts.Any();
                if(failedTheFilter) {
                    lock(ResultsStream) {
                        var eofbatch = String.Join("\n", 
                            deployedContracts.Select(pair => (TxReceipts[pair.idx].ContractAddress, pair.HexCode))
                                .Select(c => $"Contract : {c.ContractAddress} Code : {c.HexCode}"));
                        ResultsStream.WriteLine(eofbatch);
                        ResultsStream.Flush();
                    }
                }

                HandledBlocks.AddOrUpdate(i, failedTheFilter, (k, v) => failedTheFilter);

                lock(ProgressStream) {
                    ProgressStream.WriteLine($"{i}-{failedTheFilter}");
                    ProgressStream.Flush();
                }
                return failedTheFilter;
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
                    await Task.Delay(1000);
                }
            }
            return null;
        }

        static async Task<bool> HandleBlockBatch(BigInteger[] batch, bool force, int retries) {
            Console.WriteLine("Handling batch: " + batch.First() + " - " + batch.Last());
            foreach(BigInteger[] chunk in batch.Chunk(Size.OfSubBatch)) {
                var results = await Task.WhenAll(chunk.Select(async j => {
                    try {
                        var result = await HandleBlockNumber(j,  force, retries);
                        if(result ?? false) {
                            return true;
                        }
                    } catch(Exception e) {
                        lock(ErrorStream){
                            ErrorStream.WriteLine($"{j}-{e.Message}");
                            ErrorStream.Flush();
                        }
                        FailedBlocks.Add(j);
                    }
                    return false;
                }));
            }
            Console.WriteLine("Handling batch: " + batch.First() + " - " + batch.Last());
            return false;
        }


    }
}

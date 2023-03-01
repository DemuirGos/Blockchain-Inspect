﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Text;
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

        static readonly Web3 web3 = new Nethereum.Web3.Web3("http://127.0.0.1:8545");
        static (int OfBatch, int OfSubBatch) Size = (10000, 1000);
        static Stopwatch timer = new Stopwatch();
        static ConcurrentBag<BigInteger> FailedBlocks = new();
        static ConcurrentDictionary<BigInteger, bool> HandledBlocks = new();

        static FileStreamOptions StreamOptions = new FileStreamOptions {
            Mode = FileMode.OpenOrCreate,
            Access = FileAccess.Write | FileAccess.Read,
            Options = FileOptions.Asynchronous
        };

        static volatile object LockSem = new object();
        static volatile FileStream ErrorStream = new("errors.txt", StreamOptions); 
        static volatile FileStream ProgressStream = new("progress.txt", StreamOptions); 
        static readonly string[] LogFolders = new [] { "receipts", "contracts", "results" };
        static async Task Setup() {
            try {
                await Task.WhenAll(
                    Task.Run(() => {
                        ErrorStream.ReadAllLines().Where(line => !String.IsNullOrWhiteSpace(line))
                            .Select(line => line.Split('-')[0])
                            .Select(BigInteger.Parse)
                            .ToList().ForEach(FailedBlocks.Add);
                    }),
                    Task.Run(() => {
                        ProgressStream.ReadAllLines().Where(line => !String.IsNullOrWhiteSpace(line)).Select(line => line.Split('-')).ToList()
                            .ForEach(line => HandledBlocks.TryAdd(BigInteger.Parse(line[1]), bool.Parse(line[2])));
                    })
                );
                
                await Extensions.SetupSubFolder(LogFolders);
            }catch(Exception e) {
                Console.WriteLine(e.StackTrace);
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
                foreach(var block_batch in Enumerable.Range(0, BlockchainHeight).Select(i => (BigInteger)i).Chunk(Size.OfBatch))
                    failedTheChecks |= await HandleBlockBatch(block_batch, false, 20);
                Console.WriteLine("Handling first pass blocks");

                Console.WriteLine("Handling failed blocks");
                foreach(var failed_batch in FailedBlocks.Chunk(Size.OfBatch))
                    failedTheChecks |= await HandleBlockBatch(failed_batch, true, -1);
                Console.WriteLine("Done handling failed blocks");

                Console.WriteLine("Handling ignored blocks");
                foreach(var correction_batch in Enumerable.Range(0, BlockchainHeight).Select(i => (BigInteger)i).Except(HandledBlocks.Keys).Chunk(Size.OfBatch))
                    failedTheChecks |= await HandleBlockBatch(correction_batch, true, -1);
                Console.WriteLine("Done handling ignored blocks");

                string message = failedTheChecks ? "Eip:[3540-170] conflicts found" : "No conflicts found";
                Console.WriteLine(message);
                File.WriteAllText("found.txt", message);
            } catch(Exception e) {
                Console.WriteLine(e.Message);
                throw;
            } finally {
                ErrorStream.Close();
                ProgressStream.Close();
            }
        }

        static async Task<bool?> HandleBlockNumber(string[] path, BigInteger i, bool force, int retries, ConcurrentBag<string>[] TempLogSink) {
            Func<Task<bool?>> process = async () => {
                string subPath =  await Extensions.SetupSubFolder(LogFolders, path[0], path[1], i.ToString());

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
                
                lock(LockSem) {
                    foreach(var receipt in TxReceipts) {
                        File.WriteAllText($"./receipts{subPath}/{receipt.TransactionHash}.txt", System.Text.Json.JsonSerializer.Serialize(receipt));
                    }
                }
                
                // get the bytecode of the deployed contracts
                var deployedContractsBytecode = await Task.WhenAll(
                    TxReceipts
                        .Select(r => r.ContractAddress != null ? web3.Eth.GetCode.SendRequestAsync(r.ContractAddress) : Task.FromResult(string.Empty)).ToArray()); // get the bytecode of the deployed contract
                
                lock(LockSem) {
                    int bcIndex = 0;
                    foreach(string bytecode in deployedContractsBytecode) {
                        if(String.IsNullOrWhiteSpace(bytecode)) continue;
                        string address = TxReceipts[bcIndex].ContractAddress;
                        File.WriteAllText($"./contracts{subPath}/{address}.txt", bytecode);
                        bcIndex++;
                    }
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
                    lock(LockSem) {
                        foreach(var pair in deployedContracts) {
                            string address = TxReceipts[pair.idx].ContractAddress;
                            File.WriteAllText($"./results{subPath}/{address}.txt", $"Block : {i} Contract : {TxReceipts[pair.idx].ContractAddress} Code : {pair.HexCode}");
                        }
                    }
                }

                HandledBlocks.AddOrUpdate(i, failedTheFilter, (k, v) => failedTheFilter);
                TempLogSink[0].Add($"Block-{i}-{failedTheFilter}-rcts:{TxReceipts.Length}-cntcts:{deployedContracts.Count}");

                return failedTheFilter;
            };

            while(retries != 0) {
                try {
                    return await process();
                } catch(Exception e) {
                    Console.WriteLine($"Error on block {i} : {e.Message} - {retries} retries left");
                    if(retries == 1) {
                        TempLogSink[1].Add($"Block-{i}-Error-{e.Message}");
                        FailedBlocks.Add(i);
                    }
                    retries--;
                }
            }
            return null;
        }

        static async Task<bool> HandleBlockBatch(BigInteger[] batch, bool force, int retries) {
            string batchName(BigInteger[] batch) => batch.First() + "_" + batch.Last();
            ConcurrentBag<string>[] LogSinks = new ConcurrentBag<string>[2];
            LogSinks[0] = new ConcurrentBag<string>();
            LogSinks[1] = new ConcurrentBag<string>();

            Console.WriteLine("Started Handling batch: " + batchName(batch));
            foreach(BigInteger[] chunk in batch.Chunk(Size.OfSubBatch)) {
                var results = await Task.WhenAll(chunk.Select(async j => {
                    var result = await HandleBlockNumber(new[] {batchName(batch), batchName(chunk)}, j,  force, retries, LogSinks);
                    if(result ?? false) {
                        return true;
                    }
                    return false;
                }));
            }

            lock (LockSem)
            {
                foreach (var log in LogSinks[0])
                {
                    ProgressStream.WriteLine(log);
                }
                foreach (var log in LogSinks[1])
                {
                    ErrorStream.WriteLine(log);
                }
                ErrorStream.Flush();
                ProgressStream.Flush();
            }

            Console.WriteLine("Done Handling batch: " + batchName(batch));
            return false;
        }
    }

    public static class Extensions {
        public static void WriteLine(this FileStream stream, string line) {
            var bytes = Encoding.UTF8.GetBytes(line + "\n");
            stream.Seek(0, SeekOrigin.End);
            stream.Write(bytes, 0, bytes.Length);
        }

        public static IEnumerable<string> ReadAllLines(this FileStream stream) {
            var bytes = new byte[stream.Length];
            stream.Read(bytes, 0, bytes.Length);
            return Encoding.UTF8.GetString(bytes).Split("\n");
        }

        public static Task<string> SetupSubFolder(string[] targetFolders, params string[] nestedPath) {
            string GetOrderPath(string folder, int order, params string[] subfolders) {
                string path = folder;
                for(int i = 0; i < order; i++) {
                    path += "/" + subfolders[i];
                }
                return path;
            }

            if(nestedPath.Length == 0) {
                foreach(string folder in targetFolders) {
                    if(!Directory.Exists(folder)) {
                        Directory.CreateDirectory(folder);
                    }
                } 
            }

            foreach (var file in targetFolders)
            {
                for(int i = 0; i < nestedPath.Length; i++) {
                    string path = GetOrderPath(file, i, nestedPath);
                    if(!Directory.Exists(path)) {
                        Directory.CreateDirectory(path);
                    }
                }
            }

            return Task.FromResult(GetOrderPath(string.Empty, nestedPath.Length, nestedPath));
        } 
    }
}

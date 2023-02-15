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
        static Web3? web3 = new Nethereum.Web3.Web3("http://139.144.23.46:8545");
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
        static async Task Main(string[] args)
        {
            int startBatch = args.Length > 0 ? int.Parse(args[0]) : 0;
            try {
                FailedBlocks = new ConcurrentBag<BigInteger>(
                    ErrorStreamIO.Item2.ReadToEnd().Split("\n")
                        .Select(line => line.Split('-')[0])
                        .Select(BigInteger.Parse)
                        .ToList()
                );
                HandledStreamIO.Item2.ReadToEnd().Split("\n").Select(line => line.Split('-')).ToList()
                    .ForEach(line => HandledBlocks.TryAdd(BigInteger.Parse(line[0]), bool.Parse(line[1]))); 
            }catch {}
            
            bool StartWithEofPrefixTx(byte[] bytecode) => bytecode.AsSpan().StartsWith(EofPrefix);
            List<BlockWithTransactions> blocks = new(); 
            int batchSize= 1000;
            int len = BlockchainHeight / batchSize;

            var AnyEofsFound = false;
            for(int i = startBatch; !AnyEofsFound && i < len; i++) {
                var start = i * batchSize;
                var end = (i + 1) * batchSize;
                AnyEofsFound = await IsThereAnyEofInBlock(start, end, StartWithEofPrefixTx);
            }

            // open error.txt and add the failed blocks to the FailedBlocks list
            if(FailedBlocks.Count > 0) {
                Console.WriteLine("Handling failed blocks");
                int? index = null;
                do {
                    index ??= (FailedBlocks.Count - 1); 
                    var block = FailedBlocks.ElementAt(index.Value);
                    var result = await HandleBlockNumber(block, StartWithEofPrefixTx, true);
                    if(result is null) {
                        continue;
                    }
                    index--;
                    AnyEofsFound |= result.Value;
                }
                while (index >= 0);
                Console.WriteLine("Done handling failed blocks");
            }

            string message = AnyEofsFound ? "EOF found" : "No EOF found";
            Console.WriteLine(message);
            File.WriteAllText("found.txt", message);
        }

        static async Task<bool?> HandleBlockNumber(BigInteger i, Func<byte[], bool> Check, bool force = false) {
            if(HandledBlocks.ContainsKey(i)) {
                return HandledBlocks[i];
            }

            if(!force && FailedBlocks.Contains(i)) {
                Console.WriteLine("Skipping block: " + i);
                return null;
            }

            await Task.Delay(500);
            BlockWithTransactions block = await web3.Eth.Blocks.GetBlockWithTransactionsByNumber.SendRequestAsync(new BlockParameter(new HexBigInteger(i)));
                    // get the receipts of the create transactions
            var TxReceipts = await Task.WhenAll(
                block.Transactions
                    .Select(tx => web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(tx.TransactionHash)).ToArray()); // get the receipt of the create transaction
            
            lock(ReceiptStreamIO.Item1) {
                TxReceipts.ToList().ForEach(r => {
                    ReceiptStreamIO.Item1.WriteLine(
                        $"Block: {i} Tx: {r.TransactionHash} From: {r.From} To: {r.To} Contract: {r.ContractAddress} Status: {r.Status.Value} GasUsed: {r.GasUsed.Value} CumulativeGasUsed: {r.CumulativeGasUsed.Value}"
                    );
                });
            }

            // get the bytecode of the deployed contracts
            var deployedContractsBytecode = await Task.WhenAll(
                TxReceipts
                    .Select(r => r.ContractAddress) // get the address of the deployed contract
                    .Where(address => address != null) // filter out null addresses
                    .Select(address => web3.Eth.GetCode.SendRequestAsync(address)).ToArray()); // get the bytecode of the deployed contract
            
            // check if the bytecode starts with the EOF prefix
            var deployedContracts = deployedContractsBytecode
                .Select(hexCode => hexCode.HexToByteArray()).Where(Check);
            bool hasEofContracts = deployedContracts.Any();
            if(hasEofContracts) {
                Console.WriteLine("Found EOF at block: " + i);
            }
            HandledBlocks.TryAdd(i, hasEofContracts);

            lock(HandledStreamIO.Item1) {
                HandledStreamIO.Item1.WriteLine($"{i}-{hasEofContracts}");
            }
            return hasEofContracts;
        }

        static async Task<bool> IsThereAnyEofInBlock(BigInteger start, BigInteger end, Func<byte[], bool> Check) {
            int subbatchSize = 100;
            Console.WriteLine("Handling batch: " + start + " - " + end);
            for(BigInteger i = start; i < end; i+=subbatchSize) {
                var results = await Task.WhenAll(Enumerable.Range(0, subbatchSize).Select(async j => {
                    try {
                        var result = await HandleBlockNumber(i + j, Check);
                        if(result ?? false) {
                            return true;
                        }
                    } catch(Exception e) {
                        Console.WriteLine("Error handling block: " + (i + j) + " error: " + e.Message);
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
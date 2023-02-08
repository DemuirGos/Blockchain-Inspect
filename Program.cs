using System;
using System.Diagnostics;
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
        static Web3? web3 = new Nethereum.Web3.Web3("[<Node::Ip>]");
        static int BlockchainHeight = 19040000;
        static Stopwatch timer = new Stopwatch();
        static async Task Main(string[] args)
        {
            bool StartWithEofPrefixTx(byte[] bytecode) => bytecode.AsSpan().StartsWith(EofPrefix);
            List<BlockWithTransactions> blocks = new(); 
            int batchSize= 1000;
            int len = BlockchainHeight / batchSize;

            var AnyEofsFound = false;
            for(int i = 0; !AnyEofsFound && i < len; i++) {
                var start = i * batchSize;
                var end = (i + 1) * batchSize;
                int retries = 10;
                while(retries >= 0 && !AnyEofsFound) {
                    try {
                        AnyEofsFound = await IsThereAnyEofInBlock(start, end, StartWithEofPrefixTx);
                    } catch {
                        retries--;
                    }
                }
            }

            if(AnyEofsFound) {
                Console.WriteLine("EOF found");
            } else {
                Console.WriteLine("No EOF found");
            }
        }
        static async Task<bool> IsThereAnyEofInBlock(BigInteger start, BigInteger end, Func<byte[], bool> Check) {
            Console.WriteLine("Handling batch: " + start + " - " + end);
            try {
                bool IsCreateTx(Nethereum.RPC.Eth.DTOs.Transaction tx) => tx.To == null;
                for(BigInteger i = start; i < end; i++) {
                    // get the block by number
                    BlockWithTransactions block = await web3.Eth.Blocks.GetBlockWithTransactionsByNumber.SendRequestAsync(new BlockParameter(new HexBigInteger(i)));
                    // get the receipts of the create transactions
                    var createTxReceipts = await Task.WhenAll(
                        block.Transactions
                            .Where(IsCreateTx) // filter out non-create transactions
                            .Select(tx => web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(tx.TransactionHash)).ToArray()); // get the receipt of the create transaction
                    // get the bytecode of the deployed contracts
                    var deployedContractsBytecode = await Task.WhenAll(
                        createTxReceipts
                            .Select(r => r.ContractAddress) // get the address of the deployed contract
                            .Where(address => address != null) // filter out null addresses
                            .Select(address => web3.Eth.GetCode.SendRequestAsync(address)).ToArray()); // get the bytecode of the deployed contract
                    // check if the bytecode starts with the EOF prefix
                    var deployedContracts = deployedContractsBytecode
                        .Select(hexCode => hexCode.HexToByteArray()).Where(Check);
                    if(deployedContracts.Any()) {
                        Console.WriteLine("Found EOF at block: " + i);
                        return true;
                    }
                }
            } catch(RpcClientTimeoutException e) {
                throw;
            } catch(Exception e) {
                Console.WriteLine("Error handling : " + e.Message + " batch" + start + " - " + end );
            } finally {
                Console.WriteLine("Done handling batch: " + start + " - " + end);
            }
            return false;
        }
    }
}
import { task } from "hardhat/config";
import "../logger.js";
import { Logger } from "esm-iso-logger";

const logger = new Logger("SetFeeRate");

export const setFeeRate = task(
  "setFeeRate",
  "Set lock fee rate (1 unit = 0.001%, max 10000 = 10%)"
)
  .addPositionalArgument({
    name: "rate",
    description: "Fee rate (1 unit = 0.001%, max 10000 = 10%)",
  })
  .setAction(async () => ({
    default: async (args, hre) => {
      const { ethers } = await hre.network.getOrCreate();
      const { rate } = args;

      const possibleDeployedAddress = process.env.NORI_ETH_TOKEN_BRIDGE_ADDRESS;

      const issues: string[] = [];

      if (
        !possibleDeployedAddress ||
        !/^0x[a-fA-F0-9]{40}$/.test(possibleDeployedAddress)
      ) {
        issues.push(
          "Missing or invalid env: NORI_ETH_TOKEN_BRIDGE_ADDRESS (expected 0x-prefixed 40 hex chars)"
        );
      }

      const parsedRate = parseInt(rate);
      if (isNaN(parsedRate) || parsedRate < 0) {
        issues.push(`Invalid rate: ${rate} (must be a non-negative integer)`);
      } else if (parsedRate > 10000) {
        issues.push(`Rate ${parsedRate} exceeds maximum of 10000 (10%)`);
      }

      if (issues.length) {
        logger.error("SetFeeRate encountered errors:");
        issues.forEach((issue, idx) => logger.warn(`  ${idx + 1}: ${issue}`));
        logger.fatal(
          "Due to issues with arguments setFeeRate cannot continue."
        );
        process.exit(1);
      }

      const deployedAddress = possibleDeployedAddress as string;
      logger.log(`NORI_ETH_TOKEN_BRIDGE_ADDRESS: ${deployedAddress}`);

      const [signer] = await ethers.getSigners();
      logger.log(`Signer address: ${await signer.getAddress()}`);

      const tokenBridge = await ethers.getContractAt(
        "NoriTokenBridge",
        deployedAddress,
        signer
      );

      const FEE_DENOMINATOR = 100_000;
      const pct = ((parsedRate / FEE_DENOMINATOR) * 100).toFixed(3);

      const currentRate = await tokenBridge.lockFeeRate();
      logger.log(`Current lock fee rate: ${currentRate}`);
      logger.log(`Setting lock fee rate to: ${parsedRate} (${pct}%)`);
      const tx = await tokenBridge.setLockFeeRate(parsedRate);
      logger.log(`Tx sent: ${tx.hash}`);
      const receipt = await tx.wait();
      if (!receipt) throw new Error("No tx receipt was generated");
      logger.log(`Confirmed in block: ${receipt.blockNumber}`);
    },
  }))
  .build();

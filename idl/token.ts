/**
 * Program IDL in camelCase format in order to be used in JS/TS.
 *
 * Note that this is only a type helper and is not the actual IDL. The original
 * IDL can be found at `target/idl/token.json`.
 */
export type Token = {
  "address": "5xS32i7XNRk7JCjYR7RgfHVxdLepVSnnJ1gdHkmfD8W1",
  "metadata": {
    "name": "token",
    "version": "0.1.0",
    "spec": "0.1.0",
    "description": "Created with Anchor"
  },
  "instructions": [
    {
      "name": "initialize",
      "discriminator": [
        175,
        175,
        109,
        31,
        13,
        152,
        155,
        237
      ],
      "accounts": [
        {
          "name": "payer",
          "writable": true,
          "signer": true
        },
        {
          "name": "state",
          "writable": true,
          "pda": {
            "seeds": [
              {
                "kind": "const",
                "value": [
                  83,
                  84,
                  65,
                  84,
                  69
                ]
              }
            ]
          }
        },
        {
          "name": "token",
          "writable": true,
          "pda": {
            "seeds": [
              {
                "kind": "const",
                "value": [
                  78,
                  69,
                  84,
                  72
                ]
              }
            ]
          }
        },
        {
          "name": "systemProgram",
          "address": "11111111111111111111111111111111"
        },
        {
          "name": "tokenProgram"
        }
      ],
      "args": [
        {
          "name": "initValues",
          "type": {
            "defined": {
              "name": "noriSolTokenBridgeInit"
            }
          }
        }
      ]
    },
    {
      "name": "mint",
      "docs": [
        "Mint bridged tokens against a proven deposit (Merkle witness). The",
        "deposit's first collection key commits to sha256(recipient_pubkey);",
        "the recipient claims by signing the transaction."
      ],
      "discriminator": [
        51,
        57,
        225,
        47,
        182,
        146,
        137,
        166
      ],
      "accounts": [
        {
          "name": "payer",
          "writable": true,
          "signer": true
        },
        {
          "name": "recipient",
          "writable": true,
          "signer": true
        },
        {
          "name": "state",
          "pda": {
            "seeds": [
              {
                "kind": "const",
                "value": [
                  83,
                  84,
                  65,
                  84,
                  69
                ]
              }
            ]
          }
        },
        {
          "name": "token",
          "writable": true,
          "pda": {
            "seeds": [
              {
                "kind": "const",
                "value": [
                  78,
                  69,
                  84,
                  72
                ]
              }
            ]
          }
        },
        {
          "name": "tokenAccount",
          "docs": [
            "create_idempotent CPI below, which derives the ATA address itself and",
            "verifies any pre-existing account at it is a valid token account for",
            "this mint/owner/token_program."
          ],
          "writable": true
        },
        {
          "name": "tokenAccountStorage",
          "docs": [
            "created and initialized in handle_mint on first use (not via Anchor's",
            "init_if_needed, which would skip re-running our init logic on an",
            "account an attacker got created ahead of time) and left untouched if",
            "it already exists."
          ],
          "writable": true,
          "pda": {
            "seeds": [
              {
                "kind": "const",
                "value": [
                  83,
                  84,
                  79,
                  82,
                  65,
                  71,
                  69
                ]
              },
              {
                "kind": "account",
                "path": "recipient"
              }
            ]
          }
        },
        {
          "name": "proofQueueBatch",
          "docs": [
            "The committed proof queue batch the deposit witness proves against.",
            "`Account` checks it is owned by this program and carries the",
            "`ProofRequestRootEntry` discriminator; only `update` creates such",
            "accounts, at the proof queue batch PDAs."
          ]
        },
        {
          "name": "systemProgram",
          "address": "11111111111111111111111111111111"
        },
        {
          "name": "tokenProgram"
        },
        {
          "name": "associatedTokenProgram",
          "address": "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
        }
      ],
      "args": [
        {
          "name": "depositWitness",
          "type": {
            "defined": {
              "name": "verifiedRequestWitnessInput"
            }
          }
        }
      ]
    },
    {
      "name": "update",
      "docs": [
        "Permissionless state transition: advance the verified Ethereum",
        "light-client state by one SP1 Groth16 proof batch."
      ],
      "discriminator": [
        219,
        200,
        88,
        176,
        158,
        63,
        253,
        127
      ],
      "accounts": [
        {
          "name": "payer",
          "writable": true,
          "signer": true
        },
        {
          "name": "state",
          "writable": true,
          "pda": {
            "seeds": [
              {
                "kind": "const",
                "value": [
                  83,
                  84,
                  65,
                  84,
                  69
                ]
              }
            ]
          }
        },
        {
          "name": "proofQueueBatch",
          "docs": [
            "(`state.proof_queue_batch_count`). Its address can only be checked",
            "against state inside the handler, so seeds are derived and compared",
            "there; it is created and written in handle_update only when the",
            "update's batch drained at least one request, and ignored otherwise."
          ],
          "writable": true
        },
        {
          "name": "systemProgram",
          "address": "11111111111111111111111111111111"
        }
      ],
      "args": [
        {
          "name": "proof",
          "type": {
            "defined": {
              "name": "updateProof"
            }
          }
        }
      ]
    }
  ],
  "accounts": [
    {
      "name": "noriSolTokenBridge",
      "discriminator": [
        156,
        61,
        33,
        52,
        224,
        168,
        181,
        250
      ]
    },
    {
      "name": "proofRequestRootEntry",
      "discriminator": [
        118,
        111,
        5,
        174,
        165,
        184,
        191,
        96
      ]
    }
  ],
  "events": [
    {
      "name": "mintApplied",
      "discriminator": [
        167,
        56,
        99,
        198,
        217,
        201,
        142,
        66
      ]
    },
    {
      "name": "updateApplied",
      "discriminator": [
        230,
        193,
        225,
        238,
        5,
        232,
        91,
        236
      ]
    }
  ],
  "errors": [
    {
      "code": 6000,
      "name": "proofVerificationFailed",
      "msg": "SP1 Groth16 proof verification failed"
    },
    {
      "code": 6001,
      "name": "decodingProofFailed",
      "msg": "Failed to decode proof bytes"
    },
    {
      "code": 6002,
      "name": "ethProofQueueAddressMismatch",
      "msg": "ETH proof queue address mismatch"
    },
    {
      "code": 6003,
      "name": "queueCursorMismatch",
      "msg": "Queue cursor mismatch"
    },
    {
      "code": 6004,
      "name": "inputSlotMismatch",
      "msg": "Input slot does not match latest verified head"
    },
    {
      "code": 6005,
      "name": "inputStoreHashMismatch",
      "msg": "Input store hash does not match latest verified store hash"
    },
    {
      "code": 6006,
      "name": "invalidOutputSlot",
      "msg": "Output slot is not greater than input slot"
    },
    {
      "code": 6007,
      "name": "zeroSyncCommitteeHash",
      "msg": "Next sync committee hash is zero"
    },
    {
      "code": 6008,
      "name": "proofQueueBatchAccountMismatch",
      "msg": "Proof queue batch account is not the PDA for the next proof queue batch index"
    },
    {
      "code": 6009,
      "name": "notTokenBridgeRequest",
      "msg": "VerifiedRequest is not a proof of state for the token bridge contract"
    },
    {
      "code": 6010,
      "name": "mintedExceedsLocked",
      "msg": "locked_so_far is less than minted_so_far; this would cause a negative mint amount"
    },
    {
      "code": 6011,
      "name": "zeroMintAmount",
      "msg": "No new amount to mint: locked_so_far equals minted_so_far"
    },
    {
      "code": 6012,
      "name": "lockedAmountOverflow",
      "msg": "Locked amount does not fit in a u64 token amount"
    },
    {
      "code": 6013,
      "name": "commitmentMismatch",
      "msg": "Recipient pubkey does not hash to the deposit commitment"
    },
    {
      "code": 6014,
      "name": "proofQueueBatchRootMismatch",
      "msg": "Witness root does not match the committed proof queue batch root"
    },
    {
      "code": 6015,
      "name": "witnessIndexOutsideProofQueueBatch",
      "msg": "Witness index is outside the committed proof queue batch"
    },
    {
      "code": 6016,
      "name": "invalidDepositWitness",
      "msg": "Deposit witness is malformed"
    }
  ],
  "types": [
    {
      "name": "bytes32",
      "docs": [
        "32 bytes; Borsh-encoded exactly as alloy's `B256`."
      ],
      "type": {
        "kind": "struct",
        "fields": [
          {
            "array": [
              "u8",
              32
            ]
          }
        ]
      }
    },
    {
      "name": "ethAddress",
      "docs": [
        "A 20-byte Ethereum address; Borsh-encoded exactly as alloy's `Address`."
      ],
      "type": {
        "kind": "struct",
        "fields": [
          {
            "array": [
              "u8",
              20
            ]
          }
        ]
      }
    },
    {
      "name": "mintApplied",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "recipient",
            "type": "pubkey"
          },
          {
            "name": "depositRoot",
            "type": {
              "array": [
                "u8",
                32
              ]
            }
          },
          {
            "name": "amountMinted",
            "type": "u64"
          },
          {
            "name": "mintedSoFar",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "noriSolTokenBridge",
      "serialization": "bytemuck",
      "repr": {
        "kind": "c"
      },
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "authority",
            "type": "pubkey"
          },
          {
            "name": "verifiedStateRoot",
            "type": {
              "array": [
                "u8",
                32
              ]
            }
          },
          {
            "name": "latestHead",
            "type": "u64"
          },
          {
            "name": "noriBridgeVk",
            "type": {
              "array": [
                "u8",
                32
              ]
            }
          },
          {
            "name": "latestHeliosStoreInputHash",
            "type": {
              "array": [
                "u8",
                32
              ]
            }
          },
          {
            "name": "ethProofQueueAddress",
            "type": {
              "array": [
                "u8",
                20
              ]
            }
          },
          {
            "name": "ethTokenBridgeAddress",
            "type": {
              "array": [
                "u8",
                20
              ]
            }
          },
          {
            "name": "queueCursor",
            "type": "u64"
          },
          {
            "name": "proofQueueBatchCount",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "noriSolTokenBridgeInit",
      "docs": [
        "Instruction arguments for `initialize` (~120 bytes — fine on stack).",
        "Written into the zeroed state account field by field."
      ],
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "verifiedStateRoot",
            "type": {
              "defined": {
                "name": "bytes32"
              }
            }
          },
          {
            "name": "latestHeliosStoreInputHash",
            "type": {
              "defined": {
                "name": "bytes32"
              }
            }
          },
          {
            "name": "ethProofQueueAddress",
            "type": {
              "defined": {
                "name": "ethAddress"
              }
            }
          },
          {
            "name": "ethTokenBridgeAddress",
            "type": {
              "defined": {
                "name": "ethAddress"
              }
            }
          },
          {
            "name": "latestHead",
            "docs": [
              "Beacon slot of the state the bridge starts from. The first accepted",
              "`update` must have `input_slot == latest_head`, so this pins where the",
              "proven chain resumes."
            ],
            "type": "u64"
          },
          {
            "name": "queueCursor",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "proofRequestRootEntry",
      "docs": [
        "One committed proof queue batch, stored append-only in its own PDA at",
        "`[NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED, index.to_le_bytes()]`.",
        "Indices are contiguous from 0 and only updates whose batch drained at",
        "least one request create an entry, so clients can search entries by index",
        "for the batch whose cursor range covers a request id."
      ],
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "root",
            "type": {
              "array": [
                "u8",
                32
              ]
            }
          },
          {
            "name": "outputBlockNumber",
            "type": "u64"
          },
          {
            "name": "inputQueueCursor",
            "type": "u64"
          },
          {
            "name": "outputQueueCursor",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "u256Le",
      "docs": [
        "A 256-bit unsigned integer as 32 little-endian bytes; Borsh-encoded",
        "exactly as alloy's `U256`."
      ],
      "type": {
        "kind": "struct",
        "fields": [
          {
            "array": [
              "u8",
              32
            ]
          }
        ]
      }
    },
    {
      "name": "updateApplied",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "outputSlot",
            "type": "u64"
          },
          {
            "name": "queueCursor",
            "type": "u64"
          },
          {
            "name": "verifiedStateRoot",
            "type": {
              "array": [
                "u8",
                32
              ]
            }
          },
          {
            "name": "proofQueueBatchCount",
            "type": "u64"
          }
        ]
      }
    },
    {
      "name": "updateProof",
      "docs": [
        "An SP1 Groth16 proof; Borsh-encoded exactly as sp1-solana's `SP1Groth16Proof`.",
        "",
        "* `proof` - `SP1ProofWithPublicValues::bytes()`",
        "* `sp1_public_inputs` - `SP1ProofWithPublicValues::public_values.to_vec()`"
      ],
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "proof",
            "type": "bytes"
          },
          {
            "name": "sp1PublicInputs",
            "type": "bytes"
          }
        ]
      }
    },
    {
      "name": "verifiedRequest",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "target",
            "type": {
              "defined": {
                "name": "ethAddress"
              }
            }
          },
          {
            "name": "collectionKeysCount",
            "type": "u8"
          },
          {
            "name": "collectionKeys",
            "type": {
              "array": [
                {
                  "defined": {
                    "name": "bytes32"
                  }
                },
                2
              ]
            }
          },
          {
            "name": "value",
            "type": {
              "defined": {
                "name": "u256Le"
              }
            }
          }
        ]
      }
    },
    {
      "name": "verifiedRequestWitnessInput",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "path",
            "type": {
              "vec": {
                "defined": {
                  "name": "bytes32"
                }
              }
            }
          },
          {
            "name": "index",
            "type": "u64"
          },
          {
            "name": "value",
            "type": {
              "defined": {
                "name": "verifiedRequest"
              }
            }
          }
        ]
      }
    }
  ],
  "constants": [
    {
      "name": "noriSolTokenAccountStorageSeed",
      "type": "bytes",
      "value": "[83, 84, 79, 82, 65, 71, 69]"
    },
    {
      "name": "noriSolTokenBridgeProofQueueBatchSeed",
      "type": "bytes",
      "value": "[80, 82, 79, 79, 70, 95, 81, 85, 69, 85, 69, 95, 66, 65, 84, 67, 72]"
    },
    {
      "name": "noriSolTokenBridgeSeed",
      "type": "bytes",
      "value": "[78, 69, 84, 72]"
    },
    {
      "name": "noriSolTokenBridgeStateSeed",
      "type": "bytes",
      "value": "[83, 84, 65, 84, 69]"
    },
    {
      "name": "tokenDecimals",
      "type": "u8",
      "value": "6"
    }
  ]
};
